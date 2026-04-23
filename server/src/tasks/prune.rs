//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::{
    Checksum, Pack, PackRetention, Snapshot, SnapshotRetention, Store, TreeReference,
};
use crate::domain::repositories::{PackRepository, RecordRepository};
use anyhow::{Error, anyhow};
use chrono::{DateTime, Datelike, TimeDelta, Utc};
use log::{info, warn};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};

/// Status of a request.
#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// Request is waiting to be processed.
    PENDING,
    /// Request was cancelled before processing began.
    CANCELLED,
    /// Request is being processed.
    RUNNING,
    /// Request processing has completed (successfully or otherwise).
    COMPLETED,
}

/// Request to prune the snapshots or packs for a data set.
#[derive(Clone, Debug)]
pub struct Request {
    /// Unique identifier for this request.
    pub id: String,
    /// Status of this request.
    pub status: Status,
    /// Identifier of the dataset containing the data.
    pub dataset: String,
    /// The date-time when the request processing started.
    pub started: Option<DateTime<Utc>>,
    /// The date-time when the request was completed.
    pub finished: Option<DateTime<Utc>>,
}

impl Request {
    pub fn new(dataset: String) -> Self {
        let id = xid::new().to_string();
        Self {
            id,
            status: Status::PENDING,
            dataset,
            started: None,
            finished: None,
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Prune]Request({})", self.id)
    }
}

impl cmp::PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl cmp::Eq for Request {}

///
/// A `Subscriber` receives updates to the progress of a prune operation.
///
#[cfg_attr(test, automock)]
pub trait Subscriber: Send + Sync {
    /// Prune operation has begun to be processed.
    ///
    /// Returns a value for mockall tests.
    fn started(&self, request_id: &str) -> bool;

    /// Prune request has been completed.
    ///
    /// Returns a value for mockall tests.
    fn finished(&self, request_id: &str) -> bool;
}

/// A single problem discovered during a database scrub.
#[derive(Clone, Debug)]
pub struct ScrubIssue {
    /// Identifier of the dataset this issue relates to, if any.
    pub dataset_id: Option<String>,
    /// Human-readable description of the problem.
    pub message: String,
}

///
/// `Pruner` processes requests to prune snapshots and packs for a data set.
///
#[cfg_attr(test, automock)]
pub trait Pruner: Send + Sync {
    /// Synchronously perform the appropriate pruning for a certain data set.
    ///
    /// Returns the number of snapshots that were pruned.
    fn prune_snapshots(&self, request: Request) -> Result<usize, Error>;

    /// Verify that every referenced database record is reachable and readable.
    ///
    /// Walks datasets, snapshots, trees, files, chunks, packs, xattrs, and
    /// stores. Records that are referenced but missing or unreadable yield a
    /// `ScrubIssue`. Returns `Err` only for unrecoverable failures such as
    /// being unable to load the dataset or store listings.
    fn database_scrub(&self) -> Result<Vec<ScrubIssue>, Error>;

    /// Delete unreachable pack files and aged-out database archives.
    ///
    /// For each pack that is no longer reachable from any snapshot, removes
    /// the pack object from every store whose `PackRetention` permits it,
    /// shrinks `pack.locations` accordingly, and deletes the pack record once
    /// no locations remain. Database archives are pruned the same way, except
    /// the most recent archive is always preserved. Per-item failures are
    /// returned as `ScrubIssue`s so the run continues; `Err` is returned only
    /// for unrecoverable failures such as being unable to enumerate stores.
    fn prune_packs(&self) -> Result<Vec<ScrubIssue>, Error>;
}

///
/// Basic implementation of `Pruner`.
///
pub struct PrunerImpl {
    repo: Arc<dyn RecordRepository>,
    // Events related to the backup are sent to the subscriber.
    subscriber: Arc<dyn Subscriber>,
    // If the value is true, the background process should stop.
    stop_requested: Arc<RwLock<bool>>,
}

impl PrunerImpl {
    /// Construct a new instance of BackuperImpl.
    pub fn new(
        repo: Arc<dyn RecordRepository>,
        subscriber: Arc<dyn Subscriber>,
        stop_requested: Arc<RwLock<bool>>,
    ) -> Self {
        Self {
            repo,
            subscriber,
            stop_requested,
        }
    }

    // Walk backward from the given snapshot, visiting `count` snapshots.
    // Returns the digest of the last snapshot that was visited. If `None` is
    // returned, then there were were less than `count` snapshots.
    fn visit_count_snapshots(
        &self,
        start: Checksum,
        count: u16,
    ) -> Result<Option<Checksum>, Error> {
        let mut visited = 1;
        let mut digest = start;
        while visited < count {
            let snapshot = self
                .repo
                .get_snapshot(&digest)?
                .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", &digest)))?;
            if let Some(parent) = snapshot.parent {
                digest = parent;
            } else {
                break;
            }
            visited += 1;
        }
        if visited == count {
            Ok(Some(digest))
        } else {
            Ok(None)
        }
    }

    // Walk backward from the given snapshot, visiting snapshots whose start
    // time occurs before the date that is `days` in the past. Returns the digest
    // of the last snapshot that was visited. If `None` is returned, then there
    // were were no snapshots from before `days` ago.
    fn visit_days_snapshots(&self, start: Checksum, days: u16) -> Result<Option<Checksum>, Error> {
        let now = chrono::Utc::now();
        let days_delta = chrono::TimeDelta::days(days as i64);
        let then = now - days_delta;
        let mut digest = start;
        loop {
            let snapshot = self
                .repo
                .get_snapshot(&digest)?
                .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", &digest)))?;
            if snapshot.start_time < then {
                break;
            }
            if let Some(parent) = snapshot.parent {
                digest = parent;
            } else {
                return Ok(None);
            }
        }
        Ok(Some(digest))
    }

    // Remove all of the snapshot records after the one given, clearing the
    // parent reference for the given snapshot in the process. Returns the
    // number of snapshot records that were deleted.
    fn prune_snapshots_after(&self, start: Checksum) -> Result<usize, Error> {
        // clear the parent of the oldest snapshot, save to database
        let mut snapshot = self
            .repo
            .get_snapshot(&start)?
            .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", &start)))?;
        let mut maybe_parent = snapshot.parent.take();
        if maybe_parent.is_some() {
            self.repo.put_snapshot(&snapshot)?;
        }
        let mut count = 0;
        // walk backward from oldest, removing snapshot records
        while let Some(parent) = maybe_parent {
            snapshot = self
                .repo
                .get_snapshot(&parent)?
                .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", &parent)))?;
            maybe_parent = snapshot.parent.take();
            let id = snapshot.digest.to_string();
            self.repo.delete_snapshot(&id)?;
            count += 1;
        }
        Ok(count)
    }

    // Starting from the given (latest) snapshot, count backwards `count`
    // snapshots and prune all the snapshots thereafter.
    fn prune_snapshots_after_count(&self, start: Checksum, count: u16) -> Result<usize, Error> {
        let maybe_oldest_snapshot: Option<Checksum> = self.visit_count_snapshots(start, count)?;
        let pruned_count = if let Some(oldest) = maybe_oldest_snapshot {
            info!("pruning snapshots after {}", oldest);
            self.prune_snapshots_after(oldest)?
        } else {
            0
        };
        Ok(pruned_count)
    }

    // Starting from the given (latest) snapshot, walk backwards through the
    // snapshots and prune all the occur after the given days in the past.
    fn prune_snapshots_after_days(&self, start: Checksum, days: u16) -> Result<usize, Error> {
        let maybe_oldest_snapshot: Option<Checksum> = self.visit_days_snapshots(start, days)?;
        let pruned_count = if let Some(oldest) = maybe_oldest_snapshot {
            info!("pruning snapshots after {}", oldest);
            self.prune_snapshots_after(oldest)?
        } else {
            0
        };
        Ok(pruned_count)
    }

    // Prune snapshots automatically according to a convention similar to that
    // of Apple Time Machine.
    fn prune_snapshots_auto(&self, start: Checksum) -> Result<usize, Error> {
        // read all snapshots for the given dataset
        let mut digest = start;
        let mut snapshots: HashMap<Checksum, Snapshot> = HashMap::new();
        loop {
            let snapshot = self
                .repo
                .get_snapshot(&digest)?
                .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", &digest)))?;
            let maybe_parent = snapshot.parent.clone();
            snapshots.insert(snapshot.digest.clone(), snapshot);
            if let Some(parent) = maybe_parent {
                digest = parent;
            } else {
                break;
            }
        }
        // prune the snapshots down to those that should remain
        let candidates: Vec<(Checksum, DateTime<Utc>)> = snapshots
            .values()
            .map(|s| (s.digest.clone(), s.start_time))
            .collect();
        let candidates_len = candidates.len();
        let keepers = auto_prune_snapshots(candidates, Utc::now());
        // update all snapshot records accordingly -- take them in pairs and
        // update the first to point to the next, with the last one having its
        // parent set to none to cut off the remaining snapshots
        for pair in keepers.windows(2) {
            let mut snap = snapshots.remove(&pair[0].0).unwrap();
            snap.parent = Some(pair[1].0.clone());
            self.repo.put_snapshot(&snap)?;
        }
        let mut snap = snapshots.remove(&keepers.last().unwrap().0).unwrap();
        snap.parent = None;
        self.repo.put_snapshot(&snap)?;
        // delete all of the remaining snapshots
        for remaining in snapshots.keys() {
            let id = remaining.to_string();
            self.repo.delete_snapshot(&id)?;
        }
        Ok(candidates_len - keepers.len())
    }

    // Find all records that are no longer reachable from the configured
    // datasets, removing them from the database. This ignores pack records as
    // this function does not handle pack pruning.
    fn prune_unreachable_records(&self) -> Result<(), Error> {
        //
        // get the digests of all tree, file, chunk, and xattr records; after
        // visiting every reachable record, the remainder can be safely removed
        //
        let tree_digests = self.repo.get_all_tree_digests()?;
        let mut trees: HashSet<String> = tree_digests.into_iter().collect();
        let file_digests = self.repo.get_all_file_digests()?;
        let mut files: HashSet<String> = file_digests.into_iter().collect();
        let chunk_digests = self.repo.get_all_chunk_digests()?;
        let mut chunks: HashSet<String> = chunk_digests.into_iter().collect();
        let xattr_digests = self.repo.get_all_xattr_digests()?;
        let mut xattrs: HashSet<String> = xattr_digests.into_iter().collect();

        //
        // tree visitor recursively walks a given tree structure, removing any
        // tree digests found in the process, along with files, xattrs, and
        // chunks that are referenced by files
        //
        let mut visit_tree = |tree_sum: Checksum| -> Result<(), Error> {
            // Rust does not know how to compile recursive closures, so use a
            // function within the closure to get around the types issue.
            fn rec(
                repo: &Arc<dyn RecordRepository>,
                tree_sum: Checksum,
                trees: &mut HashSet<String>,
                files: &mut HashSet<String>,
                chunks: &mut HashSet<String>,
                xattrs: &mut HashSet<String>,
            ) -> Result<(), Error> {
                let tree_digest_str = tree_sum.to_string();
                if trees.contains(&tree_digest_str) {
                    // tree is reachable, consider its entries
                    trees.remove(&tree_digest_str);
                    let tree = repo
                        .get_tree(&tree_sum)?
                        .ok_or_else(|| anyhow!(format!("missing tree: {:?}", &tree_digest_str)))?;
                    for entry in tree.entries {
                        // consider only trees and files, ignore links and very
                        // short files which do not have database records
                        match entry.reference {
                            TreeReference::TREE(tree_sum) => {
                                rec(repo, tree_sum, trees, files, chunks, xattrs)?
                            }
                            TreeReference::FILE(file_sum) => {
                                let file_digest_str = file_sum.to_string();
                                if files.contains(&file_digest_str) {
                                    // file is reachable, along with any chunks
                                    files.remove(&file_digest_str);
                                    let file = repo.get_file(&file_sum)?.ok_or_else(|| {
                                        anyhow!(format!("missing file: {:?}", &file_sum))
                                    })?;
                                    // if only one "chunk" then it is a pack,
                                    // and packs are ignored by this usecase
                                    if file.chunks.len() > 1 {
                                        for (_, cd) in file.chunks.iter() {
                                            let cds = cd.to_string();
                                            chunks.remove(&cds);
                                        }
                                    }
                                }
                            }
                            _ => (),
                        }
                        // xattrs of this entry are all reachable
                        for (_, xd) in entry.xattrs.iter() {
                            let xds = xd.to_string();
                            xattrs.remove(&xds);
                        }
                    }
                }
                Ok(())
            }
            rec(
                &self.repo,
                tree_sum,
                &mut trees,
                &mut files,
                &mut chunks,
                &mut xattrs,
            )
        };

        //
        // visit snapshots of all datasets, recursively visiting their trees
        //
        let datasets = self.repo.get_datasets()?;
        for dataset in datasets {
            if let Some(latest) = dataset.snapshot.clone() {
                let mut digest = latest;
                loop {
                    let snapshot = self
                        .repo
                        .get_snapshot(&digest)?
                        .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", &digest)))?;
                    visit_tree(snapshot.tree)?;
                    if let Some(parent) = snapshot.parent {
                        digest = parent;
                    } else {
                        break;
                    }
                }
            }
        }

        //
        // any digests still remaining in the sets were not reachable and their
        // records can be safely removed from the database
        //
        info!("deleting {} unreachable tree records", trees.len());
        for tree_digest in trees.into_iter() {
            self.repo.delete_tree(&tree_digest)?;
        }
        info!("deleting {} unreachable file records", files.len());
        for file_digest in files.into_iter() {
            self.repo.delete_file(&file_digest)?;
        }
        info!("deleting {} unreachable chunk records", chunks.len());
        for chunk_digest in chunks.into_iter() {
            self.repo.delete_chunk(&chunk_digest)?;
        }
        info!("deleting {} unreachable xattr records", xattrs.len());
        for xattr_digest in xattrs.into_iter() {
            self.repo.delete_xattr(&xattr_digest)?;
        }
        Ok(())
    }

    // Recursively walk the tree rooted at `root`, queuing referenced file and
    // xattr digests for later phases. Trees that fail to load produce a
    // `ScrubIssue`; walking continues so that as much as possible is verified.
    // `visited` is shared across calls so that a tree shared between datasets
    // or snapshots is walked at most once.
    fn scrub_tree(
        &self,
        root: Checksum,
        dataset_id: &str,
        visited: &mut HashSet<Checksum>,
        file_queue: &mut HashMap<Checksum, String>,
        xattr_queue: &mut HashMap<Checksum, String>,
        issues: &mut Vec<ScrubIssue>,
    ) {
        let mut stack: Vec<Checksum> = vec![root];
        while let Some(digest) = stack.pop() {
            if *self.stop_requested.read().unwrap() {
                return;
            }
            if !visited.insert(digest.clone()) {
                continue;
            }
            match self.repo.get_tree(&digest) {
                Ok(Some(tree)) => {
                    for entry in &tree.entries {
                        match &entry.reference {
                            TreeReference::TREE(sub) => stack.push(sub.clone()),
                            TreeReference::FILE(file_digest) => {
                                file_queue
                                    .entry(file_digest.clone())
                                    .or_insert_with(|| dataset_id.to_string());
                            }
                            _ => {}
                        }
                        for xdigest in entry.xattrs.values() {
                            xattr_queue
                                .entry(xdigest.clone())
                                .or_insert_with(|| dataset_id.to_string());
                        }
                    }
                }
                Ok(None) => {
                    let msg = format!("missing tree record: {}", digest);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: Some(dataset_id.to_string()),
                        message: msg,
                    });
                }
                Err(err) => {
                    let msg = format!("error loading tree {}: {}", digest, err);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: Some(dataset_id.to_string()),
                        message: msg,
                    });
                }
            }
        }
    }

    // Walk the tree rooted at `root`, gathering the digests of every pack
    // referenced by reachable files and chunks. Any missing or unreadable
    // tree/file/chunk record sets `tainted = true` and captures an issue;
    // the caller uses that flag to skip pack-file deletions so a database
    // read error never turns into real data loss. `visited` is shared
    // across calls so shared subtrees are walked at most once.
    fn gather_reachable_packs_tree(
        &self,
        root: Checksum,
        visited: &mut HashSet<Checksum>,
        reachable: &mut HashSet<String>,
        tainted: &mut bool,
        dataset_id: Option<&str>,
        issues: &mut Vec<ScrubIssue>,
    ) {
        let mut stack: Vec<Checksum> = vec![root];
        while let Some(digest) = stack.pop() {
            if *self.stop_requested.read().unwrap() {
                return;
            }
            if !visited.insert(digest.clone()) {
                continue;
            }
            let tree = match self.repo.get_tree(&digest) {
                Ok(Some(t)) => t,
                Ok(None) => {
                    *tainted = true;
                    let msg = format!("missing tree record: {}", digest);
                    warn!("pack-prune: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: dataset_id.map(str::to_owned),
                        message: msg,
                    });
                    continue;
                }
                Err(err) => {
                    *tainted = true;
                    let msg = format!("error loading tree {}: {}", digest, err);
                    warn!("pack-prune: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: dataset_id.map(str::to_owned),
                        message: msg,
                    });
                    continue;
                }
            };
            for entry in tree.entries {
                match entry.reference {
                    TreeReference::TREE(sub) => stack.push(sub),
                    TreeReference::FILE(file_digest) => {
                        let file = match self.repo.get_file(&file_digest) {
                            Ok(Some(f)) => f,
                            Ok(None) => {
                                *tainted = true;
                                let msg =
                                    format!("missing file record: {}", file_digest);
                                warn!("pack-prune: {}", msg);
                                issues.push(ScrubIssue {
                                    dataset_id: dataset_id.map(str::to_owned),
                                    message: msg,
                                });
                                continue;
                            }
                            Err(err) => {
                                *tainted = true;
                                let msg = format!(
                                    "error loading file {}: {}",
                                    file_digest, err
                                );
                                warn!("pack-prune: {}", msg);
                                issues.push(ScrubIssue {
                                    dataset_id: dataset_id.map(str::to_owned),
                                    message: msg,
                                });
                                continue;
                            }
                        };
                        if file.chunks.len() == 1 {
                            // Single-entry "chunks" list is actually a pack reference.
                            let (_, pack_digest) = &file.chunks[0];
                            reachable.insert(pack_digest.to_string());
                        } else {
                            for (_, chunk_digest) in &file.chunks {
                                match self.repo.get_chunk(chunk_digest) {
                                    Ok(Some(chunk)) => {
                                        if let Some(packfile) = chunk.packfile {
                                            reachable.insert(packfile.to_string());
                                        } else {
                                            *tainted = true;
                                            let msg = format!(
                                                "chunk {} has no packfile reference",
                                                chunk_digest
                                            );
                                            warn!("pack-prune: {}", msg);
                                            issues.push(ScrubIssue {
                                                dataset_id: dataset_id.map(str::to_owned),
                                                message: msg,
                                            });
                                        }
                                    }
                                    Ok(None) => {
                                        *tainted = true;
                                        let msg = format!(
                                            "missing chunk record: {}",
                                            chunk_digest
                                        );
                                        warn!("pack-prune: {}", msg);
                                        issues.push(ScrubIssue {
                                            dataset_id: dataset_id.map(str::to_owned),
                                            message: msg,
                                        });
                                    }
                                    Err(err) => {
                                        *tainted = true;
                                        let msg = format!(
                                            "error loading chunk {}: {}",
                                            chunk_digest, err
                                        );
                                        warn!("pack-prune: {}", msg);
                                        issues.push(ScrubIssue {
                                            dataset_id: dataset_id.map(str::to_owned),
                                            message: msg,
                                        });
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // Attempt to delete the pack file for each `PackLocation` in `pack` whose
    // owning store's retention has elapsed; successful deletions are stripped
    // from `pack.locations`. Stores missing from `store_map` or with
    // `PackRetention::ALL` are left alone. Deletion errors are captured as
    // `ScrubIssue`s and the corresponding location is retained so the next
    // run will retry. `unknown_stores_reported` tracks which missing store
    // ids have already produced an issue so a pack-store removed from
    // config emits one issue per run rather than one per referencing pack.
    // Returns `true` iff at least one location was successfully deleted.
    fn prune_pack_locations(
        &self,
        pack: &mut Pack,
        store_map: &HashMap<String, (Store, Box<dyn PackRepository>)>,
        unknown_stores_reported: &mut HashSet<String>,
        issues: &mut Vec<ScrubIssue>,
    ) -> bool {
        let now = Utc::now();
        let mut retained: Vec<crate::domain::entities::PackLocation> =
            Vec::with_capacity(pack.locations.len());
        let mut changed = false;
        for location in pack.locations.drain(..) {
            let Some((store, repo)) = store_map.get(&location.store) else {
                if unknown_stores_reported.insert(location.store.clone()) {
                    let msg = format!(
                        "pack {} references unknown store {}",
                        pack.digest, location.store
                    );
                    warn!("pack-prune: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
                retained.push(location);
                continue;
            };
            let eligible = match store.retention {
                PackRetention::ALL => false,
                PackRetention::DAYS(days) => {
                    now - pack.upload_time >= TimeDelta::days(days as i64)
                }
            };
            if !eligible {
                retained.push(location);
                continue;
            }
            match repo.delete_pack(&location) {
                Ok(()) => {
                    info!(
                        "pack-prune: deleted pack {} from store {}",
                        pack.digest, location.store
                    );
                    changed = true;
                }
                Err(err) => {
                    let msg = format!(
                        "failed to delete pack {} from store {}: {}",
                        pack.digest, location.store, err
                    );
                    warn!("pack-prune: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                    retained.push(location);
                }
            }
        }
        pack.locations = retained;
        changed
    }
}

impl Pruner for PrunerImpl {
    fn prune_snapshots(&self, request: Request) -> Result<usize, Error> {
        self.subscriber.started(&request.id);
        //
        // For the given dataset, apply the selected retention policy to its
        // associated snapshots, removing those that are no longer needed. Next,
        // retrieve the digests for all records that can be pruned as a result
        // (trees, files, chunks, and xattrs). Then, walk through all datasets
        // and their snapshots and the associated trees, files, chunks, and
        // xattrs, removing their digests from the sets of all such records.
        // Whatever digests remain are those that are unreachable and can be
        // removed from the database.
        //
        // This is done in memory since a mark and sweep on disk would result in
        // an excessive number of disk writes. Hopefully there are not a large
        // number of file records as those are typically the bulk of all records
        // in the database.
        //
        let dataset = self
            .repo
            .get_dataset(&request.dataset)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", &request.dataset)))?;
        let latest_hash = dataset
            .snapshot
            .clone()
            .ok_or_else(|| anyhow!(format!("no snapshots for dataset: {:?}", &request.dataset)))?;
        let pruned_count = match dataset.retention {
            SnapshotRetention::ALL => {
                info!("will retain all snapshots for dataset {}", dataset.id);
                0
            }
            SnapshotRetention::COUNT(count) => {
                info!("will retain {} snapshots for dataset {}", count, dataset.id);
                self.prune_snapshots_after_count(latest_hash, count)?
            }
            SnapshotRetention::DAYS(days) => {
                info!(
                    "will retain {} days of snapshots for dataset {}",
                    days, dataset.id
                );
                self.prune_snapshots_after_days(latest_hash, days)?
            }
            SnapshotRetention::AUTO => {
                info!("will auto-prune snapshots for dataset {}", dataset.id);
                self.prune_snapshots_auto(latest_hash)?
            }
        };
        if pruned_count > 0 {
            info!(
                "pruned {} snapshots, removing unreachable records...",
                pruned_count
            );
            self.prune_unreachable_records()?;
        } else {
            info!("no snapshots to prune for dataset {}", dataset.id);
        }
        self.subscriber.finished(&request.id);
        Ok(pruned_count)
    }

    fn database_scrub(&self) -> Result<Vec<ScrubIssue>, Error> {
        info!("database scrub starting");
        let mut issues: Vec<ScrubIssue> = Vec::new();

        // Load stores up front; both the datasets phase and the packs phase
        // use this set to verify store references.
        let stores = self.repo.get_stores()?;
        let known_store_ids: HashSet<String> =
            stores.iter().map(|s| s.id.clone()).collect();
        let datasets = self.repo.get_datasets()?;

        // Queues populated during traversal and drained in their own phases.
        // Dedup by digest so each record is verified at most once. File and
        // xattr queues carry the owning dataset id for issue attribution.
        let mut visited_trees: HashSet<Checksum> = HashSet::new();
        let mut file_queue: HashMap<Checksum, String> = HashMap::new();
        let mut xattr_queue: HashMap<Checksum, String> = HashMap::new();
        let mut chunk_queue: HashSet<Checksum> = HashSet::new();
        let mut pack_queue: HashSet<Checksum> = HashSet::new();

        // Phase: datasets — verify each referenced store id exists.
        for dataset in &datasets {
            if *self.stop_requested.read().unwrap() {
                info!("database scrub stopped");
                return Ok(issues);
            }
            for store_id in &dataset.stores {
                if !known_store_ids.contains(store_id) {
                    let msg = format!(
                        "dataset {} references unknown store {}",
                        dataset.id, store_id
                    );
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: Some(dataset.id.clone()),
                        message: msg,
                    });
                }
            }
        }

        // Phase: snapshots + trees — walk each dataset's parent chain,
        // verifying snapshot records and walking the tree of each snapshot.
        for dataset in &datasets {
            if *self.stop_requested.read().unwrap() {
                info!("database scrub stopped");
                return Ok(issues);
            }
            let mut maybe_digest = dataset.snapshot.clone();
            while let Some(digest) = maybe_digest.take() {
                if *self.stop_requested.read().unwrap() {
                    info!("database scrub stopped");
                    return Ok(issues);
                }
                match self.repo.get_snapshot(&digest) {
                    Ok(Some(snapshot)) => {
                        self.scrub_tree(
                            snapshot.tree.clone(),
                            &dataset.id,
                            &mut visited_trees,
                            &mut file_queue,
                            &mut xattr_queue,
                            &mut issues,
                        );
                        maybe_digest = snapshot.parent.clone();
                    }
                    Ok(None) => {
                        let msg = format!("missing snapshot record: {}", digest);
                        warn!("scrub: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: Some(dataset.id.clone()),
                            message: msg,
                        });
                        break;
                    }
                    Err(err) => {
                        let msg = format!("error loading snapshot {}: {}", digest, err);
                        warn!("scrub: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: Some(dataset.id.clone()),
                            message: msg,
                        });
                        break;
                    }
                }
            }
        }

        // Phase: files — each queued file record must load; its chunks are
        // queued for the chunk phase, except single-entry "chunk" lists which
        // are really pack references (see File docs).
        for (digest, dataset_id) in file_queue.iter() {
            if *self.stop_requested.read().unwrap() {
                info!("database scrub stopped");
                return Ok(issues);
            }
            match self.repo.get_file(digest) {
                Ok(Some(file)) => {
                    if file.chunks.len() == 1 {
                        let (_, pack_digest) = &file.chunks[0];
                        pack_queue.insert(pack_digest.clone());
                    } else {
                        for (_, chunk_digest) in &file.chunks {
                            chunk_queue.insert(chunk_digest.clone());
                        }
                    }
                }
                Ok(None) => {
                    let msg = format!("missing file record: {}", digest);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: Some(dataset_id.clone()),
                        message: msg,
                    });
                }
                Err(err) => {
                    let msg = format!("error loading file {}: {}", digest, err);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: Some(dataset_id.clone()),
                        message: msg,
                    });
                }
            }
        }

        // Phase: chunks — each must load and reference a pack.
        for digest in chunk_queue.iter() {
            if *self.stop_requested.read().unwrap() {
                info!("database scrub stopped");
                return Ok(issues);
            }
            match self.repo.get_chunk(digest) {
                Ok(Some(chunk)) => {
                    if let Some(packfile) = chunk.packfile {
                        pack_queue.insert(packfile);
                    } else {
                        let msg =
                            format!("chunk {} has no packfile reference", digest);
                        warn!("scrub: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: None,
                            message: msg,
                        });
                    }
                }
                Ok(None) => {
                    let msg = format!("missing chunk record: {}", digest);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
                Err(err) => {
                    let msg = format!("error loading chunk {}: {}", digest, err);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
            }
        }

        // Phase: packs — each must load and each location must name a known
        // store.
        for digest in pack_queue.iter() {
            if *self.stop_requested.read().unwrap() {
                info!("database scrub stopped");
                return Ok(issues);
            }
            match self.repo.get_pack(digest) {
                Ok(Some(pack)) => {
                    for location in &pack.locations {
                        if !known_store_ids.contains(&location.store) {
                            let msg = format!(
                                "pack {} references unknown store {}",
                                digest, location.store
                            );
                            warn!("scrub: {}", msg);
                            issues.push(ScrubIssue {
                                dataset_id: None,
                                message: msg,
                            });
                        }
                    }
                }
                Ok(None) => {
                    let msg = format!("missing pack record: {}", digest);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
                Err(err) => {
                    let msg = format!("error loading pack {}: {}", digest, err);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
            }
        }

        // Phase: xattrs — each queued xattr digest must resolve.
        for (digest, dataset_id) in xattr_queue.iter() {
            if *self.stop_requested.read().unwrap() {
                info!("database scrub stopped");
                return Ok(issues);
            }
            match self.repo.get_xattr(digest) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    let msg = format!("missing xattr record: {}", digest);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: Some(dataset_id.clone()),
                        message: msg,
                    });
                }
                Err(err) => {
                    let msg = format!("error loading xattr {}: {}", digest, err);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: Some(dataset_id.clone()),
                        message: msg,
                    });
                }
            }
        }

        // Phase: stores — re-load each to confirm readability.
        for store in &stores {
            if *self.stop_requested.read().unwrap() {
                info!("database scrub stopped");
                return Ok(issues);
            }
            match self.repo.get_store(&store.id) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    let msg = format!("missing store record: {}", store.id);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
                Err(err) => {
                    let msg = format!("error loading store {}: {}", store.id, err);
                    warn!("scrub: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
            }
        }

        info!("database scrub finished with {} issue(s)", issues.len());
        Ok(issues)
    }

    fn prune_packs(&self) -> Result<Vec<ScrubIssue>, Error> {
        info!("pack prune starting");
        let mut issues: Vec<ScrubIssue> = Vec::new();

        // Build a map from store id to its (Store, PackRepository). Each pack
        // repo is built once per store so Phase A and Phase B can reuse them.
        let stores = self.repo.get_stores()?;
        let mut store_map: HashMap<String, (Store, Box<dyn PackRepository>)> = HashMap::new();
        for store in stores {
            let store_id = store.id.clone();
            match self.repo.build_pack_repo(&store) {
                Ok(repo) => {
                    store_map.insert(store_id, (store, repo));
                }
                Err(err) => {
                    let msg =
                        format!("failed to build pack repo for store {}: {}", store_id, err);
                    warn!("pack-prune: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
            }
        }

        // Compute the set of reachable pack digests by walking every dataset's
        // snapshot chain. Trees/files are walked at most once across datasets.
        // `reachability_tainted` is set if we hit any missing/unreadable
        // record: under that condition `reachable_packs` is incomplete and we
        // must not treat unlisted packs as orphaned, so Phase A is skipped.
        let mut reachable_packs: HashSet<String> = HashSet::new();
        let mut visited_trees: HashSet<Checksum> = HashSet::new();
        let mut reachability_tainted = false;
        let datasets = self.repo.get_datasets()?;
        for dataset in &datasets {
            if *self.stop_requested.read().unwrap() {
                info!("pack prune stopped");
                return Ok(issues);
            }
            let mut maybe_digest = dataset.snapshot.clone();
            while let Some(digest) = maybe_digest.take() {
                if *self.stop_requested.read().unwrap() {
                    info!("pack prune stopped");
                    return Ok(issues);
                }
                match self.repo.get_snapshot(&digest) {
                    Ok(Some(snapshot)) => {
                        self.gather_reachable_packs_tree(
                            snapshot.tree.clone(),
                            &mut visited_trees,
                            &mut reachable_packs,
                            &mut reachability_tainted,
                            Some(&dataset.id),
                            &mut issues,
                        );
                        maybe_digest = snapshot.parent.clone();
                    }
                    Ok(None) => {
                        reachability_tainted = true;
                        let msg = format!("missing snapshot record: {}", digest);
                        warn!("pack-prune: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: Some(dataset.id.clone()),
                            message: msg,
                        });
                        break;
                    }
                    Err(err) => {
                        reachability_tainted = true;
                        let msg =
                            format!("error loading snapshot {}: {}", digest, err);
                        warn!("pack-prune: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: Some(dataset.id.clone()),
                            message: msg,
                        });
                        break;
                    }
                }
            }
        }

        let mut unknown_stores_reported: HashSet<String> = HashSet::new();

        // Phase A: unreachable pack files. Skipped entirely when reachability
        // is tainted — without a complete view of live packs, any "unreachable"
        // pack we delete here could actually still be referenced.
        if reachability_tainted {
            warn!(
                "pack-prune: reachability incomplete; skipping pack-file deletion \
                 (database archives will still be processed)"
            );
        } else {
            let all_pack_digests = self.repo.get_all_pack_digests()?;
            for digest_str in all_pack_digests.into_iter() {
                if *self.stop_requested.read().unwrap() {
                    info!("pack prune stopped");
                    return Ok(issues);
                }
                if reachable_packs.contains(&digest_str) {
                    continue;
                }
                let checksum: Checksum = match digest_str.parse() {
                    Ok(c) => c,
                    Err(err) => {
                        let msg = format!(
                            "unparseable pack digest {}: {}",
                            digest_str, err
                        );
                        warn!("pack-prune: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: None,
                            message: msg,
                        });
                        continue;
                    }
                };
                let mut pack = match self.repo.get_pack(&checksum) {
                    Ok(Some(p)) => p,
                    Ok(None) => continue,
                    Err(err) => {
                        let msg = format!("error loading pack {}: {}", digest_str, err);
                        warn!("pack-prune: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: None,
                            message: msg,
                        });
                        continue;
                    }
                };
                let changed = self.prune_pack_locations(
                    &mut pack,
                    &store_map,
                    &mut unknown_stores_reported,
                    &mut issues,
                );
                if pack.locations.is_empty() {
                    if let Err(err) = self.repo.delete_pack(&digest_str) {
                        let msg = format!(
                            "failed to delete pack record {}: {}",
                            digest_str, err
                        );
                        warn!("pack-prune: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: None,
                            message: msg,
                        });
                    }
                } else if changed {
                    if let Err(err) = self.repo.put_pack(&pack) {
                        let msg = format!(
                            "failed to update pack record {}: {}",
                            digest_str, err
                        );
                        warn!("pack-prune: {}", msg);
                        issues.push(ScrubIssue {
                            dataset_id: None,
                            message: msg,
                        });
                    }
                }
            }
        }

        // Phase B: database archives. Always keep the newest record even if
        // every store's retention would otherwise allow deleting it. This
        // phase is independent of the reachability walk and runs even when
        // the tree traversal was tainted.
        let archives = self.repo.get_databases()?;
        let newest_digest: Option<Checksum> = archives
            .iter()
            .max_by_key(|p| p.upload_time)
            .map(|p| p.digest.clone());
        for mut pack in archives {
            if *self.stop_requested.read().unwrap() {
                info!("pack prune stopped");
                return Ok(issues);
            }
            if Some(&pack.digest) == newest_digest.as_ref() {
                continue;
            }
            let digest_str = pack.digest.to_string();
            let changed = self.prune_pack_locations(
                &mut pack,
                &store_map,
                &mut unknown_stores_reported,
                &mut issues,
            );
            if pack.locations.is_empty() {
                if let Err(err) = self.repo.delete_database(&digest_str) {
                    let msg = format!(
                        "failed to delete database archive record {}: {}",
                        digest_str, err
                    );
                    warn!("pack-prune: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
            } else if changed {
                if let Err(err) = self.repo.put_database(&pack) {
                    let msg = format!(
                        "failed to update database archive record {}: {}",
                        digest_str, err
                    );
                    warn!("pack-prune: {}", msg);
                    issues.push(ScrubIssue {
                        dataset_id: None,
                        message: msg,
                    });
                }
            }
        }

        info!("pack prune finished with {} issue(s)", issues.len());
        Ok(issues)
    }
}

/// Given a set of snapshots from a dataset, given here as their digest and the
/// start date-time, return those that conform to the following convention:
///
/// * Keep all within the past 24 hours
/// * The oldest for each day for the past 30 days
/// * The oldest for each week for the past 52 weeks
/// * The oldest for each year for the past 10 years
///
/// At most there will be 116 remaining snapshots, if running hourly backups.
fn auto_prune_snapshots(
    incoming: Vec<(Checksum, DateTime<Utc>)>,
    now: DateTime<Utc>,
) -> Vec<(Checksum, DateTime<Utc>)> {
    // uses the basic logic from https://github.com/bastibe/timeup with some
    // adjustments for the intended retention convention
    let mut retain: Vec<(Checksum, DateTime<Utc>)> = vec![];

    // keep all within the last day
    let twenty4_ago = now - TimeDelta::hours(24);
    for entry in incoming.iter() {
        if entry.1 > twenty4_ago {
            retain.push(entry.clone());
        }
    }

    // keep the oldest from each day within the last month
    for day in 1..30 {
        let date = (now - TimeDelta::days(day)).date_naive();
        if let Some(on_date) = incoming
            .iter()
            .filter(|(_, d)| d.date_naive() == date)
            .min_by_key(|(_, d)| d)
        {
            retain.push(on_date.to_owned());
        }
    }

    // keep the oldest from each week (same ISO 8601 week) within the last year
    for week in 1..52 {
        let date = (now - TimeDelta::weeks(week)).date_naive();
        if let Some(on_date) = incoming
            .iter()
            .filter(|(_, d)| d.year() == date.year() && d.iso_week() == date.iso_week())
            .min_by_key(|(_, d)| d)
        {
            retain.push(on_date.to_owned());
        }
    }

    // keep the oldest for each year within the last decade
    for year in 1..10 {
        let date = (now - TimeDelta::weeks(52 * year)).date_naive();
        if let Some(on_date) = incoming
            .iter()
            .filter(|(_, d)| d.year() == date.year())
            .min_by_key(|(_, d)| d)
        {
            retain.push(on_date.to_owned());
        }
    }

    // dedupe by checksum
    retain.sort_by(|a, b| a.0.cmp(&b.0));
    retain.dedup_by(|a, b| a.0 == b.0);

    // sort by date-time in reverse order (most recent first)
    retain.sort_by_key(|a| std::cmp::Reverse(a.1));
    retain
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{
        Chunk, Dataset, File, FileCounts, Pack, PackLocation, PackRetention, Snapshot, Store,
        StoreType, Tree, TreeEntry,
    };
    use crate::domain::repositories::MockRecordRepository;
    use chrono::Timelike;
    use hashed_array_tree::{HashedArrayTree, hat};

    #[test]
    fn test_pruner_auto_prune_snapshots_empty() {
        let inputs: Vec<(Checksum, DateTime<Utc>)> = vec![];
        let actual = auto_prune_snapshots(inputs, Utc::now());
        assert_eq!(actual.len(), 0);
    }

    #[test]
    fn test_pruner_auto_prune_snapshots_many() {
        #[rustfmt::skip]
        let raw_inputs = [
            // snapshot digest, time delta, specific hour, is-keeper
            ("58950e2", TimeDelta::hours(1), 0, true), // within last 24 hours
            ("e21d304", TimeDelta::hours(6), 0, true), // within last 24 hours
            ("af09fd7", TimeDelta::hours(12), 0, true), // within last 24 hours
            ("24d6ec6", TimeDelta::days(2), 1, true), // oldest from that day
            ("56662fc", TimeDelta::days(2), 2, false),
            ("e017530", TimeDelta::days(2), 3, false),
            ("6f8c483", TimeDelta::days(3), 2, true), // oldest from that day
            ("3c1cbff", TimeDelta::days(3), 3, false),
            ("c586348", TimeDelta::days(4), 1, true), // oldest from that day
            ("31e2791", TimeDelta::days(7), 1, true), // oldest from that day
            ("2b4a1e4", TimeDelta::days(14), 1, true), // oldest from that day
            ("8896097", TimeDelta::days(21), 1, true), // oldest from that day
            ("24492a8", TimeDelta::days(28), 1, true), // oldest from that day
            ("401824a", TimeDelta::days(28), 2, false),
            ("30b5ca4", TimeDelta::weeks(5), 0, true), // keep
            ("7898463", TimeDelta::weeks(6), 1, true), // keep
            ("85f9d33", TimeDelta::weeks(6), 2, false),
            ("1910012", TimeDelta::weeks(8), 0, true), // keep
            ("ff56c08", TimeDelta::weeks(9), 0, true), // keep
            ("59932e2", TimeDelta::weeks(10), 0, true), // keep
            ("481b645", TimeDelta::weeks(20), 0, true), // keep
            ("4a8e558", TimeDelta::weeks(52), 0, true), // keep
            ("5f34e9f", TimeDelta::weeks(80), 0, false),
            ("4947c86", TimeDelta::weeks(104), 0, true), // keep
            ("963fcca", TimeDelta::weeks(156), 0, false),
            ("eed11b3", TimeDelta::weeks(157), 0, true), // keep
            ("d14d877", TimeDelta::weeks(208), 0, false),
            ("3e50f75", TimeDelta::weeks(209), 0, true), // keep
            ("dc96392", TimeDelta::weeks(260), 0, false),
            ("31d1259", TimeDelta::weeks(261), 0, true), // keep
            ("00b01d2", TimeDelta::weeks(521), 0, false),
            ("d16da00", TimeDelta::weeks(522), 0, false)
        ];
        let now = Utc::now();
        let inputs: Vec<(Checksum, DateTime<Utc>)> = raw_inputs
            .iter()
            .map(|input| {
                if input.2 > 0 {
                    let that_time = now - input.1;
                    (
                        Checksum::SHA1(input.0.into()),
                        that_time.with_hour(input.2).unwrap(),
                    )
                } else {
                    (Checksum::SHA1(input.0.into()), now - input.1)
                }
            })
            .collect();
        let expected: Vec<Checksum> = raw_inputs
            .iter()
            .filter_map(|input| {
                if input.3 {
                    Some(Checksum::SHA1(input.0.into()))
                } else {
                    None
                }
            })
            .collect();
        let actual = auto_prune_snapshots(inputs, now);
        assert_eq!(actual.len(), expected.len());
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.0, *e);
        }
    }

    #[test]
    fn test_pruner_visit_count_snapshots_less() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let snapshot_c = Snapshot::new(None, tree_c, Default::default());
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let snapshot_a = Snapshot::new(Some(snapshot_b1), tree_a, Default::default());
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_b2)
            .returning(move |_| Ok(Some(snapshot_b.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_c2)
            .returning(move |_| Ok(Some(snapshot_c.clone())));
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.visit_count_snapshots(snapshot_a2, 5);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_none());
    }

    #[test]
    fn test_pruner_visit_count_snapshots_equal() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let snapshot_c = Snapshot::new(None, tree_c, Default::default());
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let expected = snapshot_c.digest.clone();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let snapshot_a = Snapshot::new(Some(snapshot_b1), tree_a, Default::default());
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_b2)
            .returning(move |_| Ok(Some(snapshot_b.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_c2)
            .returning(move |_| Ok(Some(snapshot_c.clone())));
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.visit_count_snapshots(snapshot_a2, 3);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let digest = option.unwrap();
        assert_eq!(digest, expected);
    }

    #[test]
    fn test_pruner_visit_count_snapshots_more() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let snapshot_c = Snapshot::new(None, tree_c, Default::default());
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        let expected = snapshot_b.digest.clone();
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let snapshot_a = Snapshot::new(Some(snapshot_b1), tree_a, Default::default());
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_b2)
            .returning(move |_| Ok(Some(snapshot_b.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_c2)
            .returning(move |_| Ok(Some(snapshot_c.clone())));
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.visit_count_snapshots(snapshot_a2, 2);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let digest = option.unwrap();
        assert_eq!(digest, expected);
    }

    #[test]
    fn test_pruner_visit_days_snapshots_none() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let mut snapshot_c = Snapshot::new(None, tree_c, Default::default());
        let days_ago_3 = chrono::Utc::now() - chrono::TimeDelta::days(3);
        snapshot_c.set_start_time(days_ago_3);
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let mut snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        let days_ago_2 = chrono::Utc::now() - chrono::TimeDelta::days(2);
        snapshot_b.set_start_time(days_ago_2);
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let mut snapshot_a = Snapshot::new(Some(snapshot_b1), tree_a, Default::default());
        let days_ago_1 = chrono::Utc::now() - chrono::TimeDelta::days(1);
        snapshot_a.set_start_time(days_ago_1);
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_b2)
            .returning(move |_| Ok(Some(snapshot_b.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_c2)
            .returning(move |_| Ok(Some(snapshot_c.clone())));
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.visit_days_snapshots(snapshot_a2, 5);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_none());
    }

    #[test]
    fn test_pruner_visit_days_snapshots_equal() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let mut snapshot_c = Snapshot::new(None, tree_c, Default::default());
        let days_ago_3 = chrono::Utc::now() - chrono::TimeDelta::days(3);
        snapshot_c.set_start_time(days_ago_3);
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let expected = snapshot_c.digest.clone();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let mut snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        let days_ago_2 = chrono::Utc::now() - chrono::TimeDelta::days(2);
        snapshot_b.set_start_time(days_ago_2);
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let mut snapshot_a = Snapshot::new(Some(snapshot_b1), tree_a, Default::default());
        let days_ago_1 = chrono::Utc::now() - chrono::TimeDelta::days(1);
        snapshot_a.set_start_time(days_ago_1);
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_b2)
            .returning(move |_| Ok(Some(snapshot_b.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_c2)
            .returning(move |_| Ok(Some(snapshot_c.clone())));
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.visit_days_snapshots(snapshot_a2, 3);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let digest = option.unwrap();
        assert_eq!(digest, expected);
    }

    #[test]
    fn test_pruner_visit_days_snapshots_extra() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let mut snapshot_c = Snapshot::new(None, tree_c, Default::default());
        let days_ago_3 = chrono::Utc::now() - chrono::TimeDelta::days(3);
        snapshot_c.set_start_time(days_ago_3);
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let mut snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        let days_ago_2 = chrono::Utc::now() - chrono::TimeDelta::days(2);
        snapshot_b.set_start_time(days_ago_2);
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        let expected = snapshot_b.digest.clone();
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let mut snapshot_a = Snapshot::new(Some(snapshot_b1), tree_a, Default::default());
        let days_ago_1 = chrono::Utc::now() - chrono::TimeDelta::days(1);
        snapshot_a.set_start_time(days_ago_1);
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_b2)
            .returning(move |_| Ok(Some(snapshot_b.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_c2)
            .returning(move |_| Ok(Some(snapshot_c.clone())));
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.visit_days_snapshots(snapshot_a2, 2);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let digest = option.unwrap();
        assert_eq!(digest, expected);
    }

    #[test]
    fn test_pruner_snapshots_after_none() {
        // arrange
        let tree_a = Checksum::SHA1("9f61917".to_owned());
        let snapshot_a = Snapshot::new(None, tree_a, Default::default());
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.prune_snapshots_after(snapshot_a2);
        // assert
        assert!(result.is_ok());
        let count = result.unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_pruner_snapshots_after_some() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let snapshot_c = Snapshot::new(None, tree_c, Default::default());
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let snapshot_c_sha1 = snapshot_c.digest.clone().to_string();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        let snapshot_b_sha1 = snapshot_b.digest.clone().to_string();
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let snapshot_a = Snapshot::new(Some(snapshot_b1), tree_a, Default::default());
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let snapshot_a3 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_b2)
            .returning(move |_| Ok(Some(snapshot_b.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_c2)
            .returning(move |_| Ok(Some(snapshot_c.clone())));
        mock.expect_put_snapshot()
            .withf(move |s| s.digest == snapshot_a3 && s.parent.is_none())
            .returning(|_| Ok(()));
        mock.expect_delete_snapshot()
            .withf(move |id| id == snapshot_b_sha1)
            .returning(|_| Ok(()));
        mock.expect_delete_snapshot()
            .withf(move |id| id == snapshot_c_sha1)
            .returning(|_| Ok(()));
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.prune_snapshots_after(snapshot_a2);
        // assert
        assert!(result.is_ok());
        let count = result.unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_pruner_prune_snapshots_auto() {
        // arrange
        #[rustfmt::skip]
        let raw_inputs = [
            // digest, tree, time delta, specific hour, parent, put, delete
            ("58950e2", "cafebabe", TimeDelta::hours(1), 0, "e21d304", true), // within last 24 hours
            ("e21d304", "cafebabe", TimeDelta::hours(6), 0, "af09fd7", true), // within last 24 hours
            ("af09fd7", "cafebabe", TimeDelta::hours(12), 0, "24d6ec6", true), // within last 24 hours
            ("24d6ec6", "cafebabe", TimeDelta::days(2), 1, "56662fc", true), // oldest from that day
            ("56662fc", "cafebabe", TimeDelta::days(2), 2, "e017530", false),
            ("e017530", "cafebabe", TimeDelta::days(2), 3, "6f8c483", false),
            ("6f8c483", "cafebabe", TimeDelta::days(3), 2, "3c1cbff", true), // oldest from that day
            ("3c1cbff", "cafebabe", TimeDelta::days(3), 3, "c586348", false),
            ("c586348", "cafebabe", TimeDelta::days(4), 1, "31e2791", true), // oldest from that day
            ("31e2791", "cafebabe", TimeDelta::days(7), 1, "2b4a1e4", true), // oldest from that day
            ("2b4a1e4", "cafebabe", TimeDelta::days(14), 1, "8896097", true), // oldest from that day
            ("8896097", "cafebabe", TimeDelta::days(21), 1, "24492a8", true), // oldest from that day
            ("24492a8", "cafebabe", TimeDelta::days(28), 1, "401824a", true), // oldest from that day
            ("401824a", "cafebabe", TimeDelta::days(28), 2, "30b5ca4", false),
            ("30b5ca4", "cafebabe", TimeDelta::weeks(5), 0, "7898463", true), // keep
            ("7898463", "cafebabe", TimeDelta::weeks(6), 1, "85f9d33", true), // keep
            ("85f9d33", "cafebabe", TimeDelta::weeks(6), 2, "1910012", false),
            ("1910012", "cafebabe", TimeDelta::weeks(8), 0, "ff56c08", true), // keep
            ("ff56c08", "cafebabe", TimeDelta::weeks(9), 0, "59932e2", true), // keep
            ("59932e2", "cafebabe", TimeDelta::weeks(10), 0, "481b645", true), // keep
            ("481b645", "cafebabe", TimeDelta::weeks(20), 0, "4a8e558", true), // keep
            ("4a8e558", "cafebabe", TimeDelta::weeks(52), 0, "5f34e9f", true), // keep
            ("5f34e9f", "cafebabe", TimeDelta::weeks(80), 0, "4947c86", false),
            ("4947c86", "cafebabe", TimeDelta::weeks(104), 0, "963fcca", true), // keep
            ("963fcca", "cafebabe", TimeDelta::weeks(156), 0, "eed11b3", false),
            ("eed11b3", "cafebabe", TimeDelta::weeks(157), 0, "d14d877", true), // keep
            ("d14d877", "cafebabe", TimeDelta::weeks(208), 0, "3e50f75", false),
            ("3e50f75", "cafebabe", TimeDelta::weeks(209), 0, "dc96392", true), // keep
            ("dc96392", "cafebabe", TimeDelta::weeks(260), 0, "31d1259", false),
            ("31d1259", "cafebabe", TimeDelta::weeks(261), 0, "00b01d2", true), // keep
            ("00b01d2", "cafebabe", TimeDelta::weeks(521), 0, "d16da00", false),
            ("d16da00", "cafebabe", TimeDelta::weeks(522), 0, "", false)
        ];
        let mut mock = MockRecordRepository::new();
        let now = Utc::now();
        for orig in raw_inputs.iter() {
            #[allow(clippy::clone_on_copy)]
            let input = orig.clone();
            mock.expect_get_snapshot()
                .withf(move |d| d == &Checksum::SHA1(input.0.into()))
                .returning(move |_| {
                    let parent = if input.4.is_empty() {
                        None
                    } else {
                        Some(Checksum::SHA1(input.4.into()))
                    };
                    let start_time: DateTime<Utc> = if input.3 > 0 {
                        (now - input.2).with_hour(input.3).unwrap()
                    } else {
                        now - input.2
                    };
                    Ok(Some(Snapshot {
                        digest: Checksum::SHA1(input.0.into()),
                        parent,
                        start_time,
                        end_time: Some(start_time + TimeDelta::hours(1)),
                        file_counts: FileCounts::default(),
                        tree: Checksum::SHA1(input.1.into()),
                    }))
                });
            if input.5 {
                if input.0 == "31d1259" {
                    // last snapshot has its parent set to none
                    mock.expect_put_snapshot()
                        .withf(move |s| {
                            s.digest == Checksum::SHA1(input.0.into()) && s.parent.is_none()
                        })
                        .returning(|_| Ok(()));
                } else {
                    // other snapshots have fields we won't verify
                    mock.expect_put_snapshot()
                        .withf(move |s| s.digest == Checksum::SHA1(input.0.into()))
                        .returning(|_| Ok(()));
                }
            } else {
                let expected_id = format!("sha1-{}", input.0);
                mock.expect_delete_snapshot()
                    .withf(move |id| id == expected_id)
                    .returning(|_| Ok(()));
            }
        }
        let submock = MockSubscriber::new();
        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let result = pruner.prune_snapshots_auto(Checksum::SHA1(raw_inputs[0].0.into()));
        // assert
        assert!(result.is_ok());
        let count = result.unwrap();
        assert_eq!(count, 11); // 11 pruned, 23 retained
    }

    #[test]
    fn test_pruner_snapshots_removes_all_dangling() {
        // arrange
        let mut mock = MockRecordRepository::new();

        // snapshot E:
        // - fileA with chunks C1, C2, xattr X1
        let chunk_c1_sum = Checksum::BLAKE3("7431ee5".to_owned());
        let chunk_c1_str = chunk_c1_sum.to_string();
        let chunk_c2_sum = Checksum::BLAKE3("4597073".to_owned());
        let chunk_c2_str = chunk_c2_sum.to_string();
        let file_a_sum = Checksum::BLAKE3("5c96f6d".to_owned());
        let file_a_sum2 = file_a_sum.clone();
        let file_a_str = file_a_sum.to_string();
        mock.expect_delete_file()
            .once()
            .withf(move |id| id == file_a_str)
            .returning(move |_| Ok(()));
        mock.expect_delete_chunk()
            .once()
            .withf(move |id| id == chunk_c1_str)
            .returning(move |_| Ok(()));
        mock.expect_delete_chunk()
            .once()
            .withf(move |id| id == chunk_c2_str)
            .returning(move |_| Ok(()));

        let file_a_ref = TreeReference::FILE(file_a_sum2);
        let file_a_path = std::path::Path::new("../test/fixtures/lorem-ipsum.txt");
        let mut file_a_entry = TreeEntry::new(file_a_path, file_a_ref);
        let xattr_x1_sum = Checksum::SHA1("d0711b0".to_owned());
        let xattr_x1_str = xattr_x1_sum.to_string();
        file_a_entry
            .xattrs
            .insert("kMDItemKeyphrase".into(), xattr_x1_sum);
        let snap_e_tree = Tree::new(vec![file_a_entry], 1);
        let snap_e_tree_sum2 = snap_e_tree.digest.clone();
        let snap_e_tree_str = snap_e_tree.digest.to_string();
        let snap_e_tree_str2 = snap_e_tree_str.clone();
        mock.expect_delete_tree()
            .once()
            .withf(move |d| d == snap_e_tree_str)
            .returning(move |_| Ok(()));
        mock.expect_delete_xattr()
            .once()
            .withf(move |id| id == xattr_x1_str)
            .returning(move |_| Ok(()));

        let snapshot_e = Snapshot::new(None, snap_e_tree_sum2, Default::default());
        let snapshot_e1 = snapshot_e.digest.clone();
        let snapshot_e2 = snapshot_e.digest.clone();
        let snapshot_e_str = snapshot_e.digest.to_string();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &snapshot_e1)
            .returning(move |_| Ok(Some(snapshot_e.clone())));
        mock.expect_delete_snapshot()
            .once()
            .withf(move |id| id == snapshot_e_str)
            .returning(move |_| Ok(()));

        // snapshot D:
        // - (fileA is gone)
        // - fileB
        // - treeA/fileC
        // - treeA/shorty
        // - symlink
        let file_c_sum = Checksum::BLAKE3("2fd30c9".to_owned());
        let file_c_sum2 = file_c_sum.clone();
        let file_c_str = file_c_sum.to_string();

        let file_c_ref = TreeReference::FILE(file_c_sum2);
        let file_c_path = std::path::Path::new("../test/fixtures/baby-birth.jpg");
        let file_c_entry = TreeEntry::new(file_c_path, file_c_ref);
        let shorty_path = std::path::Path::new("../test/fixtures/zero-length.txt");
        let shorty_ref = TreeReference::SMALL(vec![]);
        let shorty_entry = TreeEntry::new(shorty_path, shorty_ref);
        let tree_a_tree = Tree::new(vec![file_c_entry, shorty_entry], 2);
        let tree_a_tree_str = tree_a_tree.digest.to_string();
        let tree_a_tree_str2 = tree_a_tree_str.clone();
        mock.expect_delete_tree()
            .once()
            .withf(move |d| d == tree_a_tree_str)
            .returning(move |_| Ok(()));
        mock.expect_delete_file()
            .once()
            .withf(move |id| id == file_c_str)
            .returning(move |_| Ok(()));

        let pack_p1_sum = Checksum::BLAKE3("8a6a0e6".to_owned());
        let file_b_chunks: Vec<(u64, Checksum)> = vec![(0, pack_p1_sum)];
        let file_b_sum = Checksum::BLAKE3("f413392".to_owned());
        let file_b_sum1 = file_b_sum.clone();
        let file_b_sum2 = file_b_sum.clone();
        let file_b = File::new(file_b_sum, 1027, file_b_chunks);

        let file_b_ref = TreeReference::FILE(file_b_sum2);
        let file_b_path = std::path::Path::new("../test/fixtures/washington-journal.txt");
        let file_b_entry = TreeEntry::new(file_b_path, file_b_ref);
        let file_b_entry2 = file_b_entry.clone();
        let symlink_path = std::path::Path::new("../test/fixtures/C++98-tutorial.pdf");
        let symlink_ref = TreeReference::LINK(vec![0x6e, 0x6f, 0x77, 0x68, 0x65, 0x72, 0x65]);
        let symlink_entry = TreeEntry::new(symlink_path, symlink_ref);
        let snap_d_tree = Tree::new(vec![file_b_entry, symlink_entry], 2);
        let snap_d_tree_sum2 = snap_d_tree.digest.clone();
        let snap_d_tree_str = snap_d_tree.digest.to_string();
        let snap_d_tree_str2 = snap_d_tree_str.clone();
        mock.expect_delete_tree()
            .once()
            .withf(move |d| d == snap_d_tree_str)
            .returning(move |_| Ok(()));

        let snapshot_d = Snapshot::new(Some(snapshot_e2), snap_d_tree_sum2, Default::default());
        let snapshot_d1 = snapshot_d.digest.clone();
        let snapshot_d2 = snapshot_d.digest.clone();
        let snapshot_d_str = snapshot_d.digest.to_string();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &snapshot_d1)
            .returning(move |_| Ok(Some(snapshot_d.clone())));
        mock.expect_delete_snapshot()
            .once()
            .withf(move |id| id == snapshot_d_str)
            .returning(move |_| Ok(()));

        // snapshot C:
        // - fileB
        // - fileD
        // - (treeA is gone)
        // - (symlink is gone)
        let pack_p3_sum = Checksum::BLAKE3("4725989".to_owned());
        let file_d_chunks: Vec<(u64, Checksum)> = vec![(0, pack_p3_sum)];
        let file_d_sum = Checksum::BLAKE3("d414266".to_owned());
        let file_d_sum1 = file_d_sum.clone();
        let file_d_sum2 = file_d_sum.clone();
        let file_d = File::new(file_d_sum, 152, file_d_chunks);
        mock.expect_get_file()
            .once()
            .withf(move |id| id == &file_b_sum1)
            .returning(move |_| Ok(Some(file_b.clone())));
        mock.expect_get_file()
            .once()
            .withf(move |id| id == &file_d_sum1)
            .returning(move |_| Ok(Some(file_d.clone())));

        let file_d_ref = TreeReference::FILE(file_d_sum2);
        let file_d_path = std::path::Path::new("../test/fixtures/pack_store_local.json");
        let file_d_entry = TreeEntry::new(file_d_path, file_d_ref);
        let file_d_entry2 = file_d_entry.clone();
        let snap_c_tree = Tree::new(vec![file_b_entry2, file_d_entry], 2);
        let snap_c_tree_sum1 = snap_c_tree.digest.clone();
        let snap_c_tree_sum2 = snap_c_tree.digest.clone();
        let snap_c_tree_sum3 = snap_c_tree.digest.clone();
        let snap_c_tree_str = snap_c_tree.digest.to_string();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &snap_c_tree_sum1)
            .returning(move |_| Ok(Some(snap_c_tree.clone())));

        let snapshot_c = Snapshot::new(Some(snapshot_d2), snap_c_tree_sum2, Default::default());
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let snapshot_c3 = snapshot_c.digest.clone();
        let snapshot_c_end = Snapshot::new(None, snap_c_tree_sum3, Default::default());
        // snapshot C has a parent but later does not after pruning
        let mut snap_c_call_count = 0;
        mock.expect_get_snapshot()
            .times(2)
            .withf(move |d| d == &snapshot_c1)
            .returning(move |_| {
                snap_c_call_count += 1;
                if snap_c_call_count > 1 {
                    Ok(Some(snapshot_c_end.clone()))
                } else {
                    Ok(Some(snapshot_c.clone()))
                }
            });
        mock.expect_put_snapshot()
            .once()
            .withf(move |s| s.digest == snapshot_c3 && s.parent.is_none())
            .returning(move |_| Ok(()));

        // snapshot B:
        // - fileB2 (modified)
        // - fileD
        let pack_p4_sum = Checksum::BLAKE3("7eed809".to_owned());
        let file_b2_chunks: Vec<(u64, Checksum)> = vec![(0, pack_p4_sum)];
        let file_b2_sum = Checksum::BLAKE3("ce8ec7a".to_owned());
        let file_b2_sum1 = file_b2_sum.clone();
        let file_b2_sum2 = file_b2_sum.clone();
        let file_b2 = File::new(file_b2_sum, 83, file_b2_chunks);
        mock.expect_get_file()
            .once()
            .withf(move |id| id == &file_b2_sum1)
            .returning(move |_| Ok(Some(file_b2.clone())));

        let file_b2_ref = TreeReference::FILE(file_b2_sum2);
        let file_b2_path = std::path::Path::new("../test/fixtures/pack_store_bad_kind.json");
        let file_b2_entry = TreeEntry::new(file_b2_path, file_b2_ref);
        let file_b2_entry2 = file_b2_entry.clone();
        let snap_b_tree = Tree::new(vec![file_b2_entry, file_d_entry2], 2);
        let snap_b_tree_sum1 = snap_b_tree.digest.clone();
        let snap_b_tree_sum2 = snap_b_tree.digest.clone();
        let snap_b_tree_str = snap_b_tree.digest.to_string();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &snap_b_tree_sum1)
            .returning(move |_| Ok(Some(snap_b_tree.clone())));

        let snapshot_b = Snapshot::new(Some(snapshot_c2), snap_b_tree_sum2, Default::default());
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        mock.expect_get_snapshot()
            .times(2)
            .withf(move |d| d == &snapshot_b1)
            .returning(move |_| Ok(Some(snapshot_b.clone())));

        // snapshot A:
        // - fileB2
        // - fileD2 (modified)
        let pack_p5_sum = Checksum::BLAKE3("b9ac21a".to_owned());
        let file_d2_chunks: Vec<(u64, Checksum)> = vec![(0, pack_p5_sum)];
        let file_d2_sum = Checksum::BLAKE3("d180f4d".to_owned());
        let file_d2_sum1 = file_d2_sum.clone();
        let file_d2_sum2 = file_d2_sum.clone();
        let file_d2 = File::new(file_d2_sum, 83, file_d2_chunks);
        mock.expect_get_file()
            .once()
            .withf(move |id| id == &file_d2_sum1)
            .returning(move |_| Ok(Some(file_d2.clone())));

        let file_d2_ref = TreeReference::FILE(file_d2_sum2);
        let file_d2_path = std::path::Path::new("../test/fixtures/fixture_reader.dart");
        let file_d2_entry = TreeEntry::new(file_d2_path, file_d2_ref);
        let snap_a_tree = Tree::new(vec![file_d2_entry, file_b2_entry2], 2);
        let snap_a_tree_sum1 = snap_a_tree.digest.clone();
        let snap_a_tree_sum2 = snap_a_tree.digest.clone();
        let snap_a_tree_str = snap_a_tree.digest.to_string();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &snap_a_tree_sum1)
            .returning(move |_| Ok(Some(snap_a_tree.clone())));

        let snapshot_a = Snapshot::new(Some(snapshot_b2), snap_a_tree_sum2, Default::default());
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        mock.expect_get_snapshot()
            .times(2)
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));

        // dataset retains 3 snapshots
        let mut dataset1 = Dataset::new(std::path::Path::new("/home/planet"));
        dataset1.retention = SnapshotRetention::COUNT(3);
        dataset1.snapshot = Some(snapshot_a2.clone());
        let dataset1_1 = dataset1.clone();
        let dataset1_id = dataset1.id.clone();
        let dataset1_id2 = dataset1.id.clone();
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset1.clone()]));
        mock.expect_get_dataset()
            .once()
            .withf(move |id| id == dataset1_id)
            .returning(move |_| Ok(Some(dataset1_1.clone())));

        mock.expect_get_all_tree_digests()
            .once()
            .returning(move || {
                Ok(hat![
                    snap_e_tree_str2.clone(),
                    tree_a_tree_str2.clone(),
                    snap_d_tree_str2.clone(),
                    snap_c_tree_str.clone(),
                    snap_b_tree_str.clone(),
                    snap_a_tree_str.clone(),
                ])
            });
        mock.expect_get_all_chunk_digests().once().returning(|| {
            Ok(hat![
                "blake3-7431ee5".to_owned(),
                "blake3-4597073".to_owned(),
            ])
        });
        mock.expect_get_all_xattr_digests()
            .once()
            .returning(|| Ok(hat!["sha1-d0711b0".to_owned()]));
        mock.expect_get_all_file_digests().once().returning(|| {
            Ok(hat![
                "blake3-5c96f6d".to_owned(),
                "blake3-2fd30c9".to_owned(),
                "blake3-f413392".to_owned(),
                "blake3-d414266".to_owned(),
                "blake3-ce8ec7a".to_owned(),
                "blake3-d180f4d".to_owned(),
            ])
        });
        let mut submock = MockSubscriber::new();
        submock.expect_started().once().returning(|_| false);
        submock.expect_finished().once().returning(|_| false);

        // act
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let request = Request::new(dataset1_id2);
        let result = pruner.prune_snapshots(request);
        // assert
        assert!(result.is_ok());
        let count = result.unwrap();
        assert_eq!(count, 2);
    }

    // --- database_scrub tests ---------------------------------------------

    fn scrub_store(id: &str) -> Store {
        let mut properties = HashMap::new();
        properties.insert("basepath".to_owned(), format!("/tmp/{}", id));
        Store {
            id: id.to_owned(),
            store_type: StoreType::LOCAL,
            label: id.to_owned(),
            properties,
            retention: PackRetention::ALL,
        }
    }

    fn scrub_dataset(snapshot: Option<Checksum>, store_ids: Vec<String>) -> Dataset {
        let mut dataset = Dataset::new(std::path::Path::new("/home/planet"));
        dataset.snapshot = snapshot;
        dataset.stores = store_ids;
        dataset
    }

    #[test]
    fn test_pruner_database_scrub_healthy() {
        // Dataset → snapshot → tree (one FILE entry with one xattr) → file with
        // one chunk which is really a pack reference; pack references a known
        // store. Everything resolves, so zero issues are produced.
        let mut mock = MockRecordRepository::new();

        let store = scrub_store("store-1");
        let store_clone = store.clone();
        let store_clone2 = store.clone();
        mock.expect_get_stores()
            .once()
            .returning(move || Ok(vec![store_clone.clone()]));
        mock.expect_get_store()
            .once()
            .withf(|id| id == "store-1")
            .returning(move |_| Ok(Some(store_clone2.clone())));

        // pack (referenced via a single-chunk "file")
        let pack_digest = Checksum::BLAKE3("pack-aaaa".to_owned());
        let pack_digest_filter = pack_digest.clone();
        let pack = Pack::new(
            pack_digest.clone(),
            vec![PackLocation::new("store-1", "bucket", "object")],
        );
        mock.expect_get_pack()
            .once()
            .withf(move |d| d == &pack_digest_filter)
            .returning(move |_| Ok(Some(pack.clone())));

        // file — single-entry chunk list → pack ref
        let file_digest = Checksum::BLAKE3("file-aaaa".to_owned());
        let file_digest_filter = file_digest.clone();
        let file = File::new(file_digest.clone(), 1024, vec![(0, pack_digest)]);
        mock.expect_get_file()
            .once()
            .withf(move |d| d == &file_digest_filter)
            .returning(move |_| Ok(Some(file.clone())));

        // xattr
        let xattr_digest = Checksum::SHA1("xattr-aaaa".to_owned());
        let xattr_digest_filter = xattr_digest.clone();
        mock.expect_get_xattr()
            .once()
            .withf(move |d| d == &xattr_digest_filter)
            .returning(move |_| Ok(Some(vec![1u8, 2, 3])));

        // tree with a single FILE entry carrying the xattr
        let entry_ref = TreeReference::FILE(file_digest);
        let path = std::path::Path::new("../test/fixtures/lorem-ipsum.txt");
        let mut entry = TreeEntry::new(path, entry_ref);
        entry.xattrs.insert("kMDItemKeyphrase".into(), xattr_digest);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &tree_digest_filter)
            .returning(move |_| Ok(Some(tree.clone())));

        // single snapshot with no parent
        let snapshot = Snapshot::new(None, tree_digest, Default::default());
        let snapshot_digest = snapshot.digest.clone();
        let snapshot_digest_filter = snapshot_digest.clone();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &snapshot_digest_filter)
            .returning(move |_| Ok(Some(snapshot.clone())));

        let dataset = scrub_dataset(Some(snapshot_digest), vec!["store-1".to_owned()]);
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().expect("scrub should succeed");
        assert!(
            issues.is_empty(),
            "expected no issues, got: {:?}",
            issues
        );
    }

    #[test]
    fn test_pruner_database_scrub_missing_tree() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let tree_digest = Checksum::SHA1("deadbeef".to_owned());
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &tree_digest_filter)
            .returning(|_| Ok(None));

        let snapshot = Snapshot::new(None, tree_digest, Default::default());
        let snapshot_digest = snapshot.digest.clone();
        let snapshot_digest_filter = snapshot_digest.clone();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &snapshot_digest_filter)
            .returning(move |_| Ok(Some(snapshot.clone())));

        let dataset = scrub_dataset(Some(snapshot_digest), vec![]);
        let dataset_id = dataset.id.clone();
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].dataset_id.as_deref(), Some(dataset_id.as_str()));
        assert!(
            issues[0].message.starts_with("missing tree record:"),
            "got: {}",
            issues[0].message
        );
    }

    #[test]
    fn test_pruner_database_scrub_missing_file() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let file_digest = Checksum::BLAKE3("file-missing".to_owned());
        let file_digest_filter = file_digest.clone();
        mock.expect_get_file()
            .once()
            .withf(move |d| d == &file_digest_filter)
            .returning(|_| Ok(None));

        let entry_ref = TreeReference::FILE(file_digest);
        let path = std::path::Path::new("../test/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(path, entry_ref);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &tree_digest_filter)
            .returning(move |_| Ok(Some(tree.clone())));

        let snapshot = Snapshot::new(None, tree_digest, Default::default());
        let snapshot_digest = snapshot.digest.clone();
        let snapshot_digest_filter = snapshot_digest.clone();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &snapshot_digest_filter)
            .returning(move |_| Ok(Some(snapshot.clone())));

        let dataset = scrub_dataset(Some(snapshot_digest), vec![]);
        let dataset_id = dataset.id.clone();
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].dataset_id.as_deref(), Some(dataset_id.as_str()));
        assert!(issues[0].message.starts_with("missing file record:"));
    }

    #[test]
    fn test_pruner_database_scrub_missing_chunk() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let chunk1 = Checksum::BLAKE3("chunk-1".to_owned());
        let chunk2 = Checksum::BLAKE3("chunk-2".to_owned());
        let chunk2_filter = chunk2.clone();

        // Two chunks → multi-chunk file → chunks go through chunk phase.
        let file_digest = Checksum::BLAKE3("file-x".to_owned());
        let file_digest_filter = file_digest.clone();
        let file = File::new(
            file_digest.clone(),
            2048,
            vec![(0, chunk1.clone()), (1024, chunk2.clone())],
        );
        mock.expect_get_file()
            .once()
            .withf(move |d| d == &file_digest_filter)
            .returning(move |_| Ok(Some(file.clone())));

        let pack_digest = Checksum::BLAKE3("pack-ok".to_owned());
        let pack_digest_filter = pack_digest.clone();
        let chunk1_record = Chunk::new(chunk1.clone(), 0, 1024).packfile(pack_digest.clone());
        let chunk1_filter = chunk1.clone();
        mock.expect_get_chunk()
            .once()
            .withf(move |d| d == &chunk1_filter)
            .returning(move |_| Ok(Some(chunk1_record.clone())));
        mock.expect_get_chunk()
            .once()
            .withf(move |d| d == &chunk2_filter)
            .returning(|_| Ok(None));

        let pack = Pack::new(pack_digest.clone(), vec![]);
        mock.expect_get_pack()
            .once()
            .withf(move |d| d == &pack_digest_filter)
            .returning(move |_| Ok(Some(pack.clone())));

        let entry_ref = TreeReference::FILE(file_digest);
        let path = std::path::Path::new("../test/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(path, entry_ref);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &tree_digest_filter)
            .returning(move |_| Ok(Some(tree.clone())));

        let snapshot = Snapshot::new(None, tree_digest, Default::default());
        let snapshot_digest = snapshot.digest.clone();
        let snapshot_digest_filter = snapshot_digest.clone();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &snapshot_digest_filter)
            .returning(move |_| Ok(Some(snapshot.clone())));

        let dataset = scrub_dataset(Some(snapshot_digest), vec![]);
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().unwrap();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].dataset_id.is_none());
        assert!(
            issues[0].message.starts_with("missing chunk record:"),
            "got: {}",
            issues[0].message
        );
    }

    #[test]
    fn test_pruner_database_scrub_missing_pack() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let pack_digest = Checksum::BLAKE3("pack-missing".to_owned());
        let pack_digest_filter = pack_digest.clone();
        mock.expect_get_pack()
            .once()
            .withf(move |d| d == &pack_digest_filter)
            .returning(|_| Ok(None));

        // Single-chunk file → pack reference directly
        let file_digest = Checksum::BLAKE3("file-1".to_owned());
        let file_digest_filter = file_digest.clone();
        let file = File::new(file_digest.clone(), 500, vec![(0, pack_digest)]);
        mock.expect_get_file()
            .once()
            .withf(move |d| d == &file_digest_filter)
            .returning(move |_| Ok(Some(file.clone())));

        let entry_ref = TreeReference::FILE(file_digest);
        let path = std::path::Path::new("../test/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(path, entry_ref);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &tree_digest_filter)
            .returning(move |_| Ok(Some(tree.clone())));

        let snapshot = Snapshot::new(None, tree_digest, Default::default());
        let snapshot_digest = snapshot.digest.clone();
        let snapshot_digest_filter = snapshot_digest.clone();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &snapshot_digest_filter)
            .returning(move |_| Ok(Some(snapshot.clone())));

        let dataset = scrub_dataset(Some(snapshot_digest), vec![]);
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().unwrap();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].dataset_id.is_none());
        assert!(issues[0].message.starts_with("missing pack record:"));
    }

    #[test]
    fn test_pruner_database_scrub_missing_xattr() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let xattr_digest = Checksum::SHA1("xattr-gone".to_owned());
        let xattr_digest_filter = xattr_digest.clone();
        mock.expect_get_xattr()
            .once()
            .withf(move |d| d == &xattr_digest_filter)
            .returning(|_| Ok(None));

        // A tree with a SMALL entry (so no file lookup needed) carrying the
        // xattr. SMALL short-circuits the file/chunk/pack phases entirely.
        let path = std::path::Path::new("../test/fixtures/zero-length.txt");
        let mut entry = TreeEntry::new(path, TreeReference::SMALL(vec![]));
        entry.xattrs.insert("k".into(), xattr_digest);
        let tree = Tree::new(vec![entry], 0);
        let tree_digest = tree.digest.clone();
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .once()
            .withf(move |d| d == &tree_digest_filter)
            .returning(move |_| Ok(Some(tree.clone())));

        let snapshot = Snapshot::new(None, tree_digest, Default::default());
        let snapshot_digest = snapshot.digest.clone();
        let snapshot_digest_filter = snapshot_digest.clone();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &snapshot_digest_filter)
            .returning(move |_| Ok(Some(snapshot.clone())));

        let dataset = scrub_dataset(Some(snapshot_digest), vec![]);
        let dataset_id = dataset.id.clone();
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].dataset_id.as_deref(), Some(dataset_id.as_str()));
        assert!(issues[0].message.starts_with("missing xattr record:"));
    }

    #[test]
    fn test_pruner_database_scrub_unknown_store_ref() {
        let mut mock = MockRecordRepository::new();
        // No stores defined, but the dataset references one.
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let dataset = scrub_dataset(None, vec!["ghost-store".to_owned()]);
        let dataset_id = dataset.id.clone();
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].dataset_id.as_deref(), Some(dataset_id.as_str()));
        assert!(
            issues[0].message.contains("unknown store ghost-store"),
            "got: {}",
            issues[0].message
        );
    }

    #[test]
    fn test_pruner_database_scrub_visits_shared_tree_once() {
        // Two snapshots in a parent chain, both pointing at the same tree.
        // The tree should be loaded (get_tree) exactly once.
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let path = std::path::Path::new("../test/fixtures/zero-length.txt");
        let entry = TreeEntry::new(path, TreeReference::SMALL(vec![]));
        let tree = Tree::new(vec![entry], 0);
        let tree_digest = tree.digest.clone();
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .times(1)
            .withf(move |d| d == &tree_digest_filter)
            .returning(move |_| Ok(Some(tree.clone())));

        let older = Snapshot::new(None, tree_digest.clone(), Default::default());
        let older_digest = older.digest.clone();
        let older_filter = older_digest.clone();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &older_filter)
            .returning(move |_| Ok(Some(older.clone())));

        let newer = Snapshot::new(Some(older_digest), tree_digest, Default::default());
        let newer_digest = newer.digest.clone();
        let newer_filter = newer_digest.clone();
        mock.expect_get_snapshot()
            .once()
            .withf(move |d| d == &newer_filter)
            .returning(move |_| Ok(Some(newer.clone())));

        let dataset = scrub_dataset(Some(newer_digest), vec![]);
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().unwrap();
        assert!(issues.is_empty());
    }

    #[test]
    fn test_pruner_database_scrub_respects_stop_flag() {
        // With the stop flag set before calling, the scrub exits during the
        // datasets phase — no snapshot/tree getters should be called.
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let dataset = scrub_dataset(Some(Checksum::SHA1("does-not-matter".into())), vec![]);
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));
        // No other expectations — their absence asserts no other calls happen.

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(true));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.database_scrub().unwrap();
        assert!(issues.is_empty());
    }

    // --- prune_packs tests -------------------------------------------------

    use crate::domain::repositories::MockPackRepository;

    fn prune_store(id: &str, retention: PackRetention) -> Store {
        let mut properties = HashMap::new();
        properties.insert("basepath".to_owned(), format!("/tmp/{}", id));
        Store {
            id: id.to_owned(),
            store_type: StoreType::LOCAL,
            label: id.to_owned(),
            properties,
            retention,
        }
    }

    // Build a dataset whose latest snapshot references the given pack via a
    // single-chunk file. The snapshot digest is returned so the caller can
    // register the expected `get_snapshot` / `get_tree` / `get_file` returns.
    fn build_reachability_fixture(
        mock: &mut MockRecordRepository,
        pack_digest: Checksum,
    ) -> Checksum {
        let file_digest = Checksum::BLAKE3("file-reach".to_owned());
        let file_digest_filter = file_digest.clone();
        let file = File::new(file_digest.clone(), 500, vec![(0, pack_digest)]);
        mock.expect_get_file()
            .withf(move |d| d == &file_digest_filter)
            .returning(move |_| Ok(Some(file.clone())));

        let entry_ref = TreeReference::FILE(file_digest);
        let path = std::path::Path::new("../test/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(path, entry_ref);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .withf(move |d| d == &tree_digest_filter)
            .returning(move |_| Ok(Some(tree.clone())));

        let snapshot = Snapshot::new(None, tree_digest, Default::default());
        let snapshot_digest = snapshot.digest.clone();
        let snapshot_digest_filter = snapshot_digest.clone();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_digest_filter)
            .returning(move |_| Ok(Some(snapshot.clone())));

        snapshot_digest
    }

    #[test]
    fn test_prune_packs_reachable_untouched() {
        // A pack referenced by the latest snapshot must not be deleted.
        let mut mock = MockRecordRepository::new();

        let store = prune_store("store-1", PackRetention::DAYS(1));
        let store_clone = store.clone();
        mock.expect_get_stores()
            .once()
            .returning(move || Ok(vec![store_clone.clone()]));
        mock.expect_build_pack_repo().returning(|_| {
            Ok(Box::new(MockPackRepository::new()))
        });

        let pack_digest = Checksum::BLAKE3("pack-reach".to_owned());
        let snapshot_digest = build_reachability_fixture(&mut mock, pack_digest.clone());

        let dataset = scrub_dataset(Some(snapshot_digest), vec!["store-1".to_owned()]);
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let pack_digest_str = pack_digest.to_string();
        mock.expect_get_all_pack_digests()
            .once()
            .returning(move || Ok(hat![pack_digest_str.clone()]));
        // Reachable: neither get_pack nor delete_pack nor put_pack should fire.

        mock.expect_get_databases().once().returning(|| Ok(vec![]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        assert!(issues.is_empty(), "unexpected issues: {:?}", issues);
    }

    #[test]
    fn test_prune_packs_partial_delete_keeps_record() {
        // Unreachable pack lives on two stores: one DAYS(1) with elapsed age
        // and one ALL. The DAYS store must be cleared and the record must be
        // updated (not deleted) because the ALL location remains.
        let mut mock = MockRecordRepository::new();

        let store_days = prune_store("store-days", PackRetention::DAYS(1));
        let store_all = prune_store("store-all", PackRetention::ALL);
        let stores = vec![store_days.clone(), store_all.clone()];
        mock.expect_get_stores()
            .once()
            .returning(move || Ok(stores.clone()));

        // Build per-store pack repos: delete_pack must be called exactly once
        // on store-days and never on store-all.
        mock.expect_build_pack_repo().returning(|store| {
            let mut pack_repo = MockPackRepository::new();
            if store.id == "store-days" {
                pack_repo
                    .expect_delete_pack()
                    .once()
                    .withf(|loc| loc.store == "store-days")
                    .returning(|_| Ok(()));
            }
            Ok(Box::new(pack_repo))
        });

        // No datasets → the pack is unreachable.
        mock.expect_get_datasets().once().returning(|| Ok(vec![]));

        let pack_digest = Checksum::BLAKE3("pack-orphan".to_owned());
        let pack_digest_str = pack_digest.to_string();
        let pack_digest_filter = pack_digest.clone();
        let pack = Pack {
            digest: pack_digest.clone(),
            locations: vec![
                PackLocation::new("store-days", "bucket", "object"),
                PackLocation::new("store-all", "bucket", "object"),
            ],
            upload_time: Utc::now() - TimeDelta::days(30),
        };
        let pack_clone = pack.clone();
        mock.expect_get_all_pack_digests()
            .once()
            .returning(move || Ok(hat![pack_digest_str.clone()]));
        mock.expect_get_pack()
            .once()
            .withf(move |d| d == &pack_digest_filter)
            .returning(move |_| Ok(Some(pack_clone.clone())));
        // Record is updated (put_pack) with the ALL location retained.
        mock.expect_put_pack()
            .once()
            .withf(|p| {
                p.locations.len() == 1 && p.locations[0].store == "store-all"
            })
            .returning(|_| Ok(()));
        // delete_pack (record) must never be called.

        mock.expect_get_databases().once().returning(|| Ok(vec![]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        assert!(issues.is_empty(), "unexpected issues: {:?}", issues);
    }

    #[test]
    fn test_prune_packs_all_stores_elapsed_deletes_record() {
        // Two DAYS stores, both elapsed: delete_pack on each remote, then
        // delete_pack on the DB record because locations becomes empty.
        let mut mock = MockRecordRepository::new();

        let store_a = prune_store("store-a", PackRetention::DAYS(7));
        let store_b = prune_store("store-b", PackRetention::DAYS(14));
        let stores = vec![store_a.clone(), store_b.clone()];
        mock.expect_get_stores()
            .once()
            .returning(move || Ok(stores.clone()));

        mock.expect_build_pack_repo().returning(|_store| {
            let mut pack_repo = MockPackRepository::new();
            pack_repo
                .expect_delete_pack()
                .once()
                .returning(|_| Ok(()));
            Ok(Box::new(pack_repo))
        });

        mock.expect_get_datasets().once().returning(|| Ok(vec![]));

        let pack_digest = Checksum::BLAKE3("pack-doomed".to_owned());
        let pack_digest_str = pack_digest.to_string();
        let pack_digest_filter = pack_digest.clone();
        let pack = Pack {
            digest: pack_digest.clone(),
            locations: vec![
                PackLocation::new("store-a", "bucket", "object"),
                PackLocation::new("store-b", "bucket", "object"),
            ],
            upload_time: Utc::now() - TimeDelta::days(60),
        };
        mock.expect_get_all_pack_digests()
            .once()
            .returning(move || Ok(hat![pack_digest_str.clone()]));
        let pack_clone = pack.clone();
        mock.expect_get_pack()
            .once()
            .withf(move |d| d == &pack_digest_filter)
            .returning(move |_| Ok(Some(pack_clone.clone())));
        let expected_id = pack_digest.to_string();
        mock.expect_delete_pack()
            .once()
            .withf(move |id| id == expected_id.as_str())
            .returning(|_| Ok(()));
        // put_pack must not fire (locations went to zero).

        mock.expect_get_databases().once().returning(|| Ok(vec![]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        assert!(issues.is_empty(), "unexpected issues: {:?}", issues);
    }

    #[test]
    fn test_prune_packs_age_below_retention_skipped() {
        // Unreachable pack younger than retention: neither remote nor DB
        // record should be touched.
        let mut mock = MockRecordRepository::new();

        let store = prune_store("store-1", PackRetention::DAYS(30));
        let store_clone = store.clone();
        mock.expect_get_stores()
            .once()
            .returning(move || Ok(vec![store_clone.clone()]));
        mock.expect_build_pack_repo()
            .returning(|_| Ok(Box::new(MockPackRepository::new())));

        mock.expect_get_datasets().once().returning(|| Ok(vec![]));

        let pack_digest = Checksum::BLAKE3("pack-young".to_owned());
        let pack_digest_str = pack_digest.to_string();
        let pack_digest_filter = pack_digest.clone();
        let pack = Pack {
            digest: pack_digest.clone(),
            locations: vec![PackLocation::new("store-1", "bucket", "object")],
            upload_time: Utc::now() - TimeDelta::days(5),
        };
        mock.expect_get_all_pack_digests()
            .once()
            .returning(move || Ok(hat![pack_digest_str.clone()]));
        let pack_clone = pack.clone();
        mock.expect_get_pack()
            .once()
            .withf(move |d| d == &pack_digest_filter)
            .returning(move |_| Ok(Some(pack_clone.clone())));
        // Record preserved as-is: put_pack must NOT fire because locations
        // did not change.

        mock.expect_get_databases().once().returning(|| Ok(vec![]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        assert!(issues.is_empty(), "unexpected issues: {:?}", issues);
    }

    #[test]
    fn test_prune_packs_delete_error_captured() {
        // delete_pack on the remote fails → an issue is captured, the
        // location is retained, and put_pack is NOT called because the
        // record is unchanged (locations list identical to the input).
        let mut mock = MockRecordRepository::new();

        let store = prune_store("store-1", PackRetention::DAYS(1));
        let store_clone = store.clone();
        mock.expect_get_stores()
            .once()
            .returning(move || Ok(vec![store_clone.clone()]));
        mock.expect_build_pack_repo().returning(|_| {
            let mut pack_repo = MockPackRepository::new();
            pack_repo
                .expect_delete_pack()
                .once()
                .returning(|_| Err(anyhow!("network down")));
            Ok(Box::new(pack_repo))
        });

        mock.expect_get_datasets().once().returning(|| Ok(vec![]));

        let pack_digest = Checksum::BLAKE3("pack-err".to_owned());
        let pack_digest_str = pack_digest.to_string();
        let pack_digest_filter = pack_digest.clone();
        let pack = Pack {
            digest: pack_digest.clone(),
            locations: vec![PackLocation::new("store-1", "bucket", "object")],
            upload_time: Utc::now() - TimeDelta::days(30),
        };
        mock.expect_get_all_pack_digests()
            .once()
            .returning(move || Ok(hat![pack_digest_str.clone()]));
        let pack_clone = pack.clone();
        mock.expect_get_pack()
            .once()
            .withf(move |d| d == &pack_digest_filter)
            .returning(move |_| Ok(Some(pack_clone.clone())));
        // put_pack must NOT fire: locations unchanged.

        mock.expect_get_databases().once().returning(|| Ok(vec![]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        assert_eq!(issues.len(), 1);
        assert!(
            issues[0].message.contains("network down"),
            "got: {}",
            issues[0].message
        );
    }

    #[test]
    fn test_prune_packs_databases_preserve_newest() {
        // Three database archives; the newest is preserved. The older two are
        // eligible and get deleted from the store and from the DB.
        let mut mock = MockRecordRepository::new();

        let store = prune_store("store-1", PackRetention::DAYS(1));
        let store_clone = store.clone();
        mock.expect_get_stores()
            .once()
            .returning(move || Ok(vec![store_clone.clone()]));
        mock.expect_build_pack_repo().returning(|_| {
            let mut pack_repo = MockPackRepository::new();
            pack_repo
                .expect_delete_pack()
                .times(2)
                .returning(|_| Ok(()));
            Ok(Box::new(pack_repo))
        });

        mock.expect_get_datasets().once().returning(|| Ok(vec![]));
        mock.expect_get_all_pack_digests()
            .once()
            .returning(|| Ok(HashedArrayTree::new()));

        let old1 = Pack {
            digest: Checksum::BLAKE3("dbase-old1".to_owned()),
            locations: vec![PackLocation::new("store-1", "bucket", "old1")],
            upload_time: Utc::now() - TimeDelta::days(120),
        };
        let old2 = Pack {
            digest: Checksum::BLAKE3("dbase-old2".to_owned()),
            locations: vec![PackLocation::new("store-1", "bucket", "old2")],
            upload_time: Utc::now() - TimeDelta::days(60),
        };
        let newest = Pack {
            digest: Checksum::BLAKE3("dbase-new".to_owned()),
            locations: vec![PackLocation::new("store-1", "bucket", "new")],
            upload_time: Utc::now() - TimeDelta::days(2),
        };
        let archives = vec![old1.clone(), newest.clone(), old2.clone()];
        mock.expect_get_databases()
            .once()
            .returning(move || Ok(archives.clone()));
        let doomed_ids: HashSet<String> = [old1.digest.to_string(), old2.digest.to_string()]
            .into_iter()
            .collect();
        mock.expect_delete_database()
            .times(2)
            .withf(move |id| doomed_ids.contains(id))
            .returning(|_| Ok(()));
        // put_database must never fire; locations go to zero for both.

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        assert!(issues.is_empty(), "unexpected issues: {:?}", issues);
    }

    #[test]
    fn test_prune_packs_tainted_skips_phase_a() {
        // A missing file record during tree traversal taints reachability.
        // Phase A (pack-file deletion) must be skipped — get_all_pack_digests
        // is never consulted — but Phase B still prunes old database archives.
        let mut mock = MockRecordRepository::new();

        let store = prune_store("store-1", PackRetention::DAYS(1));
        let store_clone = store.clone();
        mock.expect_get_stores()
            .once()
            .returning(move || Ok(vec![store_clone.clone()]));
        mock.expect_build_pack_repo().returning(|_| {
            let mut pack_repo = MockPackRepository::new();
            // Only the old database archive should be deleted from the store.
            pack_repo
                .expect_delete_pack()
                .once()
                .returning(|_| Ok(()));
            Ok(Box::new(pack_repo))
        });

        // Build a snapshot/tree that references a file digest that cannot be
        // loaded: get_file returns Ok(None) → taints reachability.
        let file_digest = Checksum::BLAKE3("missing-file".to_owned());
        let file_digest_filter = file_digest.clone();
        mock.expect_get_file()
            .withf(move |d| d == &file_digest_filter)
            .returning(|_| Ok(None));

        let entry_ref = TreeReference::FILE(file_digest);
        let path = std::path::Path::new("../test/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(path, entry_ref);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let tree_digest_filter = tree_digest.clone();
        mock.expect_get_tree()
            .withf(move |d| d == &tree_digest_filter)
            .returning(move |_| Ok(Some(tree.clone())));

        let snapshot = Snapshot::new(None, tree_digest, Default::default());
        let snapshot_digest = snapshot.digest.clone();
        let snapshot_digest_filter = snapshot_digest.clone();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_digest_filter)
            .returning(move |_| Ok(Some(snapshot.clone())));

        let dataset = scrub_dataset(Some(snapshot_digest), vec!["store-1".to_owned()]);
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        // get_all_pack_digests / get_pack / delete_pack on the record
        // repository must never be called because Phase A is skipped.

        // Phase B: two archives, newest preserved, older deleted.
        let older = Pack {
            digest: Checksum::BLAKE3("dbase-old".to_owned()),
            locations: vec![PackLocation::new("store-1", "bucket", "old")],
            upload_time: Utc::now() - TimeDelta::days(60),
        };
        let newest = Pack {
            digest: Checksum::BLAKE3("dbase-new".to_owned()),
            locations: vec![PackLocation::new("store-1", "bucket", "new")],
            upload_time: Utc::now() - TimeDelta::days(2),
        };
        let archives = vec![older.clone(), newest.clone()];
        mock.expect_get_databases()
            .once()
            .returning(move || Ok(archives.clone()));
        let older_id = older.digest.to_string();
        mock.expect_delete_database()
            .once()
            .withf(move |id| id == older_id.as_str())
            .returning(|_| Ok(()));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        // One issue for the missing file record.
        assert_eq!(issues.len(), 1, "issues: {:?}", issues);
        assert!(
            issues[0].message.contains("missing file record"),
            "got: {}",
            issues[0].message
        );
    }

    #[test]
    fn test_prune_packs_unknown_store_reported_once() {
        // Two unreachable packs both reference a store that is no longer
        // configured. Only one "unknown store" issue should be captured per
        // run, not one per referencing pack.
        let mut mock = MockRecordRepository::new();

        mock.expect_get_stores().once().returning(|| Ok(vec![]));
        mock.expect_get_datasets().once().returning(|| Ok(vec![]));

        let pack1 = Pack {
            digest: Checksum::BLAKE3("pack-orphan-1".to_owned()),
            locations: vec![PackLocation::new("gone", "bucket", "obj1")],
            upload_time: Utc::now() - TimeDelta::days(60),
        };
        let pack2 = Pack {
            digest: Checksum::BLAKE3("pack-orphan-2".to_owned()),
            locations: vec![PackLocation::new("gone", "bucket", "obj2")],
            upload_time: Utc::now() - TimeDelta::days(60),
        };
        let pack1_digest = pack1.digest.clone();
        let pack2_digest = pack2.digest.clone();
        let digest_strs = vec![pack1_digest.to_string(), pack2_digest.to_string()];
        mock.expect_get_all_pack_digests()
            .once()
            .returning(move || {
                let mut hat = HashedArrayTree::new();
                for s in &digest_strs {
                    hat.push(s.clone());
                }
                Ok(hat)
            });
        let pack1_clone = pack1.clone();
        let pack1_filter = pack1_digest.clone();
        mock.expect_get_pack()
            .withf(move |d| d == &pack1_filter)
            .returning(move |_| Ok(Some(pack1_clone.clone())));
        let pack2_clone = pack2.clone();
        let pack2_filter = pack2_digest.clone();
        mock.expect_get_pack()
            .withf(move |d| d == &pack2_filter)
            .returning(move |_| Ok(Some(pack2_clone.clone())));
        // No put_pack / delete_pack: unknown store keeps the location,
        // locations stays non-empty, and nothing changed.

        mock.expect_get_databases().once().returning(|| Ok(vec![]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(false));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        assert_eq!(issues.len(), 1, "issues: {:?}", issues);
        assert!(
            issues[0].message.contains("unknown store gone"),
            "got: {}",
            issues[0].message
        );
    }

    #[test]
    fn test_prune_packs_respects_stop_flag() {
        // Stop flag set before the run: exits during the datasets phase, so
        // get_all_pack_digests / get_databases are never called.
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().once().returning(|| Ok(vec![]));

        let dataset = scrub_dataset(Some(Checksum::SHA1("n/a".into())), vec![]);
        mock.expect_get_datasets()
            .once()
            .returning(move || Ok(vec![dataset.clone()]));

        let submock = MockSubscriber::new();
        let stopper = Arc::new(RwLock::new(true));
        let pruner = PrunerImpl::new(Arc::new(mock), Arc::new(submock), stopper);
        let issues = pruner.prune_packs().expect("prune_packs should succeed");
        assert!(issues.is_empty());
    }
}
