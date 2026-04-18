//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Snapshot, SnapshotRetention, TreeReference};
use crate::domain::repositories::RecordRepository;
use anyhow::{Error, anyhow};
use chrono::{DateTime, Datelike, TimeDelta, Utc};
use log::info;
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

///
/// `Pruner` processes requests to prune snapshots and packs for a data set.
///
#[cfg_attr(test, automock)]
pub trait Pruner: Send + Sync {
    /// Synchronously perform the appropriate pruning for a certain data set.
    ///
    /// Returns the number of snapshots that were pruned.
    fn prune_snapshots(&self, request: Request) -> Result<usize, Error>;
}

///
/// Basic implementation of `Pruner`.
///
pub struct PrunerImpl {
    repo: Arc<dyn RecordRepository>,
    // Events related to the backup are sent to the subscriber.
    subscriber: Arc<dyn Subscriber>,
    // If the value is true, the backup process should stop.
    #[allow(dead_code)]
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
    retain.sort_by(|a, b| b.1.cmp(&a.1));
    retain
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{Dataset, File, FileCounts, Snapshot, Tree, TreeEntry};
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
}
