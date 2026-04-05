//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities;
use crate::domain::repositories::{PackRepository, RecordRepository};
use crate::shared::packs;
use crate::shared::thread_pool::ThreadPool;
use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use globset::{Glob, GlobSet, GlobSetBuilder};
use log::{debug, error, info, trace, warn};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, SystemTime, SystemTimeError};

/// Status of a backup request.
#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// Request is waiting to be processed.
    PENDING,
    /// Request is being processed.
    RUNNING,
    /// Request has been interrupted by the user, will stop soon.
    STOPPING,
    /// Request was paused due to scheduling.
    PAUSED,
    /// Request processing completed successfully.
    COMPLETED,
    /// Request processing encountered an error and could not finish.
    FAILED,
}

///
/// Request to backup a specific dataset.
///
#[derive(Clone, Debug)]
pub struct Request {
    /// Unique identifier for this request.
    pub id: String,
    /// Status of the request.
    pub status: Status,
    /// Identifier of the dataset to be backed up.
    pub dataset: String,
    /// Passphrase used to generate a secret key to encrypt pack files.
    pub passphrase: String,
    /// Optional time at which to stop the backup, albeit temporarily.
    pub stop_time: Option<DateTime<Utc>>,
    /// The date-time when the request processing started.
    pub started: Option<DateTime<Utc>>,
    /// The date-time when the request was completed.
    pub finished: Option<DateTime<Utc>>,
    /// Number of files that changed in this snapshot.
    pub changed_files: u64,
    /// Number of packs uploaded so far.
    pub packs_uploaded: u64,
    /// Number of files uploaded so far.
    pub files_uploaded: u64,
    /// Number of bytes uploaded so far, which may change more often than the
    /// number of files in the event that a very large file is being uploaded.
    pub bytes_uploaded: u64,
    /// Error messages if anything went wrong during processing.
    pub errors: Vec<String>,
}

impl Request {
    /// Construct a new instance of Request.
    pub fn new<T: Into<String>>(
        dataset: String,
        passphrase: T,
        stop_time: Option<DateTime<Utc>>,
    ) -> Self {
        let id = xid::new().to_string();
        Self {
            id,
            status: Status::PENDING,
            dataset,
            passphrase: passphrase.into(),
            stop_time,
            started: None,
            finished: None,
            changed_files: 0,
            packs_uploaded: 0,
            files_uploaded: 0,
            bytes_uploaded: 0,
            errors: vec![],
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Backup]Request({})", self.id)
    }
}

impl cmp::PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl cmp::Eq for Request {}

///
/// A `Subscriber` receives updates to the progress of a backup operation.
///
#[cfg_attr(test, automock)]
pub trait Subscriber: Send + Sync {
    /// Backup operation has begun to be processed.
    fn started(&self, request_id: &str);

    /// Backup found `count` many files changed since the previous snapshot.
    fn files_changed(&self, request_id: &str, count: u64);

    /// Backup process has successfully uploaded a pack file.
    fn pack_uploaded(&self, request_id: &str);

    /// An additional number of bytes have been uploaded.
    fn bytes_uploaded(&self, request_id: &str, addend: u64);

    /// An additional number of files have been uploaded.
    fn files_uploaded(&self, request_id: &str, addend: u64);

    /// An error has occurred while restoring files, directories, or links.
    fn error(&self, request_id: &str, error: String);

    /// The backup process has paused due to the schedule for the dataset.
    fn paused(&self, request_id: &str);

    /// A paused backup operation has begun to be processed again.
    fn restarted(&self, request_id: &str);

    /// Backup request has been completed.
    fn finished(&self, request_id: &str);
}

///
/// Performs the actual backup of a dataset by scanning the directory for new or
/// changed files, creating packs, uploading them, and updating the database.
///
#[cfg_attr(test, automock)]
pub trait Backuper: Send + Sync {
    /// Synchronously perform the backup for a certain data set.
    ///
    /// Returns the digest of the snapshot, or `None` if there were no changes.
    ///
    /// May return `OutOfTimeFailure` if the process reached the end of the
    /// range of time permitted in the schedule.
    fn backup(&self, request: Request) -> Result<Option<entities::Checksum>, Error>;
}

///
/// Basic implementation of `Backuper`.
///
pub struct BackuperImpl {
    dbase: Arc<dyn RecordRepository>,
    // Events related to the backup are sent to the subscriber.
    subscriber: Arc<dyn Subscriber>,
    // If the value is true, the backup process should stop.
    stop_requested: Arc<RwLock<bool>>,
}

impl BackuperImpl {
    /// Construct a new instance of BackuperImpl.
    pub fn new(
        repo: Arc<dyn RecordRepository>,
        subscriber: Arc<dyn Subscriber>,
        stop_requested: Arc<RwLock<bool>>,
    ) -> Self {
        Self {
            dbase: repo,
            subscriber,
            stop_requested,
        }
    }

    /// Process the backup request for a given dataset.
    fn start_backup(&self, request: &Request) -> Result<Option<entities::Checksum>, Error> {
        use anyhow::Context;
        let dataset = self
            .dbase
            .get_dataset(&request.dataset)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", request.dataset)))?;
        if let Some(time) = &request.stop_time {
            debug!("backup: starting for {} until {}", request.dataset, time);
        } else {
            debug!("backup: starting for {} until completion", request.dataset);
        }
        fs::create_dir_all(&dataset.workspace).with_context(|| {
            format!("backup fs::create_dir_all({})", dataset.workspace.display())
        })?;
        // Check if latest snapshot exists and lacks an end time, which indicates
        // that the previous backup did not complete successfully.
        let latest_snapshot = dataset.snapshot.clone();
        #[allow(clippy::collapsible_if)]
        if let Some(latest) = latest_snapshot.as_ref() {
            if let Some(snapshot) = self.dbase.get_snapshot(latest)? {
                if snapshot.end_time.is_none() {
                    // continue from the previous incomplete backup
                    let parent_sha1 = snapshot.parent;
                    let current_sha1 = latest.to_owned();
                    debug!("backup: continuing previous snapshot {}", &current_sha1);
                    return self.continue_backup(request, dataset, parent_sha1, current_sha1);
                }
            }
        }
        // The start time of a new backup is at the moment that a snapshot is to be
        // taken. The snapshot can take a long time to build, and another thread may
        // spawn in the mean time and start taking another snapshot, and again, and
        // again until the system runs out of resources.
        self.subscriber.started(&request.id);
        // In addition to the exclusions defined in the dataset, we exclude the
        // temporary workspace and repository database files.
        let mut excludes = self.dbase.get_excludes();
        excludes.push(dataset.workspace.clone());
        for exclusion in dataset.excludes.iter() {
            excludes.push(PathBuf::from(exclusion));
        }
        debug!("backup: dataset exclusions: {:?}", excludes);
        // Take a snapshot and record it as the new most recent snapshot for this
        // dataset, to allow detecting a running backup, and thus recover from a
        // crash or forced shutdown.
        let snap_opt = take_snapshot(
            &dataset.basepath,
            latest_snapshot.clone(),
            &self.dbase,
            excludes,
        )?;
        match snap_opt {
            None => {
                // indicate that the backup has finished (doing nothing)
                self.subscriber.finished(&request.id);
                Ok(None)
            }
            Some(new_snapshot) => {
                let mut ds = dataset.clone();
                ds.snapshot = Some(new_snapshot.clone());
                self.dbase.put_dataset(&ds)?;
                debug!("backup: starting new snapshot {}", &new_snapshot);
                self.continue_backup(request, dataset, latest_snapshot, new_snapshot)
            }
        }
    }

    ///
    /// Continue the backup for the most recent snapshot, comparing against the
    /// parent snapshot, if any.
    ///
    fn continue_backup(
        &self,
        request: &Request,
        dataset: entities::Dataset,
        parent_sha1: Option<entities::Checksum>,
        current_sha1: entities::Checksum,
    ) -> Result<Option<entities::Checksum>, Error> {
        let mut driver = BackupDriver::new(
            request.to_owned(),
            dataset.clone(),
            self.dbase.clone(),
            self.subscriber.clone(),
            self.stop_requested.clone(),
        )?;
        // if no previous snapshot, visit every file in the new snapshot, otherwise
        // find those files that changed from the previous snapshot
        match parent_sha1 {
            None => {
                let snapshot = self
                    .dbase
                    .get_snapshot(&current_sha1)?
                    .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", current_sha1)))?;
                let tree = snapshot.tree;
                // count the changed files and emit an event
                let iter = TreeWalker::new(&self.dbase, &dataset.basepath, tree.clone());
                let count: u64 = iter.count() as u64;
                self.subscriber.files_changed(&dataset.id, count);
                // perform the backup
                let iter = TreeWalker::new(&self.dbase, &dataset.basepath, tree);
                for result in iter {
                    driver.add_file(result?)?;
                }
            }
            Some(ref parent) => {
                // count the changed files and emit an event
                let iter = find_changed_files(
                    &self.dbase,
                    dataset.basepath.clone(),
                    parent.clone(),
                    current_sha1.clone(),
                )?;
                let count: u64 = iter.count() as u64;
                self.subscriber.files_changed(&dataset.id, count);
                // perform the backup
                let iter = find_changed_files(
                    &self.dbase,
                    dataset.basepath.clone(),
                    parent.clone(),
                    current_sha1.clone(),
                )?;
                for result in iter {
                    driver.add_file(result?)?;
                }
            }
        }
        // finish packing and uploading the changed files
        driver.finish_remainder()?;
        // commit everything to the database
        driver.update_snapshot(&current_sha1)?;
        driver.backup_database()?;
        Ok(Some(current_sha1))
    }
}

impl Backuper for BackuperImpl {
    fn backup(&self, request: Request) -> Result<Option<entities::Checksum>, Error> {
        info!("dataset {} to be backed up", &request.dataset);
        let start_time = SystemTime::now();
        // reset any error state in the backup
        self.subscriber.restarted(&request.id);
        let dataset_id = request.dataset.clone();
        match self.start_backup(&request) {
            Ok(Some(checksum)) => {
                self.subscriber.finished(&request.id);
                let end_time = SystemTime::now();
                let time_diff = end_time.duration_since(start_time);
                let pretty_time = pretty_print_duration(time_diff);
                info!("created new snapshot {}", &checksum);
                info!(
                    "dataset {} backup complete after {}",
                    &dataset_id, pretty_time
                );
                Ok(Some(checksum))
            }
            Ok(None) => {
                info!("no new snapshot required");
                Ok(None)
            }
            Err(err) => match err.downcast::<OutOfTimeFailure>() {
                Ok(err) => {
                    info!("backup window has reached its end");
                    // put the backup in the paused state for the time being
                    self.subscriber.paused(&request.id);
                    Err(Error::from(err))
                }
                Err(err) => {
                    // here `err` is the original error
                    error!("could not perform backup: {}", err);
                    // put the backup in the error state so we try again
                    self.subscriber.error(&request.id, err.to_string());
                    Err(err)
                }
            },
        }
    }
}

///
/// Raised when the backup has run out of time and must stop temporarily,
/// resuming at a later time.
///
#[derive(thiserror::Error, Debug)]
pub struct OutOfTimeFailure;

impl fmt::Display for OutOfTimeFailure {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ran out of time")
    }
}

///
/// Take a snapshot of the directory structure at the given path. The parent, if
/// `Some`, specifies the snapshot that will be recorded as the parent of this
/// new snapshot. If there have been no changes, then None is returned.
///
fn take_snapshot(
    basepath: &Path,
    parent: Option<entities::Checksum>,
    dbase: &Arc<dyn RecordRepository>,
    excludes: Vec<PathBuf>,
) -> Result<Option<entities::Checksum>, Error> {
    let start_time = SystemTime::now();
    let actual_start_time = Utc::now();
    let exclusions = build_exclusions(basepath, &excludes);
    let mut file_counts: entities::FileCounts = Default::default();
    let cpu_count = std::thread::available_parallelism()?.get();
    let pool = ThreadPool::new(cpu_count);
    debug!("take_snapshot: creating pool of {cpu_count} threads");
    let tree = scan_tree(basepath, dbase, &exclusions, &mut file_counts, &pool)?;
    if let Some(ref parent_sha1) = parent {
        let parent_doc = dbase
            .get_snapshot(parent_sha1)?
            .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", parent_sha1)))?;
        if parent_doc.tree == tree.digest {
            // nothing new at all with this snapshot
            return Ok(None);
        }
    }
    let end_time = SystemTime::now();
    let time_diff = end_time.duration_since(start_time);
    let pretty_time = pretty_print_duration(time_diff);
    let mut snap = entities::Snapshot::new(parent, tree.digest.clone(), file_counts);
    info!(
        "took snapshot {} with {} files after {}",
        snap.digest, tree.file_count, pretty_time
    );
    snap.set_start_time(actual_start_time);
    dbase.put_snapshot(&snap)?;
    Ok(Some(snap.digest))
}

///
/// `ChangedFile` represents a new or modified file.
///
#[derive(Debug)]
pub struct ChangedFile {
    /// File path of the changed file relative to the base path.
    pub path: PathBuf,
    /// Hash digest of the changed file.
    pub digest: entities::Checksum,
}

impl ChangedFile {
    fn new(path: &Path, digest: entities::Checksum) -> Self {
        Self {
            path: PathBuf::from(path),
            digest,
        }
    }
}

///
/// Created by calling `find_changed_files()` with the checksum for
/// two snapshots, one earlier and the other later.
///
pub struct ChangedFilesIter<'a> {
    /// Reference to Database for fetching records.
    dbase: &'a Arc<dyn RecordRepository>,
    /// Queue of pending paths to visit, where the path is relative, the first
    /// checksum is the left tree (earlier in time), and the second is the right
    /// tree (later in time).
    queue: VecDeque<(PathBuf, entities::Checksum, entities::Checksum)>,
    /// Nested iterator for visiting an entire new subdirectory.
    walker: Option<TreeWalker<'a>>,
    /// Current path being visited.
    path: Option<PathBuf>,
    /// Left tree currently being visited.
    left_tree: Option<entities::Tree>,
    /// Position within left tree currently being iterated.
    left_idx: usize,
    /// Right tree currently being visited.
    right_tree: Option<entities::Tree>,
    /// Position within right tree currently being iterated.
    right_idx: usize,
}

impl<'a> ChangedFilesIter<'a> {
    fn new(
        dbase: &'a Arc<dyn RecordRepository>,
        basepath: PathBuf,
        left_tree: entities::Checksum,
        right_tree: entities::Checksum,
    ) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back((basepath, left_tree, right_tree));
        Self {
            dbase,
            queue,
            walker: None,
            path: None,
            left_tree: None,
            left_idx: 0,
            right_tree: None,
            right_idx: 0,
        }
    }
}

impl<'a> Iterator for ChangedFilesIter<'a> {
    type Item = Result<ChangedFile, Error>;

    fn next(&mut self) -> Option<Result<ChangedFile, Error>> {
        //
        // the flow is slightly complicated because we are sometimes nesting an
        // iterator inside this iterator, so the control flow will break out of
        // inner loops to return to this top loop
        //
        loop {
            // if we are iterating on a new subtree, return the next entry
            if let Some(iter) = self.walker.as_mut() {
                let opt = iter.next();
                if opt.is_some() {
                    return opt;
                }
                // inner iterator is done, carry on with the next step
                self.walker.take();
            }
            // is there a left and right tree? iterate on that
            if let Some(ref left_tree) = self.left_tree
                && let Some(ref right_tree) = self.right_tree
            {
                while self.left_idx < left_tree.entries.len()
                    && self.right_idx < right_tree.entries.len()
                {
                    let base = self.path.as_ref().unwrap();
                    let left_entry = &left_tree.entries[self.left_idx];
                    let right_entry = &right_tree.entries[self.right_idx];
                    if left_entry.name < right_entry.name {
                        // file or directory has been removed, nothing to do
                        self.left_idx += 1;
                    } else if left_entry.name > right_entry.name {
                        // file or directory has been added
                        self.right_idx += 1;
                        if right_entry.reference.is_tree() {
                            // a new tree: add every file contained therein
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let sum = right_entry.reference.checksum().unwrap();
                            self.walker = Some(TreeWalker::new(self.dbase, &path, sum));
                            // return to the main loop
                            break;
                        } else if right_entry.reference.is_file() {
                            // return the file
                            let sum = right_entry.reference.checksum().unwrap();
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let changed = ChangedFile::new(&path, sum);
                            return Some(Ok(changed));
                        }
                    } else if left_entry.reference != right_entry.reference {
                        // they have the same name but differ somehow
                        self.left_idx += 1;
                        self.right_idx += 1;
                        let left_is_dir = left_entry.reference.is_tree();
                        let left_is_file = left_entry.reference.is_file();
                        let left_is_link = left_entry.reference.is_link();
                        let right_is_dir = right_entry.reference.is_tree();
                        let right_is_file = right_entry.reference.is_file();
                        if left_is_dir && right_is_dir {
                            // tree A & B: add both trees to the queue
                            let left_sum = left_entry.reference.checksum().unwrap();
                            let right_sum = right_entry.reference.checksum().unwrap();
                            let mut path = PathBuf::from(base);
                            path.push(&left_entry.name);
                            self.queue.push_back((path, left_sum, right_sum));
                        } else if (left_is_file || left_is_dir || left_is_link) && right_is_file {
                            // new file or a changed file
                            let sum = right_entry.reference.checksum().unwrap();
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let changed = ChangedFile::new(&path, sum);
                            return Some(Ok(changed));
                        } else if (left_is_file || left_is_link) && right_is_dir {
                            // now a directory, add everything under it
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let sum = right_entry.reference.checksum().unwrap();
                            self.walker = Some(TreeWalker::new(self.dbase, &path, sum));
                            // return to the main loop
                            break;
                        }
                    // ignore everything else
                    } else {
                        // they are the same
                        self.left_idx += 1;
                        self.right_idx += 1;
                    }
                }
                // catch everything else in the new snapshot as long as we do
                // not have a nested iterator to process
                while self.walker.is_none() && self.right_idx < right_tree.entries.len() {
                    let base = self.path.as_ref().unwrap();
                    let right_entry = &right_tree.entries[self.right_idx];
                    self.right_idx += 1;
                    if right_entry.reference.is_tree() {
                        // a new tree: add every file contained therein
                        let mut path = PathBuf::from(base);
                        path.push(&right_entry.name);
                        let sum = right_entry.reference.checksum().unwrap();
                        self.walker = Some(TreeWalker::new(self.dbase, &path, sum));
                    } else if right_entry.reference.is_file() {
                        // return the file
                        let sum = right_entry.reference.checksum().unwrap();
                        let mut path = PathBuf::from(base);
                        path.push(&right_entry.name);
                        let changed = ChangedFile::new(&path, sum);
                        return Some(Ok(changed));
                    }
                }
            }
            if self.walker.is_some() {
                // restart at the top when we have a subtree to iterate
                continue;
            }
            // Either we just started or we finished these trees, pop the queue
            // to get the next set and loop around.
            if let Some((base, left_sum, right_sum)) = self.queue.pop_front() {
                // dequeue the next entry, fetch the tree
                let result = self.dbase.get_tree(&left_sum);
                if result.is_err() {
                    return Some(Err(anyhow!(format!("failed to get tree: {:?}", left_sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(anyhow!(format!("missing tree: {:?}", left_sum))));
                }
                self.left_tree = opt;
                self.left_idx = 0;
                let result = self.dbase.get_tree(&right_sum);
                if result.is_err() {
                    return Some(Err(anyhow!(format!("failed to get tree: {:?}", right_sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(anyhow!(format!("missing tree: {:?}", right_sum))));
                }
                self.right_tree = opt;
                self.right_idx = 0;
                self.path = Some(base);
            } else {
                break;
            }
        }
        None
    }
}

///
/// Returns an `Iterator` that yields `ChangedFile` for files that were added or
/// changed between the two snapshots. Only files are considered, as changes to
/// directories are already recorded in the database and saved separately.
/// Ignores anything that is not a file or a directory. May return files that
/// were processed earlier, so the caller must filter out files that have record
/// entries in the database.
///
fn find_changed_files(
    dbase: &Arc<dyn RecordRepository>,
    basepath: PathBuf,
    snapshot1: entities::Checksum,
    snapshot2: entities::Checksum,
) -> Result<ChangedFilesIter<'_>, Error> {
    let snap1doc = dbase
        .get_snapshot(&snapshot1)?
        .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", snapshot1)))?;
    let snap2doc = dbase
        .get_snapshot(&snapshot2)?
        .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", snapshot2)))?;
    Ok(ChangedFilesIter::new(
        dbase,
        basepath,
        snap1doc.tree,
        snap2doc.tree,
    ))
}

pub struct TreeWalker<'a> {
    /// Reference to Database for fetching records.
    dbase: &'a Arc<dyn RecordRepository>,
    /// Queue of pending paths to visit, where the path is relative, the
    /// checksum is the tree to be visited.
    queue: VecDeque<(PathBuf, entities::Checksum)>,
    /// Current path being visited.
    path: Option<PathBuf>,
    /// Tree currently being visited.
    tree: Option<entities::Tree>,
    /// Position within tree currently being iterated.
    entry_idx: usize,
}

impl<'a> TreeWalker<'a> {
    fn new(
        dbase: &'a Arc<dyn RecordRepository>,
        basepath: &Path,
        tree: entities::Checksum,
    ) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back((basepath.to_owned(), tree));
        Self {
            dbase,
            queue,
            path: None,
            tree: None,
            entry_idx: 0,
        }
    }
}

impl<'a> Iterator for TreeWalker<'a> {
    type Item = Result<ChangedFile, Error>;

    fn next(&mut self) -> Option<Result<ChangedFile, Error>> {
        // loop until we produce a result for the caller
        loop {
            // if we have a tree and are not done with it, iterate on it
            if let Some(tree) = self.tree.as_ref() {
                while self.entry_idx < tree.entries.len() {
                    let base = self.path.as_ref().unwrap();
                    let entry = &tree.entries[self.entry_idx];
                    self.entry_idx += 1;
                    if entry.reference.is_tree() {
                        // enqueue the tree
                        let sum = entry.reference.checksum().unwrap();
                        let mut path = PathBuf::from(base);
                        path.push(&entry.name);
                        self.queue.push_back((path, sum));
                    } else if entry.reference.is_file() {
                        // return the file
                        let sum = entry.reference.checksum().unwrap();
                        let mut path = PathBuf::from(base);
                        path.push(&entry.name);
                        let changed = ChangedFile::new(&path, sum);
                        return Some(Ok(changed));
                    }
                }
            }
            // the tree is done, check the queue for more
            if let Some((base, sum)) = self.queue.pop_front() {
                // dequeue the next entry, fetch the tree
                let result = self.dbase.get_tree(&sum);
                if result.is_err() {
                    return Some(Err(anyhow!(format!("failed to get tree: {:?}", sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(anyhow!(format!("missing tree: {:?}", sum))));
                }
                // update the tree, index, and path fields
                self.tree = opt;
                self.entry_idx = 0;
                self.path = Some(base);
            } else {
                break;
            }
        }
        None
    }
}

//
// Build the glob set used to match file/directory exclusions.
//
fn build_exclusions(basepath: &Path, excludes: &[PathBuf]) -> GlobSet {
    let mut builder = GlobSetBuilder::new();
    if let Some(basepath_str) = basepath.to_str() {
        let mut groomed: Vec<PathBuf> = Vec::new();
        for exclusion in excludes {
            if let Some(pattern) = exclusion.to_str() {
                if pattern.starts_with("**") {
                    if pattern.ends_with("**") {
                        // no need to change the pattern
                        groomed.push(exclusion.to_owned());
                    } else {
                        // exclude the path and everything below
                        groomed.push(PathBuf::from(pattern));
                        groomed.push([pattern, "**"].iter().collect());
                    }
                } else if pattern.starts_with('*') || pattern.ends_with("**") {
                    // prepend basepath
                    groomed.push([basepath_str, pattern].iter().collect());
                } else if !pattern.is_empty() {
                    // exclude the path and everything below
                    groomed.push([basepath_str, pattern].iter().collect());
                    groomed.push([basepath_str, pattern, "**"].iter().collect());
                };
            } else {
                warn!("PathBuf::to_str() failed for {:?}", exclusion);
            }
        }
        for exclusion in groomed.iter() {
            if let Some(pattern) = exclusion.to_str() {
                if let Ok(glob) = Glob::new(pattern) {
                    builder.add(glob);
                } else {
                    warn!("could not build glob for {:?}", pattern);
                }
            } else {
                warn!("PathBuf::to_str() failed for {:?}", exclusion);
            }
        }
    }
    match builder.build() {
        Ok(set) => set,
        _ => GlobSet::empty(),
    }
}

///
/// Read the symbolic link value and convert to raw bytes.
///
fn read_link(path: &Path) -> Result<Vec<u8>, Error> {
    // convert whatever value returned by the OS into raw bytes without string conversion
    use os_str_bytes::OsStringBytes;
    let value = fs::read_link(path)?;
    Ok(value.into_os_string().into_raw_vec())
}

///
/// Create a `Tree` for the given path, recursively descending into child
/// directories. The returned tree entity will have already been added to the
/// database, along with all of the nested trees.
///
fn scan_tree(
    basepath: &Path,
    dbase: &Arc<dyn RecordRepository>,
    excludes: &GlobSet,
    file_counts: &mut entities::FileCounts,
    pool: &ThreadPool,
) -> Result<entities::Tree, Error> {
    let mut entries: Vec<entities::TreeEntry> = Vec::new();
    let mut file_count = 0;
    let mut pending_files: Vec<PathBuf> = Vec::new();
    match fs::read_dir(basepath) {
        Ok(readdir) => {
            for entry_result in readdir {
                match entry_result {
                    Ok(entry) => {
                        let path = entry.path();
                        if excludes.is_match(&path) {
                            continue;
                        }
                        // DirEntry.metadata() does not follow symlinks
                        match entry.metadata() {
                            Ok(metadata) => {
                                count_files(&metadata, file_counts);
                                if metadata.is_dir() {
                                    let scan =
                                        scan_tree(&path, dbase, excludes, file_counts, pool)?;
                                    file_count += scan.file_count;
                                    let digest = scan.digest.clone();
                                    let tref = entities::TreeReference::TREE(digest);
                                    entries.push(process_path(&path, tref, dbase));
                                } else if metadata.is_symlink() {
                                    match read_link(&path) {
                                        Ok(contents) => {
                                            let tref = entities::TreeReference::LINK(contents);
                                            entries.push(process_path(&path, tref, dbase));
                                        }
                                        Err(err) => {
                                            error!("could not read link: {:?}: {}", path, err)
                                        }
                                    }
                                } else if metadata.is_file() {
                                    if metadata.len() <= entities::FILE_SIZE_SMALL {
                                        match fs::read(&path) {
                                            Ok(contents) => {
                                                let tref = entities::TreeReference::SMALL(contents);
                                                entries.push(process_path(&path, tref, dbase));
                                            }
                                            Err(err) => {
                                                error!("could not read file: {:?}: {}", path, err)
                                            }
                                        }
                                    } else {
                                        pending_files.push(path);
                                    }
                                }
                            }
                            Err(err) => error!("metadata error for {:?}: {}", path, err),
                        }
                    }
                    Err(err) => error!("read_dir error for an entry in {:?}: {}", basepath, err),
                }
            }
        }
        Err(err) => error!("read_dir error for {:?}: {}", basepath, err),
    }
    // Process all of the files found in this directory.
    let mut file_entries = process_files(pending_files, dbase, pool);
    file_count += file_entries.len() as u32;
    for entry in file_entries.drain(..) {
        entries.push(entry);
    }
    let tree = entities::Tree::new(entries, file_count);
    dbase.insert_tree(&tree)?;
    Ok(tree)
}

// Process the given set of files, returning the TreeEntry for each. Uses the
// thread pool to compute the checksums of the files in parallel.
fn process_files(
    paths: Vec<PathBuf>,
    dbase: &Arc<dyn RecordRepository>,
    pool: &ThreadPool,
) -> Vec<entities::TreeEntry> {
    // list of results that are either successful (Some(TreeEntry)) or resulted
    // in an error (None), paired with a condvar so the main thread can wait
    let entries: Arc<(Mutex<Vec<Option<entities::TreeEntry>>>, Condvar)> =
        Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    for path in paths.iter() {
        let path = path.to_owned();
        let dbase = dbase.clone();
        let entries = entries.clone();
        pool.execute(move || {
            let entry = match entities::Checksum::blake3_from_file(&path) {
                Ok(digest) => {
                    let tref = entities::TreeReference::FILE(digest);
                    Some(process_path(&path, tref, &dbase))
                }
                Err(err) => {
                    error!("could not read file: {:?}: {}", path, err);
                    None
                }
            };
            let (lock, cvar) = &*entries;
            let mut actual = lock.lock().unwrap();
            actual.push(entry);
            cvar.notify_all();
        });
    }
    // wait for all of the entries to be processed
    let (lock, cvar) = &*entries;
    let mut actual = lock.lock().unwrap();
    while actual.len() != paths.len() {
        actual = cvar.wait(actual).unwrap();
    }
    // filter those entries that resulted in error (None)
    actual.drain(..).flatten().collect()
}

///
/// Create a `TreeEntry` record for this path, which may include storing
/// extended attributes in the database.
///
fn process_path(
    fullpath: &Path,
    reference: entities::TreeReference,
    dbase: &Arc<dyn RecordRepository>,
) -> entities::TreeEntry {
    let mut entry = entities::TreeEntry::new(fullpath, reference);
    entry = entry.mode(fullpath);
    entry = entry.owners(fullpath);
    trace!("processed path entry {:?}", fullpath);
    process_xattrs(fullpath, &mut entry, dbase);
    entry
}

#[cfg(target_family = "unix")]
fn process_xattrs(
    fullpath: &Path,
    entry: &mut entities::TreeEntry,
    dbase: &Arc<dyn RecordRepository>,
) {
    if xattr::SUPPORTED_PLATFORM {
        // The "supported" flag is not all that helpful, as it will be true even
        // for platforms where xattr operations will result in an error.
        if let Ok(xattrs) = xattr::list(fullpath) {
            for name in xattrs {
                let nm = name
                    .to_str()
                    .map(|v| v.to_owned())
                    .unwrap_or_else(|| name.to_string_lossy().into_owned());
                if let Ok(Some(value)) = xattr::get(fullpath, &name) {
                    let digest = entities::Checksum::sha1_from_bytes(value.as_ref());
                    if dbase.insert_xattr(&digest, value.as_ref()).is_ok() {
                        entry.xattrs.insert(nm, digest);
                    }
                }
            }
        }
    }
}

#[cfg(target_family = "windows")]
fn process_xattrs(
    _fullpath: &Path,
    _entry: &mut entities::TreeEntry,
    _dbase: &Arc<dyn RecordRepository>,
) {
    // nothing do to be done on Windows
}

/// Update the file_counts record to reflect this tree entry.
fn count_files(metadata: &fs::Metadata, file_counts: &mut entities::FileCounts) {
    if metadata.is_dir() {
        file_counts.directories += 1;
    } else if metadata.is_symlink() {
        file_counts.symlinks += 1;
    } else if metadata.is_file() {
        file_counts.register_file(metadata.len());
    }
}

///
/// Receives changed files, placing them in packs and uploading to the pack
/// stores. If time has run out, will raise an `OutOfTimeFailure` error.
///
pub struct BackupDriver {
    request: Request,
    dataset: entities::Dataset,
    dbase: Arc<dyn RecordRepository>,
    stores: Box<dyn PackRepository>,
    /// Preferred size of chunks in bytes.
    chunk_size: u32,
    /// Builds a pack file comprised of compressed chunks.
    builder: packs::PackBuilder,
    /// Tracks files and chunks in the current pack.
    record: PackRecord,
    /// Map of file checksum to the chunks it contains that have not yet been
    /// uploaded in a pack file.
    file_chunks: BTreeMap<entities::Checksum, Vec<entities::Chunk>>,
    /// Those chunks that have been packed using this builder.
    packed_chunks: HashSet<entities::Checksum>,
    /// Those chunks that have been uploaded previously.
    done_chunks: HashSet<entities::Checksum>,
    // Events related to the backup are sent to the subscriber.
    subscriber: Arc<dyn Subscriber>,
    stop_requested: Arc<RwLock<bool>>,
}

impl BackupDriver {
    /// Build a BackupDriver.
    pub fn new(
        request: Request,
        dataset: entities::Dataset,
        dbase: Arc<dyn RecordRepository>,
        subscriber: Arc<dyn Subscriber>,
        stop_requested: Arc<RwLock<bool>>,
    ) -> Result<Self, Error> {
        let stores = dbase.load_dataset_stores(&dataset)?;
        let chunk_size = calc_chunk_size(dataset.pack_size);
        // Because EXAF combines content into 16mb blocks, it is possible that
        // it will produce something that is just under the desired pack size,
        // and subsequently more chunks will be added, pushing it well past the
        // desired pack size.
        let target_size = (dataset.pack_size / 10) * 9;
        let passphrase = request.passphrase.clone();
        Ok(Self {
            request,
            dataset,
            dbase,
            stores,
            chunk_size,
            builder: packs::PackBuilder::new(target_size).password(passphrase),
            record: Default::default(),
            file_chunks: BTreeMap::new(),
            packed_chunks: HashSet::new(),
            done_chunks: HashSet::new(),
            subscriber,
            stop_requested,
        })
    }

    /// Process a single changed file, adding it to the pack, and possibly
    /// uploading one or more pack files as needed.
    pub fn add_file(&mut self, changed: ChangedFile) -> Result<(), Error> {
        // ignore files which already have records
        if self.dbase.get_file(&changed.digest)?.is_none() {
            if self
                .split_file(&changed.path, changed.digest.clone())
                .is_err()
            {
                // file disappeared out from under us, record it as
                // having zero length; file restore will handle it
                // without any problem
                error!("file {} went missing during backup", changed.path.display());
                let file = entities::File::new(changed.digest, 0, vec![]);
                self.dbase.insert_file(&file)?;
            }
            self.process_queue()?;
        } else {
            // count finished files for accurate progress tracking
            self.record.file_already_uploaded();
        }
        Ok(())
    }

    /// Split the given file into chunks as necessary, using the database to
    /// eliminate duplicate chunks.
    fn split_file(&mut self, path: &Path, file_digest: entities::Checksum) -> Result<(), Error> {
        if self.file_chunks.contains_key(&file_digest) {
            // do not bother processing a file we have already seen; once the
            // files have been completely uploaded, we rely on the database to
            // detect duplicate chunks
            return Ok(());
        }
        trace!("split_file '{}' digest {}", path.display(), file_digest);
        let attr = fs::metadata(path)?;
        let file_size = attr.len();
        let chunks = if file_size > self.chunk_size as u64 {
            // split large files into chunks, add chunks to the list
            packs::find_file_chunks(path, self.chunk_size)?
        } else {
            let mut chunk = entities::Chunk::new(file_digest.clone(), 0, file_size as usize);
            chunk = chunk.filepath(path);
            vec![chunk]
        };
        // find chunks that have already been recorded in the database
        chunks.iter().for_each(|chunk| {
            let result = self.dbase.get_chunk(&chunk.digest);
            #[allow(clippy::collapsible_if)]
            if let Ok(value) = result {
                if value.is_some() {
                    self.done_chunks.insert(chunk.digest.clone());
                }
            }
        });
        if chunks.len() > 120 {
            // For very large files, give some indication that we will be busy
            // for a while processing that one file since it requires many pack
            // files to completely finish this one file.
            warn!(
                "packing large file {} with {} chunks",
                path.to_string_lossy(),
                chunks.len()
            );
        }
        // save the chunks under the digest of the file they came from to make
        // it easy to update the database later
        self.file_chunks.insert(file_digest, chunks);
        Ok(())
    }

    /// Add file chunks to packs and upload until there is nothing left. Ignores
    /// files and chunks that have already been processed. Raises an error if
    /// time runs out.
    fn process_queue(&mut self) -> Result<(), Error> {
        while let Some((filesum, chunks)) = self.file_chunks.pop_first() {
            // this may run for a long time if the file is very large
            self.process_file(filesum, chunks)?;
            // check if the stop time (if any) has been reached
            if let Some(stop_time) = self.request.stop_time {
                let now = Utc::now();
                if now > stop_time {
                    return Err(Error::from(OutOfTimeFailure {}));
                }
            }
            // check if the user requested that the backup stop
            {
                let should_stop = self.stop_requested.read().unwrap();
                if *should_stop {
                    return Err(Error::from(OutOfTimeFailure {}));
                }
            }
        }
        Ok(())
    }

    /// Process a single file and all of its chunks until completion. While not
    /// necessary, the implementation is more streamlined and the ownership of
    /// the data is easier to manage without cloning.
    fn process_file(
        &mut self,
        filesum: entities::Checksum,
        chunks: Vec<entities::Chunk>,
    ) -> Result<(), Error> {
        let mut chunks_processed = 0;
        let chunks_length = chunks.len();
        for chunk in chunks.iter() {
            chunks_processed += 1;
            // determine if this chunk has already been processed
            let already_done = self.done_chunks.contains(&chunk.digest);
            let already_packed = self.packed_chunks.contains(&chunk.digest);
            if !already_done && !already_packed {
                self.record.add_chunk(chunk.clone());
                self.packed_chunks.insert(chunk.digest.clone());
                // ensure the pack builder is ready to receive chunks
                if !self.builder.is_ready() {
                    // build a "temporary" file that persists beyond the
                    // lifetime of the reference, just to get a unique name
                    let (_outfile, outpath) = tempfile::Builder::new()
                        .suffix(".pack")
                        .tempfile_in(&self.dataset.workspace)?
                        .keep()?;
                    self.builder.initialize(&outpath)?;
                }
                // add the chunk to the pack file; if the pack becomes full but
                // there are more chunks in this file that need processing, then
                // send it up now and reset
                if self.builder.add_chunk(chunk)? && chunks_processed < chunks_length {
                    let pack_path = self.builder.finalize()?;
                    self.upload_record_reset(&pack_path)?;
                }
            }
        }
        // now that we successfully visited all the chunks in this file, then
        // this file is considered done
        self.record.add_file(filesum, chunks);
        // if the builder is full, send it up now and reset in preparation for
        // the next file
        if self.builder.is_full() {
            let pack_path = self.builder.finalize()?;
            self.upload_record_reset(&pack_path)?;
        }
        Ok(())
    }

    /// If the pack builder has content, finalize the pack and upload.
    pub fn finish_remainder(&mut self) -> Result<(), Error> {
        self.process_queue()?;
        if !self.builder.is_empty() {
            let pack_path = self.builder.finalize()?;
            self.upload_record_reset(&pack_path)?;
        }
        Ok(())
    }

    /// Upload a single pack to the pack store and record the results.
    fn upload_record_reset(&mut self, pack_path: &Path) -> Result<(), Error> {
        trace!("upload_record_reset {}", pack_path.display());
        // verify that the pack contents match the record; this is not perfect
        // since the record itself could also be wrong, but it's quick and easy
        if !self
            .record
            .verify_pack(pack_path, &self.request.passphrase)?
        {
            return Err(anyhow!(
                "missing chunks from pack file {}",
                pack_path.display()
            ));
        }
        let pack_digest = entities::Checksum::blake3_from_file(pack_path)?;
        // basically impossible to produce the same pack twice because the EXAF
        // encryption involves a random nonce per archive content block
        if self.dbase.get_pack(&pack_digest)?.is_none() {
            // new pack file, need to upload this and record to database
            let config = self.dbase.get_configuration()?;
            let bucket_name = self.stores.get_bucket_name(&config.computer_id);
            let object_name = format!("{}", pack_digest);
            // capture and record the remote object name, in case it differs from
            // the name we generated ourselves; either value is expected to be
            // sufficiently unique for our purposes
            let locations = self
                .stores
                .store_pack(pack_path, &bucket_name, &object_name)?;
            self.record
                .record_completed_pack(&self.dbase, &pack_digest, locations)?;
            self.subscriber.pack_uploaded(&self.request.id);
        } else {
            info!("pack record already exists for {}", pack_digest);
        }
        fs::remove_file(pack_path)?;
        let count = self
            .record
            .record_completed_files(&self.dbase, &pack_digest)? as u64;
        self.subscriber
            .bytes_uploaded(&self.request.id, self.record.bytes_packed as u64);
        self.subscriber.files_uploaded(&self.request.id, count);
        self.record = Default::default();
        Ok(())
    }

    /// Update the current snapshot with the end time set to the current time.
    pub fn update_snapshot(&self, snap_sha1: &entities::Checksum) -> Result<(), Error> {
        let mut snapshot = self
            .dbase
            .get_snapshot(snap_sha1)?
            .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", snap_sha1)))?;
        snapshot.set_end_time(Utc::now());
        self.dbase.put_snapshot(&snapshot)?;
        Ok(())
    }

    /// Upload an archive of the database files to the pack stores.
    pub fn backup_database(&self) -> Result<(), Error> {
        // Create a stable snapshot of the database as a single file, upload it
        // to a special place in the pack store, then record the pseudo-pack to
        // enable accurate pack pruning.
        let backup_path = self.dbase.create_backup(&self.request.passphrase)?;
        let config = self.dbase.get_configuration()?;
        let coords = self
            .stores
            .store_database(&config.computer_id, &backup_path)?;
        let digest = entities::Checksum::blake3_from_file(&backup_path)?;
        let pack = entities::Pack::new(digest.clone(), coords);
        self.dbase.insert_database(&pack)?;
        Ok(())
    }
}

// The default desired chunk size should be a little larger than the typical
// image file, and small enough that packs do not end up with a wide range
// of sizes due to large chunks.
const DEFAULT_CHUNK_SIZE: u64 = 4_194_304;

/// Compute the desired size for the chunks based on the pack size.
fn calc_chunk_size(pack_size: u64) -> u32 {
    // Use our default chunk size unless the desired pack size is so small that
    // the chunks would be a significant portion of the pack file.
    let chunk_size = if pack_size < DEFAULT_CHUNK_SIZE * 4 {
        pack_size / 4
    } else {
        DEFAULT_CHUNK_SIZE
    };
    #[allow(clippy::useless_conversion)]
    chunk_size
        .try_into()
        .map_or(DEFAULT_CHUNK_SIZE as u32, |v: u64| v as u32)
}

/// Tracks the files and chunks that comprise a pack, and provides functions for
/// saving the results to the database.
#[derive(Default)]
pub struct PackRecord {
    /// Count of previously completed files.
    completed_files: usize,
    /// Sum of the lengths of all chunks in this pack.
    bytes_packed: usize,
    /// Those files that have been completed with this pack.
    files: HashMap<entities::Checksum, Vec<entities::Chunk>>,
    /// Those chunks that are contained in this pack.
    chunks: Vec<entities::Chunk>,
}

impl PackRecord {
    /// Add a completed file to this pack.
    fn add_file(&mut self, digest: entities::Checksum, chunks: Vec<entities::Chunk>) {
        self.files.insert(digest, chunks);
    }

    /// Increment the number of files uploaded previously.
    fn file_already_uploaded(&mut self) {
        self.completed_files += 1;
    }

    /// Add a chunk to this pack.
    fn add_chunk(&mut self, chunk: entities::Chunk) {
        self.bytes_packed += chunk.length;
        self.chunks.push(chunk);
    }

    /// Return true if the given (unencrypted) pack file contains everything
    /// this record expects to be in the pack file, false otherwise.
    fn verify_pack(&self, pack_path: &Path, password: &str) -> Result<bool, Error> {
        use std::str::FromStr;
        // This is an n^2 search which is fine because the number of chunks in a
        // typical pack file is not a significantly high number (10s to 1,000s).
        let mut found_count: usize = 0;
        let mut reader = exaf_rs::reader::Entries::new(pack_path)?;
        reader.enable_encryption(password)?;
        for maybe_entry in reader {
            let entry = maybe_entry?;
            // we know the names are valid UTF-8, we created them
            let digest = entities::Checksum::from_str(entry.name())?;
            let mut found = false;
            for chunk in self.chunks.iter() {
                if chunk.digest == digest {
                    found = true;
                    found_count += 1;
                    break;
                }
            }
            if !found {
                // this is wrong for an entirely different reason
                return Err(anyhow!(
                    "unexpected chunk {} found in pack {}",
                    digest,
                    pack_path.display()
                ));
            }
        }
        // ensure we found all of the chunks
        Ok(found_count == self.chunks.len())
    }

    /// Record the results of building this pack to the database. This includes
    /// all of the chunks and the pack itself.
    fn record_completed_pack(
        &mut self,
        dbase: &Arc<dyn RecordRepository>,
        digest: &entities::Checksum,
        coords: Vec<entities::PackLocation>,
    ) -> Result<(), Error> {
        // record the uploaded chunks to the database
        for chunk in self.chunks.iter_mut() {
            // Detect the case of a chunk whose digest matches an entire file,
            // which means the chunk will _not_ get a record of its own but
            // instead the file record will point directly to a pack record.
            if !self.files.contains_key(&chunk.digest) {
                // set the pack digest for each chunk record
                chunk.packfile = Some(digest.to_owned());
                dbase.insert_chunk(chunk)?;
            }
        }
        self.chunks.clear();
        // record the pack in the database
        let pack = entities::Pack::new(digest.to_owned(), coords);
        dbase.insert_pack(&pack)?;
        Ok(())
    }

    /// Record the set of files completed by uploading this pack file.
    /// Returns the number of completed files.
    fn record_completed_files(
        &mut self,
        dbase: &Arc<dyn RecordRepository>,
        digest: &entities::Checksum,
    ) -> Result<usize, Error> {
        // massage the file/chunk data into database records for those files
        // that have been completely uploaded
        for (filesum, parts) in &self.files {
            let mut length: u64 = 0;
            let mut chunks: Vec<(u64, entities::Checksum)> = Vec::new();
            // Determine if a chunk record is needed, as the information is only
            // useful when a file produces multiple chunks. In many cases the
            // files are small and will result in only a single chunk. As such,
            // do not create a chunk record and instead save the pack digest as
            // the "chunk" in the file record. The fact that the file record
            // contains only a single chunk will be sufficient information for
            // the file restore to know that the "chunk" digest is a pack.
            if parts.len() == 1 {
                length += parts[0].length as u64;
                chunks.push((0, digest.to_owned()));
            } else {
                for chunk in parts {
                    length += chunk.length as u64;
                    chunks.push((chunk.offset as u64, chunk.digest.clone()));
                }
            }
            let file = entities::File::new(filesum.clone(), length, chunks);
            dbase.insert_file(&file)?;
        }
        Ok(self.files.len() + self.completed_files)
    }
}

// Return a clear and accurate description of the duration.
fn pretty_print_duration(duration: Result<Duration, SystemTimeError>) -> String {
    let mut result = String::new();
    match duration {
        Ok(value) => {
            let mut seconds = value.as_secs();
            if seconds > 3600 {
                let hours = seconds / 3600;
                result.push_str(format!("{} hours ", hours).as_ref());
                seconds -= hours * 3600;
            }
            if seconds > 60 {
                let minutes = seconds / 60;
                result.push_str(format!("{} minutes ", minutes).as_ref());
                seconds -= minutes * 60;
            }
            if seconds > 0 {
                result.push_str(format!("{} seconds", seconds).as_ref());
            } else if result.is_empty() {
                // special case of a zero duration
                result.push_str("0 seconds");
            }
        }
        Err(_) => result.push_str("(error)"),
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::repositories::RecordRepositoryImpl;
    use crate::data::sources::EntityDataSourceImpl;
    use crate::domain::entities::{self, Checksum, PackRetention};
    use crate::domain::repositories::MockRecordRepository;
    use crate::domain::repositories::RecordRepository;
    use crate::tasks::backup::ChangedFile;
    use anyhow::Error;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_pretty_print_duration() {
        let input = Duration::from_secs(0);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "0 seconds");

        let input = Duration::from_secs(5);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "5 seconds");

        let input = Duration::from_secs(65);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "1 minutes 5 seconds");

        let input = Duration::from_secs(4949);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "1 hours 22 minutes 29 seconds");

        let input = Duration::from_secs(7300);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "2 hours 1 minutes 40 seconds");

        let input = Duration::from_secs(10090);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "2 hours 48 minutes 10 seconds");
    }

    struct DummySubscriber();

    impl Subscriber for DummySubscriber {
        fn started(&self, _request_id: &str) {}

        fn files_changed(&self, _request_id: &str, _count: u64) {}

        fn pack_uploaded(&self, _request_id: &str) {}

        fn bytes_uploaded(&self, _request_id: &str, _addend: u64) {}

        fn files_uploaded(&self, _request_id: &str, _addend: u64) {}

        fn error(&self, _request_id: &str, _error: String) {}

        fn paused(&self, _request_id: &str) {}

        fn restarted(&self, _request_id: &str) {}

        fn finished(&self, _request_id: &str) {}
    }

    #[test]
    fn test_process_path() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_insert_xattr().returning(|_, _| Ok(()));
        // act
        let path = Path::new("../test/fixtures/washington-journal.txt");
        let digest = entities::Checksum::blake3_from_file(path).unwrap();
        let tref = entities::TreeReference::FILE(digest);
        let dbase: Arc<dyn crate::domain::repositories::RecordRepository + 'static> =
            Arc::new(mock);
        let entry = process_path(path, tref, &dbase);
        // assert
        assert_eq!(entry.name, "washington-journal.txt");
        #[cfg(target_family = "unix")]
        let expected_hash = "540c45803112958ab53e31daee5eec067b1442d579eb1e787cf7684657275b60";
        #[cfg(target_family = "windows")]
        let expected_hash = "183d52ff928be3e77cccf1b78b12b31910d5079195a637a9a2b499059f99b781";
        let expected =
            entities::TreeReference::FILE(entities::Checksum::BLAKE3(expected_hash.into()));
        assert_eq!(entry.reference, expected);
    }

    #[test]
    fn test_process_files() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_insert_xattr().returning(|_, _| Ok(()));
        // act
        let paths: Vec<PathBuf> = vec![
            PathBuf::from("../test/fixtures/lorem-ipsum.txt"),
            PathBuf::from("../test/fixtures/SekienAkashita.jpg"),
            PathBuf::from("../test/fixtures/washington-journal.txt"),
            PathBuf::from("../test/fixtures/zero-length.txt"),
        ];
        let dbase: Arc<dyn crate::domain::repositories::RecordRepository + 'static> =
            Arc::new(mock);
        let pool = ThreadPool::new(1);
        let entries = process_files(paths, &dbase, &pool);
        // assert
        assert_eq!(entries.len(), 4);
        assert!(entries.iter().any(|e| e.name == "lorem-ipsum.txt"));
        assert!(entries.iter().any(|e| e.name == "SekienAkashita.jpg"));
        assert!(entries.iter().any(|e| e.name == "washington-journal.txt"));
        assert!(entries.iter().any(|e| e.name == "zero-length.txt"));
    }

    #[test]
    fn test_build_exclusions() {
        let excludes = [
            PathBuf::from("Library"),
            PathBuf::from("**/Downloads"),
            PathBuf::from("node_modules/**"),
            PathBuf::from("*.exe"),
        ];
        let basedir = PathBuf::from("/basedir");
        let globset = build_exclusions(&basedir, &excludes[..]);
        // positive cases
        assert!(globset.is_match("/basedir/Library"));
        assert!(globset.is_match("/basedir/Library/subdir"));
        assert!(globset.is_match("/basedir/Library/subdir/file.txt"));
        assert!(globset.is_match("/basedir/Downloads"));
        assert!(globset.is_match("/basedir/Downloads/subdir"));
        assert!(globset.is_match("/basedir/Downloads/subdir/file.txt"));
        assert!(globset.is_match("/basedir/subdir/Downloads"));
        assert!(globset.is_match("/basedir/subdir/Downloads/subdir"));
        assert!(globset.is_match("/basedir/subdir/Downloads/subdir/file.txt"));
        assert!(globset.is_match("/basedir/node_modules/subdir"));
        assert!(globset.is_match("/basedir/node_modules/subdir/file.txt"));
        assert!(globset.is_match("/basedir/file.exe"));
        assert!(globset.is_match("/basedir/subdir/file.exe"));
        // negative cases
        assert!(!globset.is_match("/basedir/node_modules"));
        assert!(!globset.is_match("/basedir/subdir/node_modules"));
        assert!(!globset.is_match("/basedir/subdir/node_modules/subdir"));
        assert!(!globset.is_match("/basedir/subdir/node_modules/subdir/file.txt"));
        assert!(!globset.is_match("/basedir/subdir"));
        assert!(!globset.is_match("/basedir/subdir/file.txt"));
    }

    #[test]
    fn test_build_exclusions_nearly_empty() {
        // somehow the excludes list for a dataset contained a single empty
        // string and that caused the backup process to find nothing at all
        let excludes = [
            PathBuf::from("/storage/database"),
            PathBuf::from("/basedir/.tmp"),
            PathBuf::from(""),
        ];
        let basedir = PathBuf::from("/basedir");
        let globset = build_exclusions(&basedir, &excludes[..]);
        // positive cases
        assert!(globset.is_match("/basedir/.tmp"));
        assert!(globset.is_match("/basedir/.tmp/subdir"));
        assert!(globset.is_match("/basedir/.tmp/subdir/file.txt"));
        assert!(globset.is_match("/storage/database/LOCK"));
        // negative cases
        assert!(!globset.is_match("/basedir/node_modules"));
        assert!(!globset.is_match("/basedir/subdir/node_modules"));
        assert!(!globset.is_match("/basedir/subdir/node_modules/subdir"));
        assert!(!globset.is_match("/basedir/subdir/node_modules/subdir/file.txt"));
        assert!(!globset.is_match("/basedir/file.txt"));
        assert!(!globset.is_match("/basedir/subdir/file.txt"));
    }

    #[test]
    fn test_build_exclusions_empty() {
        let excludes: Vec<PathBuf> = Vec::new();
        let basedir = PathBuf::from("/basedir");
        let globset = build_exclusions(&basedir, &excludes[..]);
        // negative cases
        assert!(!globset.is_match("/basedir/node_modules"));
        assert!(!globset.is_match("/basedir/subdir/node_modules"));
        assert!(!globset.is_match("/basedir/subdir/node_modules/subdir"));
        assert!(!globset.is_match("/basedir/subdir/node_modules/subdir/file.txt"));
        assert!(!globset.is_match("/basedir/file.txt"));
        assert!(!globset.is_match("/basedir/subdir/file.txt"));
    }

    #[test]
    fn test_read_link() -> Result<(), Error> {
        let outdir = tempdir()?;
        let link = outdir.path().join("mylink");
        let target = "link_target_is_meaningless";
        // cfg! macro doesn't work for this case it seems so we have this
        // redundant use of the cfg directive instead
        #[cfg(target_family = "unix")]
        std::os::unix::fs::symlink(target, &link)?;
        #[cfg(target_family = "windows")]
        std::os::windows::fs::symlink_file(&target, &link)?;
        let actual = read_link(&link)?;
        assert_eq!(actual, target.as_bytes());
        Ok(())
    }

    #[test]
    fn test_basic_snapshots() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        // set up dataset base directory
        let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
        fs::create_dir_all(&fixture_base)?;
        let fixture_path = tempfile::tempdir_in(&fixture_base)?;

        // take a snapshot of the dataset
        let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
        assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
        let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
        let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
        assert!(snapshot1.parent.is_none());
        assert_eq!(snapshot1.file_counts.total_files(), 1);

        // take another snapshot
        let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
        assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
        let snap2_sha =
            take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
        let snapshot2 = dbase.get_snapshot(&snap2_sha)?.unwrap();
        assert!(snapshot2.parent.is_some());
        assert_eq!(snapshot2.parent.unwrap(), snap1_sha);
        assert_eq!(snapshot2.file_counts.file_sizes[&12], 1);
        assert_eq!(snapshot2.file_counts.file_sizes[&17], 1);
        assert_eq!(snapshot2.file_counts.total_files(), 2);
        assert_ne!(snap1_sha, snap2_sha);
        assert_ne!(snapshot1.tree, snapshot2.tree);

        // compute the differences
        let iter = find_changed_files(
            &dbase,
            fixture_path.path().to_path_buf(),
            snap1_sha,
            snap2_sha.clone(),
        )?;
        let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
        assert_eq!(changed.len(), 1);
        assert!(changed[0].is_ok());
        assert_eq!(
            changed[0].as_ref().unwrap().digest,
            Checksum::BLAKE3(String::from(
                "dba425aa7292ef1209841ab3855a93d4dfa6855658a347f85c502f2c2208cf0f"
            ))
        );

        // take yet another snapshot, should find no changes
        let snap3_opt = take_snapshot(fixture_path.path(), Some(snap2_sha), &dbase, vec![])?;
        assert!(snap3_opt.is_none());
        Ok(())
    }

    #[test]
    fn test_default_excludes() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        let basepath: PathBuf = ["..", "test", "fixtures", "dataset_1"].iter().collect();
        let mut workspace = basepath.clone();
        workspace.push(".tmp");
        let excludes = vec![workspace];
        // take a snapshot of the test data
        let snap1_sha = take_snapshot(&basepath, None, &dbase, excludes)?.unwrap();
        let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
        assert!(snapshot1.parent.is_none());
        assert_eq!(snapshot1.file_counts.total_files(), 6);
        // walk the snapshot and ensure all files were included
        let tree = snapshot1.tree;
        let iter = TreeWalker::new(&dbase, &basepath, tree);
        let mut exe_count: usize = 0;
        let mut txt_count: usize = 0;
        let mut js_count: usize = 0;
        let mut jpg_count: usize = 0;
        for result in iter {
            let path = result.unwrap().path;
            let path_str = path.to_str().unwrap();
            if path_str.ends_with(".exe") {
                exe_count += 1;
            }
            if path_str.ends_with(".txt") {
                txt_count += 1;
            }
            if path_str.ends_with(".js") {
                js_count += 1;
            }
            if path_str.ends_with(".jpg") {
                jpg_count += 1;
            }
        }
        assert_eq!(exe_count, 1);
        assert_eq!(txt_count, 2);
        assert_eq!(js_count, 1);
        assert_eq!(jpg_count, 1);
        Ok(())
    }

    #[test]
    fn test_basic_excludes() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        let excludes: Vec<PathBuf> = vec![
            // individual files
            PathBuf::from("*.exe"),
            // entire directory structure by name (while technically different
            // than the pattern that ends with /**, it has the same effect for
            // our purposes since we ignore directories)
            PathBuf::from("**/node_modules"),
            // entire directory structure by name based at the root only
            PathBuf::from("workspace"),
        ];
        let basepath: PathBuf = ["..", "test", "fixtures", "dataset_1"].iter().collect();
        // take a snapshot of the test data
        let snap1_sha = take_snapshot(&basepath, None, &dbase, excludes)?.unwrap();
        let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
        assert!(snapshot1.parent.is_none());
        assert_eq!(snapshot1.file_counts.total_files(), 3);
        // walk the snapshot and ensure excluded files are excluded
        let tree = snapshot1.tree;
        let iter = TreeWalker::new(&dbase, &basepath, tree);
        for result in iter {
            let path = result.unwrap().path;
            let path_str = path.to_str().unwrap();
            assert!(!path_str.ends_with(".exe"));
            assert!(!path_str.contains("node_modules"));
            assert!(!path_str.contains("workspace"));
        }
        Ok(())
    }

    #[test]
    fn test_snapshots_xattrs() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
        fs::create_dir_all(&fixture_base)?;
        let fixture_path = tempfile::tempdir_in(&fixture_base)?;
        let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
        assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", &dest).is_ok());
        #[allow(unused_mut, unused_assignments)]
        let mut xattr_worked = false;
        #[cfg(target_family = "unix")]
        {
            use xattr;
            xattr_worked = xattr::SUPPORTED_PLATFORM
                && xattr::set(&dest, "me.fiedlers.test", b"foobar").is_ok();
        }

        let snapshot_digest = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
        let snapshot = dbase.get_snapshot(&snapshot_digest)?.unwrap();
        assert!(snapshot.parent.is_none());
        assert_eq!(snapshot.file_counts.total_files(), 1);

        // ensure extended attributes are stored in database
        if xattr_worked {
            let tree = dbase.get_tree(&snapshot.tree)?.unwrap();
            let entries: Vec<&entities::TreeEntry> = tree
                .entries
                .iter()
                .filter(|e| !e.xattrs.is_empty())
                .collect();
            assert_eq!(entries.len(), 1);
            assert!(entries[0].xattrs.contains_key("me.fiedlers.test"));
            let x_value = dbase
                .get_xattr(&entries[0].xattrs["me.fiedlers.test"])?
                .unwrap();
            assert_eq!(x_value, b"foobar");
        }
        Ok(())
    }

    #[test]
    fn test_snapshot_symlinks() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
        fs::create_dir_all(&fixture_base)?;
        let fixture_path = tempfile::tempdir_in(&fixture_base)?;
        let dest: PathBuf = fixture_path.path().join("meaningless");
        let target = "link_target_is_meaningless";
        // cfg! macro only works if all paths can compile on every platform
        {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs;
            #[cfg(target_family = "windows")]
            use std::os::windows::fs;
            #[cfg(target_family = "unix")]
            fs::symlink(target, &dest)?;
            #[cfg(target_family = "windows")]
            fs::symlink_file(&target, &dest)?;
        }

        // take a snapshot
        let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
        let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
        assert!(snapshot1.parent.is_none());
        assert_eq!(snapshot1.file_counts.total_files(), 0);
        let tree = dbase.get_tree(&snapshot1.tree)?.unwrap();

        // ensure the tree has exactly one symlink entry
        let entries: Vec<&entities::TreeEntry> = tree
            .entries
            .iter()
            .filter(|e| e.reference.is_link())
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "meaningless");
        let value = entries[0].reference.symlink().unwrap();
        assert_eq!(value, target.as_bytes());
        Ok(())
    }

    #[test]
    fn test_snapshot_ordering() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
        fs::create_dir_all(&fixture_base)?;
        let fixture_path = tempfile::tempdir_in(&fixture_base)?;
        let ccc: PathBuf = fixture_path.path().join("ccc").join("ccc.txt");
        let mmm: PathBuf = fixture_path.path().join("mmm").join("mmm.txt");
        let yyy: PathBuf = fixture_path.path().join("yyy").join("yyy.txt");
        fs::create_dir(ccc.parent().unwrap())?;
        fs::create_dir(mmm.parent().unwrap())?;
        fs::create_dir(yyy.parent().unwrap())?;
        fs::write(&ccc, b"crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs")?;
        fs::write(&mmm, b"morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins")?;
        fs::write(&yyy, b"yellow yak yodeling, yellow yak yodeling, yellow yak yodeling, yellow yak yodeling, yellow yak yodeling")?;
        // take a snapshot of the test data
        let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
        let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
        assert_eq!(snapshot1.file_counts.total_files(), 3);
        // add new files, change one file
        let bbb: PathBuf = fixture_path.path().join("bbb").join("bbb.txt");
        let nnn: PathBuf = fixture_path.path().join("nnn").join("nnn.txt");
        let zzz: PathBuf = fixture_path.path().join("zzz").join("zzz.txt");
        fs::create_dir(bbb.parent().unwrap())?;
        fs::create_dir(nnn.parent().unwrap())?;
        fs::create_dir(zzz.parent().unwrap())?;
        fs::write(&bbb, b"blue baboons bouncing balls, blue baboons bouncing balls, blue baboons bouncing balls, blue baboons bouncing balls")?;
        fs::write(&mmm, b"many mumbling mice moonlight, many mumbling mice moonlight, many mumbling mice moonlight, many mumbling mice moonlight")?;
        fs::write(&nnn, b"neat newts gnawing noodles, neat newts gnawing noodles, neat newts gnawing noodles, neat newts gnawing noodles")?;
        fs::write(&zzz, b"zebras riding on a zephyr, zebras riding on a zephyr, zebras riding on a zephyr, zebras riding on a zephyr")?;
        let snap2_sha =
            take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
        // compute the differences
        let iter = find_changed_files(
            &dbase,
            fixture_path.path().to_path_buf(),
            snap1_sha,
            snap2_sha.clone(),
        )?;
        let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
        assert_eq!(changed.len(), 4);
        // The changed mmm/mmm.txt file ends up last because its tree was changed
        // and is pushed onto the queue, while the new entries are processed
        // immediately before returning to the queue.
        assert_eq!(changed[0].as_ref().unwrap().path, bbb);
        assert_eq!(changed[1].as_ref().unwrap().path, nnn);
        assert_eq!(changed[2].as_ref().unwrap().path, zzz);
        assert_eq!(changed[3].as_ref().unwrap().path, mmm);
        // remove some files, change another
        fs::remove_file(&bbb)?;
        fs::remove_file(&yyy)?;
        fs::write(&zzz, b"zippy zip ties zooming, zippy zip ties zooming, zippy zip ties zooming, zippy zip ties zooming")?;
        let snap3_sha =
            take_snapshot(fixture_path.path(), Some(snap2_sha.clone()), &dbase, vec![])?.unwrap();
        // compute the differences
        let iter = find_changed_files(
            &dbase,
            fixture_path.path().to_path_buf(),
            snap2_sha,
            snap3_sha,
        )?;
        let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].as_ref().unwrap().path, zzz);
        Ok(())
    }

    #[test]
    fn test_snapshot_types() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
        fs::create_dir_all(&fixture_base)?;
        let fixture_path = tempfile::tempdir_in(&fixture_base)?;
        let ccc: PathBuf = fixture_path.path().join("ccc");
        let mmm: PathBuf = fixture_path.path().join("mmm").join("mmm.txt");
        fs::create_dir(mmm.parent().unwrap())?;
        fs::write(&ccc, b"crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs")?;
        fs::write(&mmm, b"morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins")?;
        // take a snapshot of the test data
        let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
        let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
        assert_eq!(snapshot1.file_counts.total_files(), 2);
        // change files to dirs and vice versa
        fs::remove_file(&ccc)?;
        let ccc: PathBuf = fixture_path.path().join("ccc").join("ccc.txt");
        let mmm: PathBuf = fixture_path.path().join("mmm");
        fs::create_dir(ccc.parent().unwrap())?;
        fs::remove_dir_all(&mmm)?;
        fs::write(&ccc, b"catastrophic catastrophes, catastrophic catastrophes, catastrophic catastrophes, catastrophic catastrophes")?;
        fs::write(&mmm, b"many mumbling mice moonlight, many mumbling mice moonlight, many mumbling mice moonlight, many mumbling mice moonlight")?;
        let snap2_sha =
            take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
        // compute the differences
        let iter = find_changed_files(
            &dbase,
            fixture_path.path().to_path_buf(),
            snap1_sha,
            snap2_sha,
        )?;
        let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
        assert_eq!(changed.len(), 2);
        assert_eq!(changed[0].as_ref().unwrap().path, ccc);
        assert_eq!(changed[1].as_ref().unwrap().path, mmm);
        Ok(())
    }

    #[test]
    fn test_snapshot_ignore_links() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
        fs::create_dir_all(&fixture_base)?;
        let fixture_path = tempfile::tempdir_in(&fixture_base)?;
        let bbb: PathBuf = fixture_path.path().join("bbb");
        let ccc: PathBuf = fixture_path.path().join("ccc").join("ccc.txt");
        fs::create_dir(ccc.parent().unwrap())?;
        fs::write(&bbb, b"bored baby baboons bathing, bored baby baboons bathing, bored baby baboons bathing, bored baby baboons bathing")?;
        fs::write(&ccc, b"crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs")?;
        // take a snapshot of the test data
        let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
        let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
        assert_eq!(snapshot1.file_counts.total_files(), 2);
        // replace the files and directories with links
        let mmm: PathBuf = fixture_path.path().join("mmm.txt");
        fs::write(&mmm, b"morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins")?;
        fs::remove_file(&bbb)?;
        fs::remove_dir_all(ccc.parent().unwrap())?;
        let ccc: PathBuf = fixture_path.path().join("ccc");
        // cfg! macro only works if all paths can compile on every platform
        {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs;
            #[cfg(target_family = "windows")]
            use std::os::windows::fs;
            #[cfg(target_family = "unix")]
            fs::symlink("mmm.txt", &bbb)?;
            #[cfg(target_family = "unix")]
            fs::symlink("mmm.txt", &ccc)?;
            #[cfg(target_family = "windows")]
            fs::symlink_file("mmm.txt", &bbb)?;
            #[cfg(target_family = "windows")]
            fs::symlink_file("mmm.txt", &ccc)?;
        }
        let snap2_sha =
            take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
        // compute the differences
        let iter = find_changed_files(
            &dbase,
            fixture_path.path().to_path_buf(),
            snap1_sha,
            snap2_sha,
        )?;
        let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].as_ref().unwrap().path, mmm);
        Ok(())
    }

    #[test]
    fn test_snapshot_was_links() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
        fs::create_dir_all(&fixture_base)?;
        let fixture_path = tempfile::tempdir_in(&fixture_base)?;
        let mmm: PathBuf = fixture_path.path().join("mmm.txt");
        fs::write(&mmm, b"morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins")?;
        let bbb: PathBuf = fixture_path.path().join("bbb");
        let ccc: PathBuf = fixture_path.path().join("ccc");
        // cfg! macro only works if all paths can compile on every platform
        {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs;
            #[cfg(target_family = "windows")]
            use std::os::windows::fs;
            #[cfg(target_family = "unix")]
            fs::symlink("mmm.txt", &bbb)?;
            #[cfg(target_family = "unix")]
            fs::symlink("mmm.txt", &ccc)?;
            #[cfg(target_family = "windows")]
            fs::symlink_file("mmm.txt", &bbb)?;
            #[cfg(target_family = "windows")]
            fs::symlink_file("mmm.txt", &ccc)?;
        }
        // take a snapshot of the test data
        let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
        let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
        assert_eq!(snapshot1.file_counts.total_files(), 1);
        // replace the links with files and directories
        fs::remove_file(&bbb)?;
        fs::write(&bbb, b"bored baby baboons bathing, bored baby baboons bathing, bored baby baboons bathing, bored baby baboons bathing")?;
        fs::remove_file(&ccc)?;
        let ccc: PathBuf = fixture_path.path().join("ccc").join("ccc.txt");
        fs::create_dir(ccc.parent().unwrap())?;
        fs::write(&ccc, b"crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs")?;
        let snap2_sha =
            take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
        // compute the differences
        let iter = find_changed_files(
            &dbase,
            fixture_path.path().to_path_buf(),
            snap1_sha,
            snap2_sha,
        )?;
        let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
        assert_eq!(changed.len(), 2);
        assert_eq!(changed[0].as_ref().unwrap().path, bbb);
        assert_eq!(changed[1].as_ref().unwrap().path, ccc);
        Ok(())
    }

    #[test]
    fn test_calc_chunk_size() {
        assert_eq!(calc_chunk_size(65_536), 16_384);
        assert_eq!(calc_chunk_size(131_072), 32_768);
        assert_eq!(calc_chunk_size(262_144), 65_536);
        assert_eq!(calc_chunk_size(16_777_216), 4_194_304);
        assert_eq!(calc_chunk_size(33_554_432), 4_194_304);
        assert_eq!(calc_chunk_size(134_217_728), 4_194_304);
    }

    #[test]
    fn test_pack_record_verify_pack() -> Result<(), Error> {
        let mut record: PackRecord = Default::default();
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let chunks = super::packs::find_file_chunks(infile, 16384)?;
        let mut builder = packs::PackBuilder::new(1048576).password("secret123");
        let outdir = tempdir()?;
        let packfile = outdir.path().join("multi-pack.pack");
        builder.initialize(&packfile)?;
        for chunk in chunks.iter() {
            if builder.add_chunk(chunk)? {
                break;
            }
            record.add_chunk(chunk.to_owned());
        }
        let _ = builder.finalize()?;
        let result = record.verify_pack(&packfile, "secret123")?;
        assert!(result);

        // inject a "missing" chunk into record, should return false
        let chunk = entities::Chunk::new(
            entities::Checksum::BLAKE3(
                "7b5352a6d7116e70b420c6e2f5ad3b49ba0af92923ab53ee43bd3fd0374d2521".to_owned(),
            ),
            0,
            11364,
        );
        record.chunks.push(chunk);
        let result = record.verify_pack(&packfile, "secret123")?;
        assert!(!result);

        // remove one of the chunks from record, should raise an error
        record.chunks.pop();
        record.chunks.pop();
        let result = record.verify_pack(&packfile, "secret123");
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("unexpected chunk"));
        Ok(())
    }

    fn download_and_verify_pack(
        pack_rec: &entities::Pack,
        chunks: &[entities::Checksum],
        workspace: &Path,
        passphrase: &str,
        stores: &Arc<dyn PackRepository>,
    ) -> Result<bool, Error> {
        use std::str::FromStr;
        // retrieve the pack file
        let mut archive = PathBuf::new();
        archive.push(workspace);
        archive.push(pack_rec.digest.to_string());
        stores.retrieve_pack(&pack_rec.locations, &archive)?;
        // unpack the contents
        let mut reader = exaf_rs::reader::Entries::new(&archive)?;
        reader.enable_encryption(passphrase)?;
        let mut found_count: usize = 0;
        for maybe_entry in reader {
            let entry = maybe_entry?;
            let digest = entities::Checksum::from_str(entry.name())?;
            let mut found = false;
            for chunk in chunks {
                if chunk == &digest {
                    found = true;
                    found_count += 1;
                    break;
                }
            }
            if !found {
                // this is wrong for an entirely different reason
                return Err(anyhow!(
                    "unexpected chunk {} found in pack {}",
                    digest,
                    archive.display()
                ));
            }
        }
        // ensure we found all of the chunks
        fs::remove_file(archive)?;
        Ok(found_count == chunks.len())
    }

    #[test]
    fn test_backup_driver_small_file_finishes_pack() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        // set up local pack store
        let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
        fs::create_dir_all(&pack_base)?;
        let pack_path = tempfile::tempdir_in(&pack_base)?;
        let mut local_props: HashMap<String, String> = HashMap::new();
        local_props.insert(
            "basepath".to_owned(),
            pack_path.keep().to_string_lossy().into(),
        );
        let store = entities::Store {
            id: "local123".to_owned(),
            store_type: entities::StoreType::LOCAL,
            label: "my local".to_owned(),
            properties: local_props,
            retention: PackRetention::ALL,
        };
        dbase.put_store(&store)?;

        // create a dataset
        let fixture_base: PathBuf = ["test", "fixtures"].iter().collect();
        let mut dataset = entities::Dataset::new(&fixture_base);
        dataset.add_store("local123");
        dataset.pack_size = 131072;
        fs::create_dir_all(&dataset.workspace)?;
        let workspace: PathBuf = ["tmp", "test", "workspace"].iter().collect();
        fs::create_dir_all(&workspace)?;
        let stores = Arc::from(dbase.load_dataset_stores(&dataset)?);

        //
        // Create the driver and add two files such that the second one will
        // cause the pack being built to reach capacity; note that these two
        // files are sized perfectly to fill the pack without causing a split
        // and resulting in two packs being built.
        //
        // Then continue by adding one more small file and ensuring that it also
        // is recorded properly in the database.
        //
        // This test case exposed two related bugs with respect to packing and
        // recording the chunk and file records.
        //
        let stopper = Arc::new(RwLock::new(false));
        let dataset_id = dataset.id.clone();
        let request = super::Request::new(dataset_id, "secret123", None);
        let subscriber = Arc::new(DummySubscriber());
        let mut driver =
            BackupDriver::new(request, dataset.clone(), dbase.clone(), subscriber, stopper)?;
        let file1_digest = Checksum::BLAKE3(
            "dba425aa7292ef1209841ab3855a93d4dfa6855658a347f85c502f2c2208cf0f".to_owned(),
        );
        let changed_file = ChangedFile::new(
            Path::new("../test/fixtures/SekienAkashita.jpg"),
            file1_digest.clone(),
        );
        driver.add_file(changed_file)?;
        let file2_digest = Checksum::BLAKE3(
            "2cd10c8eafa9bb6562eae34758bd7bcffd840afb1c503b42d0659b0718cafe99".to_owned(),
        );
        let changed_file = ChangedFile::new(
            Path::new("../test/fixtures/baby-birth.jpg"),
            file2_digest.clone(),
        );
        driver.add_file(changed_file)?;
        let file3_digest = Checksum::BLAKE3(
            "540c45803112958ab53e31daee5eec067b1442d579eb1e787cf7684657275b60".to_owned(),
        );
        let changed_file = ChangedFile::new(
            Path::new("../test/fixtures/washington-journal.txt"),
            file3_digest.clone(),
        );
        driver.add_file(changed_file)?;
        driver.finish_remainder()?;

        // verify that the first file record exists, and its chunks, and that
        // the chunks are both stored in the same pack
        let maybe_file = dbase.get_file(&file1_digest)?;
        assert!(maybe_file.is_some());
        let file_rec = maybe_file.unwrap();
        assert_eq!(file_rec.length, 109466);
        assert_eq!(file_rec.chunks.len(), 2);
        let chunk_rec = dbase
            .get_chunk(&file_rec.chunks[0].1)?
            .ok_or_else(|| anyhow!("missing chunk 1 of 2"))?;
        assert!(chunk_rec.packfile.is_some());
        let pack_digest = chunk_rec.packfile.clone().unwrap();
        let chunk_rec2 = dbase
            .get_chunk(&file_rec.chunks[1].1)?
            .ok_or_else(|| anyhow!("missing chunk 2 of 2"))?;
        assert_eq!(chunk_rec.packfile, chunk_rec2.packfile);

        // verify that the second file record exists, and its "chunk", which is
        // actually a pack digest, and that it matches the first pack
        let maybe_file = dbase.get_file(&file2_digest)?;
        assert!(maybe_file.is_some());
        let file_rec = maybe_file.unwrap();
        assert_eq!(file_rec.length, 31399);
        assert_eq!(file_rec.chunks.len(), 1);
        let maybe_pack = dbase.get_pack(&file_rec.chunks[0].1)?;
        assert!(maybe_pack.is_some());
        let pack_rec = maybe_pack.unwrap();
        assert_eq!(pack_rec.digest, pack_digest);

        // verify that the pack file actually contains the expected chunks
        let chunks: Vec<entities::Checksum> = vec![
            // first file is split into two chunks
            Checksum::BLAKE3(
                "c3a9c101999bcd14212cbac34a78a5018c6d1548a32c084f43499c254adf07ef".to_owned(),
            ),
            Checksum::BLAKE3(
                "4b5f350ca573fc4f44b0da18d6aef9cdb2bcb7eeab1ad371af82557d0f353454".to_owned(),
            ),
            // second file is a single chunk
            Checksum::BLAKE3(
                "2cd10c8eafa9bb6562eae34758bd7bcffd840afb1c503b42d0659b0718cafe99".to_owned(),
            ),
        ];
        assert!(download_and_verify_pack(
            &pack_rec,
            &chunks,
            &workspace,
            "secret123",
            &stores
        )?);

        // verify that the third file record exists, and its "chunk", which is
        // actually a pack digest, and that it does not match the first pack
        let maybe_file = dbase.get_file(&file3_digest)?;
        assert!(maybe_file.is_some());
        let file_rec = maybe_file.unwrap();
        #[cfg(target_family = "unix")]
        assert_eq!(file_rec.length, 3375);
        #[cfg(target_family = "windows")]
        assert_eq!(file_rec.length, 3428);
        assert_eq!(file_rec.chunks.len(), 1);
        let maybe_pack = dbase.get_pack(&file_rec.chunks[0].1)?;
        assert!(maybe_pack.is_some());
        let pack_rec = maybe_pack.unwrap();
        assert_ne!(pack_rec.digest, pack_digest);

        // verify that the pack file actually contains the expected chunk(s)
        let chunks: Vec<entities::Checksum> = vec![Checksum::BLAKE3(
            "540c45803112958ab53e31daee5eec067b1442d579eb1e787cf7684657275b60".to_owned(),
        )];
        let stores = Arc::from(dbase.load_dataset_stores(&dataset)?);
        assert!(download_and_verify_pack(
            &pack_rec,
            &chunks,
            &workspace,
            "secret123",
            &stores
        )?);

        Ok(())
    }

    #[test]
    fn test_backup_driver_large_file_multiple_packs() -> Result<(), Error> {
        let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
        fs::create_dir_all(&db_base)?;
        let db_path = tempfile::tempdir_in(&db_base)?;
        let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
        let repo = RecordRepositoryImpl::new(Arc::new(datasource));
        let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

        // set up local pack store
        let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
        fs::create_dir_all(&pack_base)?;
        let pack_path = tempfile::tempdir_in(&pack_base)?;
        let mut local_props: HashMap<String, String> = HashMap::new();
        local_props.insert(
            "basepath".to_owned(),
            pack_path.keep().to_string_lossy().into(),
        );
        let store = entities::Store {
            id: "local123".to_owned(),
            store_type: entities::StoreType::LOCAL,
            label: "my local".to_owned(),
            properties: local_props,
            retention: PackRetention::ALL,
        };
        dbase.put_store(&store)?;

        // create a dataset
        let fixture_base: PathBuf = ["test", "fixtures"].iter().collect();
        let mut dataset = entities::Dataset::new(&fixture_base);
        dataset.add_store("local123");
        dataset.pack_size = 582540;
        fs::create_dir_all(&dataset.workspace)?;
        let workspace: PathBuf = ["tmp", "test", "workspace"].iter().collect();
        fs::create_dir_all(&workspace)?;
        let stores: Arc<dyn PackRepository> = Arc::from(dbase.load_dataset_stores(&dataset)?);

        //
        // Create the driver and add the one large file that should result in
        // two pack files and seven chunks being generated.
        //
        let stopper = Arc::new(RwLock::new(false));
        let dataset_id = dataset.id.clone();
        let request = super::Request::new(dataset_id, "secret123", None);
        let subscriber = Arc::new(DummySubscriber());
        let mut driver =
            BackupDriver::new(request, dataset.clone(), dbase.clone(), subscriber, stopper)?;
        let file1_digest = Checksum::BLAKE3(
            "b740be03e10f454b6f45acdc821822b455aa4ab3721bbe8e3f03923f5cd688b8".to_owned(),
        );
        let changed_file = ChangedFile::new(
            Path::new("../test/fixtures/C++98-tutorial.pdf"),
            file1_digest.clone(),
        );
        driver.add_file(changed_file)?;
        driver.finish_remainder()?;

        // verify information about the file, collect unique pack digests
        let maybe_file = dbase.get_file(&file1_digest)?;
        assert!(maybe_file.is_some());
        let file_rec = maybe_file.unwrap();
        assert_eq!(file_rec.length, 1272254);
        assert_eq!(file_rec.chunks.len(), 7);
        // need the pack digests in the correct order since the checksums will
        // change when encryption is enabled
        let mut pack_digests: Vec<Checksum> = vec![];
        for (_, checksum) in file_rec.chunks.iter() {
            let chunk_rec = dbase
                .get_chunk(checksum)?
                .ok_or_else(|| anyhow!("missing chunk {}", checksum))?;
            assert!(chunk_rec.packfile.is_some());
            let pack_digest = chunk_rec.packfile.clone().unwrap();
            if !pack_digests.contains(&pack_digest) {
                pack_digests.push(pack_digest);
            }
        }

        // verify that there are two packs and their records exist
        assert_eq!(pack_digests.len(), 2);
        for pack_digest in pack_digests.iter() {
            let maybe_pack = dbase.get_pack(pack_digest)?;
            assert!(maybe_pack.is_some());
        }

        // verify the contents of the first pack file
        let pack_rec = dbase.get_pack(&pack_digests[0])?.unwrap();
        let chunks: Vec<entities::Checksum> = vec![
            Checksum::BLAKE3(
                "0480af365eef43f62ce523bbc027018594fc58f60ef83373c0747833c5a76a34".to_owned(),
            ),
            Checksum::BLAKE3(
                "0652fd2632ffff1dae524121485d0f36a538eaaff0873091a827f88e1e87e532".to_owned(),
            ),
            Checksum::BLAKE3(
                "fcc513b817b91c5a65dff05977b7efacf4f7b3c66ab3d1148c4d6fda8657901e".to_owned(),
            ),
        ];
        assert!(download_and_verify_pack(
            &pack_rec,
            &chunks,
            &workspace,
            "secret123",
            &stores
        )?);

        // verify the contents of the second pack file
        let pack_rec = dbase.get_pack(&pack_digests[1])?.unwrap();
        let chunks: Vec<entities::Checksum> = vec![
            Checksum::BLAKE3(
                "84ffcbd58ba181caa30ee1c22025f3c5a3a0d0572570d8e19573ed2b20459bba".to_owned(),
            ),
            Checksum::BLAKE3(
                "b71e6d19e69fc78ca8f09cc789e52517ee328b6f84ec0587a5aa02437c6d7b0c".to_owned(),
            ),
            Checksum::BLAKE3(
                "676fc9716d83f0c279d7aa45193459f2671cc39c12e466b0122dd565ab260bfb".to_owned(),
            ),
            Checksum::BLAKE3(
                "7ca63166ddd184501ece9a84adf9b5d6d1193bdc5343006bbe23e2a3da1694f9".to_owned(),
            ),
        ];
        assert!(download_and_verify_pack(
            &pack_rec,
            &chunks,
            &workspace,
            "secret123",
            &stores
        )?);

        Ok(())
    }
}
