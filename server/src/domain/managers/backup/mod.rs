//
// Copyright (c) 2023 Nathan Fiedler
//

//! Defines the `Performer` trait and implementation that perform backups by
//! taking a snapshot of a dataset, storing new entries in the database, finding
//! the differences from the previous snapshot, building pack files, and sending
//! them to the store.

use crate::domain::entities;
use crate::domain::helpers::thread_pool::ThreadPool;
use crate::domain::managers::state::{BackupAction, StateStore};
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Context, Error};
use chrono::{DateTime, Utc};
use globset::{Glob, GlobSet, GlobSetBuilder};
use log::{debug, error, info, trace, warn};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::SystemTime;

mod driver;
pub mod scheduler;
pub use scheduler::{Scheduler, SchedulerImpl};

///
/// Request to backup a specific dataset.
///
#[derive(Clone)]
pub struct Request {
    /// Dataset to be backed up.
    dataset: entities::Dataset,
    /// Reference to the record repository.
    repo: Arc<dyn RecordRepository>,
    /// Reference to the shared application state.
    state: Arc<dyn StateStore>,
    /// Passphrase used to generate a secret key to encrypt pack files.
    passphrase: String,
    /// Optional time at which to stop the backup, albeit temporarily.
    stop_time: Option<DateTime<Utc>>,
}

impl Request {
    /// Construct a new instance of Request.
    pub fn new<T: Into<String>>(
        dataset: entities::Dataset,
        repo: Arc<dyn RecordRepository>,
        state: Arc<dyn StateStore>,
        passphrase: T,
        stop_time: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            dataset,
            repo,
            state,
            passphrase: passphrase.into(),
            stop_time,
        }
    }
}

///
/// Performs the actual backup of a dataset by scanning the directory for new or
/// changed files, creating packs, uploading them, and updating the database.
///
#[cfg_attr(test, automock)]
pub trait Performer: Send + Sync {
    /// Perform a backup immediately on the current thread.
    fn backup(&self, request: Request) -> Result<Option<entities::Checksum>, Error>;
}

///
/// A simple concrete implementation of Performer that processes backups linearly on
/// the current thread, allowing for easier management of the database locks
/// when performing a full database restore.
///
#[derive(Default)]
pub struct PerformerImpl();

impl Performer for PerformerImpl {
    fn backup(&self, request: Request) -> Result<Option<entities::Checksum>, Error> {
        if let Some(time) = &request.stop_time {
            debug!("backup: starting for {} until {}", request.dataset, time);
        } else {
            debug!("backup: starting for {} until completion", request.dataset);
        }
        fs::create_dir_all(&request.dataset.workspace).with_context(|| {
            format!(
                "backup fs::create_dir_all({})",
                request.dataset.workspace.display()
            )
        })?;
        // Check if latest snapshot exists and lacks an end time, which indicates
        // that the previous backup did not complete successfully.
        let latest_snapshot = request.repo.get_latest_snapshot(&request.dataset.id)?;
        if let Some(latest) = latest_snapshot.as_ref() {
            if let Some(snapshot) = request.repo.get_snapshot(latest)? {
                if snapshot.end_time.is_none() {
                    // continue from the previous incomplete backup
                    let parent_sha1 = snapshot.parent;
                    let current_sha1 = latest.to_owned();
                    debug!("backup: continuing previous snapshot {}", &current_sha1);
                    return continue_backup(
                        &request.dataset,
                        &request.repo,
                        &request.state,
                        &request.passphrase,
                        parent_sha1,
                        current_sha1,
                        request.stop_time,
                    );
                }
            }
        }
        // The start time of a new backup is at the moment that a snapshot is to be
        // taken. The snapshot can take a long time to build, and another thread may
        // spawn in the mean time and start taking another snapshot, and again, and
        // again until the system runs out of resources.
        request
            .state
            .backup_event(BackupAction::Start(request.dataset.id.clone()));
        // In addition to the exclusions defined in the dataset, we exclude the
        // temporary workspace and repository database files.
        let mut excludes = request.repo.get_excludes();
        excludes.push(request.dataset.workspace.clone());
        for exclusion in request.dataset.excludes.iter() {
            excludes.push(PathBuf::from(exclusion));
        }
        debug!("backup: dataset exclusions: {:?}", excludes);
        // Take a snapshot and record it as the new most recent snapshot for this
        // dataset, to allow detecting a running backup, and thus recover from a
        // crash or forced shutdown.
        let snap_opt = take_snapshot(
            &request.dataset.basepath,
            latest_snapshot.clone(),
            &request.repo,
            excludes,
        )?;
        match snap_opt {
            None => {
                // indicate that the backup has finished (doing nothing)
                request
                    .state
                    .backup_event(BackupAction::Finish(request.dataset.id.clone()));
                Ok(None)
            }
            Some(current_sha1) => {
                request
                    .repo
                    .put_latest_snapshot(&request.dataset.id, &current_sha1)?;
                debug!("backup: starting new snapshot {}", &current_sha1);
                continue_backup(
                    &request.dataset,
                    &request.repo,
                    &request.state,
                    &request.passphrase,
                    latest_snapshot,
                    current_sha1,
                    request.stop_time,
                )
            }
        }
    }
}

///
/// Continue the backup for the most recent snapshot, comparing against the
/// parent snapshot, if any.
///
fn continue_backup(
    dataset: &entities::Dataset,
    repo: &Arc<dyn RecordRepository>,
    state: &Arc<dyn StateStore>,
    passphrase: &str,
    parent_sha1: Option<entities::Checksum>,
    current_sha1: entities::Checksum,
    stop_time: Option<DateTime<Utc>>,
) -> Result<Option<entities::Checksum>, Error> {
    let mut driver = driver::BackupDriver::new(dataset, repo, state, passphrase, stop_time)?;
    // if no previous snapshot, visit every file in the new snapshot, otherwise
    // find those files that changed from the previous snapshot
    match parent_sha1 {
        None => {
            let snapshot = repo
                .get_snapshot(&current_sha1)?
                .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", current_sha1)))?;
            let tree = snapshot.tree;
            let iter = TreeWalker::new(repo, &dataset.basepath, tree);
            for result in iter {
                driver.add_file(result?)?;
            }
        }
        Some(ref parent) => {
            let iter = find_changed_files(
                repo,
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
    let pretty_time = super::pretty_print_duration(time_diff);
    let mut snap = entities::Snapshot::new(parent, tree.digest.clone(), file_counts);
    info!(
        "took snapshot {} with {} files after {}",
        snap.digest, tree.file_count, pretty_time
    );
    snap = snap.start_time(actual_start_time);
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
            if self.left_tree.is_some() && self.right_tree.is_some() {
                let left_tree = self.left_tree.as_ref().unwrap();
                let right_tree = self.right_tree.as_ref().unwrap();
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
) -> Result<ChangedFilesIter, Error> {
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
    if let Ok(set) = builder.build() {
        set
    } else {
        GlobSet::empty()
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
            let entry = match entities::Checksum::sha256_from_file(&path) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::repositories::RecordRepositoryImpl;
    use crate::data::sources::EntityDataSourceImpl;
    use crate::domain::entities::{self, Checksum};
    use crate::domain::repositories::MockRecordRepository;
    use crate::domain::repositories::RecordRepository;
    use anyhow::Error;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_process_path() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_insert_xattr().returning(|_, _| Ok(()));
        // act
        let path = Path::new("../test/fixtures/washington-journal.txt");
        let digest = entities::Checksum::sha256_from_file(&path).unwrap();
        let tref = entities::TreeReference::FILE(digest);
        let dbase: Arc<(dyn crate::domain::repositories::RecordRepository + 'static)> =
            Arc::new(mock);
        let entry = process_path(&path, tref, &dbase);
        // assert
        assert_eq!(entry.name, "washington-journal.txt");
        #[cfg(target_family = "unix")]
        let expected_hash = "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05";
        #[cfg(target_family = "windows")]
        let expected_hash = "494cb077670d424f47a3d33929d6f1cbcf408a06d28be11259b2fe90666010dc";
        let expected =
            entities::TreeReference::FILE(entities::Checksum::SHA256(expected_hash.into()));
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
        let dbase: Arc<(dyn crate::domain::repositories::RecordRepository + 'static)> =
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
        let excludes = vec![
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
        let excludes = vec![
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
        std::os::unix::fs::symlink(&target, &link)?;
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
            Checksum::SHA256(String::from(
                "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
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

        let mut excludes: Vec<PathBuf> = Vec::new();
        // individual files
        excludes.push(PathBuf::from("*.exe"));
        // entire directory structure by name (while technically different than the
        // pattern that ends with /**, it has the same effect for our purposes since
        // we ignore directories)
        excludes.push(PathBuf::from("**/node_modules"));
        // entire directory structure by name based at the root only
        excludes.push(PathBuf::from("workspace"));
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
            fs::symlink(&target, &dest)?;
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
}
