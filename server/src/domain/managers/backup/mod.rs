//
// Copyright (c) 2022 Nathan Fiedler
//

//! The manager for performing backups by taking a snapshot of a dataset,
//! storing new entries in the database, finding the differences from the
//! previous snapshot, building pack files, and sending them to the store.

use crate::domain::entities;
use crate::domain::managers::state::{BackupAction, StateStore};
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use globset::{Glob, GlobSet, GlobSetBuilder};
use log::{debug, error, info, trace, warn};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
pub struct PerformerImpl();

impl PerformerImpl {
    /// Construct an instance of PerformerImpl.
    pub fn new() -> Self {
        Self()
    }
}

impl Performer for PerformerImpl {
    fn backup(&self, request: Request) -> Result<Option<entities::Checksum>, Error> {
        if let Some(time) = &request.stop_time {
            debug!("backup: starting for {} until {}", request.dataset, time);
        } else {
            debug!("backup: starting for {} until completion", request.dataset);
        }
        fs::create_dir_all(&request.dataset.workspace)?;
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
pub fn take_snapshot(
    basepath: &Path,
    parent: Option<entities::Checksum>,
    dbase: &Arc<dyn RecordRepository>,
    excludes: Vec<PathBuf>,
) -> Result<Option<entities::Checksum>, Error> {
    let start_time = SystemTime::now();
    let actual_start_time = Utc::now();
    let exclusions = build_exclusions(basepath, &excludes);
    let mut file_counts: entities::FileCounts = Default::default();
    let tree = scan_tree(basepath, dbase, &exclusions, &mut file_counts)?;
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
pub fn find_changed_files(
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
    pub fn new(
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
                } else if pattern.ends_with("**") {
                    // prepend basepath
                    groomed.push([basepath_str, pattern].iter().collect());
                } else if pattern.starts_with("*") {
                    // prepend basepath
                    groomed.push([basepath_str, pattern].iter().collect());
                } else if pattern.len() > 0 {
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
                if let Ok(glob) = Glob::new(&pattern) {
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
    #[cfg(target_family = "unix")]
    use std::os::unix::ffi::OsStringExt;
    #[cfg(target_family = "windows")]
    use std::os::windows::ffi::OsStringExt;
    let value = fs::read_link(path)?;
    Ok(value.into_os_string().into_vec())
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
) -> Result<entities::Tree, Error> {
    let mut entries: Vec<entities::TreeEntry> = Vec::new();
    let mut file_count = 0;
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
                                    let scan = scan_tree(&path, dbase, excludes, file_counts)?;
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
                                        match entities::Checksum::sha256_from_file(&path) {
                                            Ok(digest) => {
                                                let tref = entities::TreeReference::FILE(digest);
                                                entries.push(process_path(&path, tref, dbase));
                                                file_count += 1;
                                            }
                                            Err(err) => {
                                                error!("could not read file: {:?}: {}", path, err)
                                            }
                                        }
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
    let tree = entities::Tree::new(entries, file_count);
    dbase.insert_tree(&tree)?;
    Ok(tree)
}

///
/// Create a `TreeEntry` record for this path, which may include storing
/// extended attributes in the database.
///
#[allow(unused_variables)]
fn process_path(
    fullpath: &Path,
    reference: entities::TreeReference,
    dbase: &Arc<dyn RecordRepository>,
) -> entities::TreeEntry {
    let mut entry = entities::TreeEntry::new(fullpath, reference);
    entry = entry.mode(fullpath);
    entry = entry.owners(fullpath);
    trace!("processed path entry {:?}", fullpath);
    #[cfg(target_family = "unix")]
    {
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
    entry
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
    #[cfg(target_family = "unix")]
    use std::os::unix::fs;
    #[cfg(target_family = "windows")]
    use std::os::windows::fs;
    use tempfile::tempdir;

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
        fs::symlink(&target, &link)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file(&target, &link)?;
        let actual = read_link(&link)?;
        assert_eq!(actual, target.as_bytes());
        Ok(())
    }
}
