//
// Copyright (c) 2020 Nathan Fiedler
//

//! The manager for performing backups by taking a snapshot of a dataset,
//! storing new entries in the database, finding the differences from the
//! previous snapshot, building pack files, and sending them to the store.

use crate::domain::entities;
use crate::domain::managers::state::{self, Action};
use crate::domain::repositories::{PackRepository, RecordRepository};
use chrono::Utc;
use failure::{err_msg, Error};
use log::{debug, error, info, trace};
use rusty_ulid::generate_ulid_string;
use sodiumoxide::crypto::pwhash::Salt;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
// use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Take a snapshot, collect the new/changed files, assemble them into pack
/// files, upload them to the pack repository, and record the results in the
/// repository. The snapshot and dataset are also updated. Returns the snapshot
/// checksum, or `None` if there were no changes.
pub fn perform_backup(
    dataset: &mut entities::Dataset,
    repo: &Box<dyn RecordRepository>,
    stores: &Box<dyn PackRepository>,
    passphrase: &str,
) -> Result<Option<entities::Checksum>, Error> {
    debug!("performing backup for {}", dataset);
    fs::create_dir_all(&dataset.workspace)?;
    // Check if latest snapshot exists and lacks an end time, which indicates
    // that the previous backup did not complete successfully.
    let latest_snapshot = repo.get_latest_snapshot(&dataset.id)?;
    if let Some(latest) = latest_snapshot.as_ref() {
        if let Some(snapshot) = repo.get_snapshot(latest)? {
            if snapshot.end_time.is_none() {
                // continue from the previous incomplete backup
                let parent_sha1 = snapshot.parent;
                let current_sha1 = latest.to_owned();
                debug!("continuing previous backup {}", &current_sha1);
                return continue_backup(
                    dataset,
                    repo,
                    stores,
                    passphrase,
                    parent_sha1,
                    current_sha1,
                );
            }
        }
    }
    // The start of a _new_ backup is at the moment that a snapshot is to be
    // taken. The snapshot can take a very long time to build, and another
    // thread may spawn in the mean time and start taking another snapshot, and
    // again, and again until the system runs out of resources.
    state::dispatch(Action::StartBackup(dataset.id.clone()));
    // Take a snapshot and record it as the new most recent snapshot for this
    // dataset, to allow detecting a running backup, and thus recover from a
    // crash or forced shutdown.
    //
    // For now, build a set of excludes for files we do not want to have in the
    // backup set, such as the database and temporary files.
    let mut excludes = repo.get_excludes();
    excludes.push(dataset.workspace.clone());
    let snap_opt = take_snapshot(&dataset.basepath, latest_snapshot.clone(), &repo, excludes)?;
    match snap_opt {
        None => {
            // indicate that the backup has finished (doing nothing)
            state::dispatch(Action::FinishBackup(dataset.id.clone()));
            Ok(None)
        }
        Some(current_sha1) => {
            repo.put_latest_snapshot(&dataset.id, &current_sha1)?;
            debug!("starting new backup {}", &current_sha1);
            continue_backup(
                dataset,
                repo,
                stores,
                passphrase,
                latest_snapshot,
                current_sha1,
            )
        }
    }
}

///
/// Continue the backup for the most recent snapshot, comparing against the
/// parent snapshot, if any.
///
fn continue_backup(
    dataset: &mut entities::Dataset,
    repo: &Box<dyn RecordRepository>,
    stores: &Box<dyn PackRepository>,
    passphrase: &str,
    parent_sha1: Option<entities::Checksum>,
    current_sha1: entities::Checksum,
) -> Result<Option<entities::Checksum>, Error> {
    let mut bmaster = BackupMaster::new(dataset, repo, stores, passphrase)?;
    // if no previous snapshot, visit every file in the new snapshot, otherwise
    // find those files that changed from the previous snapshot
    match parent_sha1 {
        None => {
            let snapshot = repo
                .get_snapshot(&current_sha1)?
                .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", current_sha1)))?;
            let tree = snapshot.tree;
            let iter = TreeWalker::new(repo, &dataset.basepath, tree);
            for result in iter {
                bmaster.handle_file(result)?;
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
                bmaster.handle_file(result)?;
            }
        }
    }
    // upload the remaining chunks in the pack builder
    bmaster.finish_pack()?;
    // commit everything to the database
    bmaster.update_snapshot(&current_sha1)?;
    bmaster.backup_database()?;
    Ok(Some(current_sha1))
}

// ///
// /// Raised when the backup has run out of time and must stop temporarily,
// /// resuming at a later time.
// ///
// #[derive(Fail, Debug)]
// pub struct OutOfTimeError;

// impl fmt::Display for OutOfTimeError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "ran out of time")
//     }
// }

///
/// Holds the state of the backup process to keep the code slim.
///
struct BackupMaster<'a> {
    dataset: &'a entities::Dataset,
    dbase: &'a Box<dyn RecordRepository>,
    builder: PackBuilder<'a>,
    passphrase: String,
    bucket_name: String,
    stores: &'a Box<dyn PackRepository>,
}

impl<'a> BackupMaster<'a> {
    /// Build a BackupMaster.
    fn new(
        dataset: &'a entities::Dataset,
        dbase: &'a Box<dyn RecordRepository>,
        stores: &'a Box<dyn PackRepository>,
        passphrase: &str,
    ) -> Result<Self, Error> {
        let computer_id = dbase.get_computer_id(&dataset.id)?.unwrap();
        let bucket_name = super::generate_bucket_name(&computer_id);
        let builder = PackBuilder::new(&dbase, dataset.pack_size);
        Ok(Self {
            dataset,
            dbase,
            builder,
            passphrase: passphrase.to_owned(),
            bucket_name,
            stores,
        })
    }

    /// Handle a single changed file, adding it to the pack, and possibly
    /// uploading one or more pack files as needed.
    fn handle_file(&mut self, changed: Result<ChangedFile, Error>) -> Result<(), Error> {
        let entry = changed?;
        // ignore files which already have records
        if self.dbase.get_file(&entry.digest)?.is_none() {
            if self
                .builder
                .add_file(&entry.path, entry.digest.clone())
                .is_err()
            {
                // file disappeared out from under us, record it as
                // having zero length; file restore will handle it
                // without any problem
                error!("file {:?} went missing during backup", entry.path);
                let file = entities::File::new(entry.digest, 0, vec![]);
                self.dbase.insert_file(&file)?;
            }
            // loop until pack builder is below desired size
            // (adding a very large file may require multiple packs)
            while self.builder.is_full() {
                self.send_one_pack()?;
            }
        }
        Ok(())
    }

    /// Build and send a single pack to the pack store. Record the results in
    /// the database for posterity.
    fn send_one_pack(&mut self) -> Result<(), Error> {
        let outfile = tempfile::Builder::new()
            .prefix("pack")
            .suffix(".bin")
            .tempfile_in(&self.dataset.workspace)?;
        let mut pack = self.builder.build_pack(outfile.path(), &self.passphrase)?;
        let pack_rec = self.dbase.get_pack(pack.digest.as_ref().unwrap())?;
        if pack_rec.is_none() {
            // new pack file, need to upload this and record to database
            let object_name = format!("{}", pack.digest.as_ref().unwrap());
            // capture and record the remote object name, in case it differs from
            // the name we generated ourselves; either value is expected to be
            // sufficiently unique for our purposes
            let locations =
                self.stores
                    .store_pack(outfile.path(), &self.bucket_name, &object_name)?;
            pack.record_completed_pack(self.dbase, locations)?;
            state::dispatch(Action::UploadPack(self.dataset.id.clone()));
        }
        let count = pack.record_completed_files(self.dbase)? as u64;
        state::dispatch(Action::UploadFiles(self.dataset.id.clone(), count));
        Ok(())
    }

    /// While the pack builder has chunks to pack, keep building pack files and
    /// uploading them to the store.
    fn finish_pack(&mut self) -> Result<(), Error> {
        while self.builder.has_chunks() {
            self.send_one_pack()?;
        }
        Ok(())
    }

    /// Update the current snapshot with the end time set to the current time.
    fn update_snapshot(&self, snap_sha1: &entities::Checksum) -> Result<(), Error> {
        let mut snapshot = self
            .dbase
            .get_snapshot(snap_sha1)?
            .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", snap_sha1)))?;
        snapshot = snapshot.end_time(Utc::now());
        self.dbase.put_snapshot(&snapshot)?;
        state::dispatch(Action::FinishBackup(self.dataset.id.clone()));
        Ok(())
    }

    /// Upload a compressed tarball of the database files to a special bucket.
    fn backup_database(&self) -> Result<(), Error> {
        // create a stable copy of the database
        let backup_path = self.dbase.create_backup(None)?;
        // use a ULID as the object name so they sort by time
        let object_name = generate_ulid_string();
        let mut tarball = self.dataset.workspace.clone();
        tarball.push(&object_name);
        super::create_tar(&backup_path, &tarball)?;
        // use a predictable bucket name so we can find it later
        let computer_id = self.dbase.get_computer_id(&self.dataset.id)?.unwrap();
        let bucket_name = super::computer_bucket_name(&computer_id);
        self.stores
            .store_pack(&tarball, &bucket_name, &object_name)?;
        fs::remove_file(tarball)?;
        Ok(())
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
    dbase: &Box<dyn RecordRepository>,
    excludes: Vec<PathBuf>,
) -> Result<Option<entities::Checksum>, Error> {
    let start_time = SystemTime::now();
    let tree = scan_tree(basepath, dbase, &excludes)?;
    if let Some(ref parent_sha1) = parent {
        let parent_doc = dbase
            .get_snapshot(parent_sha1)?
            .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", parent_sha1)))?;
        if parent_doc.tree == tree.digest {
            // nothing new at all with this snapshot
            return Ok(None);
        }
    }
    let end_time = SystemTime::now();
    let time_diff = end_time.duration_since(start_time);
    let pretty_time = super::pretty_print_duration(time_diff);
    let snap = entities::Snapshot::new(parent, tree.digest.clone(), tree.file_count);
    info!(
        "took snapshot {} with {} files after {}",
        snap.digest, tree.file_count, pretty_time
    );
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
    dbase: &'a Box<dyn RecordRepository>,
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
        dbase: &'a Box<dyn RecordRepository>,
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
                        if right_entry.fstype.is_dir() {
                            // a new tree: add every file contained therein
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let sum = right_entry.reference.checksum().unwrap();
                            self.walker = Some(TreeWalker::new(self.dbase, &path, sum));
                            // return to the main loop
                            break;
                        } else if right_entry.fstype.is_file() {
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
                        let left_is_dir = left_entry.fstype.is_dir();
                        let left_is_file = left_entry.fstype.is_file();
                        let left_is_link = left_entry.fstype.is_link();
                        let right_is_dir = right_entry.fstype.is_dir();
                        let right_is_file = right_entry.fstype.is_file();
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
                    if right_entry.fstype.is_dir() {
                        // a new tree: add every file contained therein
                        let mut path = PathBuf::from(base);
                        path.push(&right_entry.name);
                        let sum = right_entry.reference.checksum().unwrap();
                        self.walker = Some(TreeWalker::new(self.dbase, &path, sum));
                    } else if right_entry.fstype.is_file() {
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
                    return Some(Err(err_msg(format!("failed to get tree: {:?}", left_sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(err_msg(format!("missing tree: {:?}", left_sum))));
                }
                self.left_tree = opt;
                self.left_idx = 0;
                let result = self.dbase.get_tree(&right_sum);
                if result.is_err() {
                    return Some(Err(err_msg(format!("failed to get tree: {:?}", right_sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(err_msg(format!("missing tree: {:?}", right_sum))));
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
    dbase: &Box<dyn RecordRepository>,
    basepath: PathBuf,
    snapshot1: entities::Checksum,
    snapshot2: entities::Checksum,
) -> Result<ChangedFilesIter, Error> {
    let snap1doc = dbase
        .get_snapshot(&snapshot1)?
        .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", snapshot1)))?;
    let snap2doc = dbase
        .get_snapshot(&snapshot2)?
        .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", snapshot2)))?;
    Ok(ChangedFilesIter::new(
        dbase,
        basepath,
        snap1doc.tree,
        snap2doc.tree,
    ))
}

pub struct TreeWalker<'a> {
    /// Reference to Database for fetching records.
    dbase: &'a Box<dyn RecordRepository>,
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
        dbase: &'a Box<dyn RecordRepository>,
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
                    return Some(Err(err_msg(format!("failed to get tree: {:?}", sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(err_msg(format!("missing tree: {:?}", sum))));
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

///
/// Read the symbolic link value.
///
fn read_link(path: &Path) -> String {
    if let Ok(value) = fs::read_link(path) {
        if let Some(vstr) = value.to_str() {
            base64::encode(vstr)
        } else {
            let s: String = value.to_string_lossy().into_owned();
            base64::encode(&s)
        }
    } else {
        path.to_string_lossy().into_owned()
    }
}

///
/// Create a `Tree` for the given path, recursively descending into child
/// directories. Any new trees found, as identified by their hash digest, will
/// be inserted into the database. The same is true for any files found, and
/// their extended attributes. The return value itself will also be added to the
/// database. The result will be that everything new will have been added as new
/// records.
///
fn scan_tree(
    basepath: &Path,
    dbase: &Box<dyn RecordRepository>,
    excludes: &[PathBuf],
) -> Result<entities::Tree, Error> {
    let mut entries: Vec<entities::TreeEntry> = Vec::new();
    let mut file_count = 0;
    match fs::read_dir(basepath) {
        Ok(readdir) => {
            for entry_result in readdir {
                match entry_result {
                    Ok(entry) => {
                        let path = entry.path();
                        if is_excluded(&path, excludes) {
                            continue;
                        }
                        // DirEntry.metadata() does not follow symlinks
                        match entry.metadata() {
                            Ok(metadata) => {
                                let file_type = metadata.file_type();
                                if file_type.is_dir() {
                                    let scan = scan_tree(&path, dbase, excludes)?;
                                    file_count += scan.file_count;
                                    let digest = scan.digest.clone();
                                    let tref = entities::TreeReference::TREE(digest);
                                    let ent = process_path(&path, tref, dbase);
                                    entries.push(ent);
                                } else if file_type.is_symlink() {
                                    let link = read_link(&path);
                                    let tref = entities::TreeReference::LINK(link);
                                    let ent = process_path(&path, tref, dbase);
                                    entries.push(ent);
                                } else if file_type.is_file() {
                                    match entities::Checksum::sha256_from_file(&path) {
                                        Ok(digest) => {
                                            let tref = entities::TreeReference::FILE(digest);
                                            let ent = process_path(&path, tref, dbase);
                                            entries.push(ent);
                                            file_count += 1;
                                        }
                                        Err(err) => {
                                            error!("could not read file: {:?}: {}", path, err)
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
/// Indicate if the given path is excluded or not.
///
fn is_excluded(fullpath: &Path, excludes: &[PathBuf]) -> bool {
    for exclusion in excludes {
        if fullpath.starts_with(exclusion) {
            return true;
        }
    }
    false
}

///
/// Create a `TreeEntry` record for this path, which may include storing
/// extended attributes in the database.
///
#[allow(unused_variables)]
fn process_path(
    fullpath: &Path,
    reference: entities::TreeReference,
    dbase: &Box<dyn RecordRepository>,
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

// The default desired chunk size should be a little larger than the typical
// image file, and small enough that packs do not end up with a wide range
// of sizes due to large chunks.
const DEFAULT_CHUNK_SIZE: u64 = 4_194_304;

/// Compute the desired size for the chunks based on the pack size.
fn calc_chunk_size(pack_size: u64) -> u64 {
    // Use our default chunk size unless the desired pack size is so small that
    // the chunks would be a significant portion of the pack file.
    if pack_size < DEFAULT_CHUNK_SIZE * 4 {
        pack_size / 4
    } else {
        DEFAULT_CHUNK_SIZE
    }
}

/// Builds pack files by splitting incoming files into chunks.
pub struct PackBuilder<'a> {
    /// Reference to Database for fetching records.
    dbase: &'a Box<dyn RecordRepository>,
    /// Preferred size of pack files in bytes.
    pack_size: u64,
    /// Preferred size of chunks in bytes.
    chunk_size: u64,
    /// Map of file checksum to the chunks it contains that have not yet been
    /// uploaded in a pack file.
    file_chunks: BTreeMap<entities::Checksum, Vec<entities::Chunk>>,
    /// Those chunks that have been packed using this builder.
    packed_chunks: HashSet<entities::Checksum>,
    /// Those chunks that have been uploaded previously.
    done_chunks: HashSet<entities::Checksum>,
}

impl<'a> PackBuilder<'a> {
    /// Create a new builder with the desired size.
    pub fn new(dbase: &'a Box<dyn RecordRepository>, pack_size: u64) -> Self {
        let chunk_size = calc_chunk_size(pack_size);
        Self {
            dbase,
            pack_size,
            chunk_size,
            file_chunks: BTreeMap::new(),
            packed_chunks: HashSet::new(),
            done_chunks: HashSet::new(),
        }
    }

    /// For testing purposes.
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    /// Return number of files in this pack, primarily for testing.
    pub fn file_count(&self) -> usize {
        self.file_chunks.len()
    }

    /// Return number of chunks in this pack, primarily for testing. This does
    /// not consider done or packed chunks.
    pub fn chunk_count(&self) -> usize {
        let mut count: usize = 0;
        for chunks in self.file_chunks.values() {
            count += chunks.len();
        }
        count
    }

    /// Add the given file to this builder, splitting into chunks as necessary,
    /// and using the database to find duplicate chunks.
    pub fn add_file(&mut self, path: &Path, file_digest: entities::Checksum) -> Result<(), Error> {
        if self.file_chunks.contains_key(&file_digest) {
            // do not bother processing a file we have already seen; once the
            // files have been completely uploaded, we rely on the database to
            // detect duplicate chunks
            return Ok(());
        }
        let attr = fs::metadata(path)?;
        let file_size = attr.len();
        let chunks = if file_size > self.chunk_size {
            // split large files into chunks, add chunks to the list
            find_file_chunks(path, self.chunk_size)?
        } else {
            let mut chunk = entities::Chunk::new(file_digest.clone(), 0, file_size as usize);
            chunk = chunk.filepath(path);
            vec![chunk]
        };
        // find chunks that have already been recorded in the database
        chunks.iter().for_each(|chunk| {
            let result = self.dbase.get_chunk(&chunk.digest);
            if let Ok(value) = result {
                if value.is_some() {
                    self.done_chunks.insert(chunk.digest.clone());
                }
            }
        });
        // save the chunks under the digest of the file they came from to make
        // it easy to save everything to the database later
        self.file_chunks.insert(file_digest, chunks);
        Ok(())
    }

    /// Return true if this builder has chunks to pack.
    pub fn has_chunks(&self) -> bool {
        !self.file_chunks.is_empty()
    }

    /// If the builder has no files and chunks, then clear the cache of
    /// "packed" and "done" chunks to conserve space.
    pub fn clear_cache(&mut self) {
        if self.file_chunks.is_empty() {
            self.packed_chunks.clear();
            self.done_chunks.clear();
        }
    }

    /// Return true if this builder is ready to produce a pack.
    pub fn is_full(&self) -> bool {
        // This approach seemed better than tracking the size as a field, and
        // possibly making a mistake and not realizing for a long time.
        let mut total_size: u64 = 0;
        for chunks in self.file_chunks.values() {
            for chunk in chunks {
                let already_done = self.done_chunks.contains(&chunk.digest);
                let already_packed = self.packed_chunks.contains(&chunk.digest);
                if !already_done && !already_packed {
                    total_size += chunk.length as u64;
                }
            }
        }
        total_size > self.pack_size
    }

    /// Write a pack file to the given path, encrypting using libsodium with the
    /// given passphrase. If nothing has been added to the builder, then nothing
    /// is written and an empty pack is returned.
    pub fn build_pack(&mut self, outfile: &Path, passphrase: &str) -> Result<Pack, Error> {
        let mut pack: Pack = Default::default();
        let mut bytes_packed: u64 = 0;
        // while there are files to process and the pack is not too big...
        while !self.file_chunks.is_empty() && bytes_packed < self.pack_size {
            // Get the first file from the map and start putting its chunks into
            // the pack, ignoring any duplicates.
            //
            // Would use first_key_value() but that is experimental in 1.41.
            let filesum = self.file_chunks.keys().take(1).next().unwrap().to_owned();
            let mut chunks_processed = 0;
            let chunks = &self.file_chunks[&filesum];
            for chunk in chunks {
                chunks_processed += 1;
                let already_done = self.done_chunks.contains(&chunk.digest);
                let already_packed = self.packed_chunks.contains(&chunk.digest);
                if !already_done && !already_packed {
                    pack.add_chunk(chunk.clone());
                    self.packed_chunks.insert(chunk.digest.clone());
                    bytes_packed += chunk.length as u64;
                    if bytes_packed > self.pack_size {
                        break;
                    }
                }
            }
            // if we successfully visited all of the chunks in this file,
            // including duplicates, then this file is considered "done"
            if chunks_processed == chunks.len() {
                let chunks = self.file_chunks.remove(&filesum).unwrap();
                pack.add_file(filesum, chunks);
            }
        }
        if bytes_packed > 0 {
            pack.build_pack(outfile, passphrase)?;
        }
        Ok(pack)
    }
}

///
/// Find the chunk boundaries within the given file, using the FastCDC
/// algorithm. The given `size` is the desired average size in bytes for the
/// chunks, but they may be between half and twice that size.
///
fn find_file_chunks(infile: &Path, size: u64) -> io::Result<Vec<entities::Chunk>> {
    let file = fs::File::open(infile)?;
    let mmap = unsafe {
        memmap::MmapOptions::new()
            .map(&file)
            .expect("cannot create mmap?")
    };
    let avg_size = size as usize;
    let min_size = avg_size / 2;
    let max_size = avg_size * 2;
    let chunker = fastcdc::FastCDC::new(&mmap[..], min_size, avg_size, max_size);
    let mut results = Vec::new();
    for entry in chunker {
        let end = entry.offset + entry.length;
        let chksum = entities::Checksum::sha256_from_bytes(&mmap[entry.offset..end]);
        let mut chunk = entities::Chunk::new(chksum, entry.offset, entry.length);
        chunk = chunk.filepath(infile);
        results.push(chunk);
    }
    Ok(results)
}

/// Contains the results of building a pack, and provides functions for saving
/// the results to the database.
pub struct Pack {
    /// Checksum of this pack file once it has been written.
    digest: Option<entities::Checksum>,
    /// Those files that have been completed with this pack.
    files: HashMap<entities::Checksum, Vec<entities::Chunk>>,
    /// Those chunks that are contained in this pack.
    chunks: Vec<entities::Chunk>,
    /// Salt used to hash the password for this pack.
    salt: Option<Salt>,
}

impl Pack {
    /// Add a completed file to this pack.
    pub fn add_file(&mut self, digest: entities::Checksum, chunks: Vec<entities::Chunk>) {
        self.files.insert(digest, chunks);
    }

    /// Add a chunk to this pack.
    pub fn add_chunk(&mut self, chunk: entities::Chunk) {
        self.chunks.push(chunk);
    }

    /// Return a reference to this pack's hash digest.
    pub fn get_digest(&self) -> Option<&entities::Checksum> {
        self.digest.as_ref()
    }

    /// Write the chunks in this pack to the specified path, compressing using
    /// zlib, and then encrypting using libsodium and the given passphrase.
    pub fn build_pack(&mut self, outfile: &Path, passphrase: &str) -> Result<(), Error> {
        // sort the chunks by digest to produce identical results
        self.chunks
            .sort_unstable_by(|a, b| a.digest.partial_cmp(&b.digest).unwrap());
        // Write to a temporary file first, encrypt that to the desired path,
        // and then delete the temporary file. Trying to rename a file is tricky
        // on Windows, for whatever reason, but this works.
        let mut packed = outfile.to_path_buf();
        packed.set_extension("pack");
        self.digest = Some(super::pack_chunks(&self.chunks, &packed)?);
        let mut zipped = outfile.to_path_buf();
        zipped.set_extension("gz");
        super::compress_file(&packed, &zipped)?;
        fs::remove_file(packed)?;
        self.salt = Some(super::encrypt_file(passphrase, &zipped, outfile)?);
        fs::remove_file(zipped)?;
        Ok(())
    }

    /// Record the results of building this pack to the database. This includes
    /// all of the chunks and the pack itself.
    pub fn record_completed_pack(
        &mut self,
        dbase: &Box<dyn RecordRepository>,
        coords: Vec<entities::PackLocation>,
    ) -> Result<(), Error> {
        let digest = self.digest.as_ref().unwrap();
        // record the uploaded chunks to the database
        for chunk in self.chunks.iter_mut() {
            // set the pack digest for each chunk record
            chunk.packfile = Some(digest.clone());
            dbase.insert_chunk(chunk)?;
        }
        self.chunks.clear();
        // record the pack in the database
        let mut pack = entities::Pack::new(digest.clone(), coords);
        pack.crypto_salt = self.salt;
        dbase.insert_pack(&pack)?;
        Ok(())
    }

    /// Record the set of files completed by uploading this pack file.
    /// Returns the number of completed files.
    pub fn record_completed_files(
        &mut self,
        dbase: &Box<dyn RecordRepository>,
    ) -> Result<usize, Error> {
        // massage the file/chunk data into database records for those files
        // that have been completely uploaded
        for (filesum, parts) in &self.files {
            let mut length: u64 = 0;
            let mut chunks: Vec<(u64, entities::Checksum)> = Vec::new();
            for chunk in parts {
                length += chunk.length as u64;
                chunks.push((chunk.offset as u64, chunk.digest.clone()));
            }
            let file = entities::File::new(filesum.clone(), length, chunks);
            dbase.insert_file(&file)?;
        }
        Ok(self.files.len())
    }
}

impl Default for Pack {
    fn default() -> Self {
        Self {
            digest: None,
            files: HashMap::new(),
            chunks: Vec::new(),
            salt: None,
        }
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
    fn test_calc_chunk_size() {
        assert_eq!(calc_chunk_size(65_536), 16_384);
        assert_eq!(calc_chunk_size(131_072), 32_768);
        assert_eq!(calc_chunk_size(262_144), 65_536);
        assert_eq!(calc_chunk_size(16_777_216), 4_194_304);
        assert_eq!(calc_chunk_size(33_554_432), 4_194_304);
        assert_eq!(calc_chunk_size(134_217_728), 4_194_304);
    }

    #[test]
    fn test_file_chunking_16k() -> io::Result<()> {
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 16384)?;
        assert_eq!(results.len(), 6);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 22366);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-103159aa68bb1ea98f64248c647b8fe9a303365d80cb63974a73bba8bc3167d7"
        );
        assert_eq!(results[1].offset, 22366);
        assert_eq!(results[1].length, 8282);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-c95e0d6a53f61dc7b6039cfb8618f6e587fc6395780cf28169f4013463c89db3"
        );
        assert_eq!(results[2].offset, 30648);
        assert_eq!(results[2].length, 16303);
        assert_eq!(
            results[2].digest.to_string(),
            "sha256-e03c4de56410b680ef69d8f8cfe140c54bb33f295015b40462d260deb9a60b82"
        );
        assert_eq!(results[3].offset, 46951);
        assert_eq!(results[3].length, 18696);
        assert_eq!(
            results[3].digest.to_string(),
            "sha256-bd1198535cdb87c5571378db08b6e886daf810873f5d77000a54795409464138"
        );
        assert_eq!(results[4].offset, 65647);
        assert_eq!(results[4].length, 32768);
        assert_eq!(
            results[4].digest.to_string(),
            "sha256-5c8251cce144b5291be3d4b161461f3e5ed441a7a24a1a65fdcc3d7b21bfc29d"
        );
        assert_eq!(results[5].offset, 98415);
        assert_eq!(results[5].length, 11051);
        assert_eq!(
            results[5].digest.to_string(),
            "sha256-a566243537738371133ecff524501290f0621f786f010b45d20a9d5cf82365f8"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_32k() -> io::Result<()> {
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 32768)?;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 32857);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d"
        );
        assert_eq!(results[1].offset, 32857);
        assert_eq!(results[1].length, 16408);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-13f6a4c6d42df2b76c138c13e86e1379c203445055c2b5f043a5f6c291fa520d"
        );
        assert_eq!(results[2].offset, 49265);
        assert_eq!(results[2].length, 60201);
        assert_eq!(
            results[2].digest.to_string(),
            "sha256-0fe7305ba21a5a5ca9f89962c5a6f3e29cd3e2b36f00e565858e0012e5f8df36"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_64k() -> io::Result<()> {
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 65536)?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 32857);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d"
        );
        assert_eq!(results[1].offset, 32857);
        assert_eq!(results[1].length, 76609);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-5420a3bcc7d57eaf5ca9bb0ab08a1bd3e4d89ae019b1ffcec39b1a5905641115"
        );
        Ok(())
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
        let actual = read_link(&link);
        assert_eq!(actual, "bGlua190YXJnZXRfaXNfbWVhbmluZ2xlc3M=");
        Ok(())
    }

    #[test]
    fn test_is_excluded() {
        let path1 = PathBuf::from("/Users/susan/database");
        let path2 = PathBuf::from("/Users/susan/dataset/.tmp");
        let path3 = PathBuf::from("/Users/susan/private");
        let excludes = vec![path1, path2, path3];
        assert!(!is_excluded(Path::new("/not/excluded"), &excludes));
        assert!(!is_excluded(Path::new("/Users/susan/public"), &excludes));
        assert!(is_excluded(
            Path::new("/Users/susan/database/LOCK"),
            &excludes
        ));
        assert!(is_excluded(
            Path::new("/Users/susan/dataset/.tmp/foobar"),
            &excludes
        ));
    }
}
