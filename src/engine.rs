//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `engine` module performs backups by taking a snapshot of a dataset,
//! storing new entries in the database, finding the differences from the
//! previous snapshot, building pack files, and sending them to the store.

use super::core;
use super::database::Database;
use super::state::{self, Action};
use super::store;
use base64::encode;
use failure::{err_msg, Error};
use log::{debug, error, info, trace};
use sodiumoxide::crypto::pwhash::Salt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, SystemTimeError};
use tempfile;
use xattr;

///
/// Take a snapshot, collect the new/changed files, assemble them into pack
/// files, upload them to the pack store, and record the results in the
/// database. The snapshot and dataset are updated in the database. Returns the
/// snapshot checksum, or None if there were no changes.
///
pub fn perform_backup(
    dataset: &mut core::Dataset,
    dbase: &Database,
    passphrase: &str,
) -> Result<Option<core::Checksum>, Error> {
    debug!("performing backup for {}", dataset);
    fs::create_dir_all(&dataset.workspace)?;
    // Check if latest snapshot exists and lacks an end time, which indicates
    // that the previous backup did not complete successfully.
    let latest_snap_ref = dataset.latest_snapshot.as_ref();
    if let Some(latest) = latest_snap_ref {
        if let Some(snapshot) = dbase.get_snapshot(latest)? {
            if snapshot.end_time.is_none() {
                // continue from the previous incomplete backup
                let parent_sha1 = snapshot.parent;
                let current_sha1 = latest.to_owned();
                debug!("continuing previous backup {}", &current_sha1);
                return continue_backup(dataset, dbase, passphrase, parent_sha1, current_sha1);
            }
        }
    }
    // The start of a _new_ backup is at the moment that a snapshot is to be
    // taken. The snapshot can take a very long time to build, and another
    // thread may spawn in the mean time and start taking another snapshot, and
    // again, and again until the system runs out of resources.
    state::dispatch(Action::StartBackup(dataset.key.clone()));
    // Take a snapshot and record it as the new most recent snapshot for this
    // dataset, to allow detecting a running backup, and thus recover from a
    // crash or forced shutdown.
    //
    // For now, build a set of excludes for files we do not want to have in the
    // backup set, such as the database and temporary files.
    let excludes = vec![dbase.get_path(), dataset.workspace.as_ref()];
    let snap_opt = take_snapshot(
        &dataset.basepath,
        dataset.latest_snapshot.clone(),
        &dbase,
        excludes,
    )?;
    match snap_opt {
        None => Ok(None),
        Some(current_sha1) => {
            let parent_sha1 = dataset.latest_snapshot.take();
            dataset.latest_snapshot = Some(current_sha1.clone());
            dbase.put_dataset(&dataset)?;
            debug!("starting new backup {}", &current_sha1);
            continue_backup(dataset, dbase, passphrase, parent_sha1, current_sha1)
        }
    }
}

///
/// Continue the backup for the most recent snapshot, comparing against the
/// parent snapshot, if any.
///
fn continue_backup(
    dataset: &mut core::Dataset,
    dbase: &Database,
    passphrase: &str,
    parent_sha1: Option<core::Checksum>,
    current_sha1: core::Checksum,
) -> Result<Option<core::Checksum>, Error> {
    let mut bmaster = BackupMaster::new(dataset, dbase, passphrase)?;
    // if no previous snapshot, visit every file in the new snapshot, otherwise
    // find those files that changed from the previous snapshot
    match parent_sha1 {
        None => {
            let snapshot = dbase
                .get_snapshot(&current_sha1)?
                .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", current_sha1)))?;
            let tree = snapshot.tree.clone();
            let iter = TreeWalker::new(dbase, &dataset.basepath, tree);
            for result in iter {
                bmaster.handle_file(result)?;
            }
        }
        Some(ref parent) => {
            let iter = find_changed_files(
                dbase,
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

///
/// Holds the state of the backup process to keep the code slim.
///
struct BackupMaster<'a> {
    dataset: &'a core::Dataset,
    dbase: &'a Database,
    builder: PackBuilder<'a>,
    passphrase: String,
    bucket_name: String,
    stores: Vec<Box<dyn store::Store>>,
}

impl<'a> BackupMaster<'a> {
    /// Build a BackupMaster.
    fn new(
        dataset: &'a core::Dataset,
        dbase: &'a Database,
        passphrase: &str,
    ) -> Result<Self, Error> {
        let bucket_name = core::generate_bucket_name(&dataset.computer_id);
        let builder = PackBuilder::new(&dbase, dataset.pack_size);
        let stores_boxed = store::load_stores(dbase, dataset.stores.as_slice())?;
        Ok(Self {
            dataset,
            dbase,
            builder,
            passphrase: passphrase.to_owned(),
            bucket_name,
            stores: stores_boxed,
        })
    }

    /// Handle a single changed file, adding it to the pack, and possibly
    /// uploading one or more pack files as needed.
    fn handle_file(&mut self, changed: Result<ChangedFile, Error>) -> Result<(), Error> {
        match changed {
            Ok(entry) => {
                // ignore files which already have records
                if self.dbase.get_file(&entry.digest)?.is_none() {
                    self.builder.add_file(&entry.path, entry.digest.clone())?;
                    // loop until pack builder is below desired size
                    // (adding a very large file may require multiple packs)
                    while self.builder.is_full() {
                        self.send_one_pack()?;
                    }
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
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
            let locations = store::store_pack(
                outfile.path(),
                &self.bucket_name,
                &object_name,
                &self.stores,
            )?;
            pack.record_completed_pack(self.dbase, locations)?;
            state::dispatch(Action::UploadPack(self.dataset.key.clone()));
        }
        let count = pack.record_completed_files(self.dbase)? as u64;
        state::dispatch(Action::UploadFiles(self.dataset.key.clone(), count));
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
    fn update_snapshot(&self, snap_sha1: &core::Checksum) -> Result<(), Error> {
        let mut snapshot = self
            .dbase
            .get_snapshot(snap_sha1)?
            .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", snap_sha1)))?;
        snapshot = snapshot.end_time(SystemTime::now());
        self.dbase.put_snapshot(snap_sha1, &snapshot)?;
        state::dispatch(Action::FinishBackup(self.dataset.key.clone()));
        Ok(())
    }

    /// Upload a compressed tarball of the database files to a special bucket.
    fn backup_database(&self) -> Result<(), Error> {
        // create a stable copy of the database
        let mut backup_path: PathBuf = PathBuf::from(self.dbase.get_path());
        backup_path.set_extension("backup");
        self.dbase.create_backup(&backup_path)?;
        // use a ULID as the object name so they sort by time
        let object_name = ulid::Ulid::new().to_string();
        let mut tarball = self.dataset.workspace.clone();
        tarball.push(&object_name);
        core::create_tar(&backup_path, &tarball)?;
        // use a predictable bucket name so we can find it later
        let bucket_name = core::computer_bucket_name(&self.dataset.computer_id);
        store::store_pack(&tarball, &bucket_name, &object_name, &self.stores)?;
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
    parent: Option<core::Checksum>,
    dbase: &Database,
    excludes: Vec<&Path>,
) -> Result<Option<core::Checksum>, Error> {
    let start_time = SystemTime::now();
    let tree = scan_tree(basepath, dbase, &excludes)?;
    let tree_sha1 = tree.checksum();
    if let Some(ref parent_sha1) = parent {
        let parent_doc = dbase
            .get_snapshot(parent_sha1)?
            .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", parent_sha1)))?;
        if parent_doc.tree == tree_sha1 {
            // nothing new at all with this snapshot
            return Ok(None);
        }
    }
    let end_time = SystemTime::now();
    let time_diff = end_time.duration_since(start_time);
    let pretty_time = pretty_print_duration(time_diff);
    let mut snap = core::Snapshot::new(parent, tree_sha1);
    snap = snap.file_count(tree.file_count);
    let sha1 = snap.checksum();
    info!(
        "took snapshot {} with {} files after {}",
        sha1, tree.file_count, pretty_time
    );
    dbase.insert_snapshot(&sha1, &snap)?;
    Ok(Some(sha1))
}

// Return a clear and accurate description of the duration.
pub fn pretty_print_duration(duration: Result<Duration, SystemTimeError>) -> String {
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

///
/// Restore a single file identified by the given checksum.
///
pub fn restore_file(
    dbase: &Database,
    dataset: &core::Dataset,
    passphrase: &str,
    checksum: core::Checksum,
    outfile: &Path,
) -> Result<(), Error> {
    let stores_boxed = store::load_stores(dbase, dataset.stores.as_slice())?;
    // look up the file record to get chunks
    let saved_file = dbase
        .get_file(&checksum)?
        .ok_or_else(|| err_msg(format!("missing file: {:?}", checksum)))?;
    // create an index of all the chunks we want to collect (using strings
    // because the extracted chunks consist of a list of file names)
    let mut desired_chunks: HashSet<String> = HashSet::new();
    for (_offset, chunk) in &saved_file.chunks {
        desired_chunks.insert(chunk.to_string());
    }
    // track pack files that have already been processed
    let mut finished_packs: HashSet<core::Checksum> = HashSet::new();
    // look up chunk records to get pack record(s)
    for (_offset, chunk) in &saved_file.chunks {
        let chunk_rec = dbase
            .get_chunk(&chunk)?
            .ok_or_else(|| err_msg(format!("missing chunk: {:?}", chunk)))?;
        let pack_digest = chunk_rec.packfile.as_ref().unwrap();
        if !finished_packs.contains(pack_digest) {
            let saved_pack = dbase
                .get_pack(pack_digest)?
                .ok_or_else(|| err_msg(format!("missing pack record: {:?}", pack_digest)))?;
            // check the salt before downloading the pack, otherwise we waste
            // time fetching it when we would not be able to decrypt it
            let salt = saved_pack
                .crypto_salt
                .ok_or_else(|| err_msg(format!("missing pack salt: {:?}", pack_digest)))?;
            // retrieve the pack file
            let packfile = tempfile::Builder::new()
                .prefix("pack")
                .suffix(".bin")
                .tempfile_in(&dataset.workspace)?;
            store::retrieve_pack(&stores_boxed, &saved_pack.locations, packfile.path())?;
            // extract chunks from pack (temporarily use the output file path)
            core::decrypt_file(passphrase, &salt, packfile.path(), outfile)?;
            let chunk_names = core::unpack_chunks(outfile, &dataset.workspace)?;
            fs::remove_file(outfile)?;
            // remove unrelated chunks to conserve space
            for cname in chunk_names {
                if !desired_chunks.contains(&cname) {
                    let mut chunk_path = PathBuf::from(&dataset.workspace);
                    chunk_path.push(cname);
                    fs::remove_file(&chunk_path)?;
                }
            }
            // remember this pack as being completed
            finished_packs.insert(pack_digest.to_owned());
        }
    }
    // sort the chunks by offset to produce the ordered file list
    let mut chunks = saved_file.chunks.clone();
    chunks.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let chunk_bufs: Vec<PathBuf> = chunks
        .iter()
        .map(|c| {
            let mut cpath = PathBuf::from(&dataset.workspace);
            cpath.push(c.1.to_string());
            cpath
        })
        .collect();
    let chunk_paths: Vec<&Path> = chunk_bufs.iter().map(|b| b.as_path()).collect();
    core::assemble_chunks(&chunk_paths, outfile)?;
    Ok(())
}

///
/// `ChangedFile` represents a new or modified file.
///
#[derive(Debug)]
pub struct ChangedFile {
    /// File path of the changed file relative to the base path.
    pub path: PathBuf,
    /// Hash digest of the changed file.
    pub digest: core::Checksum,
}

impl ChangedFile {
    fn new(path: &Path, digest: core::Checksum) -> Self {
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
    dbase: &'a Database,
    /// Queue of pending paths to visit, where the path is relative, the first
    /// checksum is the left tree (earlier in time), and the second is the right
    /// tree (later in time).
    queue: VecDeque<(PathBuf, core::Checksum, core::Checksum)>,
    /// Nested iterator for visiting an entire new subdirectory.
    walker: Option<TreeWalker<'a>>,
    /// Current path being visited.
    path: Option<PathBuf>,
    /// Left tree currently being visited.
    left_tree: Option<core::Tree>,
    /// Position within left tree currently being iterated.
    left_idx: usize,
    /// Right tree currently being visited.
    right_tree: Option<core::Tree>,
    /// Position within right tree currently being iterated.
    right_idx: usize,
}

impl<'a> ChangedFilesIter<'a> {
    fn new(
        dbase: &'a Database,
        basepath: PathBuf,
        left_tree: core::Checksum,
        right_tree: core::Checksum,
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
    dbase: &Database,
    basepath: PathBuf,
    snapshot1: core::Checksum,
    snapshot2: core::Checksum,
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
    dbase: &'a Database,
    /// Queue of pending paths to visit, where the path is relative, the
    /// checksum is the tree to be visited.
    queue: VecDeque<(PathBuf, core::Checksum)>,
    /// Current path being visited.
    path: Option<PathBuf>,
    /// Tree currently being visited.
    tree: Option<core::Tree>,
    /// Position within tree currently being iterated.
    entry_idx: usize,
}

impl<'a> TreeWalker<'a> {
    pub fn new(dbase: &'a Database, basepath: &Path, tree: core::Checksum) -> Self {
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
            encode(vstr)
        } else {
            let s: String = value.to_string_lossy().into_owned();
            encode(&s)
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
fn scan_tree(basepath: &Path, dbase: &Database, excludes: &[&Path]) -> Result<core::Tree, Error> {
    let mut entries: Vec<core::TreeEntry> = Vec::new();
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
                                    let digest = scan.checksum();
                                    let tref = core::TreeReference::TREE(digest);
                                    let ent = process_path(&path, tref, dbase);
                                    entries.push(ent);
                                } else if file_type.is_symlink() {
                                    let link = read_link(&path);
                                    let tref = core::TreeReference::LINK(link);
                                    let ent = process_path(&path, tref, dbase);
                                    entries.push(ent);
                                } else if file_type.is_file() {
                                    match core::checksum_file(&path) {
                                        Ok(digest) => {
                                            let tref = core::TreeReference::FILE(digest);
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
    let tree = core::Tree::new(entries, file_count);
    let digest = tree.checksum();
    dbase.insert_tree(&digest, &tree)?;
    Ok(tree)
}

///
/// Indicate if the given path is excluded or not.
///
fn is_excluded(fullpath: &Path, excludes: &[&Path]) -> bool {
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
fn process_path(
    fullpath: &Path,
    reference: core::TreeReference,
    dbase: &Database,
) -> core::TreeEntry {
    let mut entry = core::TreeEntry::new(fullpath, reference);
    entry = entry.mode(fullpath);
    entry = entry.owners(fullpath);
    trace!("processed path entry {:?}", fullpath);
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
                    let digest = core::checksum_data_sha1(value.as_ref());
                    if dbase.insert_xattr(&digest, value.as_ref()).is_ok() {
                        entry.xattrs.insert(nm, digest);
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

/// Builds pack files by splitting incoming files into chunks.
pub struct PackBuilder<'a> {
    /// Reference to Database for fetching records.
    dbase: &'a Database,
    /// Preferred size of pack files in bytes.
    pack_size: u64,
    /// Preferred size of chunks in bytes.
    chunk_size: u64,
    /// Map of file checksum to the chunks it contains that have not yet been
    /// uploaded in a pack file.
    file_chunks: HashMap<core::Checksum, Vec<core::Chunk>>,
    /// Those chunks that have been packed using this builder.
    packed_chunks: HashSet<core::Checksum>,
    /// Those chunks that have been uploaded previously.
    done_chunks: HashSet<core::Checksum>,
}

impl<'a> PackBuilder<'a> {
    /// Create a new builder with the desired size.
    pub fn new(dbase: &'a Database, pack_size: u64) -> Self {
        // Use our default chunk size unless the desired pack size is so
        // small that the chunks would be a significant portion of the pack
        // file (this is mostly for testing purposes).
        let chunk_size = if pack_size < DEFAULT_CHUNK_SIZE * 4 {
            pack_size / 4
        } else {
            DEFAULT_CHUNK_SIZE
        };
        Self {
            dbase,
            pack_size,
            chunk_size,
            file_chunks: HashMap::new(),
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
    pub fn add_file(&mut self, path: &Path, file_digest: core::Checksum) -> Result<(), Error> {
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
            core::find_file_chunks(path, self.chunk_size)?
        } else {
            let mut chunk = core::Chunk::new(file_digest.clone(), 0, file_size as usize);
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
            // get a random file from the map and start putting its chunks into
            // the pack, ignoring any duplicates
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

/// Contains the results of building a pack, and provides functions for saving
/// the results to the database.
pub struct Pack {
    /// Checksum of this pack file once it has been written.
    digest: Option<core::Checksum>,
    /// Those files that have been completed with this pack.
    files: HashMap<core::Checksum, Vec<core::Chunk>>,
    /// Those chunks that are contained in this pack.
    chunks: Vec<core::Chunk>,
    /// Salt used to hash the password for this pack.
    salt: Option<Salt>,
}

impl Pack {
    /// Add a completed file to this pack.
    pub fn add_file(&mut self, digest: core::Checksum, chunks: Vec<core::Chunk>) {
        self.files.insert(digest, chunks);
    }

    /// Add a chunk to this pack.
    pub fn add_chunk(&mut self, chunk: core::Chunk) {
        self.chunks.push(chunk);
    }

    /// Return a reference to this pack's hash digest.
    pub fn get_digest(&self) -> Option<&core::Checksum> {
        self.digest.as_ref()
    }

    /// Write the chunks in this pack to the specified path, encrypting using
    /// libsodium with the given passphrase.
    pub fn build_pack(&mut self, outfile: &Path, passphrase: &str) -> Result<(), Error> {
        // sort the chunks by digest to produce identical results
        self.chunks
            .sort_unstable_by(|a, b| a.digest.partial_cmp(&b.digest).unwrap());
        self.digest = Some(core::pack_chunks(&self.chunks, outfile)?);
        let mut cipher = outfile.to_path_buf();
        cipher.set_extension(".pgp");
        self.salt = Some(core::encrypt_file(passphrase, outfile, &cipher)?);
        fs::rename(cipher, outfile)?;
        Ok(())
    }

    /// Record the results of building this pack to the database. This includes
    /// all of the chunks and the pack itself.
    pub fn record_completed_pack(
        &mut self,
        dbase: &Database,
        coords: Vec<core::PackLocation>,
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
        let mut pack = core::SavedPack::new(digest.clone(), coords);
        pack.crypto_salt = self.salt;
        dbase.insert_pack(&pack)?;
        Ok(())
    }

    /// Record the set of files completed by uploading this pack file.
    /// Returns the number of completed files.
    pub fn record_completed_files(&mut self, dbase: &Database) -> Result<usize, Error> {
        // massage the file/chunk data into database records for those files
        // that have been completely uploaded
        for (filesum, parts) in &self.files {
            let mut length: u64 = 0;
            let mut chunks: Vec<(u64, core::Checksum)> = Vec::new();
            for chunk in parts {
                length += chunk.length as u64;
                chunks.push((chunk.offset as u64, chunk.digest.clone()));
            }
            let file = core::SavedFile::new(filesum.clone(), length, chunks);
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

///
/// Retrieve the configuration record from the database, or build a new one
/// using default values.
///
pub fn get_configuration(dbase: &Database) -> Result<core::Configuration, Error> {
    if let Some(conf) = dbase.get_config()? {
        return Ok(conf);
    }
    Ok(Default::default())
}

#[cfg(test)]
mod tests {
    use super::core::*;
    use super::*;
    use std::collections::HashMap;
    #[cfg(target_family = "unix")]
    use std::os::unix::fs;
    #[cfg(target_family = "windows")]
    use std::os::windows::fs;
    use std::time::SystemTime;
    use tempfile::tempdir;

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
    fn test_checksum_tree() {
        let tref1 = TreeReference::FILE(Checksum::SHA1("cafebabe".to_owned()));
        let entry1 = core::TreeEntry {
            name: String::from("madoka.kaname"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: tref1,
            xattrs: HashMap::new(),
        };
        let tref2 = TreeReference::FILE(Checksum::SHA1("babecafe".to_owned()));
        let entry2 = core::TreeEntry {
            name: String::from("homura.akemi"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: tref2,
            xattrs: HashMap::new(),
        };
        let tref3 = TreeReference::FILE(Checksum::SHA1("babebabe".to_owned()));
        let entry3 = core::TreeEntry {
            name: String::from("sayaka.miki"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: tref3,
            xattrs: HashMap::new(),
        };
        let tree = core::Tree::new(vec![entry1, entry2, entry3], 2);
        // would look something like this, if we used "now" instead of unix epoch
        // 644 100:100 1552877320 1552877320 sha1-babecafe homura.akemi
        // 644 100:100 1552877320 1552877320 sha1-cafebabe madoka.kaname
        // 644 100:100 1552877320 1552877320 sha1-babebabe sayaka.miki
        let result = tree.to_string();
        // results should be sorted lexicographically by filename
        assert!(result.find("homura").unwrap() < result.find("madoka").unwrap());
        assert!(result.find("madoka").unwrap() < result.find("sayaka").unwrap());
        let sum = tree.checksum();
        // because the timestamps are always 0, sha1 is always the same
        assert_eq!(
            sum.to_string(),
            "sha1-086f6c6ba3e51882c4fd55fc9733316c4ee1b15d"
        );
    }

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

    #[test]
    fn test_is_excluded() {
        let path1 = PathBuf::from("/Users/susan/database");
        let path2 = PathBuf::from("/Users/susan/dataset/.tmp");
        let path3 = PathBuf::from("/Users/susan/private");
        let excludes = vec![path1.as_path(), path2.as_path(), path3.as_path()];
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
