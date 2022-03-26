//
// Copyright (c) 2022 Nathan Fiedler
//

//! The manager for performing backups by taking a snapshot of a dataset,
//! storing new entries in the database, finding the differences from the
//! previous snapshot, building pack files, and sending them to the store.

use crate::domain::entities;
use crate::domain::managers::state::{BackupAction, StateStore};
use crate::domain::repositories::{PackRepository, RecordRepository};
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use globset::{Glob, GlobSet, GlobSetBuilder};
use log::{debug, error, info, trace, warn};
use sodiumoxide::crypto::pwhash::Salt;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

/// Take a snapshot, collect the new/changed files, assemble them into pack
/// files, upload them to the pack repository, and record the results in the
/// repository. The snapshot and dataset are also updated. Returns the snapshot
/// checksum, or `None` if there were no changes.
pub fn perform_backup(
    dataset: &entities::Dataset,
    repo: &Arc<dyn RecordRepository>,
    state: &Arc<dyn StateStore>,
    passphrase: &str,
    stop_time: Option<DateTime<Utc>>,
) -> Result<Option<entities::Checksum>, Error> {
    if let Some(time) = stop_time {
        debug!("performing backup for {} until {}", dataset, time);
    } else {
        debug!("performing backup for {} until completion", dataset);
    }
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
                    state,
                    passphrase,
                    parent_sha1,
                    current_sha1,
                    stop_time,
                );
            }
        }
    }
    // The start time of a new backup is at the moment that a snapshot is to be
    // taken. The snapshot can take a long time to build, and another thread may
    // spawn in the mean time and start taking another snapshot, and again, and
    // again until the system runs out of resources.
    state.backup_event(BackupAction::Start(dataset.id.clone()));
    // In addition to the exclusions defined in the dataset, we exclude the
    // temporary workspace and repository database files.
    let mut excludes = repo.get_excludes();
    excludes.push(dataset.workspace.clone());
    for exclusion in dataset.excludes.iter() {
        excludes.push(PathBuf::from(exclusion));
    }
    // Take a snapshot and record it as the new most recent snapshot for this
    // dataset, to allow detecting a running backup, and thus recover from a
    // crash or forced shutdown.
    let snap_opt = take_snapshot(&dataset.basepath, latest_snapshot.clone(), &repo, excludes)?;
    match snap_opt {
        None => {
            // indicate that the backup has finished (doing nothing)
            state.backup_event(BackupAction::Finish(dataset.id.clone()));
            Ok(None)
        }
        Some(current_sha1) => {
            repo.put_latest_snapshot(&dataset.id, &current_sha1)?;
            debug!("starting new backup {}", &current_sha1);
            continue_backup(
                dataset,
                repo,
                state,
                passphrase,
                latest_snapshot,
                current_sha1,
                stop_time,
            )
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
    let mut driver = BackupDriver::new(dataset, repo, state, passphrase, stop_time)?;
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
/// Receives changed files, placing them in packs and uploading to the pack
/// stores. If time has run out, will raise an `OutOfTimeFailure` error.
///
struct BackupDriver<'a> {
    dataset: &'a entities::Dataset,
    dbase: &'a Arc<dyn RecordRepository>,
    state: &'a Arc<dyn StateStore>,
    passphrase: String,
    stores: Box<dyn PackRepository>,
    stop_time: Option<DateTime<Utc>>,
    /// Preferred size of chunks in bytes.
    chunk_size: u64,
    /// Builds a pack file comprised of compressed chunks.
    builder: super::PackBuilder,
    /// Tracks files and chunks in the current pack.
    record: PackRecord,
    /// Map of file checksum to the chunks it contains that have not yet been
    /// uploaded in a pack file.
    file_chunks: BTreeMap<entities::Checksum, Vec<entities::Chunk>>,
    /// Those chunks that have been packed using this builder.
    packed_chunks: HashSet<entities::Checksum>,
    /// Those chunks that have been uploaded previously.
    done_chunks: HashSet<entities::Checksum>,
}

impl<'a> BackupDriver<'a> {
    /// Build a BackupMaster.
    fn new(
        dataset: &'a entities::Dataset,
        dbase: &'a Arc<dyn RecordRepository>,
        state: &'a Arc<dyn StateStore>,
        passphrase: &str,
        stop_time: Option<DateTime<Utc>>,
    ) -> Result<Self, Error> {
        let stores = dbase.load_dataset_stores(&dataset)?;
        let chunk_size = calc_chunk_size(dataset.pack_size);
        Ok(Self {
            dataset,
            dbase,
            state,
            passphrase: passphrase.to_owned(),
            stores,
            stop_time,
            chunk_size,
            builder: super::PackBuilder::new(dataset.pack_size),
            record: Default::default(),
            file_chunks: BTreeMap::new(),
            packed_chunks: HashSet::new(),
            done_chunks: HashSet::new(),
        })
    }

    /// Process a single changed file, adding it to the pack, and possibly
    /// uploading one or more pack files as needed.
    fn add_file(&mut self, changed: ChangedFile) -> Result<(), Error> {
        // ignore files which already have records
        if self.dbase.get_file(&changed.digest)?.is_none() {
            if self
                .split_file(&changed.path, changed.digest.clone())
                .is_err()
            {
                // file disappeared out from under us, record it as
                // having zero length; file restore will handle it
                // without any problem
                error!("file {:?} went missing during backup", changed.path);
                let file = entities::File::new(changed.digest, 0, vec![]);
                self.dbase.insert_file(&file)?;
            }
            self.process_queue()?;
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
        let attr = fs::metadata(path)?;
        let file_size = attr.len();
        let chunks = if file_size > self.chunk_size {
            // split large files into chunks, add chunks to the list
            super::find_file_chunks(path, self.chunk_size)?
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
        if chunks.len() > 120 {
            // For very large files, give some indication that we will be busy
            // for a while processing that one file since it requires many pack
            // files to completely finish this one file.
            info!(
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
        // would use first_key_value() but that is experimental in 1.59
        while let Some(key) = self.file_chunks.keys().take(1).next() {
            let filesum = key.to_owned();
            let mut chunks_processed = 0;
            let chunks = &self.file_chunks[key].to_owned();
            for chunk in chunks {
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
                            .prefix("pack")
                            .suffix(".tar")
                            .tempfile_in(&self.dataset.workspace)?
                            .keep()?;
                        self.builder.initialize(&outpath)?;
                    }
                    // add the chunk to the pack file, uploading when ready
                    if self.builder.add_chunk(chunk)? {
                        let pack_path = self.builder.finalize()?;
                        self.upload_pack(&pack_path)?;
                        fs::remove_file(pack_path)?;
                        self.record = Default::default();
                    }
                }
                // check if the stop time (if any) has been reached
                if let Some(stop_time) = self.stop_time {
                    let now = Utc::now();
                    if now > stop_time {
                        return Err(Error::from(OutOfTimeFailure {}));
                    }
                }
            }
            // if we successfully visited all of the chunks in this file,
            // including duplicates, then this file is considered "done"
            if chunks_processed == chunks.len() {
                let chunks = self.file_chunks.remove(&filesum).unwrap();
                self.record.add_file(filesum, chunks);
            }
        }
        Ok(())
    }

    /// If the pack builder has content, finalize the pack and upload.
    fn finish_remainder(&mut self) -> Result<(), Error> {
        self.process_queue()?;
        if !self.builder.is_empty() {
            let pack_path = self.builder.finalize()?;
            self.upload_pack(&pack_path)?;
            fs::remove_file(pack_path)?;
            self.record = Default::default();
        }
        Ok(())
    }

    /// Upload a single pack to the pack store and record the results.
    fn upload_pack(&mut self, pack_path: &Path) -> Result<(), Error> {
        let pack_digest = entities::Checksum::sha256_from_file(&pack_path)?;
        // possible that we just happened to build an identical pack file
        if self.dbase.get_pack(&pack_digest)?.is_none() {
            let mut outfile = pack_path.to_path_buf();
            outfile.set_extension("nacl");
            let salt = super::encrypt_file(&self.passphrase, &pack_path, &outfile)?;
            // new pack file, need to upload this and record to database
            let computer_id = self.dbase.get_computer_id(&self.dataset.id)?.unwrap();
            let bucket_name = self.stores.get_bucket_name(&computer_id);
            let object_name = format!("{}", pack_digest);
            // capture and record the remote object name, in case it differs from
            // the name we generated ourselves; either value is expected to be
            // sufficiently unique for our purposes
            let locations = self
                .stores
                .store_pack(&outfile, &bucket_name, &object_name)?;
            self.record
                .record_completed_pack(self.dbase, &pack_digest, locations, salt)?;
            self.state
                .backup_event(BackupAction::UploadPack(self.dataset.id.clone()));
        }
        let count = self
            .record
            .record_completed_files(self.dbase, &pack_digest)? as u64;
        self.state
            .backup_event(BackupAction::UploadFiles(self.dataset.id.clone(), count));
        Ok(())
    }

    /// Update the current snapshot with the end time set to the current time.
    fn update_snapshot(&self, snap_sha1: &entities::Checksum) -> Result<(), Error> {
        let mut snapshot = self
            .dbase
            .get_snapshot(snap_sha1)?
            .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", snap_sha1)))?;
        snapshot = snapshot.end_time(Utc::now());
        self.dbase.put_snapshot(&snapshot)?;
        self.state
            .backup_event(BackupAction::Finish(self.dataset.id.clone()));
        Ok(())
    }

    /// Upload an archive of the database files to the pack stores.
    fn backup_database(&self) -> Result<(), Error> {
        // Create a stable snapshot of the database as a single file, upload it
        // to a special place in the pack store, then record the pseudo-pack to
        // enable accurate pack pruning.
        let backup_path = self.dbase.create_backup()?;
        let computer_id = self.dbase.get_computer_id(&self.dataset.id)?.unwrap();
        let coords = self.stores.store_database(&computer_id, &backup_path)?;
        let digest = entities::Checksum::sha256_from_file(&backup_path)?;
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
fn calc_chunk_size(pack_size: u64) -> u64 {
    // Use our default chunk size unless the desired pack size is so small that
    // the chunks would be a significant portion of the pack file.
    if pack_size < DEFAULT_CHUNK_SIZE * 4 {
        pack_size / 4
    } else {
        DEFAULT_CHUNK_SIZE
    }
}

/// Tracks the files and chunks that comprise a pack, and provides functions for
/// saving the results to the database.
pub struct PackRecord {
    /// Those files that have been completed with this pack.
    files: HashMap<entities::Checksum, Vec<entities::Chunk>>,
    /// Those chunks that are contained in this pack.
    chunks: Vec<entities::Chunk>,
}

impl PackRecord {
    /// Add a completed file to this pack.
    pub fn add_file(&mut self, digest: entities::Checksum, chunks: Vec<entities::Chunk>) {
        self.files.insert(digest, chunks);
    }

    /// Add a chunk to this pack.
    pub fn add_chunk(&mut self, chunk: entities::Chunk) {
        self.chunks.push(chunk);
    }

    /// Record the results of building this pack to the database. This includes
    /// all of the chunks and the pack itself.
    pub fn record_completed_pack(
        &mut self,
        dbase: &Arc<dyn RecordRepository>,
        digest: &entities::Checksum,
        coords: Vec<entities::PackLocation>,
        salt: Salt,
    ) -> Result<(), Error> {
        // record the uploaded chunks to the database
        for chunk in self.chunks.iter_mut() {
            // The chunk is the entire file, which will be recorded soon and its
            // chunk digest will in fact by the pack digest, thereby eliminating
            // the need for a chunk record at all.
            if !self.files.contains_key(&chunk.digest) {
                // set the pack digest for each chunk record
                chunk.packfile = Some(digest.to_owned());
                dbase.insert_chunk(chunk)?;
            }
        }
        self.chunks.clear();
        // record the pack in the database
        let mut pack = entities::Pack::new(digest.to_owned(), coords);
        pack.crypto_salt = Some(salt);
        dbase.insert_pack(&pack)?;
        Ok(())
    }

    /// Record the set of files completed by uploading this pack file.
    /// Returns the number of completed files.
    pub fn record_completed_files(
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
        Ok(self.files.len())
    }
}

impl Default for PackRecord {
    fn default() -> Self {
        Self {
            files: HashMap::new(),
            chunks: Vec::new(),
        }
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
    let exclusions = build_exclusions(&excludes);
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
fn build_exclusions(excludes: &[PathBuf]) -> GlobSet {
    let mut builder = GlobSetBuilder::new();
    for exclusion in excludes {
        if let Some(path_str) = exclusion.to_str() {
            if let Ok(glob) = Glob::new(path_str) {
                builder.add(glob);
            } else {
                warn!("could not build glob for {:?}", exclusion);
            }
        } else {
            warn!("PathBuf::to_str() failed for {:?}", exclusion);
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
/// directories. Any new trees found, as identified by their hash digest, will
/// be inserted into the database. The same is true for any files found, and
/// their extended attributes. The return value itself will also be added to the
/// database. The result will be that everything new will have been added as new
/// records.
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
                                    if metadata.len() <= 80 {
                                        // file smaller than FileDef record
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
        let len = metadata.len();
        if len <= 80 {
            file_counts.files_below_80 += 1;
        } else if len <= 1024 {
            file_counts.files_below_1k += 1;
        } else if len <= 10240 {
            file_counts.files_below_10k += 1;
        } else if len <= 102400 {
            file_counts.files_below_100k += 1;
        } else if len <= 1048576 {
            file_counts.files_below_1m += 1;
        } else if len <= 10485760 {
            file_counts.files_below_10m += 1;
        } else if len <= 104857600 {
            file_counts.files_below_100m += 1;
        } else {
            file_counts.very_large_files += 1;
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
