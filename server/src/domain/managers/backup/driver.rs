//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::domain::entities;
use crate::domain::helpers::{self, crypto, pack};
use crate::domain::managers::state::{BackupAction, StateStore};
use crate::domain::repositories::{PackRepository, RecordRepository};
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use log::{error, info};
use sodiumoxide::crypto::pwhash::Salt;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

///
/// Receives changed files, placing them in packs and uploading to the pack
/// stores. If time has run out, will raise an `OutOfTimeFailure` error.
///
pub struct BackupDriver<'a> {
    dataset: &'a entities::Dataset,
    dbase: &'a Arc<dyn RecordRepository>,
    state: &'a Arc<dyn StateStore>,
    passphrase: String,
    stores: Box<dyn PackRepository>,
    stop_time: Option<DateTime<Utc>>,
    /// Preferred size of chunks in bytes.
    chunk_size: u32,
    /// Builds a pack file comprised of compressed chunks.
    builder: pack::PackBuilder,
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
    pub fn new(
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
            builder: pack::PackBuilder::new(dataset.pack_size),
            record: Default::default(),
            file_chunks: BTreeMap::new(),
            packed_chunks: HashSet::new(),
            done_chunks: HashSet::new(),
        })
    }

    /// Process a single changed file, adding it to the pack, and possibly
    /// uploading one or more pack files as needed.
    pub fn add_file(&mut self, changed: super::ChangedFile) -> Result<(), Error> {
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
        let chunks = if file_size > self.chunk_size as u64 {
            // split large files into chunks, add chunks to the list
            helpers::find_file_chunks(path, self.chunk_size)?
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
        while let Some((key, _)) = self.file_chunks.first_key_value() {
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
                        return Err(Error::from(super::OutOfTimeFailure {}));
                    }
                }
                // check if the user requested that the backup stop
                if let Some(backup) = self.state.get_state().backups(&self.dataset.id) {
                    if backup.should_stop() {
                        return Err(Error::from(super::OutOfTimeFailure {}));
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
    pub fn finish_remainder(&mut self) -> Result<(), Error> {
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
            let salt = crypto::encrypt_file(&self.passphrase, &pack_path, &outfile)?;
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
            fs::remove_file(outfile)?;
        }
        let count = self
            .record
            .record_completed_files(self.dbase, &pack_digest)? as u64;
        self.state
            .backup_event(BackupAction::UploadFiles(self.dataset.id.clone(), count));
        Ok(())
    }

    /// Update the current snapshot with the end time set to the current time.
    pub fn update_snapshot(&self, snap_sha1: &entities::Checksum) -> Result<(), Error> {
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
    pub fn backup_database(&self) -> Result<(), Error> {
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
fn calc_chunk_size(pack_size: u64) -> u32 {
    // Use our default chunk size unless the desired pack size is so small that
    // the chunks would be a significant portion of the pack file.
    let chunk_size = if pack_size < DEFAULT_CHUNK_SIZE * 4 {
        pack_size / 4
    } else {
        DEFAULT_CHUNK_SIZE
    };
    chunk_size.try_into().map_or(DEFAULT_CHUNK_SIZE as u32, |v: u64| v as u32)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calc_chunk_size() {
        assert_eq!(calc_chunk_size(65_536), 16_384);
        assert_eq!(calc_chunk_size(131_072), 32_768);
        assert_eq!(calc_chunk_size(262_144), 65_536);
        assert_eq!(calc_chunk_size(16_777_216), 4_194_304);
        assert_eq!(calc_chunk_size(33_554_432), 4_194_304);
        assert_eq!(calc_chunk_size(134_217_728), 4_194_304);
    }
}
