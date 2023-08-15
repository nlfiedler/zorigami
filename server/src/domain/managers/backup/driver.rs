//
// Copyright (c) 2023 Nathan Fiedler
//

//! The `driver` module defines the `BackupDriver` and `PackRecord` types.
//!
//! `BackupDriver` is responsible for receiving incoming files that are to be
//! backed up, splitting them as needed, adding them to the `PackBuilder`
//! (defined in a separate module), and adding the chunks and files metadata to
//! the `PackRecord` struct.
//!
//! `PackRecord` holds all of the chunk and file metadata that is associated
//! with a single pack file as it is being built by `BackupDriver`. When a pack
//! file has been successfully uploaded by the driver, `PackRecord` will create
//! records in the database that track which chunks belong to which files, and
//! where those chunks are located.

use crate::domain::entities;
use crate::domain::helpers::{self, crypto, pack};
use crate::domain::managers::state::{BackupAction, StateStore};
use crate::domain::repositories::{PackRepository, RecordRepository};
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use log::{error, info, trace, warn};
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
                error!("file {} went missing during backup", changed.path.display());
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
        trace!("split_file '{}' digest {}", path.display(), file_digest);
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
                        .prefix("pack")
                        .suffix(".tar")
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
        if !self.record.verify_pack(pack_path)? {
            return Err(anyhow!(
                "missing chunks from pack file {}",
                pack_path.display()
            ));
        }
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
        } else {
            info!("pack record already exists for {}", pack_digest);
        }
        let count = self
            .record
            .record_completed_files(self.dbase, &pack_digest)? as u64;
        self.state
            .backup_event(BackupAction::UploadFiles(self.dataset.id.clone(), count));
        fs::remove_file(pack_path)?;
        self.record = Default::default();
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
    chunk_size
        .try_into()
        .map_or(DEFAULT_CHUNK_SIZE as u32, |v: u64| v as u32)
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
    fn add_file(&mut self, digest: entities::Checksum, chunks: Vec<entities::Chunk>) {
        self.files.insert(digest, chunks);
    }

    /// Add a chunk to this pack.
    fn add_chunk(&mut self, chunk: entities::Chunk) {
        self.chunks.push(chunk);
    }

    /// Return true if the given (unencrypted) pack file contains everything
    /// this record expects to be in the pack file, false otherwise.
    fn verify_pack(&self, pack_path: &Path) -> Result<bool, Error> {
        use std::str::FromStr;
        let file = fs::File::open(&pack_path)?;
        let mut ar = tar::Archive::new(file);
        // This is an n^2 search which is fine because the number of chunks in a
        // typical pack file number in the tens, never any significant number.
        let mut found_count: usize = 0;
        for maybe_entry in ar.entries()? {
            let entry = maybe_entry?;
            // we know the names are valid UTF-8, we created them
            let digest = entities::Checksum::from_str(entry.path()?.to_str().unwrap())?;
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
        salt: Salt,
    ) -> Result<(), Error> {
        // record the uploaded chunks to the database
        for chunk in self.chunks.iter_mut() {
            // Detect the case of a chunk whose digest matches an entire file,
            // which means it will _not_ get a record of its own but instead the
            // file record will point directly to a pack record.
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
    use crate::data::repositories::RecordRepositoryImpl;
    use crate::data::sources::EntityDataSourceImpl;
    use crate::domain::entities::Checksum;
    use crate::domain::managers::backup::ChangedFile;
    use crate::domain::managers::state::{StateStore, StateStoreImpl};
    use std::path::PathBuf;
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
    fn test_pack_record_verify_pack() -> Result<(), Error> {
        let mut record: PackRecord = Default::default();
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let chunks = super::helpers::find_file_chunks(&infile, 16384)?;
        let mut builder = pack::PackBuilder::new(1048576);
        let outdir = tempdir()?;
        let packfile = outdir.path().join("multi-pack.tar");
        builder.initialize(&packfile)?;
        for chunk in chunks.iter() {
            if builder.add_chunk(chunk)? {
                break;
            }
            record.add_chunk(chunk.to_owned());
        }
        let _ = builder.finalize()?;
        let result = record.verify_pack(&packfile)?;
        assert!(result);

        // inject a "missing" chunk into record, should return false
        let chunk = entities::Chunk::new(
            entities::Checksum::SHA256(
                "7b5352a6d7116e70b420c6e2f5ad3b49ba0af92923ab53ee43bd3fd0374d2521".to_owned(),
            ),
            0,
            11364,
        );
        record.chunks.push(chunk);
        let result = record.verify_pack(&packfile)?;
        assert_eq!(result, false);

        // remove one of the chunks from record, should raise an error
        record.chunks.pop();
        record.chunks.pop();
        let result = record.verify_pack(&packfile);
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
        let salt = pack_rec
            .crypto_salt
            .ok_or_else(|| anyhow!(format!("missing pack salt: {:?}", pack_rec.digest)))?;
        // retrieve the pack file
        let mut encrypted = PathBuf::new();
        encrypted.push(workspace);
        encrypted.push(pack_rec.digest.to_string());
        stores.retrieve_pack(&pack_rec.locations, &encrypted)?;
        // decrypt and then unpack the contents
        let mut tarball = encrypted.clone();
        tarball.set_extension("tar");
        crypto::decrypt_file(passphrase, &salt, &encrypted, &tarball)?;
        fs::remove_file(&encrypted)?;
        let file = fs::File::open(&tarball)?;
        let mut ar = tar::Archive::new(file);
        // This is an n^2 search which is fine because the number of chunks in a
        // typical pack file number in the tens, never any significant number.
        let mut found_count: usize = 0;
        for maybe_entry in ar.entries()? {
            let entry = maybe_entry?;
            // we know the names are valid UTF-8, we created them
            let digest = entities::Checksum::from_str(entry.path()?.to_str().unwrap())?;
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
                    tarball.display()
                ));
            }
        }
        // ensure we found all of the chunks
        fs::remove_file(tarball)?;
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
            pack_path.into_path().to_string_lossy().into(),
        );
        let store = entities::Store {
            id: "local123".to_owned(),
            store_type: entities::StoreType::LOCAL,
            label: "my local".to_owned(),
            properties: local_props,
        };
        dbase.put_store(&store)?;

        // create a dataset
        let fixture_base: PathBuf = ["test", "fixtures"].iter().collect();
        let mut dataset = entities::Dataset::new(&fixture_base);
        dataset = dataset.add_store("local123");
        dataset.pack_size = 131072 as u64;
        let computer_id = entities::Configuration::generate_unique_id("mr.ed", "stable");
        dbase.put_computer_id(&dataset.id, &computer_id)?;
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
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let mut driver = BackupDriver::new(&dataset, &dbase, &state, "secret123", None)?;
        let file1_digest = Checksum::SHA256(
            "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed".to_owned(),
        );
        let changed_file = ChangedFile::new(
            Path::new("../test/fixtures/SekienAkashita.jpg"),
            file1_digest.clone(),
        );
        driver.add_file(changed_file)?;
        let file2_digest = Checksum::SHA256(
            "59d1ff1b1df864f071e3efe637bc9009b28282a203002f52988bb9b01910a0b5".to_owned(),
        );
        let changed_file = ChangedFile::new(
            Path::new("../test/fixtures/baby-birth.jpg"),
            file2_digest.clone(),
        );
        driver.add_file(changed_file)?;
        let file3_digest = Checksum::SHA256(
            "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05".to_owned(),
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
            Checksum::SHA256(
                "c451d8d136529890c3ecc169177c036029d2b684f796f254bf795c96783fc483".to_owned(),
            ),
            Checksum::SHA256(
                "b4da74176d97674c78baa2765c77f0ccf4a9602f229f6d2b565cf94447ac7af0".to_owned(),
            ),
            // second file is a single chunk
            Checksum::SHA256(
                "59d1ff1b1df864f071e3efe637bc9009b28282a203002f52988bb9b01910a0b5".to_owned(),
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
        assert_eq!(file_rec.length, 3375);
        assert_eq!(file_rec.chunks.len(), 1);
        let maybe_pack = dbase.get_pack(&file_rec.chunks[0].1)?;
        assert!(maybe_pack.is_some());
        let pack_rec = maybe_pack.unwrap();
        assert_ne!(pack_rec.digest, pack_digest);

        // verify that the pack file actually contains the expected chunk(s)
        let chunks: Vec<entities::Checksum> = vec![Checksum::SHA256(
            "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05".to_owned(),
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
            pack_path.into_path().to_string_lossy().into(),
        );
        let store = entities::Store {
            id: "local123".to_owned(),
            store_type: entities::StoreType::LOCAL,
            label: "my local".to_owned(),
            properties: local_props,
        };
        dbase.put_store(&store)?;

        // create a dataset
        let fixture_base: PathBuf = ["test", "fixtures"].iter().collect();
        let mut dataset = entities::Dataset::new(&fixture_base);
        dataset = dataset.add_store("local123");
        dataset.pack_size = 524288 as u64;
        let computer_id = entities::Configuration::generate_unique_id("mr.ed", "stable");
        dbase.put_computer_id(&dataset.id, &computer_id)?;
        fs::create_dir_all(&dataset.workspace)?;
        let workspace: PathBuf = ["tmp", "test", "workspace"].iter().collect();
        fs::create_dir_all(&workspace)?;
        let stores: Arc<dyn PackRepository> = Arc::from(dbase.load_dataset_stores(&dataset)?);

        //
        // Create the driver and add the one large file that should result in
        // two pack files and seven chunks being generated.
        //
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let mut driver = BackupDriver::new(&dataset, &dbase, &state, "secret123", None)?;
        let file1_digest = Checksum::SHA256(
            "aafd64b759b896ceed90c88625c08f215f2a3b0a01ccf47e64239875c5710aa6".to_owned(),
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
        let mut pack_digests: HashSet<Checksum> = HashSet::new();
        for (_, checksum) in file_rec.chunks.iter() {
            let chunk_rec = dbase
                .get_chunk(&checksum)?
                .ok_or_else(|| anyhow!("missing chunk {}", checksum))?;
            assert!(chunk_rec.packfile.is_some());
            let pack_digest = chunk_rec.packfile.clone().unwrap();
            pack_digests.insert(pack_digest);
        }

        // verify that there are two packs and their records exist
        assert_eq!(pack_digests.len(), 2);
        for pack_digest in pack_digests.iter() {
            let maybe_pack = dbase.get_pack(&pack_digest)?;
            assert!(maybe_pack.is_some());
        }

        // verify the contents of the first pack file; the pack digests are
        // predictable and we also happen to know the chunk order
        let pack_digest = Checksum::SHA256(
            "c704183a8a25ba8f87eb949993edb2d596f11b3f2223d2a1430f747caababc32".to_owned(),
        );
        let pack_rec = dbase.get_pack(&pack_digest)?.unwrap();
        let chunks: Vec<entities::Checksum> = vec![
            Checksum::SHA256(
                "752262ccd96e1f3f47ce83475058630f1dafa55b15fa28ca7929815e084132d7".to_owned(),
            ),
            Checksum::SHA256(
                "062755f9c6f3463f18c82ae0c940326894029c51689eccd3b1dc6ba4a7158bc2".to_owned(),
            ),
            Checksum::SHA256(
                "cb02ed33bddb9a9279dae322655b2f9906318e60c0837b57bf292449d137330b".to_owned(),
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
        let pack_digest = Checksum::SHA256(
            "0fe5d765309795ff4de3440c64d019528c869c6b96634e4227d8e6d3e89f675d".to_owned(),
        );
        let pack_rec = dbase.get_pack(&pack_digest)?.unwrap();
        let chunks: Vec<entities::Checksum> = vec![
            Checksum::SHA256(
                "c8b5e0313ad3265424ece141a011523eca2f573748c648932314f4e069358381".to_owned(),
            ),
            Checksum::SHA256(
                "af529e3b7ec8aab0ab91cf53868858a36602340c6098759469dc6c9a08cb6c3a".to_owned(),
            ),
            Checksum::SHA256(
                "2757840ed97fbfbc5ce6985c5e6090ac9e3834999fe9f60b679c308f26797e69".to_owned(),
            ),
            Checksum::SHA256(
                "f9c35688f670649ffb1e0d58f7a19c46926a6ff2c89acae3940605f70a65614e".to_owned(),
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
