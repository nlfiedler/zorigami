//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::data::sources::{
    EntityDataSource, PackDataSource, PackSourceBuilder, PackSourceBuilderImpl,
};
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, PackLocation, RecordCounts, Snapshot,
    Store, Tree,
};
use crate::domain::repositories::{PackRepository, RecordRepository};
use anyhow::{anyhow, Context, Error, Result};
use lazy_static::lazy_static;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use store_core::CollisionError;

lazy_static! {
    // Name that will be returned by get_bucket_name(), unless of course it is
    // blank, in which case a new name will be generated.
    static ref BUCKET_NAME: Mutex<String> = Mutex::new("".into());
    // Number of times get_bucket_name() has been called and returned the same
    // bucket name. When the value is zero, a new name is generated.
    static ref NAME_COUNT: Mutex<usize> = Mutex::new(0);
}

// Use an `Arc` to hold the data source to make cloning easy for the caller. If
// using a `Box` instead, cloning it would involve adding fake clone operations
// to the data source trait, which works, but is ugly. It gets even uglier when
// mocking the calls on the data source, which gets cloned during the test.
pub struct RecordRepositoryImpl {
    datasource: Arc<dyn EntityDataSource>,
}

impl RecordRepositoryImpl {
    pub fn new(datasource: Arc<dyn EntityDataSource>) -> Self {
        Self { datasource }
    }
}

impl RecordRepository for RecordRepositoryImpl {
    fn get_configuration(&self) -> Result<Configuration, Error> {
        if let Some(conf) = self.datasource.get_configuration()? {
            return Ok(conf);
        }
        let config: Configuration = Default::default();
        self.datasource.put_configuration(&config)?;
        Ok(config)
    }

    fn get_excludes(&self) -> Vec<PathBuf> {
        let path = self.datasource.get_db_path();
        vec![path]
    }

    fn put_computer_id(&self, dataset: &str, computer_id: &str) -> Result<(), Error> {
        self.datasource.put_computer_id(dataset, computer_id)
    }

    fn get_computer_id(&self, dataset: &str) -> Result<Option<String>, Error> {
        self.datasource.get_computer_id(dataset)
    }

    fn delete_computer_id(&self, dataset: &str) -> Result<(), Error> {
        self.datasource.delete_computer_id(dataset)
    }

    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error> {
        self.datasource.put_latest_snapshot(dataset, latest)
    }

    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error> {
        self.datasource.get_latest_snapshot(dataset)
    }

    fn delete_latest_snapshot(&self, dataset: &str) -> Result<(), Error> {
        self.datasource.delete_latest_snapshot(dataset)
    }

    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error> {
        self.datasource.insert_chunk(chunk)
    }

    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error> {
        self.datasource.get_chunk(digest)
    }

    fn insert_pack(&self, pack: &Pack) -> Result<(), Error> {
        self.datasource.insert_pack(pack)
    }

    fn put_pack(&self, pack: &Pack) -> Result<(), Error> {
        self.datasource.put_pack(pack)
    }

    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        self.datasource.get_pack(digest)
    }

    fn get_packs(&self, store_id: &str) -> Result<Vec<Pack>, Error> {
        self.datasource.get_packs(store_id)
    }

    fn get_all_packs(&self) -> Result<Vec<Pack>, Error> {
        self.datasource.get_all_packs()
    }

    fn insert_database(&self, pack: &Pack) -> Result<(), Error> {
        self.datasource.insert_database(pack)
    }

    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        self.datasource.get_database(digest)
    }

    fn get_databases(&self) -> Result<Vec<Pack>, Error> {
        self.datasource.get_databases()
    }

    fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error> {
        self.datasource.insert_xattr(digest, xattr)
    }

    fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error> {
        self.datasource.get_xattr(digest)
    }

    fn insert_file(&self, file: &File) -> Result<(), Error> {
        self.datasource.insert_file(file)
    }

    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error> {
        self.datasource.get_file(digest)
    }

    fn insert_tree(&self, tree: &Tree) -> Result<(), Error> {
        self.datasource.insert_tree(tree)
    }

    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error> {
        self.datasource.get_tree(digest)
    }

    fn put_store(&self, store: &Store) -> Result<(), Error> {
        // validate the store configuration
        let builder = PackSourceBuilderImpl {};
        builder.build_source(store)?;
        self.datasource.put_store(store)
    }

    fn get_stores(&self) -> Result<Vec<Store>, Error> {
        self.datasource.get_stores()
    }

    fn get_store(&self, id: &str) -> Result<Option<Store>, Error> {
        self.datasource.get_store(id)
    }

    fn delete_store(&self, id: &str) -> Result<(), Error> {
        self.datasource.delete_store(id)
    }

    fn load_dataset_stores(&self, dataset: &Dataset) -> Result<Box<dyn PackRepository>, Error> {
        let stores: Vec<Store> = dataset
            .stores
            .iter()
            .map(|store_id| self.get_store(store_id))
            .filter_map(|s| s.ok())
            .flatten()
            .collect();
        if stores.is_empty() {
            return Err(anyhow!(format!(
                "no stores found for dataset {}",
                dataset.id
            )));
        }
        let store_builder = Box::new(PackSourceBuilderImpl {});
        let packs: Box<dyn PackRepository> =
            Box::new(PackRepositoryImpl::new(stores, store_builder)?);
        Ok(packs)
    }

    fn build_pack_repo(&self, store: &Store) -> Result<Box<dyn PackRepository>, Error> {
        let stores: Vec<Store> = vec![store.to_owned()];
        let store_builder = Box::new(PackSourceBuilderImpl {});
        let pack: Box<dyn PackRepository> =
            Box::new(PackRepositoryImpl::new(stores, store_builder)?);
        Ok(pack)
    }

    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error> {
        self.datasource.put_dataset(dataset)
    }

    fn get_datasets(&self) -> Result<Vec<Dataset>, Error> {
        self.datasource.get_datasets()
    }

    fn get_dataset(&self, id: &str) -> Result<Option<Dataset>, Error> {
        self.datasource.get_dataset(id)
    }

    fn delete_dataset(&self, id: &str) -> Result<(), Error> {
        self.datasource.delete_dataset(id)
    }

    fn put_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error> {
        self.datasource.put_snapshot(snapshot)
    }

    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error> {
        self.datasource.get_snapshot(digest)
    }

    fn create_backup(&self, password: &str) -> Result<tempfile::TempPath, Error> {
        let backup_path = self.datasource.create_backup(None)?;
        let file = tempfile::NamedTempFile::new()?;
        let path = file.into_temp_path();
        create_archive(&backup_path, &path, password)?;
        Ok(path)
    }

    fn restore_from_backup(&self, path: &Path, password: &str) -> Result<(), Error> {
        let tempdir = tempfile::tempdir()?;
        let temppath = tempdir.path().to_path_buf();
        extract_archive(path, &temppath, password)?;
        self.datasource.restore_from_backup(Some(temppath))
    }

    fn get_entity_counts(&self) -> Result<RecordCounts, Error> {
        self.datasource.get_entity_counts()
    }
}

///
/// Create a compressed archive from the given directory structure.
///
fn create_archive(basepath: &Path, outfile: &Path, password: &str) -> Result<(), Error> {
    let output = std::fs::File::create(outfile)?;
    let mut writer = exaf_rs::writer::Writer::new(output)?;
    writer.enable_encryption(
        exaf_rs::KeyDerivation::Argon2id,
        exaf_rs::Encryption::AES256GCM,
        password,
    )?;
    // Simply calling add_dir_all() on basepath will result in the name of the
    // basepath directory being a part of the archive entry paths, so instead
    // examine the entries within basepath and add them individually.
    let readdir = std::fs::read_dir(basepath)?;
    for entry_result in readdir {
        let entry = entry_result?;
        let path = entry.path();
        // DirEntry.metadata() does not follow symlinks and that is good
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            writer.add_dir_all(path)?;
        } else if metadata.is_file() {
            writer.add_file(path, None)?;
        } else if metadata.is_symlink() {
            writer.add_symlink(path, None)?;
        }
    }
    writer.finish()?;
    Ok(())
}

///
/// Extract the contents of the compressed archive to the given directory.
///
fn extract_archive(infile: &Path, outdir: &Path, password: &str) -> Result<(), Error> {
    let mut reader = exaf_rs::reader::from_file(infile)?;
    reader.enable_encryption(password)?;
    reader.extract_all(&outdir)?;
    Ok(())
}

pub struct PackRepositoryImpl {
    sources: HashMap<Store, Box<dyn PackDataSource>>,
}

impl PackRepositoryImpl {
    /// Construct a pack repository that will delegate to the given stores.
    ///
    /// Defers to the provided builder to construct the pack sources.
    pub fn new(stores: Vec<Store>, builder: Box<dyn PackSourceBuilder>) -> Result<Self, Error> {
        let mut sources: HashMap<Store, Box<dyn PackDataSource>> = HashMap::new();
        for store in stores {
            let source = builder.build_source(&store)?;
            sources.insert(store, source);
        }
        Ok(Self { sources })
    }

    // Use the old bucket name to generate a new one.
    fn get_new_bucket_name(&self, bucket_name: &str) -> String {
        let mut count = NAME_COUNT.lock().unwrap();
        *count = 0;
        let mut name = BUCKET_NAME.lock().unwrap();
        *name = generate_new_bucket_name(bucket_name);
        name.clone()
    }

    // Try to store the pack file up to three times before giving up.
    fn store_pack_retry(
        &self,
        source: &Box<dyn PackDataSource>,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> anyhow::Result<PackLocation, Error> {
        let mut retries = 3;
        let mut bucket_name: String = bucket.to_owned();
        // Loop a few times if the pack store (typically a network operation)
        // fails, in case it is successful on retry. If there is a bucket
        // collision, generate a new bucket name for that store (and each one
        // after), returning the updated pack location.
        loop {
            match source.store_pack(packfile, &bucket_name, object) {
                Ok(coords) => return Ok(coords),
                Err(err) => match err.downcast::<CollisionError>() {
                    Ok(_) => {
                        bucket_name = self.get_new_bucket_name(&bucket_name);
                    }
                    Err(e) => {
                        retries -= 1;
                        if retries == 0 {
                            return Err(e);
                        }
                        warn!("pack store failed, will retry: {:?}", e);
                    }
                },
            }
        }
    }
}

impl PackRepository for PackRepositoryImpl {
    fn get_bucket_name(&self, computer_id: &str) -> String {
        let mut count = NAME_COUNT.lock().unwrap();
        if *count == 0 {
            let mut name = BUCKET_NAME.lock().unwrap();
            *name = generate_bucket_name(computer_id);
        }
        *count += 1;
        if *count > 127 {
            *count = 0;
        }
        let name = BUCKET_NAME.lock().unwrap();
        name.clone()
    }

    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Vec<PackLocation>, Error> {
        let mut results: Vec<PackLocation> = Vec::new();
        for (store, source) in self.sources.iter() {
            let ctx = format!(
                "pack store {} ({}) failed for {}/{}",
                store.id, store.label, bucket, object
            );
            let loc = self
                .store_pack_retry(source, packfile, bucket, object)
                .context(ctx)?;
            results.push(loc)
        }
        Ok(results)
    }

    fn retrieve_pack(&self, locations: &[PackLocation], outfile: &Path) -> Result<(), Error> {
        // find a local store, if available
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id && source.is_local() {
                    let result = source.retrieve_pack(loc, outfile);
                    if result.is_ok() {
                        return result;
                    }
                    warn!(
                        "pack retrieval failed, will try another source: {:?}",
                        result
                    );
                }
            }
        }

        // find a store that is not slow, if available
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id && !source.is_slow() {
                    let result = source.retrieve_pack(loc, outfile);
                    if result.is_ok() {
                        return result;
                    }
                    warn!(
                        "pack retrieval failed, will try another source: {:?}",
                        result
                    );
                }
            }
        }

        // find any matching store
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id {
                    let result = source.retrieve_pack(loc, outfile);
                    if result.is_ok() {
                        return result;
                    }
                    warn!(
                        "pack retrieval failed, will try another source: {:?}",
                        result
                    );
                }
            }
        }

        Err(anyhow!("unable to retrieve pack file: {:?}", locations))
    }

    fn test_store(&self, store_id: &str) -> Result<(), Error> {
        for (store, source) in self.sources.iter() {
            if store_id == store.id {
                let _ = source.list_buckets()?;
                break;
            }
        }
        Ok(())
    }

    fn store_database(&self, computer_id: &str, infile: &Path) -> Result<Vec<PackLocation>, Error> {
        // Use a ULID as the object name so they sort by time which will make
        // it easier to find the latest database archive later.
        let object = ulid::Ulid::new().to_string();
        // Use a predictable bucket name so we can find it easily later.
        let bucket = computer_bucket_name(computer_id);
        let mut results: Vec<PackLocation> = Vec::new();
        for (store, source) in self.sources.iter() {
            let ctx = format!(
                "database store {} ({}) failed for {}/{}",
                store.id, store.label, bucket, object
            );
            let loc = store_database_retry(source, infile, &bucket, &object).context(ctx)?;
            results.push(loc)
        }
        Ok(results)
    }

    fn retrieve_latest_database(&self, computer_id: &str, outfile: &Path) -> Result<(), Error> {
        let bucket_name = computer_bucket_name(computer_id);
        // use the first store returned by the iterator, probably only one anyway
        if let Some((store, source)) = self.sources.iter().next() {
            let mut objects = source.list_databases(&bucket_name)?;
            objects.sort();
            if let Some(latest) = objects.last() {
                let loc = PackLocation::new(&store.id, &bucket_name, latest);
                source
                    .retrieve_database(&loc, outfile)
                    .context("database archive retrieval")?;
                return Ok(());
            } else {
                return Err(anyhow!("no database archives available"));
            }
        }
        Err(anyhow!("no matching store found"))
    }

    fn find_missing(&self, store_id: &str, packs: &[Pack]) -> Result<Vec<Checksum>, Error> {
        for (store, source) in self.sources.iter() {
            if store.id == store_id {
                // Rather than trying to make this fast by using up a lot of
                // memory (i.e. making sets of store/bucket/object tuples), just
                // scan through the entire pack list B*O times. In the average
                // case, the list of packs will not get any smaller so it will
                // end up being an O(B*O*P) operation, which is regretable.
                let mut missing_packs = packs.to_owned();
                let buckets = source.list_buckets()?;
                for bucket in buckets.iter() {
                    info!("find_missing scanning bucket {}", bucket);
                    let objects = source.list_objects(bucket.as_str())?;
                    for object in objects.iter() {
                        // remove any packs that have a location that matches
                        // the current store/bucket/object tuple
                        let not_yet_missing = missing_packs.into_iter().filter(|p| {
                            p.locations.iter().all(|l| {
                                l.store != store_id
                                    || l.bucket != bucket.as_str()
                                    || l.object != object.as_str()
                            })
                        });
                        missing_packs = not_yet_missing.collect();
                    }
                }
                let digests = missing_packs.into_iter().map(|p| p.digest).collect();
                return Ok(digests);
            }
        }
        Err(anyhow!("no matching store found"))
    }

    fn prune_extra(&self, store_id: &str, packs: &[Pack]) -> Result<u32, Error> {
        for (store, source) in self.sources.iter() {
            if store.id == store_id {
                let mut count: u32 = 0;
                let buckets = source.list_buckets()?;
                for bucket in buckets.iter() {
                    info!("prune_extra scanning bucket {}", bucket);
                    if is_bucket_referenced(store_id, bucket, packs) {
                        count += remove_objects(store_id, bucket, source, packs)?;
                    } else {
                        count += remove_bucket(bucket, source)?;
                    }
                }
                return Ok(count);
            }
        }
        Err(anyhow!("no matching store found"))
    }
}

///
/// Return the unique bucket name for this computer and user.
///
pub fn computer_bucket_name(unique_id: &str) -> String {
    match blob_uuid::to_uuid(unique_id) {
        Ok(uuid) => uuid.simple().to_string(),
        Err(err) => {
            error!("failed to convert unique ID: {:?}", err);
            ulid::Ulid::new().to_string().to_lowercase()
        }
    }
}

// Generate a suitable bucket name, using a ULID and the given unique ID.
//
// The unique ID is assumed to be a shorted version of the UUID returned from
// `generate_unique_id()`, and will be converted back to a full UUID for the
// purposes of generating a bucket name consisting only of lowercase letters.
fn generate_bucket_name(unique_id: &str) -> String {
    match blob_uuid::to_uuid(unique_id) {
        Ok(uuid) => {
            let shorter = uuid.simple().to_string();
            let mut ulid = ulid::Ulid::new().to_string();
            ulid.push_str(&shorter);
            ulid.to_lowercase()
        }
        Err(err) => {
            error!("failed to convert unique ID: {:?}", err);
            ulid::Ulid::new().to_string().to_lowercase()
        }
    }
}

// Split the given bucket name apart and generate a new prefix such that the new
// name will be different and possibly work-around a collision error.
fn generate_new_bucket_name(bucket_name: &str) -> String {
    // ULID as ASCII is 26 characters long, computer id is everything else
    let suffix = &bucket_name[26..];
    let mut ulid = ulid::Ulid::new().to_string();
    ulid.push_str(suffix);
    ulid.to_lowercase()
}

// Determine if the named bucket is referenced by any of the packs.
//
// Returns `true` if the bucket is referenced by at least one pack, and `false`
// if none of the packs references the bucket.
fn is_bucket_referenced(store: &str, bucket: &str, packs: &[Pack]) -> bool {
    for pack in packs.iter() {
        for location in pack.locations.iter() {
            if store == location.store && bucket == location.bucket {
                return true;
            }
        }
    }
    false
}

// Remove all unreferenced objects from the bucket.
//
// If the bucket becomes empty, remove it.
fn remove_objects(
    store: &str,
    bucket: &str,
    source: &Box<dyn PackDataSource>,
    packs: &[Pack],
) -> Result<u32, Error> {
    // build a set of object names associated with store_id+bucket
    let mut bucket_objects: HashSet<String> = HashSet::new();
    for pack in packs.iter() {
        for location in pack.locations.iter() {
            if store == location.store && bucket == location.bucket {
                bucket_objects.insert(location.object.clone());
            }
        }
    }
    // delete all objects not referenced by the set
    let objects = source.list_objects(bucket)?;
    let mut deleted: usize = 0;
    for object in objects.iter() {
        if !bucket_objects.contains(object) {
            info!("remove_objects: deleting object {}", object);
            source.delete_object(bucket, object)?;
            deleted += 1;
        }
    }
    // delete bucket if all objects within were deleted
    if deleted == objects.len() {
        info!("remove_objects: deleting bucket {}", bucket);
        source.delete_bucket(bucket)?;
    }
    Ok(deleted as u32)
}

// Remove all objects from the bucket, and the bucket itself.
//
// Return the number of objects in the bucket that were removed.
fn remove_bucket(bucket: &str, source: &Box<dyn PackDataSource>) -> Result<u32, Error> {
    let objects = source.list_objects(bucket)?;
    for object in objects.iter() {
        info!("remove_bucket: deleting object {}", object);
        source.delete_object(bucket, object)?;
    }
    info!("remove_bucket: deleting bucket {}", bucket);
    source.delete_bucket(bucket)?;
    Ok(objects.len() as u32)
}

// Try to store the database archive up to three times before giving up.
fn store_database_retry(
    source: &Box<dyn PackDataSource>,
    packfile: &Path,
    bucket: &str,
    object: &str,
) -> anyhow::Result<PackLocation, Error> {
    let mut retries = 3;
    loop {
        let result = source.store_database(packfile, bucket, object);
        if result.is_ok() {
            return result;
        }
        retries -= 1;
        if retries == 0 {
            return result;
        }
        warn!("database store failed, will retry: {:?}", result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::sources::{MockEntityDataSource, MockPackDataSource, MockPackSourceBuilder};
    use crate::domain::entities::{PackLocation, StoreType};
    use mockall::predicate::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    #[test]
    fn test_get_configuration() {
        // arrange
        let config: Configuration = Default::default();
        let mut mock = MockEntityDataSource::new();
        let mut call_count = 0;
        mock.expect_get_configuration().times(2).returning(move || {
            call_count += 1;
            if call_count > 1 {
                Ok(Some(config.clone()))
            } else {
                Ok(None)
            }
        });
        mock.expect_put_configuration()
            .times(1)
            .returning(|_| Ok(()));
        // act & assert
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_configuration();
        assert!(result.is_ok());
        let result = repo.get_configuration();
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_bucket_name() {
        let uuid = Configuration::generate_unique_id("charlie", "localhost");
        let bucket = generate_bucket_name(&uuid);
        // Ensure the generated name is safe for the "cloud", which so far means
        // Google Cloud Storage and Amazon Glacier. It needs to be reasonably
        // short, must consist only of lowercase letters or digits.
        assert_eq!(bucket.len(), 58, "bucket name is 58 characters");
        for c in bucket.chars() {
            assert!(c.is_ascii_alphanumeric());
            if c.is_ascii_alphabetic() {
                assert!(c.is_ascii_lowercase());
            }
        }
    }

    #[test]
    fn test_load_dataset_stores_ok() {
        // arrange
        let mut local_props: HashMap<String, String> = HashMap::new();
        local_props.insert("basepath".to_owned(), "/data/packs".to_owned());
        let store = Store {
            id: "local123".to_owned(),
            store_type: StoreType::LOCAL,
            label: "my local".to_owned(),
            properties: local_props,
        };
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_store()
            .with(eq("local123"))
            .returning(move |_| Ok(Some(store.clone())));
        // act
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        dataset = dataset.add_store("local123");
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.load_dataset_stores(&dataset);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_dataset_stores_none() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_store()
            .with(eq("local123"))
            .returning(move |_| Ok(None));
        // act
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        dataset = dataset.add_store("local123");
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.load_dataset_stores(&dataset);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no stores found for dataset"));
    }

    #[test]
    fn test_build_pack_repo() {
        // arrange
        let mut local_props: HashMap<String, String> = HashMap::new();
        local_props.insert("basepath".to_owned(), "/data/packs".to_owned());
        let store = Store {
            id: "local123".to_owned(),
            store_type: StoreType::LOCAL,
            label: "my local".to_owned(),
            properties: local_props,
        };
        let mock = MockEntityDataSource::new();
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.build_pack_repo(&store);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_restore_database() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_create_backup()
            .returning(|_| Ok(PathBuf::from("../test/features")));
        mock.expect_restore_from_backup().returning(|_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.create_backup("Secret123");
        // assert
        assert!(result.is_ok());
        // act
        let archive_path = result.unwrap();
        let result = repo.restore_from_backup(&archive_path, "Secret123");
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_bucket_name() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder
            .expect_build_source()
            .returning(|_| Ok(Box::new(MockPackDataSource::new())));
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let computer_id = Configuration::generate_unique_id("charlie", "localhost");
        let actual = repo.get_bucket_name(&computer_id);
        // assert
        assert!(!actual.is_empty());
        let again = repo.get_bucket_name(&computer_id);
        assert_eq!(actual, again);
        for _ in 1..200 {
            repo.get_bucket_name(&computer_id);
        }
        let last1 = repo.get_bucket_name(&computer_id);
        assert_ne!(actual, last1);
    }

    #[test]
    fn test_get_new_bucket_name() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder
            .expect_build_source()
            .returning(|_| Ok(Box::new(MockPackDataSource::new())));
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let computer_id = Configuration::generate_unique_id("charlie", "localhost");
        let first = repo.get_bucket_name(&computer_id);
        // assert
        assert!(!first.is_empty());
        let second = repo.get_new_bucket_name(&first);
        assert_ne!(first, second);
        assert_eq!(first.len(), second.len());
        assert_eq!(&first[26..], &second[26..]);
    }

    #[test]
    fn test_store_pack_collision() {
        // arrange
        let computer_id = Configuration::generate_unique_id("charlie", "localhost");
        let bucket_name = generate_bucket_name(&computer_id);
        let name_clone = bucket_name.clone();
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(move |_| {
            let mut source = MockPackDataSource::new();
            let name1 = name_clone.clone();
            let name2 = name_clone.clone();
            source
                .expect_store_pack()
                .with(always(), eq(name1), always())
                .returning(|_, _, _| Err(Error::from(CollisionError {})));
            source
                .expect_store_pack()
                .with(always(), ne(name2), always())
                .returning(|_, bucket, object| Ok(PackLocation::new("store", bucket, object)));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.store_pack(&input_file, &bucket_name, "object1");
        // assert
        assert!(result.is_ok());
        let locations = result.unwrap();
        assert_eq!(locations.len(), 1);
        let location = &locations[0];
        assert_ne!(location.bucket, bucket_name);
        assert_eq!(&location.bucket[26..], &bucket_name[26..]);
    }

    #[test]
    fn test_store_pack_single_source() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source
                .expect_store_pack()
                .with(always(), eq("bucket1"), eq("object1"))
                .returning(|_, bucket, object| Ok(PackLocation::new("store", bucket, object)));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.store_pack(&input_file, "bucket1", "object1");
        // assert
        assert!(result.is_ok());
        let locations = result.unwrap();
        assert_eq!(locations.len(), 1);
    }

    #[test]
    fn test_store_pack_multiple_sources() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source
                .expect_store_pack()
                .with(always(), eq("bucket1"), eq("object1"))
                .returning(|_, bucket, object| Ok(PackLocation::new("store", bucket, object)));
            Ok(Box::new(source))
        });
        let stores = vec![
            Store {
                id: "localtmp".to_owned(),
                store_type: StoreType::LOCAL,
                label: "temporary".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "minio".to_owned(),
                store_type: StoreType::MINIO,
                label: "server".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "secureftp".to_owned(),
                store_type: StoreType::SFTP,
                label: "other_server".to_owned(),
                properties: HashMap::new(),
            },
        ];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.store_pack(&input_file, "bucket1", "object1");
        // assert
        assert!(result.is_ok());
        let locations = result.unwrap();
        assert_eq!(locations.len(), 3);
    }

    #[test]
    fn test_retrieve_pack_multiple_local() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().times(3).returning(|store| {
            if store.store_type == StoreType::LOCAL {
                let mut source = MockPackDataSource::new();
                source.expect_is_local().returning(|| true);
                source
                    .expect_retrieve_pack()
                    .withf(|location, _| location.store == "local123")
                    .returning(|_, _| Ok(()));
                Ok(Box::new(source))
            } else if store.store_type == StoreType::MINIO {
                let mut source = MockPackDataSource::new();
                source.expect_is_local().times(0..2).returning(|| false);
                source.expect_is_slow().times(0..2).returning(|| false);
                Ok(Box::new(source))
            } else {
                // lump all other store types into this clause
                let mut source = MockPackDataSource::new();
                source.expect_is_local().times(0..2).returning(|| false);
                source.expect_is_slow().times(0..2).returning(|| true);
                Ok(Box::new(source))
            }
        });
        let stores = vec![
            Store {
                id: "local123".to_owned(),
                store_type: StoreType::LOCAL,
                label: "temporary".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "minio123".to_owned(),
                store_type: StoreType::MINIO,
                label: "server".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "sftp123".to_owned(),
                store_type: StoreType::SFTP,
                label: "other_server".to_owned(),
                properties: HashMap::new(),
            },
        ];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let locations = vec![
            PackLocation::new("local123", "bucket1", "object1"),
            PackLocation::new("minio123", "bucket1", "object1"),
            PackLocation::new("sftp123", "bucket1", "object1"),
        ];
        let output_file = PathBuf::from("/home/planet/restored.txt");
        let result = repo.retrieve_pack(&locations, &output_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_retrieve_pack_multiple_fast() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().times(2).returning(|store| {
            if store.store_type == StoreType::MINIO {
                let mut source = MockPackDataSource::new();
                source.expect_is_local().times(0..2).returning(|| false);
                source.expect_is_slow().times(0..2).returning(|| false);
                source
                    .expect_retrieve_pack()
                    .withf(|location, _| location.store == "minio123")
                    .returning(|_, _| Ok(()));
                Ok(Box::new(source))
            } else {
                // lump all other store types into this clause
                let mut source = MockPackDataSource::new();
                source.expect_is_local().times(0..2).returning(|| false);
                source.expect_is_slow().times(0..2).returning(|| true);
                Ok(Box::new(source))
            }
        });
        let stores = vec![
            Store {
                id: "minio123".to_owned(),
                store_type: StoreType::MINIO,
                label: "server".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "sftp123".to_owned(),
                store_type: StoreType::SFTP,
                label: "other_server".to_owned(),
                properties: HashMap::new(),
            },
        ];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        // pass more locations than defined stores just because
        let locations = vec![
            PackLocation::new("local123", "bucket1", "object1"),
            PackLocation::new("minio123", "bucket1", "object1"),
            PackLocation::new("sftp123", "bucket1", "object1"),
        ];
        let output_file = PathBuf::from("/home/planet/restored.txt");
        let result = repo.retrieve_pack(&locations, &output_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_retrieve_pack_multiple_any() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().times(1).returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_is_local().times(1).returning(|| false);
            source.expect_is_slow().times(1).returning(|| true);
            source
                .expect_retrieve_pack()
                .withf(|location, _| location.store == "sftp123")
                .returning(|_, _| Ok(()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "sftp123".to_owned(),
            store_type: StoreType::SFTP,
            label: "other_server".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        // pass more locations than defined stores just because
        let locations = vec![
            PackLocation::new("local123", "bucket1", "object1"),
            PackLocation::new("minio123", "bucket1", "object1"),
            PackLocation::new("sftp123", "bucket1", "object1"),
        ];
        let output_file = PathBuf::from("/home/planet/restored.txt");
        let result = repo.retrieve_pack(&locations, &output_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_retrieve_pack_multiple_err() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder
            .expect_build_source()
            .times(1)
            .returning(|_| Ok(Box::new(MockPackDataSource::new())));
        let stores = vec![Store {
            id: "sftp123".to_owned(),
            store_type: StoreType::SFTP,
            label: "other_server".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        // none of the locations match any of the defined stores
        let locations = vec![
            PackLocation::new("local123", "bucket1", "object1"),
            PackLocation::new("minio123", "bucket1", "object1"),
        ];
        let output_file = PathBuf::from("/home/planet/restored.txt");
        let result = repo.retrieve_pack(&locations, &output_file);
        // assert
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("unable to retrieve pack file"));
    }

    #[test]
    fn test_test_store() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| Ok(Vec::new()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let result = repo.test_store("localtmp");
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_store_database() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source
                .expect_store_database()
                .returning(|_, bucket, object| Ok(PackLocation::new("store", bucket, object)));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.store_database("hal9000", &input_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_retrieve_latest_database_none() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_databases().returning(|_| Ok(Vec::new()));
            source.expect_retrieve_database().returning(|_, _| Ok(()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.retrieve_latest_database("hal9000", &input_file);
        // assert
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("no database archives available"));
    }

    #[test]
    fn test_retrieve_latest_database_single() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source
                .expect_list_databases()
                .returning(|_| Ok(vec!["007".to_owned()]));
            source
                .expect_retrieve_database()
                .withf(|location, _| location.object == "007")
                .returning(|_, _| Ok(()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.retrieve_latest_database("hal9000", &input_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_retrieve_latest_database_multiple() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_databases().returning(|_| {
                let objects = vec![
                    "01arz3ndektsv4rrffq69g5fav".to_owned(),
                    "01edn29q3m3n7ccpd2sfh4244b".to_owned(),
                    "01ce0d526z6cyzgm02ap0jv281".to_owned(),
                ];
                Ok(objects)
            });
            source
                .expect_retrieve_database()
                .withf(|loc, _| loc.object == "01edn29q3m3n7ccpd2sfh4244b")
                .returning(|_, _| Ok(()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.retrieve_latest_database("hal9000", &input_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_missing_no_store() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let source = MockPackDataSource::new();
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let packs: Vec<Pack> = vec![];
        let result = repo.find_missing("nostore", &packs);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no matching store found"));
    }

    #[test]
    fn test_find_missing_no_buckets() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| Ok(Vec::new()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object1")];
        let pack = Pack::new(digest.clone(), coords);
        let packs: Vec<Pack> = vec![pack];
        let result = repo.find_missing("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        let missing_packs = result.unwrap();
        assert_eq!(missing_packs.len(), 1);
        assert_eq!(
            missing_packs[0].to_string(),
            "sha1-ed841695851abdcfe6a50ce3d01d770eb053356b"
        );
    }

    #[test]
    fn test_find_missing_no_objects() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| {
                let buckets = vec!["bucket1".to_owned()];
                Ok(buckets)
            });
            source
                .expect_list_objects()
                .with(eq("bucket1"))
                .returning(|_| Ok(Vec::new()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object1")];
        let pack = Pack::new(digest.clone(), coords);
        let packs: Vec<Pack> = vec![pack];
        let result = repo.find_missing("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        let missing_packs = result.unwrap();
        assert_eq!(missing_packs.len(), 1);
        assert_eq!(
            missing_packs[0].to_string(),
            "sha1-ed841695851abdcfe6a50ce3d01d770eb053356b"
        );
    }

    #[test]
    fn test_find_missing_empty_pack_list() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| {
                let buckets = vec!["bucket1".to_owned()];
                Ok(buckets)
            });
            source
                .expect_list_objects()
                .with(eq("bucket1"))
                .returning(|_| {
                    let objects = vec!["object1".to_owned()];
                    Ok(objects)
                });
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let packs: Vec<Pack> = Vec::new();
        let result = repo.find_missing("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        let missing_packs = result.unwrap();
        assert_eq!(missing_packs.len(), 0);
    }

    #[test]
    fn test_find_missing_no_missing() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| {
                let buckets = vec!["bucket1".to_owned()];
                Ok(buckets)
            });
            source
                .expect_list_objects()
                .with(eq("bucket1"))
                .returning(|_| {
                    let objects = vec!["object1".to_owned()];
                    Ok(objects)
                });
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object1")];
        let pack = Pack::new(digest.clone(), coords);
        let packs: Vec<Pack> = vec![pack];
        let result = repo.find_missing("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        let missing_packs = result.unwrap();
        assert_eq!(missing_packs.len(), 0);
    }

    #[test]
    fn test_find_missing_some_missing() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| {
                let buckets = vec!["bucket1".to_owned()];
                Ok(buckets)
            });
            source
                .expect_list_objects()
                .with(eq("bucket1"))
                .returning(|_| {
                    let objects = vec!["object1".to_owned(), "object3".to_owned()];
                    Ok(objects)
                });
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
        let coords = vec![
            PackLocation::new("localtmp", "bucket1", "object1"),
            PackLocation::new("remotely", "bucket1", "object1"),
        ];
        let pack1 = Pack::new(digest.clone(), coords);
        let digest = Checksum::SHA1(String::from("ad9fec27d7e071f5380af0c1499b651e9fadfb48"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object2")];
        let pack2 = Pack::new(digest.clone(), coords);
        let digest = Checksum::SHA1(String::from("849e2de5cc0fdd047982f4606840f956c9d1c8a1"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object3")];
        let pack3 = Pack::new(digest.clone(), coords);
        let packs: Vec<Pack> = vec![pack1, pack2, pack3];
        let result = repo.find_missing("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        let missing_packs = result.unwrap();
        assert_eq!(missing_packs.len(), 1);
        assert_eq!(
            missing_packs[0].to_string(),
            "sha1-ad9fec27d7e071f5380af0c1499b651e9fadfb48"
        );
    }

    #[test]
    fn test_prune_extra_no_store() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let source = MockPackDataSource::new();
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let packs: Vec<Pack> = vec![];
        let result = repo.prune_extra("nostore", &packs);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no matching store found"));
    }

    #[test]
    fn test_prune_extra_no_buckets() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| Ok(Vec::new()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object1")];
        let pack = Pack::new(digest.clone(), coords);
        let packs: Vec<Pack> = vec![pack];
        let result = repo.prune_extra("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_prune_extra_no_objects() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| {
                let buckets = vec!["bucket1".to_owned()];
                Ok(buckets)
            });
            source
                .expect_list_objects()
                .with(eq("bucket1"))
                .returning(|_| Ok(Vec::new()));
            // empty bucket gets deleted regardless
            source
                .expect_delete_bucket()
                .with(eq("bucket1"))
                .returning(|_| Ok(()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object1")];
        let pack = Pack::new(digest.clone(), coords);
        let packs: Vec<Pack> = vec![pack];
        let result = repo.prune_extra("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_prune_extra_empty_pack_list() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| {
                let buckets = vec!["bucket1".to_owned()];
                Ok(buckets)
            });
            source
                .expect_list_objects()
                .with(eq("bucket1"))
                .returning(|_| {
                    let objects = vec!["object1".to_owned()];
                    Ok(objects)
                });
            // unreferenced object and now empty bucket are removed
            source
                .expect_delete_object()
                .with(eq("bucket1"), eq("object1"))
                .returning(|_, _| Ok(()));
            source
                .expect_delete_bucket()
                .with(eq("bucket1"))
                .returning(|_| Ok(()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let packs: Vec<Pack> = Vec::new();
        let result = repo.prune_extra("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_prune_extra_no_extra() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| {
                let buckets = vec!["bucket1".to_owned()];
                Ok(buckets)
            });
            source
                .expect_list_objects()
                .with(eq("bucket1"))
                .returning(|_| {
                    let objects = vec!["object1".to_owned()];
                    Ok(objects)
                });
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object1")];
        let pack = Pack::new(digest.clone(), coords);
        let packs: Vec<Pack> = vec![pack];
        let result = repo.prune_extra("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_prune_extra_some_extra() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().returning(|_| {
            let mut source = MockPackDataSource::new();
            source.expect_list_buckets().returning(|| {
                let buckets = vec!["bucket1".into(), "bucket2".into(), "bucket3".into()];
                Ok(buckets)
            });
            source
                .expect_list_objects()
                .with(eq("bucket1"))
                .returning(|_| {
                    let objects = vec!["object1".into(), "object2".into()];
                    Ok(objects)
                });
            source
                .expect_list_objects()
                .with(eq("bucket2"))
                .returning(|_| Ok(Vec::new()));
            source
                .expect_list_objects()
                .with(eq("bucket3"))
                .returning(|_| {
                    let objects = vec!["object1".into(), "object2".into()];
                    Ok(objects)
                });
            source
                .expect_delete_object()
                .with(eq("bucket1"), eq("object2"))
                .returning(|_, _| Ok(()));
            source
                .expect_delete_bucket()
                .with(eq("bucket2"))
                .returning(|_| Ok(()));
            source
                .expect_delete_object()
                .with(eq("bucket3"), eq("object1"))
                .returning(|_, _| Ok(()));
            source
                .expect_delete_object()
                .with(eq("bucket3"), eq("object2"))
                .returning(|_, _| Ok(()));
            source
                .expect_delete_bucket()
                .with(eq("bucket3"))
                .returning(|_| Ok(()));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
        let coords = vec![PackLocation::new("localtmp", "bucket1", "object1")];
        let pack = Pack::new(digest.clone(), coords);
        let packs: Vec<Pack> = vec![pack];
        let result = repo.prune_extra("localtmp", &packs);
        // assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);
    }

    #[test]
    fn test_archive_file() -> Result<(), Error> {
        let outdir = tempfile::tempdir()?;
        let packfile = outdir.path().join("archive.pack");
        create_archive(Path::new("../test/fixtures"), &packfile, "Secret123")?;
        extract_archive(&packfile, outdir.path(), "Secret123")?;

        let file = outdir.path().join("SekienAkashita.jpg");
        let chksum = Checksum::sha256_from_file(&file)?;
        assert_eq!(
            chksum.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        let file = outdir.path().join("lorem-ipsum.txt");
        let chksum = Checksum::sha256_from_file(&file)?;
        #[cfg(target_family = "unix")]
        assert_eq!(
            chksum.to_string(),
            "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
        );
        // line endings differ
        #[cfg(target_family = "windows")]
        assert_eq!(
            chksum.to_string(),
            "sha256-1ed890fb1b875a5d7637d54856dc36195bed2e8e40fe6c155a2908b8dd00ebee"
        );
        let file = outdir.path().join("washington-journal.txt");
        let chksum = Checksum::sha256_from_file(&file)?;
        #[cfg(target_family = "unix")]
        assert_eq!(
            chksum.to_string(),
            "sha256-314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05"
        );
        #[cfg(target_family = "windows")]
        assert_eq!(
            chksum.to_string(),
            "sha256-494cb077670d424f47a3d33929d6f1cbcf408a06d28be11259b2fe90666010dc"
        );

        Ok(())
    }
}
