//
// Copyright (c) 2020 Nathan Fiedler
//

//! Module root for entity and pack data sources. The `EntityDataSource` trait
//! is implemented by both [`rocksdb::RocksDBEntityDataSource`] (the historic
//! key/value backend) and [`sqlite::SQLiteEntityDataSource`] (a relational
//! alternative). The CBOR-based [`crate::data::models::Model`] trait is only
//! used by the RocksDB backend; the SQLite backend stores entity fields in
//! normalized columns.

use crate::domain::entities::{PackLocation, Store, StoreType};
use crate::domain::sources::{EntityDataSource, PackDataSource, PackSourceBuilder};
use anyhow::{Error, anyhow};
use std::path::Path;
use std::sync::Arc;

mod amazon;
mod azure;
mod google;
mod local;
mod minio;
mod rocksdb;
mod sftp;
mod sqlite;

pub use rocksdb::RocksDBEntityDataSource;
pub use sqlite::SQLiteEntityDataSource;

/// Schema version expected by the current build of the application. Bumped
/// when entity layouts change in a way that would corrupt or mis-read on-disk
/// data. Both backends persist this value and refuse to start if a stored
/// value disagrees with this constant.
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Construct an `EntityDataSource` for the configured backend. The
/// `DATABASE_TYPE` environment variable selects between `rocksdb` (default)
/// and `sqlite`.
pub fn build_entity_data_source(db_path: &Path) -> Result<Arc<dyn EntityDataSource>, Error> {
    match std::env::var("DATABASE_TYPE")
        .unwrap_or_else(|_| "rocksdb".into())
        .as_str()
    {
        "rocksdb" => Ok(Arc::new(RocksDBEntityDataSource::new(db_path)?)),
        "sqlite" => Ok(Arc::new(SQLiteEntityDataSource::new(db_path)?)),
        other => Err(anyhow!("unsupported DATABASE_TYPE: {}", other)),
    }
}

/// Verify the on-disk schema version matches `CURRENT_SCHEMA_VERSION`. A fresh
/// database (version 0) is initialized to the current version. A populated
/// database with a mismatched version yields an error so the caller can refuse
/// to enter normal operation.
pub fn verify_schema_version(ds: &dyn EntityDataSource) -> Result<(), Error> {
    let version = ds.get_schema_version()?;
    if version == 0 {
        ds.set_schema_version(CURRENT_SCHEMA_VERSION)?;
        Ok(())
    } else if version == CURRENT_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(anyhow!(
            "database schema version {} does not match current version {}; \
             wipe DB_PATH and restart to recreate the database",
            version,
            CURRENT_SCHEMA_VERSION
        ))
    }
}

pub struct PackSourceBuilderImpl {}

impl PackSourceBuilder for PackSourceBuilderImpl {
    fn build_source(&self, store: &Store) -> Result<Box<dyn PackDataSource>, Error> {
        // If it helps any, could cache the pack source by the store id to avoid
        // repeatedly constructing the same thing. The lru crate would be perfect
        // for managing the cache.
        let source: Box<dyn PackDataSource> = match store.store_type {
            StoreType::AMAZON => Box::new(amazon::AmazonPackSource::new(store)?),
            StoreType::AZURE => Box::new(azure::AzurePackSource::new(store)?),
            StoreType::LOCAL => Box::new(local::LocalPackSource::new(store)?),
            StoreType::GOOGLE => Box::new(google::GooglePackSource::new(store)?),
            StoreType::MINIO => Box::new(minio::MinioPackSource::new(store)?),
            StoreType::SFTP => Box::new(sftp::SftpPackSource::new(store)?),
        };
        if store.append_only() {
            return Ok(Box::new(AppendOnlyPackSource::new(source)));
        }
        Ok(source)
    }
}

/// Wrapper that strips the delete operations from a pack data source.
///
/// The credentials of an append-only store are not expected to permit deletes
/// in the first place (see `doc/DEPLOY.md`), and the pruner already declines to
/// delete from such a store. This wrapper makes that guarantee structural
/// rather than a property of one call site: no code path can issue a delete
/// against an append-only store, and an attempt is a loud local error instead
/// of a request that leans on the storage provider to refuse it.
struct AppendOnlyPackSource {
    inner: Box<dyn PackDataSource>,
}

impl AppendOnlyPackSource {
    fn new(inner: Box<dyn PackDataSource>) -> Self {
        Self { inner }
    }
}

impl PackDataSource for AppendOnlyPackSource {
    fn is_local(&self) -> bool {
        self.inner.is_local()
    }

    fn is_slow(&self) -> bool {
        self.inner.is_slow()
    }

    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error> {
        self.inner.store_pack(packfile, bucket, object)
    }

    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        self.inner.retrieve_pack(location, outfile)
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        self.inner.list_buckets()
    }

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        self.inner.list_objects(bucket)
    }

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        Err(anyhow!(
            "store is append-only, refusing to delete object {} from bucket {}",
            object,
            bucket
        ))
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        Err(anyhow!(
            "store is append-only, refusing to delete bucket {}",
            bucket
        ))
    }

    fn store_database(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error> {
        self.inner.store_database(packfile, bucket, object)
    }

    fn retrieve_database(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        self.inner.retrieve_database(location, outfile)
    }

    fn list_databases(&self, bucket: &str) -> Result<Vec<String>, Error> {
        self.inner.list_databases(bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::PackRetention;
    use std::collections::HashMap;

    #[test]
    fn test_build_source_local() {
        let builder = PackSourceBuilderImpl {};
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/tmp".to_owned());
        let store = Store {
            id: "local123".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let source = builder.build_source(&store).unwrap();
        assert!(source.is_local());
        assert!(!source.is_slow());
    }

    #[test]
    fn test_build_source_append_only_refuses_deletes() {
        // An append-only store still reads and writes normally, but the delete
        // operations are refused locally rather than sent to the backend. A
        // local store makes this checkable without touching the network: the
        // basepath does not even have to exist for the deletes to fail.
        let builder = PackSourceBuilderImpl {};
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/tmp".to_owned());
        properties.insert("append_only".to_owned(), "true".to_owned());
        let store = Store {
            id: "local123".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let source = builder.build_source(&store).unwrap();
        // the wrapper delegates the informational calls
        assert!(source.is_local());
        assert!(!source.is_slow());
        // ...and refuses both deletes
        let result = source.delete_object("bucket1", "object1");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("append-only"), "unexpected error: {}", msg);
        let result = source.delete_bucket("bucket1");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("append-only"), "unexpected error: {}", msg);
    }

    #[test]
    fn test_build_source_without_append_only_permits_deletes() {
        // The mirror of the test above, and the one that guards against an
        // inverted or unconditional wrap: an ordinary store must still reach
        // the real delete. Without this, a refactor that wrapped every store
        // would silently disable all pruning and pass the whole suite.
        let basepath = std::path::PathBuf::from("../tmp/test/append-only-off");
        let bucket = "bucket1";
        let object = "object1";
        let bucket_path = basepath.join(bucket);
        std::fs::create_dir_all(&bucket_path).unwrap();
        std::fs::write(bucket_path.join(object), b"pack contents").unwrap();

        let builder = PackSourceBuilderImpl {};
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert(
            "basepath".to_owned(),
            basepath.to_string_lossy().into_owned(),
        );
        properties.insert("append_only".to_owned(), "false".to_owned());
        let store = Store {
            id: "local123".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let source = builder.build_source(&store).unwrap();
        source
            .delete_object(bucket, object)
            .expect("delete should reach the real store");
        assert!(!bucket_path.join(object).exists());
        source
            .delete_bucket(bucket)
            .expect("delete should reach the real store");
        assert!(!bucket_path.exists());
    }

    #[test]
    fn test_append_only_source_delegates_everything_else() {
        // Only the two deletes are intercepted; every other method must reach
        // the wrapped source. A slip that returned the refusal from, say,
        // retrieve_pack would break all restores from an append-only store.
        let mut inner = crate::domain::sources::MockPackDataSource::new();
        inner
            .expect_store_pack()
            .once()
            .returning(|_, b, o| Ok(PackLocation::new("store1", b, o)));
        inner.expect_retrieve_pack().once().returning(|_, _| Ok(()));
        inner
            .expect_list_buckets()
            .once()
            .returning(|| Ok(vec!["bucket1".to_owned()]));
        inner
            .expect_list_objects()
            .once()
            .returning(|_| Ok(vec!["object1".to_owned()]));
        inner
            .expect_store_database()
            .once()
            .returning(|_, b, o| Ok(PackLocation::new("store1", b, o)));
        inner
            .expect_retrieve_database()
            .once()
            .returning(|_, _| Ok(()));
        inner
            .expect_list_databases()
            .once()
            .returning(|_| Ok(vec!["archive1".to_owned()]));
        // the deletes must never reach the inner source
        inner.expect_delete_object().never();
        inner.expect_delete_bucket().never();

        let source = AppendOnlyPackSource::new(Box::new(inner));
        let path = Path::new("/tmp/pack");
        let location = PackLocation::new("store1", "bucket1", "object1");
        assert!(source.store_pack(path, "bucket1", "object1").is_ok());
        assert!(source.retrieve_pack(&location, path).is_ok());
        assert_eq!(source.list_buckets().unwrap().len(), 1);
        assert_eq!(source.list_objects("bucket1").unwrap().len(), 1);
        assert!(source.store_database(path, "bucket1", "object1").is_ok());
        assert!(source.retrieve_database(&location, path).is_ok());
        assert_eq!(source.list_databases("bucket1").unwrap().len(), 1);
        assert!(source.delete_object("bucket1", "object1").is_err());
        assert!(source.delete_bucket("bucket1").is_err());
    }

    #[test]
    fn test_build_source_minio() {
        let builder = PackSourceBuilderImpl {};
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), "us-west2".to_owned());
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        properties.insert("access_key".to_owned(), "minio".to_owned());
        properties.insert("secret_key".to_owned(), "shminio".to_owned());
        let store = Store {
            id: "minio123".to_owned(),
            store_type: StoreType::MINIO,
            label: "s3clone".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let source = builder.build_source(&store).unwrap();
        assert!(!source.is_local());
        assert!(!source.is_slow());
    }

    #[test]
    fn test_build_source_sftp() {
        let builder = PackSourceBuilderImpl {};
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("address".to_owned(), "localhost:22".to_owned());
        properties.insert("username".to_owned(), "charlie".to_owned());
        let store = Store {
            id: "sftp123".to_owned(),
            store_type: StoreType::SFTP,
            label: "other_server".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let source = builder.build_source(&store).unwrap();
        assert!(!source.is_local());
        assert!(!source.is_slow());
    }
}
