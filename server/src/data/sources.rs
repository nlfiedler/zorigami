//
// Copyright (c) 2020 Nathan Fiedler
//

//! Module root for entity and pack data sources. The `EntityDataSource` trait
//! is implemented by both [`rocksdb::RocksDBEntityDataSource`] (the historic
//! key/value backend) and [`sqlite::SQLiteEntityDataSource`] (a relational
//! alternative). The CBOR-based [`crate::data::models::Model`] trait is only
//! used by the RocksDB backend; the SQLite backend stores entity fields in
//! normalized columns.

use crate::domain::entities::{Store, StoreType};
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
        Ok(source)
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
