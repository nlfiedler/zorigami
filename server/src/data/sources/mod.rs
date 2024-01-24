//
// Copyright (c) 2024 Nathan Fiedler
//

//! Performs serde on entities and stores them in a database.

use crate::data::models::{
    ChunkDef, ConfigurationDef, DatasetDef, FileDef, PackDef, SnapshotDef, StoreDef,
};
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, PackLocation, RecordCounts, Snapshot,
    Store, StoreType, Tree,
};
use anyhow::Error;
use database_core::Database;
use database_rocks;
use log::debug;
#[cfg(test)]
use mockall::automock;
use std::str::FromStr;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

mod amazon;
mod azure;
mod google;
mod local;
mod minio;
mod sftp;

/// Data source for entity objects.
#[cfg_attr(test, automock)]
pub trait EntityDataSource: Send + Sync {
    /// Save the configuration record to the data source.
    fn put_configuration(&self, config: &Configuration) -> Result<(), Error>;

    /// Retrieve the configuration from the data source.
    fn get_configuration(&self) -> Result<Option<Configuration>, Error>;

    /// Save the computer identifier for the dataset with the given key.
    fn put_computer_id(&self, dataset: &str, computer_id: &str) -> Result<(), Error>;

    /// Retrieve the computer identifier for the dataset with the given key.
    fn get_computer_id(&self, dataset: &str) -> Result<Option<String>, Error>;

    /// Remove the computer identifier for the dataset with the given key.
    fn delete_computer_id(&self, dataset: &str) -> Result<(), Error>;

    /// Save the digest of the latest snapshot for the dataset with the given key.
    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error>;

    /// Retrieve the digest of the latest snapshot for the dataset with the given key.
    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error>;

    /// Remvoe the digest of the latest snapshot for the dataset with the given key.
    fn delete_latest_snapshot(&self, dataset: &str) -> Result<(), Error>;

    /// Insert the given chunk into the data source, if one with the same digest does
    /// not already exist. Chunks with the same digest are assumed to be identical.
    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error>;

    /// Retrieve the chunk by the given digest, returning `None` if not found.
    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error>;

    /// Insert the given pack into the data source, if one with the same digest
    /// does not already exist. Packs with the same digest are assumed to be
    /// identical.
    fn insert_pack(&self, pack: &Pack) -> Result<(), Error>;

    /// Save the given pack to the data source, overwriting any existing entry.
    fn put_pack(&self, pack: &Pack) -> Result<(), Error>;

    /// Retrieve the pack by the given digest, returning `None` if not found.
    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error>;

    /// Retrieve all pack records that should be in the given store.
    fn get_packs(&self, store_id: &str) -> Result<Vec<Pack>, Error>;

    /// Retrieve all pack records in the system regardless of store.
    fn get_all_packs(&self) -> Result<Vec<Pack>, Error>;

    /// Insert the given psedo-pack for the database snapshot, if one with the
    /// same digest does not already exist. Packs with the same digest are
    /// assumed to be identical.
    fn insert_database(&self, pack: &Pack) -> Result<(), Error>;

    /// Retrieve the database pseudo-pack by the given digest, returning `None`
    /// if not found.
    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error>;

    /// Retrieve all database pseudo-pack records.
    fn get_databases(&self) -> Result<Vec<Pack>, Error>;

    /// Insert the extended file attributes value into the data source, if one
    /// with the same digest does not already exist. Values with the same digest
    /// are assumed to be identical.
    fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error>;

    /// Retrieve the extended attributes by the given digest, returning `None`
    /// if not found.
    fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error>;

    /// Insert the given file into the data source, if one with the same digest
    /// does not already exist. Files with the same digest are assumed to be
    /// identical.
    fn insert_file(&self, file: &File) -> Result<(), Error>;

    /// Retrieve the file by the given digest, returning `None` if not found.
    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error>;

    /// Insert the given tree into the data source, if one with the same digest
    /// does not already exist. Trees with the same digest are assumed to be
    /// identical.
    fn insert_tree(&self, tree: &Tree) -> Result<(), Error>;

    /// Retrieve the tree by the given digest, returning `None` if not found.
    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error>;

    /// Save the given store to the data source.
    fn put_store(&self, store: &Store) -> Result<(), Error>;

    /// Retrieve all registered pack store configurations.
    fn get_stores(&self) -> Result<Vec<Store>, Error>;

    /// Retrieve the store by identifier, returning `None` if not found.
    fn get_store(&self, id: &str) -> Result<Option<Store>, Error>;

    /// Remove the store by the given identifier.
    fn delete_store(&self, id: &str) -> Result<(), Error>;

    /// Save the given dataset to the data source.
    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error>;

    /// Retrieve all defined dataset configurations.
    fn get_datasets(&self) -> Result<Vec<Dataset>, Error>;

    /// Retrieve the dataset by the given identifier.
    fn get_dataset(&self, id: &str) -> Result<Option<Dataset>, Error>;

    /// Remove the dataset by the given identifier.
    fn delete_dataset(&self, id: &str) -> Result<(), Error>;

    /// Save the given snapshot to the data source.
    fn put_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error>;

    /// Retrieve a snapshot by its digest, returning `None` if not found.
    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error>;

    /// Retrieve the path to the database files.
    fn get_db_path(&self) -> PathBuf;

    /// Create a backup of the database, returning its path.
    fn create_backup(&self, path: Option<PathBuf>) -> Result<PathBuf, Error>;

    /// Restore the database from the backup path.
    fn restore_from_backup(&self, path: Option<PathBuf>) -> Result<(), Error>;

    /// Retrieve the counts of the various record types in the data source.
    fn get_entity_counts(&self) -> Result<RecordCounts, Error>;
}

/// Implementation of the entity data source backed by RocksDB.
pub struct EntityDataSourceImpl {
    database: Mutex<database_rocks::Database>,
}

impl EntityDataSourceImpl {
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self, Error> {
        use anyhow::Context;
        std::fs::create_dir_all(&db_path).with_context(|| {
            format!(
                "EntityDataSourceImpl::new fs::create_dir_all({})",
                db_path.as_ref().display()
            )
        })?;
        let database = Mutex::new(database_rocks::Database::new(db_path)?);
        Ok(Self { database })
    }
}

impl EntityDataSource for EntityDataSourceImpl {
    fn get_configuration(&self) -> Result<Option<Configuration>, Error> {
        let key = "configuration";
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let result = ConfigurationDef::deserialize(&mut de)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn put_configuration(&self, config: &Configuration) -> Result<(), Error> {
        let key = "configuration";
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        ConfigurationDef::serialize(config, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &encoded)
    }

    fn put_computer_id(&self, dataset: &str, computer_id: &str) -> Result<(), Error> {
        let key = format!("computer/{}", dataset);
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), computer_id.as_bytes())
    }

    fn get_computer_id(&self, dataset: &str) -> Result<Option<String>, Error> {
        let key = format!("computer/{}", dataset);
        let db = self.database.lock().unwrap();
        let option = db.get_document(key.as_bytes())?;
        match option {
            Some(value) => {
                let result = String::from_utf8(value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn delete_computer_id(&self, dataset: &str) -> Result<(), Error> {
        let key = format!("computer/{}", dataset);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error> {
        let key = format!("latest/{}", dataset);
        // use simple approach as serde can be tricky to compile
        let as_string = latest.to_string();
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), as_string.as_bytes())
    }

    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error> {
        let key = format!("latest/{}", dataset);
        let db = self.database.lock().unwrap();
        let option = db.get_document(key.as_bytes())?;
        match option {
            Some(value) => {
                let as_string = String::from_utf8(value)?;
                let result: Result<Checksum, Error> = FromStr::from_str(&as_string);
                result.map(Some)
            }
            None => Ok(None),
        }
    }

    fn delete_latest_snapshot(&self, dataset: &str) -> Result<(), Error> {
        let key = format!("latest/{}", dataset);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error> {
        let key = format!("chunk/{}", chunk.digest);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        ChunkDef::serialize(chunk, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &encoded)
    }

    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error> {
        let key = format!("chunk/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = ChunkDef::deserialize(&mut de)?;
                result.digest = digest.clone();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn insert_pack(&self, pack: &Pack) -> Result<(), Error> {
        let key = format!("pack/{}", pack.digest);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        PackDef::serialize(pack, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &encoded)
    }

    fn put_pack(&self, pack: &Pack) -> Result<(), Error> {
        let key = format!("pack/{}", pack.digest);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        PackDef::serialize(pack, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &encoded)
    }

    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        let key = format!("pack/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = PackDef::deserialize(&mut de)?;
                result.digest = digest.clone();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_packs(&self, store_id: &str) -> Result<Vec<Pack>, Error> {
        let db = self.database.lock().unwrap();
        let packs = db.fetch_prefix("pack/")?;
        let mut results: Vec<Pack> = Vec::new();
        for (key, value) in packs {
            let mut de = serde_cbor::Deserializer::from_slice(&value);
            let mut result = PackDef::deserialize(&mut de)?;
            // pack must have at least one pack location whose store identifier
            // matches the one given
            if result.locations.iter().any(|l| l.store == store_id) {
                // strip leading "pack/" from 'key' and convert to a Checksum
                let digest: Result<Checksum, Error> = FromStr::from_str(&key);
                if let Ok(value) = digest {
                    result.digest = value;
                    results.push(result);
                }
            }
        }
        Ok(results)
    }

    fn get_all_packs(&self) -> Result<Vec<Pack>, Error> {
        let db = self.database.lock().unwrap();
        let packs = db.fetch_prefix("pack/")?;
        let mut results: Vec<Pack> = Vec::new();
        for (key, value) in packs {
            let mut de = serde_cbor::Deserializer::from_slice(&value);
            let mut result = PackDef::deserialize(&mut de)?;
            // strip leading "pack/" from 'key' and convert to a Checksum
            let digest: Result<Checksum, Error> = FromStr::from_str(&key);
            if let Ok(value) = digest {
                result.digest = value;
                results.push(result);
            }
        }
        Ok(results)
    }

    fn insert_database(&self, pack: &Pack) -> Result<(), Error> {
        let key = format!("dbase/{}", pack.digest);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        PackDef::serialize(pack, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &encoded)
    }

    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        let key = format!("dbase/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = PackDef::deserialize(&mut de)?;
                result.digest = digest.clone();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_databases(&self) -> Result<Vec<Pack>, Error> {
        let db = self.database.lock().unwrap();
        let packs = db.fetch_prefix("dbase/")?;
        let mut results: Vec<Pack> = Vec::new();
        for (key, value) in packs {
            let mut de = serde_cbor::Deserializer::from_slice(&value);
            let mut result = PackDef::deserialize(&mut de)?;
            let digest: Result<Checksum, Error> = FromStr::from_str(&key);
            if let Ok(value) = digest {
                result.digest = value;
                results.push(result);
            }
        }
        Ok(results)
    }

    fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error> {
        let key = format!("xattr/{}", digest);
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), xattr)
    }

    fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error> {
        let key = format!("xattr/{}", digest);
        let db = self.database.lock().unwrap();
        let result = db.get_document(key.as_bytes())?;
        Ok(result.map(|v| v.to_vec()))
    }

    fn insert_file(&self, file: &File) -> Result<(), Error> {
        let key = format!("file/{}", file.digest);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        FileDef::serialize(file, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &encoded)
    }

    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error> {
        let key = format!("file/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = FileDef::deserialize(&mut de)?;
                result.digest = digest.clone();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn insert_tree(&self, tree: &Tree) -> Result<(), Error> {
        let key = format!("tree/{}", tree.digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&tree)?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &encoded)
    }

    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error> {
        let key = format!("tree/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut result: Tree = serde_cbor::from_slice(&value)?;
                result.digest = digest.clone();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn put_store(&self, store: &Store) -> Result<(), Error> {
        let key = format!("store/{}", store.id);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        StoreDef::serialize(store, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &encoded)
    }

    fn get_stores(&self) -> Result<Vec<Store>, Error> {
        let db = self.database.lock().unwrap();
        let stores = db.fetch_prefix("store/")?;
        let mut results: Vec<Store> = Vec::new();
        for (key, value) in stores {
            let mut de = serde_cbor::Deserializer::from_slice(&value);
            let mut result = StoreDef::deserialize(&mut de)?;
            result.id = key;
            results.push(result);
        }
        Ok(results)
    }

    fn get_store(&self, id: &str) -> Result<Option<Store>, Error> {
        let key = format!("store/{}", id);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = StoreDef::deserialize(&mut de)?;
                result.id = id.to_owned();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn delete_store(&self, id: &str) -> Result<(), Error> {
        let key = format!("store/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error> {
        let key = format!("dataset/{}", dataset.id);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        DatasetDef::serialize(dataset, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &encoded)
    }

    fn get_datasets(&self) -> Result<Vec<Dataset>, Error> {
        let db = self.database.lock().unwrap();
        let datasets = db.fetch_prefix("dataset/")?;
        let mut results: Vec<Dataset> = Vec::new();
        for (key, value) in datasets {
            let mut de = serde_cbor::Deserializer::from_slice(&value);
            let mut result = DatasetDef::deserialize(&mut de)?;
            result.id = key;
            results.push(result);
        }
        Ok(results)
    }

    fn get_dataset(&self, id: &str) -> Result<Option<Dataset>, Error> {
        let key = format!("dataset/{}", id);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = DatasetDef::deserialize(&mut de)?;
                result.id = id.to_owned();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn delete_dataset(&self, id: &str) -> Result<(), Error> {
        let key = format!("dataset/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn put_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error> {
        let key = format!("snapshot/{}", snapshot.digest);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        SnapshotDef::serialize(snapshot, &mut ser)?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &encoded)
    }

    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error> {
        let key = format!("snapshot/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = SnapshotDef::deserialize(&mut de)?;
                result.digest = digest.clone();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_db_path(&self) -> PathBuf {
        let db = self.database.lock().unwrap();
        db.get_path().to_path_buf()
    }

    fn create_backup(&self, path: Option<PathBuf>) -> Result<PathBuf, Error> {
        let db = self.database.lock().unwrap();
        db.create_backup(path)
    }

    fn restore_from_backup(&self, path: Option<PathBuf>) -> Result<(), Error> {
        // Create a temporary database in order to release the lock on the
        // current database, then restore from the backup, and finish by
        // creating a new database instance using the restored data.
        let outdir = tempfile::tempdir()?;
        let tmpdb = outdir.path().join("zoritempura");
        let mut db = self.database.lock().unwrap();
        let db_path = db.get_path().to_path_buf();
        debug!("restore_from_backup opening tmp db in {:?}", tmpdb);
        *db = database_rocks::Database::new(tmpdb)?;
        drop(db);
        database_rocks::Database::restore_from_backup(path, &db_path)?;
        let mut db = self.database.lock().unwrap();
        *db = database_rocks::Database::new(&db_path)?;
        debug!("restore_from_backup open new db in {:?}", db_path);
        Ok(())
    }

    fn get_entity_counts(&self) -> Result<RecordCounts, Error> {
        let db = self.database.lock().unwrap();
        let chunks = db.count_prefix("chunk/")?;
        let datasets = db.count_prefix("dataset/")?;
        let files = db.count_prefix("file/")?;
        let packs = db.count_prefix("pack/")?;
        let snapshots = db.count_prefix("snapshot/")?;
        let stores = db.count_prefix("store/")?;
        let trees = db.count_prefix("tree/")?;
        let xattrs = db.count_prefix("xattr/")?;
        Ok(RecordCounts {
            chunk: chunks,
            dataset: datasets,
            file: files,
            pack: packs,
            snapshot: snapshots,
            store: stores,
            tree: trees,
            xattr: xattrs,
        })
    }
}

///
/// Data source for pack files.
///
#[cfg_attr(test, automock)]
pub trait PackDataSource: Send + Sync {
    /// Return `true` if this store is local to the system.
    fn is_local(&self) -> bool;

    /// Return `true` if this store is remarkably slow compared to usual.
    fn is_slow(&self) -> bool;

    /// Store the pack file under the named bucket and referenced by the object
    /// name. Returns the remote location of the pack, in case it was assigned
    /// new values by the backing store.
    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error>;

    /// Retrieve a pack from the given location, writing the contents to the
    /// given path.
    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error>;

    /// List the known buckets in the repository.
    fn list_buckets(&self) -> Result<Vec<String>, Error>;

    /// List of all objects in the named bucket.
    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error>;

    /// Delete the named object from the given bucket.
    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error>;

    /// Delete the named bucket. It almost certainly needs to be empty first, so
    /// use `list_objects()` and `delete_object()` to remove the objects.
    fn delete_bucket(&self, bucket: &str) -> Result<(), Error>;

    /// Store the database archive under the named bucket and referenced by the
    /// object name. Returns the remote location of the pack, in case it was
    /// assigned new values by the backing store.
    fn store_database(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error>;

    /// Retrieve a database archive from the given location, writing the
    /// contents to the given path.
    fn retrieve_database(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error>;

    /// List all database archives in the named bucket.
    fn list_databases(&self, bucket: &str) -> Result<Vec<String>, Error>;
}

/// Builder for pack data sources.
#[cfg_attr(test, automock)]
pub trait PackSourceBuilder {
    /// Construct pack data source for the given store.
    fn build_source(&self, store: &Store) -> Result<Box<dyn PackDataSource>, Error>;
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
        };
        let source = builder.build_source(&store).unwrap();
        assert!(!source.is_local());
        assert!(!source.is_slow());
    }

    #[test]
    fn test_build_source_sftp() {
        let builder = PackSourceBuilderImpl {};
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("remote_addr".to_owned(), "localhost:22".to_owned());
        properties.insert("username".to_owned(), "charlie".to_owned());
        let store = Store {
            id: "sftp123".to_owned(),
            store_type: StoreType::SFTP,
            label: "other_server".to_owned(),
            properties,
        };
        let source = builder.build_source(&store).unwrap();
        assert!(!source.is_local());
        assert!(!source.is_slow());
    }
}
