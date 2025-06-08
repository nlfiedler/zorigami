//
// Copyright (c) 2020 Nathan Fiedler
//

//! Performs serde on entities and stores them in a database.

use crate::data::models::Model;
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, RecordCounts, Snapshot, Store, StoreType,
    Tree,
};
use crate::domain::sources::{EntityDataSource, PackDataSource};
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
                let blank_key: Vec<u8> = vec![];
                let result = Configuration::from_bytes(&blank_key, &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn put_configuration(&self, config: &Configuration) -> Result<(), Error> {
        let key = "configuration";
        let as_bytes = config.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &as_bytes)
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
        let encoded = chunk.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &encoded)
    }

    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error> {
        let key = format!("chunk/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                // converting from str to bytes and back is unavoidable
                let result = Chunk::from_bytes(&key[6..].as_bytes(), &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_all_chunk_digests(&self) -> Result<Vec<String>, Error> {
        let db = self.database.lock().unwrap();
        let trees = db.find_prefix("chunk/")?;
        let mut digests: Vec<String> = Vec::new();
        for key in trees {
            digests.push(key);
        }
        Ok(digests)
    }

    fn delete_chunk(&self, id: &str) -> Result<(), Error> {
        let key = format!("chunk/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn insert_pack(&self, pack: &Pack) -> Result<(), Error> {
        let key = format!("pack/{}", pack.digest);
        let as_bytes = pack.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &as_bytes)
    }

    fn put_pack(&self, pack: &Pack) -> Result<(), Error> {
        let key = format!("pack/{}", pack.digest);
        let as_bytes = pack.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &as_bytes)
    }

    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        let key = format!("pack/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let result = Pack::from_bytes(&key[5..].as_bytes(), &value)?;
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
            let result = Pack::from_bytes(&key.as_bytes(), &value)?;
            // pack must have at least one pack location whose store identifier
            // matches the one given
            if result.locations.iter().any(|l| l.store == store_id) {
                results.push(result);
            }
        }
        Ok(results)
    }

    fn get_all_packs(&self) -> Result<Vec<Pack>, Error> {
        let db = self.database.lock().unwrap();
        let packs = db.fetch_prefix("pack/")?;
        let mut results: Vec<Pack> = Vec::new();
        for (key, value) in packs {
            let result = Pack::from_bytes(&key.as_bytes(), &value)?;
            results.push(result);
        }
        Ok(results)
    }

    fn insert_database(&self, pack: &Pack) -> Result<(), Error> {
        let key = format!("dbase/{}", pack.digest);
        let as_bytes = pack.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &as_bytes)
    }

    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        let key = format!("dbase/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let result = Pack::from_bytes(&key[6..].as_bytes(), &value)?;
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
            let result = Pack::from_bytes(&key.as_bytes(), &value)?;
            results.push(result);
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

    fn get_all_xattr_digests(&self) -> Result<Vec<String>, Error> {
        let db = self.database.lock().unwrap();
        let trees = db.find_prefix("xattr/")?;
        let mut digests: Vec<String> = Vec::new();
        for key in trees {
            digests.push(key);
        }
        Ok(digests)
    }

    fn delete_xattr(&self, id: &str) -> Result<(), Error> {
        let key = format!("xattr/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn insert_file(&self, file: &File) -> Result<(), Error> {
        let key = format!("file/{}", file.digest);
        let encoded = file.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &encoded)
    }

    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error> {
        let key = format!("file/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let result = File::from_bytes(&key[5..].as_bytes(), &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_all_file_digests(&self) -> Result<Vec<String>, Error> {
        let db = self.database.lock().unwrap();
        let trees = db.find_prefix("file/")?;
        let mut digests: Vec<String> = Vec::new();
        for key in trees {
            digests.push(key);
        }
        Ok(digests)
    }

    fn delete_file(&self, id: &str) -> Result<(), Error> {
        let key = format!("file/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn insert_tree(&self, tree: &Tree) -> Result<(), Error> {
        let key = format!("tree/{}", tree.digest);
        let encoded = tree.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &encoded)
    }

    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error> {
        let key = format!("tree/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let result = Tree::from_bytes(&key[5..].as_bytes(), &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_all_tree_digests(&self) -> Result<Vec<String>, Error> {
        let db = self.database.lock().unwrap();
        let trees = db.find_prefix("tree/")?;
        let mut digests: Vec<String> = Vec::new();
        for key in trees {
            digests.push(key);
        }
        Ok(digests)
    }

    fn delete_tree(&self, id: &str) -> Result<(), Error> {
        let key = format!("tree/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn put_store(&self, store: &Store) -> Result<(), Error> {
        let key = format!("store/{}", store.id);
        let encoded = store.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &encoded)
    }

    fn get_stores(&self) -> Result<Vec<Store>, Error> {
        let db = self.database.lock().unwrap();
        let stores = db.fetch_prefix("store/")?;
        let mut results: Vec<Store> = Vec::new();
        for (key, value) in stores {
            let result = Store::from_bytes(&key.as_bytes(), &value)?;
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
                let result = Store::from_bytes(&key[6..].as_bytes(), &value)?;
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
        let encoded = dataset.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &encoded)
    }

    fn get_datasets(&self) -> Result<Vec<Dataset>, Error> {
        let db = self.database.lock().unwrap();
        let datasets = db.fetch_prefix("dataset/")?;
        let mut results: Vec<Dataset> = Vec::new();
        let blank_key: Vec<u8> = vec![];
        for (key, value) in datasets {
            // because fetch_prefix() already converts the key from bytes to
            // string, let's not do it again in from_bytes()
            let mut result = Dataset::from_bytes(&blank_key, &value)?;
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
                let blank_key: Vec<u8> = vec![];
                // because fetch_prefix() already converts the key from bytes to
                // string, let's not do it again in from_bytes()
                let mut result = Dataset::from_bytes(&blank_key, &value)?;
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
        let encoded = snapshot.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &encoded)
    }

    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error> {
        let key = format!("snapshot/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let result = Snapshot::from_bytes(&key[9..].as_bytes(), &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn delete_snapshot(&self, id: &str) -> Result<(), Error> {
        let key = format!("snapshot/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
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
