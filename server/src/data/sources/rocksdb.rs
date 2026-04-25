//
// Copyright (c) 2020 Nathan Fiedler
//

//! RocksDB-backed implementation of the `EntityDataSource` trait. Entities are
//! serialized to CBOR via the [`crate::data::models::Model`] trait and stored
//! under prefix-namespaced keys in a single key/value column family.

use crate::data::models::Model;
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, RecordCounts, Snapshot, Store, Tree,
};
use crate::domain::sources::EntityDataSource;
use anyhow::Error;
use database_core::Database;
use database_rocks;
use hashed_array_tree::HashedArrayTree;
use log::debug;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// Reserved key for the schema-version document. Distinct from any of the
// prefix-namespaced entity keys so it cannot collide.
const SCHEMA_VERSION_KEY: &[u8] = b"schema_version";

/// Implementation of the entity data source backed by RocksDB.
pub struct RocksDBEntityDataSource {
    database: Mutex<database_rocks::Database>,
}

impl RocksDBEntityDataSource {
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self, Error> {
        use anyhow::Context;
        std::fs::create_dir_all(&db_path).with_context(|| {
            format!(
                "RocksDBEntityDataSource::new fs::create_dir_all({})",
                db_path.as_ref().display()
            )
        })?;
        let database = Mutex::new(database_rocks::Database::new(db_path)?);
        Ok(Self { database })
    }
}

impl EntityDataSource for RocksDBEntityDataSource {
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
                let result = Chunk::from_bytes(&key.as_bytes()[6..], &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_all_chunk_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let db = self.database.lock().unwrap();
        let trees = db.find_prefix("chunk/")?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
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
                let result = Pack::from_bytes(&key.as_bytes()[5..], &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_all_pack_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let db = self.database.lock().unwrap();
        let packs = db.find_prefix("pack/")?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
        for key in packs {
            digests.push(key);
        }
        Ok(digests)
    }

    fn delete_pack(&self, id: &str) -> Result<(), Error> {
        let key = format!("pack/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
    }

    fn insert_database(&self, pack: &Pack) -> Result<(), Error> {
        let key = format!("dbase/{}", pack.digest);
        let as_bytes = pack.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), &as_bytes)
    }

    fn put_database(&self, pack: &Pack) -> Result<(), Error> {
        let key = format!("dbase/{}", pack.digest);
        let as_bytes = pack.to_bytes()?;
        let db = self.database.lock().unwrap();
        db.put_document(key.as_bytes(), &as_bytes)
    }

    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        let key = format!("dbase/{}", digest);
        let db = self.database.lock().unwrap();
        let encoded = db.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let result = Pack::from_bytes(&key.as_bytes()[6..], &value)?;
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
            let result = Pack::from_bytes(key.as_bytes(), &value)?;
            results.push(result);
        }
        Ok(results)
    }

    fn delete_database(&self, id: &str) -> Result<(), Error> {
        let key = format!("dbase/{}", id);
        let db = self.database.lock().unwrap();
        db.delete_document(key.as_bytes())
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

    fn get_all_xattr_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let db = self.database.lock().unwrap();
        let trees = db.find_prefix("xattr/")?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
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
                let result = File::from_bytes(&key.as_bytes()[5..], &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_all_file_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let db = self.database.lock().unwrap();
        let trees = db.find_prefix("file/")?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
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
                let result = Tree::from_bytes(&key.as_bytes()[5..], &value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn get_all_tree_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let db = self.database.lock().unwrap();
        let trees = db.find_prefix("tree/")?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
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
            let result = Store::from_bytes(key.as_bytes(), &value)?;
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
                let result = Store::from_bytes(&key.as_bytes()[6..], &value)?;
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
                let result = Snapshot::from_bytes(&key.as_bytes()[9..], &value)?;
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

    fn add_bucket(&self, name: &str) -> Result<(), Error> {
        // Split the bucket name so the first 30 chars form the key (with a
        // "bucket/" prefix) and the remainder is stored as the value. Real
        // bucket names are 61-char ASCII strings, so byte-level split_at is
        // safe.
        let (key_tail, value_part) = name.split_at(30);
        let key = format!("bucket/{}", key_tail);
        let db = self.database.lock().unwrap();
        db.insert_document(key.as_bytes(), value_part.as_bytes())
    }

    fn count_buckets(&self) -> Result<usize, Error> {
        let db = self.database.lock().unwrap();
        db.count_prefix("bucket/")
    }

    fn get_last_bucket(&self) -> Result<Option<String>, Error> {
        let db = self.database.lock().unwrap();
        match db.fetch_prefix_last("bucket/")? {
            Some((key_tail, value)) => {
                let value_str = std::str::from_utf8(&value)?;
                Ok(Some(format!("{}{}", key_tail, value_str)))
            }
            None => Ok(None),
        }
    }

    fn get_random_bucket(&self) -> Result<Option<String>, Error> {
        use rand::RngExt;
        let db = self.database.lock().unwrap();
        let count = db.count_prefix("bucket/")?;
        if count == 0 {
            return Ok(None);
        }
        let offset = rand::rng().random_range(0..count);
        match db.fetch_prefix_single("bucket/", offset)? {
            Some((key_tail, value)) => {
                let value_str = std::str::from_utf8(&value)?;
                Ok(Some(format!("{}{}", key_tail, value_str)))
            }
            None => Ok(None),
        }
    }

    fn get_schema_version(&self) -> Result<u32, Error> {
        let db = self.database.lock().unwrap();
        match db.get_document(SCHEMA_VERSION_KEY)? {
            Some(value) if value.len() == 4 => {
                let bytes: [u8; 4] = value[..4].try_into().unwrap();
                Ok(u32::from_le_bytes(bytes))
            }
            _ => Ok(0),
        }
    }

    fn set_schema_version(&self, version: u32) -> Result<(), Error> {
        let db = self.database.lock().unwrap();
        db.put_document(SCHEMA_VERSION_KEY, &version.to_le_bytes())
    }
}
