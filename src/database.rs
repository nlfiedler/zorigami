//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `database` module provides high-level functions for storing and
//! retrieving the various core data structures to a database. This module
//! performs the serde functions to convert the structures to a format suitable
//! for on-disk storage.

use super::core::{Checksum, Chunk, Configuration, Dataset, SavedFile, SavedPack, Snapshot, Tree};
use failure::Error;
use lazy_static::lazy_static;
use rocksdb::{DBVector, DB};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::{Arc, Mutex, Weak};

lazy_static! {
    // Keep a map of weakly held references to shared DB instances. RocksDB
    // itself is thread-safe for get/put/write, and the DB type implements Send
    // and Sync. We just need to make sure the instance is eventually closed
    // when the last reference is dropped.
    //
    // The key is the path to the database files.
    static ref DBASE_REFS: Mutex<HashMap<PathBuf, Weak<DB>>> = Mutex::new(HashMap::new());
}

///
/// An instance of the database for reading and writing records to disk.
///
pub struct Database {
    /// RocksDB instance.
    db: Arc<DB>,
    /// Path to the database files.
    path: Box<Path>,
}

// Mark the Database as a valid context type for Juniper.
impl juniper::Context for Database {}

impl Database {
    ///
    /// Create an instance of Database using the given path for storage. Will
    /// reuse an existing `DB` instance for the given path, if one has already
    /// been opened.
    ///
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self, Error> {
        // should be able to recover from a poisoned mutex without any problem
        let mut db_refs = DBASE_REFS.lock().unwrap();
        if let Some(weak) = db_refs.get(db_path.as_ref()) {
            if let Some(arc) = weak.upgrade() {
                let path = db_path.as_ref().to_path_buf().into_boxed_path();
                return Ok(Self { db: arc, path });
            }
        }
        let buf = db_path.as_ref().to_path_buf();
        let db = DB::open_default(db_path)?;
        let arc = Arc::new(db);
        db_refs.insert(buf.clone(), Arc::downgrade(&arc));
        Ok(Self { db: arc, path: buf.into_boxed_path() })
    }

    ///
    /// Return the path to the database files.
    ///
    pub fn get_path(&self) -> &Path {
        self.path.as_ref()
    }

    ///
    /// Insert the value if the database does not already contain the given key.
    ///
    pub fn insert_document(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let existing = self.db.get(key)?;
        if existing.is_none() {
            self.db.put(key, value)?;
        }
        Ok(())
    }

    ///
    /// Retrieve the value with the given key.
    ///
    pub fn get_document(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let result = self.db.get(key)?;
        Ok(result)
    }

    ///
    /// Put the key/value pair into the database.
    ///
    pub fn put_document(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.db.put(key, value)?;
        Ok(())
    }

    ///
    /// Delete the database record associated with the given key.
    ///
    pub fn delete_document(&self, key: &[u8]) -> Result<(), Error> {
        self.db.delete(key)?;
        Ok(())
    }

    ///
    /// Put the given dataset into the database.
    ///
    pub fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error> {
        let key = format!("dataset/{}", dataset.key);
        let encoded: Vec<u8> = serde_cbor::to_vec(&dataset)?;
        self.put_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the dataset by the given key, returning None if not found.
    ///
    pub fn get_dataset(&self, key: &str) -> Result<Option<Dataset>, Error> {
        let db_key = format!("dataset/{}", key);
        let encoded = self.get_document(db_key.as_bytes())?;
        match encoded {
            Some(dbv) => {
                let mut serde_result: Dataset = serde_cbor::from_slice(&dbv)?;
                serde_result.key = key.to_owned();
                Ok(Some(serde_result))
            }
            None => Ok(None),
        }
    }

    ///
    /// Retrieve all of the datasets in the database.
    ///
    pub fn get_all_datasets(&self) -> Result<Vec<Dataset>, Error> {
        let datasets = self.fetch_prefix("dataset/")?;
        let mut results: Vec<Dataset> = Vec::new();
        for (key, value) in datasets {
            let mut serde_result: Dataset = serde_cbor::from_slice(&value)?;
            // strip the "dataset/" prefix from the key
            serde_result.key = key[8..].to_string();
            results.push(serde_result);
        }
        Ok(results)
    }

    ///
    /// Delete the given dataset from the database.
    ///
    pub fn delete_dataset(&self, key: &str) -> Result<(), Error> {
        let key = format!("dataset/{}", key);
        self.delete_document(key.as_bytes())
    }

    ///
    /// Put the configuration record into the database.
    ///
    pub fn put_config(&self, conf: Configuration) -> Result<(), Error> {
        let key = "configuration";
        let encoded: Vec<u8> = serde_cbor::to_vec(&conf)?;
        self.put_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the configuration record, returning None if not found.
    ///
    pub fn get_config(&self) -> Result<Option<Configuration>, Error> {
        let key = "configuration";
        let encoded = self.get_document(key.as_bytes())?;
        match encoded {
            Some(dbv) => {
                let serde_result: Configuration = serde_cbor::from_slice(&dbv)?;
                Ok(Some(serde_result))
            }
            None => Ok(None),
        }
    }

    ///
    /// Insert the given chunk into the database, if one with the same digest does
    /// not already exist. Chunks with the same digest are assumed to be identical.
    ///
    pub fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error> {
        let key = format!("chunk/{}", chunk.digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&chunk)?;
        self.insert_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the chunk by the given digest, returning None if not found.
    ///
    pub fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error> {
        let key = format!("chunk/{}", digest);
        let encoded = self.get_document(key.as_bytes())?;
        match encoded {
            Some(dbv) => {
                let mut serde_result: Chunk = serde_cbor::from_slice(&dbv)?;
                serde_result.digest = digest.clone();
                Ok(Some(serde_result))
            }
            None => Ok(None),
        }
    }

    ///
    /// Insert the extended file attributes value into the database. Values with
    /// the same digest are assumed to be identical.
    ///
    pub fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error> {
        let key = format!("xattr/{}", digest);
        self.insert_document(key.as_bytes(), xattr)
    }

    ///
    /// Retrieve the extended attributes by the given digest, returning None if
    /// not found.
    ///
    pub fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error> {
        let key = format!("xattr/{}", digest);
        let result = self.get_document(key.as_bytes())?;
        Ok(result.map(|v| v.to_vec()))
    }

    ///
    /// Insert the tree into the database, using the given digest as part of the
    /// key (plus a fixed prefix for namespacing). Trees with the same digest are
    /// assumed to be identical.
    ///
    pub fn insert_tree(&self, digest: &Checksum, tree: &Tree) -> Result<(), Error> {
        let key = format!("tree/{}", digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&tree)?;
        self.insert_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the tree by the given digest, returning None if not found.
    ///
    pub fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error> {
        let key = format!("tree/{}", digest);
        let encoded = self.get_document(key.as_bytes())?;
        match encoded {
            Some(dbv) => {
                let serde_result: Tree = serde_cbor::from_slice(&dbv)?;
                Ok(Some(serde_result))
            }
            None => Ok(None),
        }
    }

    ///
    /// Insert the snapshot into the database, using the given digest as part of the
    /// key (plus a fixed prefix for namespacing). Snapshots with the same digest are
    /// assumed to be identical.
    ///
    pub fn insert_snapshot(&self, digest: &Checksum, snapshot: &Snapshot) -> Result<(), Error> {
        let key = format!("snapshot/{}", digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&snapshot)?;
        self.insert_document(key.as_bytes(), &encoded)
    }

    ///
    /// Update the snapshot in the database, using the given digest as part of
    /// the key (plus a fixed prefix for namespacing).
    ///
    pub fn put_snapshot(&self, digest: &Checksum, snapshot: &Snapshot) -> Result<(), Error> {
        let key = format!("snapshot/{}", digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&snapshot)?;
        self.put_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the snapshot by the given digest, returning None if not found.
    ///
    pub fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error> {
        let key = format!("snapshot/{}", digest);
        let encoded = self.get_document(key.as_bytes())?;
        match encoded {
            Some(dbv) => {
                let serde_result: Snapshot = serde_cbor::from_slice(&dbv)?;
                Ok(Some(serde_result))
            }
            None => Ok(None),
        }
    }

    ///
    /// Insert the file into the database, using the given digest as part of the
    /// key (plus a fixed prefix for namespacing). Files with the same digest are
    /// assumed to be identical.
    ///
    pub fn insert_file(&self, file: &SavedFile) -> Result<(), Error> {
        let key = format!("file/{}", file.digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&file)?;
        self.insert_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the file by the given digest, returning None if not found.
    ///
    pub fn get_file(&self, digest: &Checksum) -> Result<Option<SavedFile>, Error> {
        let key = format!("file/{}", digest);
        let encoded = self.get_document(key.as_bytes())?;
        match encoded {
            Some(dbv) => {
                let mut serde_result: SavedFile = serde_cbor::from_slice(&dbv)?;
                serde_result.digest = digest.clone();
                Ok(Some(serde_result))
            }
            None => Ok(None),
        }
    }

    ///
    /// Insert the pack into the database, using the given digest as part of the
    /// key (plus a fixed prefix for namespacing). Packs with the same digest are
    /// assumed to be identical.
    ///
    pub fn insert_pack(&self, pack: &SavedPack) -> Result<(), Error> {
        let key = format!("pack/{}", pack.digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&pack)?;
        self.insert_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the pack by the given digest, returning None if not found.
    ///
    pub fn get_pack(&self, digest: &Checksum) -> Result<Option<SavedPack>, Error> {
        let key = format!("pack/{}", digest);
        let encoded = self.get_document(key.as_bytes())?;
        match encoded {
            Some(dbv) => {
                let mut serde_result: SavedPack = serde_cbor::from_slice(&dbv)?;
                serde_result.digest = digest.clone();
                Ok(Some(serde_result))
            }
            None => Ok(None),
        }
    }

    ///
    /// Count those keys that start with the given prefix.
    ///
    pub fn count_prefix(&self, prefix: &str) -> Result<usize, Error> {
        let pre_bytes = prefix.as_bytes();
        // this only gets us started, we then have to check for the end of the range
        let iter = self.db.prefix_iterator(pre_bytes);
        let mut count = 0;
        for (key, _value) in iter {
            let pre = &key[..pre_bytes.len()];
            if pre != pre_bytes {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    ///
    /// Find all those keys that start with the given prefix.
    ///
    pub fn find_prefix(&self, prefix: &str) -> Result<Vec<String>, Error> {
        let pre_bytes = prefix.as_bytes();
        // this only gets us started, we then have to check for the end of the range
        let iter = self.db.prefix_iterator(pre_bytes);
        let mut results: Vec<String> = Vec::new();
        for (key, _value) in iter {
            let pre = &key[..pre_bytes.len()];
            if pre != pre_bytes {
                break;
            }
            let key_str = str::from_utf8(&key)?;
            results.push(key_str.to_owned());
        }
        Ok(results)
    }

    ///
    /// Fetch the key/value pairs for those keys that start with the given
    /// prefix.
    ///
    pub fn fetch_prefix(&self, prefix: &str) -> Result<HashMap<String, Box<[u8]>>, Error> {
        let pre_bytes = prefix.as_bytes();
        // this only gets us started, we then have to check for the end of the range
        let iter = self.db.prefix_iterator(pre_bytes);
        let mut results: HashMap<String, Box<[u8]>> = HashMap::new();
        for (key, value) in iter {
            let pre = &key[..pre_bytes.len()];
            if pre != pre_bytes {
                break;
            }
            let key_str = str::from_utf8(&key)?;
            results.insert(key_str.to_owned(), value);
        }
        Ok(results)
    }
}

// maybe useful some time...
// let files = dbase.find_prefix("file/")?;
// for key in files {
//     let sum = Checksum::SHA256(key[12..].to_string());
//     let result = dbase.get_file(&sum)?.unwrap();
//     println!("file: {:?}", result);
// }
// let chunks = dbase.find_prefix("chunk/")?;
// for key in chunks {
//     let sum = Checksum::SHA256(key[13..].to_string());
//     let result = dbase.get_chunk(&sum)?.unwrap();
//     println!("chunk: {:?}", result);
// }
// let packs = dbase.find_prefix("pack/")?;
// for key in packs {
//     let sum = Checksum::SHA256(key[12..].to_string());
//     let result = dbase.get_pack(&sum)?.unwrap();
//     println!("pack: {:?}", result);
// }
