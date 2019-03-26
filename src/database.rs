//
// Copyright (c) 2019 Nathan Fiedler
//
use super::core::{Chunk, Tree, Snapshot};
use failure::Error;
use rocksdb::{DBVector, DB};
use std::path::Path;

///
/// An instance of the database for reading and writing records to disk.
///
pub struct Database {
    /// RocksDB instance.
    db: DB,
}

impl Database {
    ///
    /// Create an instance of Database using the given path for storage.
    ///
    pub fn new(db_path: &Path) -> Result<Self, Error> {
        let db = DB::open_default(db_path)?;
        Ok(Self { db })
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
    /// Insert the given chunk into the database, if one with the same digest does
    /// not already exist. Chunks with the same digest are assumed to be identical.
    ///
    pub fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error> {
        let mut key = String::from("chunk/");
        key.push_str(&chunk.digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&chunk)?;
        self.insert_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the chunk by the given digest, returning None if not found.
    ///
    pub fn get_chunk(&self, digest: &str) -> Result<Option<Chunk>, Error> {
        let mut key = String::from("chunk/");
        key.push_str(digest);
        let encoded = self.get_document(key.as_bytes())?;
        match encoded {
            Some(dbv) => {
                let serde_result: Chunk = serde_cbor::from_slice(&dbv)?;
                Ok(Some(serde_result))
            }
            None => Ok(None),
        }
    }

    ///
    /// Insert the extended file attributes value into the database. Values with
    /// the same digest are assumed to be identical.
    ///
    pub fn insert_xattr(&self, digest: &str, xattr: &[u8]) -> Result<(), Error> {
        let mut key = String::from("xattr/");
        key.push_str(&digest);
        self.insert_document(key.as_bytes(), xattr)
    }

    ///
    /// Retrieve the extended attributes by the given digest, returning None if
    /// not found.
    ///
    pub fn get_xattr(&self, digest: &str) -> Result<Option<Vec<u8>>, Error> {
        let mut key = String::from("xattr/");
        key.push_str(&digest);
        let result = self.get_document(key.as_bytes())?;
        Ok(result.map(|v| v.to_vec()))
    }

    ///
    /// Insert the a tree into the database, using the given digest as part of the
    /// key (plus a fixed prefix for namespacing). Trees with the same digest are
    /// assumed to be identical.
    ///
    pub fn insert_tree(&self, digest: &str, tree: &Tree) -> Result<(), Error> {
        let mut key = String::from("tree/");
        key.push_str(&digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&tree)?;
        self.insert_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the tree by the given digest, returning None if not found.
    ///
    pub fn get_tree(&self, digest: &str) -> Result<Option<Tree>, Error> {
        let mut key = String::from("tree/");
        key.push_str(digest);
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
    /// Insert the a snapshot into the database, using the given digest as part of the
    /// key (plus a fixed prefix for namespacing). Snapshots with the same digest are
    /// assumed to be identical.
    ///
    pub fn insert_snapshot(&self, digest: &str, snapshot: &Snapshot) -> Result<(), Error> {
        let mut key = String::from("snapshot/");
        key.push_str(&digest);
        let encoded: Vec<u8> = serde_cbor::to_vec(&snapshot)?;
        self.insert_document(key.as_bytes(), &encoded)
    }

    ///
    /// Retrieve the snapshot by the given digest, returning None if not found.
    ///
    pub fn get_snapshot(&self, digest: &str) -> Result<Option<Snapshot>, Error> {
        let mut key = String::from("snapshot/");
        key.push_str(digest);
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
}
