//
// Copyright (c) 2019 Nathan Fiedler
//
use super::core::{Checksum, Chunk, SavedFile, SavedPack, Snapshot, Tree};
use failure::Error;
use rocksdb::{DBVector, DB};
use std::path::Path;
use std::str;

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
                let serde_result: SavedFile = serde_cbor::from_slice(&dbv)?;
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
                let serde_result: SavedPack = serde_cbor::from_slice(&dbv)?;
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
}
