//
// Copyright (c) 2020 Nathan Fiedler
//

//! Manages instances of RocksDB associated with file paths.

use failure::Error;
use lazy_static::lazy_static;
use rocksdb::backup::{BackupEngine, BackupEngineOptions};
use rocksdb::{Options, DB};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
}

impl Database {
    /// Create an instance of Database using the given path for storage. Will
    /// reuse an existing `DB` instance for the given path, if one has already
    /// been opened.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self, Error> {
        // should be able to recover from a poisoned mutex without any problem
        let mut db_refs = DBASE_REFS.lock().unwrap();
        if let Some(weak) = db_refs.get(db_path.as_ref()) {
            if let Some(arc) = weak.upgrade() {
                return Ok(Self { db: arc });
            }
        }
        let buf = db_path.as_ref().to_path_buf();
        // prevent the proliferation of old log files
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_keep_log_file_num(10);
        let db = DB::open(&opts, db_path)?;
        let arc = Arc::new(db);
        db_refs.insert(buf, Arc::downgrade(&arc));
        Ok(Self { db: arc })
    }

    /// Return the path to the database files.
    pub fn get_path(&self) -> &Path {
        self.db.path()
    }

    /// Create a backup of the database at the given path.
    pub fn create_backup(&self, path: &Path) -> Result<(), Error> {
        let backup_opts = BackupEngineOptions::default();
        let mut backup_engine = BackupEngine::open(&backup_opts, path)?;
        backup_engine.create_new_backup(&self.db)?;
        backup_engine.purge_old_backups(1)?;
        Ok(())
    }

    /// Restore the database from the backup path to the given db path.
    pub fn restore_from_backup(backup_path: &Path, db_path: &Path) -> Result<(), Error> {
        let backup_opts = BackupEngineOptions::default();
        let mut backup_engine = BackupEngine::open(&backup_opts, backup_path).unwrap();
        let mut restore_option = rocksdb::backup::RestoreOptions::default();
        restore_option.set_keep_log_files(true);
        backup_engine.restore_from_latest_backup(db_path, db_path, &restore_option)?;
        Ok(())
    }

    /// Insert the value if the database does not already contain the given key.
    pub fn insert_document(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let existing = self.db.get(key)?;
        if existing.is_none() {
            self.db.put(key, value)?;
        }
        Ok(())
    }

    /// Retrieve the value with the given key.
    pub fn get_document(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let result = self.db.get(key)?;
        Ok(result)
    }

    /// Put the key/value pair into the database.
    pub fn put_document(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.db.put(key, value)?;
        Ok(())
    }

    /// Delete the database record associated with the given key.
    pub fn delete_document(&self, key: &[u8]) -> Result<(), Error> {
        self.db.delete(key)?;
        Ok(())
    }

//     /// Count those keys that start with the given prefix.
//     pub fn count_prefix(&self, prefix: &str) -> Result<usize, Error> {
//         let pre_bytes = prefix.as_bytes();
//         // this only gets us started, we then have to check for the end of the range
//         let iter = self.db.prefix_iterator(pre_bytes);
//         let mut count = 0;
//         for (key, _value) in iter {
//             let pre = &key[..pre_bytes.len()];
//             if pre != pre_bytes {
//                 break;
//             }
//             count += 1;
//         }
//         Ok(count)
//     }

//     /// Find all those keys that start with the given prefix.
//     ///
//     /// Returns the key without the prefix.
//     pub fn find_prefix(&self, prefix: &str) -> Result<Vec<String>, Error> {
//         let pre_bytes = prefix.as_bytes();
//         // this only gets us started, we then have to check for the end of the range
//         let iter = self.db.prefix_iterator(pre_bytes);
//         let mut results: Vec<String> = Vec::new();
//         for (key, _value) in iter {
//             let pre = &key[..pre_bytes.len()];
//             if pre != pre_bytes {
//                 break;
//             }
//             let key_str = std::str::from_utf8(&key[pre_bytes.len()..])?;
//             results.push(key_str.to_owned());
//         }
//         Ok(results)
//     }

    /// Fetch the key/value pairs for those keys that start with the given
    /// prefix. The prefix is stripped from the keys before being returned.
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
            let key_str = std::str::from_utf8(&key[pre_bytes.len()..])?;
            results.insert(key_str.to_owned(), value);
        }
        Ok(results)
    }
}
