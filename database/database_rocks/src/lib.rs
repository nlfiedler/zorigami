//
// Copyright (c) 2025 Nathan Fiedler
//

//! Manages instances of RocksDB associated with file paths.

use anyhow::{anyhow, Error};
use rocksdb::backup::{BackupEngine, BackupEngineOptions};
use rocksdb::{Options, DB};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

// Keep a map of weakly held references to shared DB instances. RocksDB itself
// is thread-safe for get/put/write, and the DB type implements Send and Sync.
// We just need to make sure the instance is eventually closed when the last
// reference is dropped.
//
// The key is the path to the database files.
static DBASE_REFS: LazyLock<Mutex<HashMap<PathBuf, Weak<DB>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
}

impl database_core::Database for Database {
    /// Return the path to the database files.
    fn get_path(&self) -> &Path {
        self.db.path()
    }

    /// Create a backup of the database, returning its path.
    ///
    /// If `path` is `None`, the default behavior is to add the extension
    /// `.backup` to the database path.
    fn create_backup(&self, path: Option<PathBuf>) -> Result<PathBuf, Error> {
        let backup_path = path.unwrap_or_else(|| {
            let mut backup_path: PathBuf = PathBuf::from(self.db.path());
            backup_path.set_extension("backup");
            backup_path
        });
        let backup_opts = BackupEngineOptions::new(&backup_path)?;
        let env = rocksdb::Env::new()?;
        let mut backup_engine = BackupEngine::open(&backup_opts, &env)?;
        backup_engine.create_new_backup_flush(&self.db, true)?;
        backup_engine.purge_old_backups(1)?;
        Ok(backup_path)
    }

    /// Restore the database from the backup path.
    ///
    /// If `path` is `None`, the default behavior is to add the extension
    /// `.backup` to the database path.
    ///
    /// There must be zero strong references to the database when this function
    /// is invoked, otherwise the RocksDB backup cannot acquire an exclusive
    /// lock. The caller must open the database again via `new()`.
    fn restore_from_backup(path: Option<PathBuf>, db_path: &Path) -> Result<(), Error> {
        // Release any weak database references so that when the caller "opens"
        // it again, they get a new instance and thus see the restored data.
        // From the RocksDB wiki: "You must reopen any live databases to see the
        // restored data."
        let mut db_refs = DBASE_REFS.lock().unwrap();
        if let Some(reph) = db_refs.remove(db_path) {
            // Ensure that there are no strong references (i.e. the database is
            // not currently opened) otherwise the backup will fail to get the
            // exclusive lock.
            let strong_count = reph.strong_count();
            if strong_count != 0 {
                return Err(anyhow!(format!("non-zero strong count: {}", strong_count)));
            }
        }
        drop(db_refs);
        let backup_path = path.unwrap_or_else(|| {
            let mut backup_path: PathBuf = PathBuf::from(db_path);
            backup_path.set_extension("backup");
            backup_path
        });
        let backup_opts = BackupEngineOptions::new(backup_path)?;
        let env = rocksdb::Env::new()?;
        let mut backup_engine = BackupEngine::open(&backup_opts, &env).unwrap();
        let restore_option = rocksdb::backup::RestoreOptions::default();
        backup_engine.restore_from_latest_backup(db_path, db_path, &restore_option)?;
        Ok(())
    }

    /// Insert the value if the database does not already contain the given key.
    fn insert_document(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let existing = self.db.get(key)?;
        if existing.is_none() {
            self.db.put(key, value)?;
        }
        Ok(())
    }

    /// Retrieve the value with the given key.
    fn get_document(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let result = self.db.get(key)?;
        Ok(result)
    }

    /// Put the key/value pair into the database.
    fn put_document(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.db.put(key, value)?;
        Ok(())
    }

    /// Delete the database record associated with the given key.
    fn delete_document(&self, key: &[u8]) -> Result<(), Error> {
        self.db.delete(key)?;
        Ok(())
    }

    /// Count those keys that start with the given prefix.
    fn count_prefix(&self, prefix: &str) -> Result<usize, Error> {
        let pre_bytes = prefix.as_bytes();
        // this only gets us started, we then have to check for the end of the range
        let iter = self.db.prefix_iterator(pre_bytes);
        let mut count = 0;
        for item in iter {
            let (key, _value) = item?;
            let pre = &key[..pre_bytes.len()];
            if pre != pre_bytes {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    //     /// Find all those keys that start with the given prefix.
    //     ///
    //     /// Returns the key without the prefix.
    //     fn find_prefix(&self, prefix: &str) -> Result<Vec<String>, Error> {
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
    fn fetch_prefix(&self, prefix: &str) -> Result<HashMap<String, Box<[u8]>>, Error> {
        let pre_bytes = prefix.as_bytes();
        // this only gets us started, we then have to check for the end of the range
        let iter = self.db.prefix_iterator(pre_bytes);
        let mut results: HashMap<String, Box<[u8]>> = HashMap::new();
        for item in iter {
            let (key, value) = item?;
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
