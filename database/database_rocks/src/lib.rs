//
// Copyright (c) 2020 Nathan Fiedler
//

//! Manages instances of RocksDB associated with file paths.

use anyhow::{Error, anyhow};
use database_core::MaybeEntry;
use rocksdb::backup::{BackupEngine, BackupEngineOptions};
use rocksdb::{DB, Options};
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
        if let Some(weak) = db_refs.get(db_path.as_ref())
            && let Some(arc) = weak.upgrade()
        {
            return Ok(Self { db: arc });
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

    /// Fetch the keys that start with the given prefix. The prefix is stripped
    /// before being returned.
    fn find_prefix(&self, prefix: &str) -> Result<Vec<String>, Error> {
        let pre_bytes = prefix.as_bytes();
        // this only gets us started, we then have to check for the end of the range
        let iter = self.db.prefix_iterator(pre_bytes);
        let mut results: Vec<String> = Vec::new();
        for item in iter {
            let (key, _value) = item?;
            let pre = &key[..pre_bytes.len()];
            if pre != pre_bytes {
                break;
            }
            let key_str = std::str::from_utf8(&key[pre_bytes.len()..])?;
            results.push(key_str.to_owned());
        }
        Ok(results)
    }

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

    /// Fetch the key/value pair at the given zero-based offset within the set
    /// of keys that start with the prefix. The prefix is stripped from the
    /// returned key. Returns `None` when the offset is past the end of the
    /// matching range.
    fn fetch_prefix_single(&self, prefix: &str, offset: usize) -> Result<MaybeEntry, Error> {
        let pre_bytes = prefix.as_bytes();
        let iter = self.db.prefix_iterator(pre_bytes);
        for (seen, item) in iter.enumerate() {
            let (key, value) = item?;
            if key.len() < pre_bytes.len() || &key[..pre_bytes.len()] != pre_bytes {
                break;
            }
            if seen == offset {
                let key_str = std::str::from_utf8(&key[pre_bytes.len()..])?.to_owned();
                return Ok(Some((key_str, value)));
            }
        }
        Ok(None)
    }

    /// Fetch the lexicographically greatest key/value pair whose key starts
    /// with the given prefix. The prefix is stripped from the returned key.
    /// Returns `None` when no keys match.
    fn fetch_prefix_last(&self, prefix: &str) -> Result<MaybeEntry, Error> {
        use rocksdb::{Direction, IteratorMode};
        let pre_bytes = prefix.as_bytes();
        // Seek to just past the prefix range (prefix with last byte
        // incremented) and iterate backward so the first yielded key is the
        // largest key still starting with the prefix. Relies on the prefix
        // being non-empty and its last byte not being 0xFF, both true for the
        // "bucket/" style prefixes used in this codebase.
        let mut upper = pre_bytes.to_vec();
        let last = upper.len() - 1;
        upper[last] = upper[last].saturating_add(1);
        let mut iter = self
            .db
            .iterator(IteratorMode::From(&upper, Direction::Reverse));
        if let Some(item) = iter.next() {
            let (key, value) = item?;
            if key.len() < pre_bytes.len() || &key[..pre_bytes.len()] != pre_bytes {
                return Ok(None);
            }
            let key_str = std::str::from_utf8(&key[pre_bytes.len()..])?.to_owned();
            return Ok(Some((key_str, value)));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use database_core::Database as _;

    fn new_db() -> (tempfile::TempDir, Database) {
        let tmp = tempfile::tempdir().unwrap();
        let db = Database::new(tmp.path()).unwrap();
        (tmp, db)
    }

    #[test]
    fn fetch_prefix_single_empty() {
        let (_tmp, db) = new_db();
        assert!(db.fetch_prefix_single("bucket/", 0).unwrap().is_none());
        assert!(db.fetch_prefix_single("bucket/", 5).unwrap().is_none());
    }

    #[test]
    fn fetch_prefix_single_offsets() {
        let (_tmp, db) = new_db();
        db.insert_document(b"bucket/aaa", b"val-a").unwrap();
        db.insert_document(b"bucket/bbb", b"val-b").unwrap();
        db.insert_document(b"bucket/ccc", b"val-c").unwrap();
        // Unrelated prefixes that sort before and after "bucket/"
        db.insert_document(b"apple/zzz", b"val-z").unwrap();
        db.insert_document(b"chunk/xxx", b"val-x").unwrap();

        let (k0, v0) = db.fetch_prefix_single("bucket/", 0).unwrap().unwrap();
        assert_eq!(k0, "aaa");
        assert_eq!(&*v0, b"val-a");
        let (k1, v1) = db.fetch_prefix_single("bucket/", 1).unwrap().unwrap();
        assert_eq!(k1, "bbb");
        assert_eq!(&*v1, b"val-b");
        let (k2, v2) = db.fetch_prefix_single("bucket/", 2).unwrap().unwrap();
        assert_eq!(k2, "ccc");
        assert_eq!(&*v2, b"val-c");
        assert!(db.fetch_prefix_single("bucket/", 3).unwrap().is_none());
    }

    #[test]
    fn fetch_prefix_last_empty() {
        let (_tmp, db) = new_db();
        assert!(db.fetch_prefix_last("bucket/").unwrap().is_none());
    }

    #[test]
    fn fetch_prefix_last_returns_greatest() {
        let (_tmp, db) = new_db();
        db.insert_document(b"bucket/aaa", b"val-a").unwrap();
        db.insert_document(b"bucket/ccc", b"val-c").unwrap();
        db.insert_document(b"bucket/bbb", b"val-b").unwrap();
        // Keys that come after "bucket/" lexicographically; must be skipped.
        db.insert_document(b"chunk/zzz", b"val-z").unwrap();
        db.insert_document(b"pack/yyy", b"val-y").unwrap();

        let (key, value) = db.fetch_prefix_last("bucket/").unwrap().unwrap();
        assert_eq!(key, "ccc");
        assert_eq!(&*value, b"val-c");
    }

    #[test]
    fn fetch_prefix_last_none_when_only_other_prefixes() {
        let (_tmp, db) = new_db();
        db.insert_document(b"chunk/zzz", b"val-z").unwrap();
        assert!(db.fetch_prefix_last("bucket/").unwrap().is_none());
    }
}
