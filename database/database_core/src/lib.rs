//
// Copyright (c) 2020 Nathan Fiedler
//

//! Define traits and types for all database implementations.

use anyhow::Error;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub trait Database {
    /// Return the path to the database files.
    fn get_path(&self) -> &Path;

    /// Create a backup of the database, returning its path.
    ///
    /// If `path` is `None`, the default behavior is to add the extension
    /// `.backup` to the database path.
    fn create_backup(&self, path: Option<PathBuf>) -> Result<PathBuf, Error>;

    /// Restore the database from the backup path.
    ///
    /// If `path` is `None`, the default behavior is to add the extension
    /// `.backup` to the database path.
    fn restore_from_backup(path: Option<PathBuf>, db_path: &Path) -> Result<(), Error>;

    /// Insert the value if the database does not already contain the given key.
    fn insert_document(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;

    /// Retrieve the value with the given key.
    fn get_document(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;

    /// Put the key/value pair into the database.
    fn put_document(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;

    /// Delete the database record associated with the given key.
    fn delete_document(&self, key: &[u8]) -> Result<(), Error>;

    /// Count those keys that start with the given prefix.
    fn count_prefix(&self, prefix: &str) -> Result<usize, Error>;

    /// Fetch the keys that start with the given prefix. The prefix is stripped
    /// before being returned.
    fn find_prefix(&self, prefix: &str) -> Result<Vec<String>, Error>;

    /// Fetch the key/value pairs for those keys that start with the given
    /// prefix. The prefix is stripped from the keys before being returned.
    fn fetch_prefix(&self, prefix: &str) -> Result<HashMap<String, Box<[u8]>>, Error>;
}
