//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::{
    CapturedError, Checksum, Chunk, Configuration, Dataset, ErrorOperation, File, Pack,
    PackLocation, RecordCounts, Snapshot, Store, Tree,
};
use crate::domain::services::buckets::BucketNameGenerator;
use anyhow::Error;
use hashed_array_tree::HashedArrayTree;
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::path::{Path, PathBuf};

///
/// Repository for entity records.
///
#[cfg_attr(test, automock)]
pub trait RecordRepository: Send + Sync {
    /// Retrieve the configuration, or build a new one using default values.
    fn get_configuration(&self) -> Result<Configuration, Error>;

    /// Save the given configuration to the repository.
    fn put_configuration(&self, config: &Configuration) -> Result<(), Error>;

    /// Provide the set of paths that should be excluded from backup, if any.
    fn get_excludes(&self) -> Vec<PathBuf>;

    /// Insert the given chunk into the repository, if one with the same digest does
    /// not already exist. Chunks with the same digest are assumed to be identical.
    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error>;

    /// Retrieve the chunk by the given digest, returning `None` if not found.
    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error>;

    /// Retrieve the digests of all chunk records.
    fn get_all_chunk_digests(&self) -> Result<HashedArrayTree<String>, Error>;

    /// Remove the chunk record by the given identifier.
    fn delete_chunk(&self, id: &str) -> Result<(), Error>;

    /// Insert the given pack into the repository, if one with the same digest
    /// does not already exist. Packs with the same digest are assumed to be
    /// identical.
    fn insert_pack(&self, pack: &Pack) -> Result<(), Error>;

    /// Write the given pack record to the repository, overwriting any existing
    /// record with the same digest.
    fn put_pack(&self, pack: &Pack) -> Result<(), Error>;

    /// Retrieve the pack by the given digest, returning `None` if not found.
    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error>;

    /// Retrieve the digests of all pack records.
    fn get_all_pack_digests(&self) -> Result<HashedArrayTree<String>, Error>;

    /// Remove the pack record by the given identifier.
    fn delete_pack(&self, id: &str) -> Result<(), Error>;

    /// Insert the given psedo-pack for the database snapshot, if one with the
    /// same digest does not already exist. Packs with the same digest are
    /// assumed to be identical.
    fn insert_database(&self, pack: &Pack) -> Result<(), Error>;

    /// Write the given database pseudo-pack record to the repository,
    /// overwriting any existing record with the same digest.
    fn put_database(&self, pack: &Pack) -> Result<(), Error>;

    /// Retrieve the database pseudo-pack by the given digest, returning `None`
    /// if not found.
    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error>;

    /// Retrieve all database pseudo-pack records.
    fn get_databases(&self) -> Result<Vec<Pack>, Error>;

    /// Remove the database pseudo-pack record by the given identifier.
    fn delete_database(&self, id: &str) -> Result<(), Error>;

    /// Insert the extended file attributes value into the repository, if one
    /// with the same digest does not already exist. Values with the same digest
    /// are assumed to be identical.
    fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error>;

    /// Retrieve the extended attributes by the given digest, returning `None`
    /// if not found.
    fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error>;

    /// Retrieve the digests of all xattr records.
    fn get_all_xattr_digests(&self) -> Result<HashedArrayTree<String>, Error>;

    /// Remove the xattr record by the given identifier.
    fn delete_xattr(&self, id: &str) -> Result<(), Error>;

    /// Insert the given file into the repository, if one with the same digest
    /// does not already exist. Files with the same digest are assumed to be
    /// identical.
    fn insert_file(&self, file: &File) -> Result<(), Error>;

    /// Retrieve the file by the given digest, returning `None` if not found.
    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error>;

    /// Retrieve the digests of all file records.
    fn get_all_file_digests(&self) -> Result<HashedArrayTree<String>, Error>;

    /// Remove the file record by the given identifier.
    fn delete_file(&self, id: &str) -> Result<(), Error>;

    /// Insert the given tree into the repository, if one with the same digest
    /// does not already exist. Trees with the same digest are assumed to be
    /// identical.
    fn insert_tree(&self, tree: &Tree) -> Result<(), Error>;

    /// Retrieve the tree by the given digest, returning `None` if not found.
    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error>;

    /// Retrieve the digests of all tree records.
    fn get_all_tree_digests(&self) -> Result<HashedArrayTree<String>, Error>;

    /// Remove the tree record by the given identifier.
    fn delete_tree(&self, id: &str) -> Result<(), Error>;

    /// Save the given store to the repository.
    fn put_store(&self, store: &Store) -> Result<(), Error>;

    /// Retrieve all registered pack store configurations.
    fn get_stores(&self) -> Result<Vec<Store>, Error>;

    /// Retrieve the store by identifier, returning `None` if not found.
    fn get_store(&self, id: &str) -> Result<Option<Store>, Error>;

    /// Remove the store by the given identifier.
    fn delete_store(&self, id: &str) -> Result<(), Error>;

    /// Construct a pack repository for the given dataset.
    ///
    /// If the dataset does not have any valid stores defined, an error is
    /// returned, rather than producing a useless pack repository.
    fn load_dataset_stores(&self, dataset: &Dataset) -> Result<Box<dyn PackRepository>, Error>;

    /// Construct a pack repository for the given pack store.
    fn build_pack_repo(&self, store: &Store) -> Result<Box<dyn PackRepository>, Error>;

    /// Save the given dataset to the repository.
    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error>;

    /// Retrieve all defined dataset configurations.
    fn get_datasets(&self) -> Result<Vec<Dataset>, Error>;

    /// Retrieve the dataset by the given identifier.
    fn get_dataset(&self, id: &str) -> Result<Option<Dataset>, Error>;

    /// Remove the dataset by the given identifier.
    fn delete_dataset(&self, id: &str) -> Result<(), Error>;

    /// Save the given snapshot to the repository.
    fn put_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error>;

    /// Retrieve a snapshot by its digest, returning `None` if not found.
    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error>;

    /// Remove the snapshot record by the given identifier.
    fn delete_snapshot(&self, id: &str) -> Result<(), Error>;

    /// Create a backup of the database, returning the path of the archive file.
    fn create_backup(&self, password: &str) -> Result<tempfile::TempPath, Error>;

    /// Restore the database from the provided archive file.
    fn restore_from_backup(&self, path: &Path, password: &str) -> Result<(), Error>;

    /// Retrieve the counts of the various record types in the data source.
    fn get_entity_counts(&self) -> Result<RecordCounts, Error>;

    /// Record a generated bucket name for future lookup.
    fn add_bucket(&self, name: &str) -> Result<(), Error>;

    /// Return a randomly selected bucket name, or `None` if no buckets exist.
    fn get_random_bucket(&self) -> Result<Option<String>, Error>;

    /// Return the number of recorded bucket names.
    fn count_buckets(&self) -> Result<usize, Error>;

    /// Return the most recently generated bucket name (lexicographically
    /// greatest), or `None` if no buckets exist.
    fn get_last_bucket(&self) -> Result<Option<String>, Error>;

    /// Return a bucket name generator for the currently configured naming
    /// policy. Falls back to `BucketNamingPolicy::RandomPool(100)` when no
    /// policy is stored in the configuration record.
    fn bucket_namer(&self) -> Result<Box<dyn BucketNameGenerator>, Error>;
}

///
/// Repository for pack files.
///
#[cfg_attr(test, automock)]
pub trait PackRepository: Send + Sync {
    /// Save the given pack to the stores provided in the constructor.
    ///
    /// Returns the list of all pack locations, which can be used to retrieve
    /// the pack at a later time.
    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Vec<PackLocation>, Error>;

    /// Retrieve the pack from one of the stores provided in the constructor.
    ///
    /// The most suitable store will be utilized, preferring a local store over
    /// a remote one, and fast one over a slow one.
    fn retrieve_pack(&self, locations: &[PackLocation], outfile: &Path) -> Result<(), Error>;

    /// Delete the object identified by the given pack location from whichever
    /// store in this repository matches `location.store`.
    ///
    /// Used by the pack pruner for both pack files and database archives,
    /// since both are stored as objects in the underlying pack store.
    fn delete_pack(&self, location: &PackLocation) -> Result<(), Error>;

    /// Test the connection to the store with the given identifier.
    ///
    /// Only tests the connection and read access by listing buckets. Any errors
    /// raised by the data source are returned as-is.
    fn test_store(&self, store: &str) -> Result<(), Error>;

    /// Store the compressed database snapshot in the pack stores.
    ///
    /// This archive should be stored in such a manner that it can be retrieved
    /// using only the computer identifier. Regardless, the pack locations are
    /// returned for the purpose of tracking them, to support accurate pruning.
    fn store_database(&self, computer_id: &str, infile: &Path) -> Result<Vec<PackLocation>, Error>;

    /// Retrieve the most recent database snapshot for the given computer.
    ///
    /// Uses a random pack store to fetch the database. It is expected that only
    /// one pack store is defined at this point and the user has configured the
    /// most suitable pack store in order to retrieve the database.
    fn retrieve_latest_database(&self, computer_id: &str, outfile: &Path) -> Result<(), Error>;
}

///
/// Repository for errors captured from background operations (prune, test
/// restore, backup, future database scrub). Surfaces those failures to the
/// user via the web interface so they are not limited to the log file.
///
#[cfg_attr(test, automock)]
pub trait ErrorRepository: Send + Sync {
    /// Persist an error. Must not fail loudly to the caller in a way that
    /// masks the original error; record-site callers should log and continue
    /// if this returns an error.
    fn record_error(
        &self,
        operation: ErrorOperation,
        dataset_id: Option<String>,
        message: &str,
    ) -> Result<(), Error>;

    /// Return the captured errors, most recent first. `limit` caps the number
    /// of rows returned; `None` returns all rows.
    fn list_errors(&self, limit: Option<u32>) -> Result<Vec<CapturedError>, Error>;

    /// Return the number of captured errors currently stored.
    fn count_errors(&self) -> Result<u64, Error>;

    /// Delete a single captured error by identifier. Returns true if a row
    /// was deleted.
    fn delete_error(&self, id: i64) -> Result<bool, Error>;

    /// Delete every captured error. Returns the number of rows removed.
    fn clear_all(&self) -> Result<u64, Error>;

    /// Delete captured errors older than the given number of days. Returns the
    /// number of rows removed.
    fn prune_older_than(&self, days: u32) -> Result<u64, Error>;
}
