//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, PackLocation, Snapshot, Store, Tree,
};
use failure::Error;
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

    /// Provide the set of paths that should be excluded from backup, if any.
    fn get_excludes(&self) -> Vec<PathBuf>;

    /// Store the computer identifier for the dataset with the given key.
    fn put_computer_id(&self, dataset: &str, computer_id: &str) -> Result<(), Error>;

    /// Retrieve the computer identifier for dataset with the given key.
    fn get_computer_id(&self, dataset: &str) -> Result<Option<String>, Error>;

    /// Remove the computer identifier for the dataset with the given key.
    fn delete_computer_id(&self, dataset: &str) -> Result<(), Error>;

    /// Store the digest of the latest snapshot for the dataset with the given key.
    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error>;

    /// Retrieve the digest of the latest snapshot for the dataset with the given key.
    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error>;

    /// Remove the digest of the latest snapshot for the dataset with the given key.
    fn delete_latest_snapshot(&self, dataset: &str) -> Result<(), Error>;

    /// Insert the given chunk into the repository, if one with the same digest does
    /// not already exist. Chunks with the same digest are assumed to be identical.
    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error>;

    /// Retrieve the chunk by the given digest, returning `None` if not found.
    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error>;

    /// Insert the given pack into the repository, if one with the same digest
    /// does not already exist. Packs with the same digest are assumed to be
    /// identical.
    fn insert_pack(&self, pack: &Pack) -> Result<(), Error>;

    /// Save the given pack to the repository, overwriting any existing entry.
    fn put_pack(&self, pack: &Pack) -> Result<(), Error>;

    /// Retrieve the pack by the given digest, returning `None` if not found.
    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error>;

    /// Retrieve all pack records that should be in the given store.
    fn get_packs(&self, store_id: &str) -> Result<Vec<Pack>, Error>;

    /// Insert the given psedo-pack for the database snapshot, if one with the
    /// same digest does not already exist. Packs with the same digest are
    /// assumed to be identical.
    fn insert_database(&self, pack: &Pack) -> Result<(), Error>;

    /// Retrieve the database pseudo-pack by the given digest, returning `None`
    /// if not found.
    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error>;

    /// Retrieve all database pseudo-pack records.
    fn get_databases(&self) -> Result<Vec<Pack>, Error>;

    /// Insert the extended file attributes value into the repository, if one
    /// with the same digest does not already exist. Values with the same digest
    /// are assumed to be identical.
    fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error>;

    /// Retrieve the extended attributes by the given digest, returning `None`
    /// if not found.
    fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error>;

    /// Insert the given file into the repository, if one with the same digest
    /// does not already exist. Files with the same digest are assumed to be
    /// identical.
    fn insert_file(&self, file: &File) -> Result<(), Error>;

    /// Retrieve the file by the given digest, returning `None` if not found.
    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error>;

    /// Insert the given tree into the repository, if one with the same digest
    /// does not already exist. Trees with the same digest are assumed to be
    /// identical.
    fn insert_tree(&self, tree: &Tree) -> Result<(), Error>;

    /// Retrieve the tree by the given digest, returning `None` if not found.
    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error>;

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

    /// Create a backup of the database, returning the path of the archive file.
    fn create_backup(&self) -> Result<tempfile::TempPath, Error>;

    /// Restore the database from the provided archive file.
    fn restore_from_backup(&self, path: &Path) -> Result<(), Error>;
}

///
/// Repository for pack files.
///
#[cfg_attr(test, automock)]
pub trait PackRepository {
    /// Generate a unique bucket name for storing pack files.
    ///
    /// This function should be called for each call to `store_pack()` in order
    /// to ensure buckets are reused and yet not overused, as appropriate.
    ///
    /// The computer identifier is typically used in generating the bucket name.
    fn get_bucket_name(&self, computer_id: &str) -> String;

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

    /// Find any packs that are missing from the given pack store.
    ///
    /// Returns a new list of the pack digests for those packs that were not
    /// found on the pack store.
    fn find_missing(&self, store_id: &str, packs: &[Pack]) -> Result<Vec<Checksum>, Error>;

    /// Remove any extraneous objects and empty buckets.
    ///
    /// Returns the number of objects removed by this operation.
    fn prune_extra(&self, store_id: &str, packs: &[Pack]) -> Result<u32, Error>;
}
