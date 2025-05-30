//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, PackLocation, RecordCounts, Snapshot, Store, Tree,
};
use anyhow::Error;
#[cfg(test)]
use mockall::automock;
use std::path::{Path, PathBuf};

/// Data source for entity objects.
#[cfg_attr(test, automock)]
pub trait EntityDataSource: Send + Sync {
    /// Save the configuration record to the data source.
    fn put_configuration(&self, config: &Configuration) -> Result<(), Error>;

    /// Retrieve the configuration from the data source.
    fn get_configuration(&self) -> Result<Option<Configuration>, Error>;

    /// Save the computer identifier for the dataset with the given key.
    fn put_computer_id(&self, dataset: &str, computer_id: &str) -> Result<(), Error>;

    /// Retrieve the computer identifier for the dataset with the given key.
    fn get_computer_id(&self, dataset: &str) -> Result<Option<String>, Error>;

    /// Remove the computer identifier for the dataset with the given key.
    fn delete_computer_id(&self, dataset: &str) -> Result<(), Error>;

    /// Save the digest of the latest snapshot for the dataset with the given key.
    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error>;

    /// Retrieve the digest of the latest snapshot for the dataset with the given key.
    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error>;

    /// Remvoe the digest of the latest snapshot for the dataset with the given key.
    fn delete_latest_snapshot(&self, dataset: &str) -> Result<(), Error>;

    /// Insert the given chunk into the data source, if one with the same digest does
    /// not already exist. Chunks with the same digest are assumed to be identical.
    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error>;

    /// Retrieve the chunk by the given digest, returning `None` if not found.
    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error>;

    /// Retrieve the digests of all chunk records.
    fn get_all_chunk_digests(&self) -> Result<Vec<String>, Error>;

    /// Remove the chunk record by the given identifier.
    fn delete_chunk(&self, id: &str) -> Result<(), Error>;

    /// Insert the given pack into the data source, if one with the same digest
    /// does not already exist. Packs with the same digest are assumed to be
    /// identical.
    fn insert_pack(&self, pack: &Pack) -> Result<(), Error>;

    /// Save the given pack to the data source, overwriting any existing entry.
    fn put_pack(&self, pack: &Pack) -> Result<(), Error>;

    /// Retrieve the pack by the given digest, returning `None` if not found.
    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error>;

    /// Retrieve all pack records that should be in the given store.
    fn get_packs(&self, store_id: &str) -> Result<Vec<Pack>, Error>;

    /// Retrieve all pack records in the system regardless of store.
    fn get_all_packs(&self) -> Result<Vec<Pack>, Error>;

    /// Insert the given psedo-pack for the database snapshot, if one with the
    /// same digest does not already exist. Packs with the same digest are
    /// assumed to be identical.
    fn insert_database(&self, pack: &Pack) -> Result<(), Error>;

    /// Retrieve the database pseudo-pack by the given digest, returning `None`
    /// if not found.
    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error>;

    /// Retrieve all database pseudo-pack records.
    fn get_databases(&self) -> Result<Vec<Pack>, Error>;

    /// Insert the extended file attributes value into the data source, if one
    /// with the same digest does not already exist. Values with the same digest
    /// are assumed to be identical.
    fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error>;

    /// Retrieve the extended attributes by the given digest, returning `None`
    /// if not found.
    fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error>;

    /// Retrieve the digests of all xattr records.
    fn get_all_xattr_digests(&self) -> Result<Vec<String>, Error>;

    /// Remove the xattr record by the given identifier.
    fn delete_xattr(&self, id: &str) -> Result<(), Error>;

    /// Insert the given file into the data source, if one with the same digest
    /// does not already exist. Files with the same digest are assumed to be
    /// identical.
    fn insert_file(&self, file: &File) -> Result<(), Error>;

    /// Retrieve the file by the given digest, returning `None` if not found.
    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error>;

    /// Retrieve the digests of all file records.
    fn get_all_file_digests(&self) -> Result<Vec<String>, Error>;

    /// Remove the file record by the given identifier.
    fn delete_file(&self, id: &str) -> Result<(), Error>;

    /// Insert the given tree into the data source, if one with the same digest
    /// does not already exist. Trees with the same digest are assumed to be
    /// identical.
    fn insert_tree(&self, tree: &Tree) -> Result<(), Error>;

    /// Retrieve the tree by the given digest, returning `None` if not found.
    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error>;

    /// Retrieve the digests of all tree records.
    fn get_all_tree_digests(&self) -> Result<Vec<String>, Error>;

    /// Remove the tree record by the given identifier.
    fn delete_tree(&self, id: &str) -> Result<(), Error>;

    /// Save the given store to the data source.
    fn put_store(&self, store: &Store) -> Result<(), Error>;

    /// Retrieve all registered pack store configurations.
    fn get_stores(&self) -> Result<Vec<Store>, Error>;

    /// Retrieve the store by identifier, returning `None` if not found.
    fn get_store(&self, id: &str) -> Result<Option<Store>, Error>;

    /// Remove the store by the given identifier.
    fn delete_store(&self, id: &str) -> Result<(), Error>;

    /// Save the given dataset to the data source.
    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error>;

    /// Retrieve all defined dataset configurations.
    fn get_datasets(&self) -> Result<Vec<Dataset>, Error>;

    /// Retrieve the dataset by the given identifier.
    fn get_dataset(&self, id: &str) -> Result<Option<Dataset>, Error>;

    /// Remove the dataset by the given identifier.
    fn delete_dataset(&self, id: &str) -> Result<(), Error>;

    /// Save the given snapshot to the data source.
    fn put_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error>;

    /// Retrieve a snapshot by its digest, returning `None` if not found.
    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error>;

    /// Remove the snapshot record by the given identifier.
    fn delete_snapshot(&self, id: &str) -> Result<(), Error>;

    /// Retrieve the path to the database files.
    fn get_db_path(&self) -> PathBuf;

    /// Create a backup of the database, returning its path.
    fn create_backup(&self, path: Option<PathBuf>) -> Result<PathBuf, Error>;

    /// Restore the database from the backup path.
    fn restore_from_backup(&self, path: Option<PathBuf>) -> Result<(), Error>;

    /// Retrieve the counts of the various record types in the data source.
    fn get_entity_counts(&self) -> Result<RecordCounts, Error>;
}

///
/// Data source for pack files.
///
#[cfg_attr(test, automock)]
pub trait PackDataSource: Send + Sync {
    /// Return `true` if this store is local to the system.
    fn is_local(&self) -> bool;

    /// Return `true` if this store is remarkably slow compared to usual.
    fn is_slow(&self) -> bool;

    /// Store the pack file under the named bucket and referenced by the object
    /// name. Returns the remote location of the pack, in case it was assigned
    /// new values by the backing store.
    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error>;

    /// Retrieve a pack from the given location, writing the contents to the
    /// given path.
    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error>;

    /// List the known buckets in the repository.
    fn list_buckets(&self) -> Result<Vec<String>, Error>;

    /// List of all objects in the named bucket.
    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error>;

    /// Delete the named object from the given bucket.
    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error>;

    /// Delete the named bucket. It almost certainly needs to be empty first, so
    /// use `list_objects()` and `delete_object()` to remove the objects.
    fn delete_bucket(&self, bucket: &str) -> Result<(), Error>;

    /// Store the database archive under the named bucket and referenced by the
    /// object name. Returns the remote location of the pack, in case it was
    /// assigned new values by the backing store.
    fn store_database(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error>;

    /// Retrieve a database archive from the given location, writing the
    /// contents to the given path.
    fn retrieve_database(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error>;

    /// List all database archives in the named bucket.
    fn list_databases(&self, bucket: &str) -> Result<Vec<String>, Error>;
}
