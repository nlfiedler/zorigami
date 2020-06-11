//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::Store;
use crate::domain::entities::{Checksum, Chunk, Configuration, Dataset, Snapshot};
use failure::Error;
#[cfg(test)]
use mockall::{automock, predicate::*};

///
/// Repository for entity records.
///
#[cfg_attr(test, automock)]
pub trait RecordRepository {
    /// Retrieve the configuration, or build a new one using default values.
    fn get_configuration(&self) -> Result<Configuration, Error>;

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

    /// Remvoe the digest of the latest snapshot for the dataset with the given key.
    fn delete_latest_snapshot(&self, dataset: &str) -> Result<(), Error>;

    /// Insert the given chunk into the database, if one with the same digest does
    /// not already exist. Chunks with the same digest are assumed to be identical.
    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error>;

    /// Retrieve the chunk by the given digest, returning `None` if not found.
    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error>;

    /// Save the given store to the data source.
    fn put_store(&self, store: &Store) -> Result<(), Error>;

    /// Retrieve all registered pack store configurations.
    fn get_stores(&self) -> Result<Vec<Store>, Error>;

    /// Remove the store by the given identifier.
    fn delete_store(&self, id: &str) -> Result<(), Error>;

    /// Save the given dataset to the data source.
    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error>;

    /// Retrieve all defined dataset configurations.
    fn get_datasets(&self) -> Result<Vec<Dataset>, Error>;

    /// Remove the dataset by the given identifier.
    fn delete_dataset(&self, id: &str) -> Result<(), Error>;

    /// Retrieve a snapshot by its digest, returning `None` if not found.
    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error>;
}

// ///
// /// Repository for asset blobs.
// ///
// #[cfg_attr(test, automock)]
// pub trait BlobRepository {
//     /// Move the given file into the blob store.
//     ///
//     /// Existing blobs will not be overwritten.
//     fn store_blob(&self, filepath: &Path, asset: &Asset) -> Result<(), Error>;

//     /// Return the full path to the asset in blob storage.
//     fn blob_path(&self, asset_id: &str) -> Result<PathBuf, Error>;

//     /// Change the identity of the asset in blob storage.
//     fn rename_blob(&self, old_id: &str, new_id: &str) -> Result<(), Error>;

//     /// Produce a thumbnail of the desired size for the asset.
//     fn thumbnail(&self, width: u32, height: u32, asset_id: &str) -> Result<Vec<u8>, Error>;
// }
