//
// Copyright (c) 2020 Nathan Fiedler
//

//! Performs serde on entities and stores them in a database.

use crate::data::models::{ChunkDef, ConfigurationDef, DatasetDef, SnapshotDef, StoreDef};
use crate::domain::entities::{Checksum, Chunk, Configuration, Dataset, Snapshot, Store};
use failure::Error;
#[cfg(test)]
use mockall::automock;
use std::path::Path;
use std::str::FromStr;

mod database;

/// Data source for entity objects.
#[cfg_attr(test, automock)]
pub trait EntityDataSource {
    /// Retrieve the configuration from the datasource.
    fn get_configuration(&self) -> Result<Option<Configuration>, Error>;

    /// Store the configuration record in the datasource.
    fn put_configuration(&self, config: &Configuration) -> Result<(), Error>;

    /// Store the computer identifier for the dataset with the given key.
    fn put_computer_id(&self, dataset: &str, computer_id: &str) -> Result<(), Error>;

    /// Retrieve the computer identifier for the dataset with the given key.
    fn get_computer_id(&self, dataset: &str) -> Result<Option<String>, Error>;

    /// Store the digest of the latest snapshot for the dataset with the given key.
    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error>;

    /// Retrieve the digest of the latest snapshot for the dataset with the given key.
    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error>;

    /// Insert the given chunk into the database, if one with the same digest does
    /// not already exist. Chunks with the same digest are assumed to be identical.
    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error>;

    /// Retrieve the chunk by the given digest, returning `None` if not found.
    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error>;

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

    /// Retrieve a snapshot by its digest, returning `None` if not found.
    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error>;
}

/// Implementation of the entity data source backed by RocksDB.
pub struct EntityDataSourceImpl {
    database: database::Database,
}

impl EntityDataSourceImpl {
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self, Error> {
        std::fs::create_dir_all(&db_path)?;
        let database = database::Database::new(db_path)?;
        Ok(Self { database })
    }
}

impl EntityDataSource for EntityDataSourceImpl {
    fn get_configuration(&self) -> Result<Option<Configuration>, Error> {
        let key = "configuration";
        let encoded = self.database.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let result = ConfigurationDef::deserialize(&mut de)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn put_configuration(&self, config: &Configuration) -> Result<(), Error> {
        let key = "configuration";
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        ConfigurationDef::serialize(config, &mut ser)?;
        self.database.put_document(key.as_bytes(), &encoded)
    }

    fn put_computer_id(&self, dataset: &str, computer_id: &str) -> Result<(), Error> {
        let key = format!("computer/{}", dataset);
        self.database
            .put_document(key.as_bytes(), computer_id.as_bytes())
    }

    fn get_computer_id(&self, dataset: &str) -> Result<Option<String>, Error> {
        let key = format!("computer/{}", dataset);
        let option = self.database.get_document(key.as_bytes())?;
        match option {
            Some(value) => {
                let result = String::from_utf8(value)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error> {
        let key = format!("latest/{}", dataset);
        // use simple approach as serde can be tricky to compile
        let as_string = latest.to_string();
        self.database
            .put_document(key.as_bytes(), as_string.as_bytes())
    }

    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error> {
        let key = format!("latest/{}", dataset);
        let option = self.database.get_document(key.as_bytes())?;
        match option {
            Some(value) => {
                let as_string = String::from_utf8(value)?;
                let result: Result<Checksum, Error> = FromStr::from_str(&as_string);
                result.map(|v| Some(v))
            }
            None => Ok(None),
        }
    }

    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error> {
        let key = format!("chunk/{}", chunk.digest);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        ChunkDef::serialize(chunk, &mut ser)?;
        self.database.insert_document(key.as_bytes(), &encoded)
    }

    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error> {
        let key = format!("chunk/{}", digest);
        let encoded = self.database.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = ChunkDef::deserialize(&mut de)?;
                result.digest = digest.clone();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn put_store(&self, store: &Store) -> Result<(), Error> {
        let key = format!("store/{}", store.id);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        StoreDef::serialize(store, &mut ser)?;
        self.database.put_document(key.as_bytes(), &encoded)
    }

    fn get_stores(&self) -> Result<Vec<Store>, Error> {
        let stores = self.database.fetch_prefix("store/")?;
        let mut results: Vec<Store> = Vec::new();
        for (key, value) in stores {
            let mut de = serde_cbor::Deserializer::from_slice(&value);
            let mut result = StoreDef::deserialize(&mut de)?;
            result.id = key;
            results.push(result);
        }
        Ok(results)
    }

    fn get_store(&self, id: &str) -> Result<Option<Store>, Error> {
        let key = format!("store/{}", id);
        let encoded = self.database.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = StoreDef::deserialize(&mut de)?;
                result.id = key;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn delete_store(&self, id: &str) -> Result<(), Error> {
        let key = format!("store/{}", id);
        self.database.delete_document(key.as_bytes())
    }

    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error> {
        let key = format!("dataset/{}", dataset.key);
        let mut encoded: Vec<u8> = Vec::new();
        let mut ser = serde_cbor::Serializer::new(&mut encoded);
        DatasetDef::serialize(dataset, &mut ser)?;
        self.database.put_document(key.as_bytes(), &encoded)
    }

    fn get_datasets(&self) -> Result<Vec<Dataset>, Error> {
        let datasets = self.database.fetch_prefix("dataset/")?;
        let mut results: Vec<Dataset> = Vec::new();
        for (key, value) in datasets {
            let mut de = serde_cbor::Deserializer::from_slice(&value);
            let mut result = DatasetDef::deserialize(&mut de)?;
            result.key = key;
            results.push(result);
        }
        Ok(results)
    }

    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error> {
        let key = format!("snapshot/{}", digest);
        let encoded = self.database.get_document(key.as_bytes())?;
        match encoded {
            Some(value) => {
                let mut de = serde_cbor::Deserializer::from_slice(&value);
                let mut result = SnapshotDef::deserialize(&mut de)?;
                result.digest = digest.clone();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}
