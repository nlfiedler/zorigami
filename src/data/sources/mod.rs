//
// Copyright (c) 2020 Nathan Fiedler
//

//! Performs serde on entities and stores them in a database.

use crate::data::models::{ChunkDef, StoreDef};
use crate::domain::entities::{Checksum, Chunk, Store};
use failure::Error;
#[cfg(test)]
use mockall::automock;
use std::path::Path;

mod database;

/// Data source for entity objects.
#[cfg_attr(test, automock)]
pub trait EntityDataSource {
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

    fn delete_store(&self, id: &str) -> Result<(), Error> {
        let key = format!("store/{}", id);
        self.database.delete_document(key.as_bytes())
    }
}
