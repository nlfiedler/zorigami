//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use rocksdb::{DBVector, DB};
use super::core::Chunk;

///
/// Insert the value if the database does not already contain the given key.
///
pub fn insert_document(db: &DB, key: &[u8], value: &[u8]) -> Result<(), Error> {
    let existing = db.get(key)?;
    if existing.is_none() {
        db.put(key, value)?;
    }
    Ok(())
}

///
/// Retrieve the value with the given key.
///
pub fn get_document(db: &DB, key: &[u8]) -> Result<Option<DBVector>, Error> {
    let result = db.get(key)?;
    Ok(result)
}

///
/// Insert the given chunk into the database, if one with the same digest does
/// not already exist. Chunks with the same digest are assumed to be identical.
///
pub fn insert_chunk(db: &DB, chunk: &Chunk) -> Result<(), Error> {
    let mut key = String::from("chunk/");
    key.push_str(&chunk.digest);
    let encoded: Vec<u8> = serde_cbor::to_vec(&chunk)?;
    insert_document(db, key.as_bytes(), &encoded)
}

///
/// Retrieve the chunk by the given digest, returning None if not found.
///
pub fn get_chunk(db: &DB, digest: &str) -> Result<Option<Chunk>, Error> {
    let mut key = String::from("chunk/");
    key.push_str(digest);
    let encoded = get_document(db, key.as_bytes())?;
    match encoded {
        Some(dbv) => {
            let serde_result: Chunk = serde_cbor::from_slice(&dbv)?;
            Ok(Some(serde_result))
        },
        None => Ok(None)
    }
}
