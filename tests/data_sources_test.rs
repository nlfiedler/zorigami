//
// Copyright (c) 2020 Nathan Fiedler
//
mod common;

use common::DBPath;
use std::collections::HashMap;
use zorigami::data::sources::EntityDataSource;
use zorigami::data::sources::EntityDataSourceImpl;
use zorigami::domain::entities::{self, Checksum};

#[test]
fn test_insert_get_chunk() {
    let db_path = DBPath::new("_test_insert_get_chunk");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    // missing Chunk returns None
    let missingsum = Checksum::SHA1("cafebabedeadbeef".to_owned());
    let result = datasource.get_chunk(&missingsum);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // insert/get should return something a little different since not all
    // fields are serialized to the database
    let digest1 = Checksum::SHA256(
        "ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1".to_owned(),
    );
    let packsum1 = Checksum::SHA1("bc1a3198db79036e56b30f0ab307cee55e845907".to_owned());
    let chunk1 = entities::Chunk::new(digest1, 0, 40000).packfile(packsum1);
    assert!(chunk1.packfile.is_some());
    assert!(datasource.insert_chunk(&chunk1).is_ok());
    let result = datasource.get_chunk(&chunk1.digest);
    assert!(result.is_ok());
    let record: Option<entities::Chunk> = result.unwrap();
    assert!(record.is_some());
    let actual: entities::Chunk = record.unwrap();
    assert_eq!(
        actual.digest.to_string(),
        "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1"
    );
    // skipped offset is always zero
    assert_eq!(actual.offset, 0);
    assert_eq!(actual.length, 40000);
    assert!(actual.filepath.is_none());
    assert!(actual.packfile.is_some());
    assert_eq!(
        actual.packfile.unwrap().to_string(),
        "sha1-bc1a3198db79036e56b30f0ab307cee55e845907"
    );

    // Inserting a chunk with different property values but the same digest
    // (which is wrong regardless) will _not_ overwrite the entry already in the
    // database.
    let digest2 = Checksum::SHA256(
        "ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1".to_owned(),
    );
    let packsum2 = Checksum::SHA1("bc1a3198db79036e56b30f0ab307cee55e845907".to_owned());
    let chunk2 = entities::Chunk::new(digest2, 0, 65356).packfile(packsum2);
    assert!(chunk2.packfile.is_some());
    assert!(datasource.insert_chunk(&chunk2).is_ok());
    let result = datasource.get_chunk(&chunk1.digest);
    assert!(result.is_ok());
    let record: Option<entities::Chunk> = result.unwrap();
    assert!(record.is_some());
    let actual: entities::Chunk = record.unwrap();
    assert_eq!(
        actual.digest.to_string(),
        "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1"
    );
    // skipped offset is always zero
    assert_eq!(actual.offset, 0);
    assert_eq!(actual.length, 40000);
    assert!(actual.filepath.is_none());
    assert!(actual.packfile.is_some());
    assert_eq!(
        actual.packfile.unwrap().to_string(),
        "sha1-bc1a3198db79036e56b30f0ab307cee55e845907"
    );
}

#[test]
fn test_put_get_store() {
    let db_path = DBPath::new("_test_put_get_store");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    // populate the data source with stores
    let mut properties: HashMap<String, String> = HashMap::new();
    properties.insert("basepath".to_owned(), "/home/planet".to_owned());
    let store = entities::Store {
        id: "cafebabe".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "local disk".to_owned(),
        properties,
    };
    datasource.put_store(&store).unwrap();
    properties = HashMap::new();
    properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
    let store = entities::Store {
        id: "deadbeef".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "minio host".to_owned(),
        properties,
    };
    datasource.put_store(&store).unwrap();

    // retrieve all known pack stores
    let stores = datasource.get_stores().unwrap();
    assert_eq!(stores.len(), 2);
    assert!(stores.iter().any(|s| s.id == "cafebabe"));
    assert!(stores.iter().any(|s| s.id == "deadbeef"));
}
