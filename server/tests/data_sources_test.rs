//
// Copyright (c) 2020 Nathan Fiedler
//
mod common;

use common::DBPath;
use server::data::sources::{EntityDataSource, EntityDataSourceImpl};
use server::domain::entities::{self, Checksum};
use sodiumoxide::crypto::pwhash;
use std::collections::HashMap;
use std::path::Path;

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
fn test_insert_get_pack() {
    let db_path = DBPath::new("_test_insert_get_pack");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    let digest1 = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
    let coords = vec![entities::PackLocation::new("store1", "bucket1", "object1")];
    let mut pack = entities::Pack::new(digest1.clone(), coords);
    // (normally should init sodiumoxide but for the tests it is okay)
    pack.crypto_salt = Some(pwhash::gen_salt());
    datasource.insert_pack(&pack).unwrap();
    datasource.insert_pack(&pack).unwrap();
    datasource.insert_pack(&pack).unwrap();
    let option = datasource.get_pack(&pack.digest).unwrap();
    assert!(option.is_some());
    let actual = option.unwrap();
    assert_eq!(actual.digest, pack.digest);
    assert_eq!(actual.locations.len(), pack.locations.len());
    assert_eq!(actual.locations.len(), 1);
    assert_eq!(actual.locations[0], pack.locations[0]);
    assert_eq!(actual.upload_time, pack.upload_time);
    assert_eq!(actual.crypto_salt, pack.crypto_salt);

    // insert a bunch more pack records to test get_packs()
    let digest2 = Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
    let coords = vec![
        entities::PackLocation::new("store1", "bucket1", "object2"),
        entities::PackLocation::new("store2", "bucket1", "object2"),
    ];
    let mut pack = entities::Pack::new(digest2.clone(), coords);
    pack.crypto_salt = Some(pwhash::gen_salt());
    datasource.insert_pack(&pack).unwrap();

    let digest3 = Checksum::SHA1(String::from("bf24db8ccd274daad5fe73a71b95cd00ffa56a37"));
    let coords = vec![entities::PackLocation::new("store2", "bucket1", "object3")];
    let mut pack = entities::Pack::new(digest3.clone(), coords);
    pack.crypto_salt = Some(pwhash::gen_salt());
    datasource.insert_pack(&pack).unwrap();

    let digest4 = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
    let coords = vec![
        entities::PackLocation::new("store2", "bucket1", "object4"),
        entities::PackLocation::new("store3", "bucket1", "object4"),
        entities::PackLocation::new("store11", "bucket1", "object4"),
    ];
    let mut pack = entities::Pack::new(digest4.clone(), coords);
    pack.crypto_salt = Some(pwhash::gen_salt());
    datasource.insert_pack(&pack).unwrap();

    let digest5 = Checksum::SHA1(String::from("1619f1be8e89c810fb213efa2f7b30ab788d3ada"));
    let coords = vec![entities::PackLocation::new("store1", "bucket1", "object5")];
    let mut pack = entities::Pack::new(digest5.clone(), coords);
    pack.crypto_salt = Some(pwhash::gen_salt());
    datasource.insert_pack(&pack).unwrap();

    // test get_packs()
    let mut packs = datasource.get_packs("store1").unwrap();
    assert_eq!(packs.len(), 3);
    packs.sort_unstable_by(|a, b| a.digest.partial_cmp(&b.digest).unwrap());
    assert_eq!(packs[0].digest, digest5);
    assert_eq!(packs[1].digest, digest2);
    assert_eq!(packs[2].digest, digest1);
}

#[test]
fn test_insert_get_database() {
    let db_path = DBPath::new("_test_insert_get_database");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    let digest1 = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
    let coords = vec![entities::PackLocation::new("store1", "bucket1", "object1")];
    let pack = entities::Pack::new(digest1.clone(), coords);
    datasource.insert_database(&pack).unwrap();
    datasource.insert_database(&pack).unwrap();
    datasource.insert_database(&pack).unwrap();
    let option = datasource.get_database(&pack.digest).unwrap();
    assert!(option.is_some());
    let actual = option.unwrap();
    assert_eq!(actual.digest, pack.digest);
    assert_eq!(actual.locations.len(), pack.locations.len());
    assert_eq!(actual.locations.len(), 1);
    assert_eq!(actual.locations[0], pack.locations[0]);
    assert_eq!(actual.upload_time, pack.upload_time);
    assert_eq!(actual.crypto_salt, pack.crypto_salt);

    // insert some more database records to test get_databases()
    let digest2 = Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
    let coords = vec![entities::PackLocation::new("store1", "bucket1", "object2")];
    let pack = entities::Pack::new(digest2.clone(), coords);
    datasource.insert_database(&pack).unwrap();

    let digest3 = Checksum::SHA1(String::from("bf24db8ccd274daad5fe73a71b95cd00ffa56a37"));
    let coords = vec![entities::PackLocation::new("store2", "bucket1", "object3")];
    let pack = entities::Pack::new(digest3.clone(), coords);
    datasource.insert_database(&pack).unwrap();

    // test get_databases()
    let mut packs = datasource.get_databases().unwrap();
    assert_eq!(packs.len(), 3);
    packs.sort_unstable_by(|a, b| a.digest.partial_cmp(&b.digest).unwrap());
    assert_eq!(packs[0].digest, digest2);
    assert_eq!(packs[1].digest, digest1);
    assert_eq!(packs[2].digest, digest3);
}

#[test]
fn test_put_get_delete_store() {
    let db_path = DBPath::new("_test_put_get_delete_store");
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
    assert!(!stores[0].id.starts_with("store/"));
    assert!(stores.iter().any(|s| s.id == "cafebabe"));
    assert!(stores.iter().any(|s| s.id == "deadbeef"));

    let result = datasource.get_store("cafebabe");
    assert!(result.is_ok());
    let option = result.unwrap();
    assert!(option.is_some());
    let store = option.unwrap();
    assert!(!store.id.starts_with("store/"));
    let result = datasource.get_store("cafed00d");
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // delete one of the stores
    datasource.delete_store("deadbeef").unwrap();
    let stores = datasource.get_stores().unwrap();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].id, "cafebabe");
}

#[test]
fn test_put_get_delete_datasets() {
    let db_path = DBPath::new("_test_put_get_delete_datasets");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    // populate the data source with datasets
    let dataset = entities::Dataset::new(Path::new("/home/planet"));
    datasource.put_dataset(&dataset).unwrap();
    let dataset = entities::Dataset::new(Path::new("/home/town"));
    datasource.put_dataset(&dataset).unwrap();

    // retrieve all known datasets
    let datasets = datasource.get_datasets().unwrap();
    assert_eq!(datasets.len(), 2);
    assert!(!datasets[0].id.starts_with("dataset/"));
    assert!(datasets
        .iter()
        .any(|s| s.basepath.to_string_lossy() == "/home/planet"));
    assert!(datasets
        .iter()
        .any(|s| s.basepath.to_string_lossy() == "/home/town"));

    let actual = datasource.get_dataset(&datasets[0].id).unwrap();
    assert!(actual.is_some());

    // delete one of the datasets
    datasource.delete_dataset(&datasets[0].id).unwrap();
    let datasets = datasource.get_datasets().unwrap();
    assert_eq!(datasets.len(), 1);
}

#[test]
fn test_put_get_configuration() {
    let db_path = DBPath::new("_test_put_get_configuration");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    let expected: entities::Configuration = Default::default();
    datasource.put_configuration(&expected).unwrap();
    let option = datasource.get_configuration().unwrap();
    assert!(option.is_some());
    let actual = option.unwrap();
    assert_eq!(actual.username, expected.username);
    assert_eq!(actual.hostname, expected.hostname);
    assert_eq!(actual.computer_id, expected.computer_id);
}

#[test]
fn test_put_get_computer_id() {
    let db_path = DBPath::new("_test_put_get_computer_id");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    datasource
        .put_computer_id("cafebabe", "charlietuna")
        .unwrap();
    let opt = datasource.get_computer_id("deadbeef").unwrap();
    assert!(opt.is_none());
    let opt = datasource.get_computer_id("cafebabe").unwrap();
    assert!(opt.is_some());
    assert_eq!(opt.unwrap(), "charlietuna");
    datasource.delete_computer_id("cafebabe").unwrap();
    let opt = datasource.get_computer_id("cafebabe").unwrap();
    assert!(opt.is_none());
}

#[test]
fn test_put_get_latest_snapshot() {
    let db_path = DBPath::new("_test_put_get_latest_snapshot");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    let digest = Checksum::SHA1("e1c3cc593da3c696ddc3200ad137ef79681c8052".to_owned());
    datasource.put_latest_snapshot("cafebabe", &digest).unwrap();
    let opt = datasource.get_latest_snapshot("deadbeef").unwrap();
    assert!(opt.is_none());
    let opt = datasource.get_latest_snapshot("cafebabe").unwrap();
    assert!(opt.is_some());
    assert_eq!(opt.unwrap(), digest);
    datasource.delete_latest_snapshot("cafebabe").unwrap();
    let opt = datasource.get_latest_snapshot("cafebabe").unwrap();
    assert!(opt.is_none());
}

#[test]
fn test_insert_get_file() {
    let db_path = DBPath::new("_test_insert_get_file");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
    let file_digest = Checksum::SHA256(String::from(sha256sum));
    let chunks = vec![(0, file_digest.clone())];
    let file = entities::File::new(file_digest.clone(), 3129, chunks);
    datasource.insert_file(&file).unwrap();
    datasource.insert_file(&file).unwrap();
    datasource.insert_file(&file).unwrap();
    let option = datasource.get_file(&file_digest).unwrap();
    assert!(option.is_some());
    let actual = option.unwrap();
    assert_eq!(actual.digest, file_digest);
    assert_eq!(actual.length, file.length);
    assert_eq!(actual.chunks.len(), 1);
    assert_eq!(actual.chunks[0].0, 0);
    assert_eq!(actual.chunks[0].1, file_digest);
}

#[test]
fn test_put_get_tree() {
    let db_path = DBPath::new("_test_put_get_tree");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
    let file_digest = Checksum::SHA256(String::from(sha256sum));
    let reference = entities::TreeReference::FILE(file_digest);
    let filepath = Path::new("../test/fixtures/lorem-ipsum.txt");
    let entry = entities::TreeEntry::new(filepath, reference);
    let tree = entities::Tree::new(vec![entry], 1);
    datasource.insert_tree(&tree).unwrap();
    datasource.insert_tree(&tree).unwrap();
    datasource.insert_tree(&tree).unwrap();
    let option = datasource.get_tree(&tree.digest).unwrap();
    assert!(option.is_some());
    let actual = option.unwrap();
    assert_eq!(actual.digest, tree.digest);
    assert_eq!(actual.entries.len(), 1);
    assert_eq!(actual.entries[0].name, "lorem-ipsum.txt");
}

#[test]
fn test_put_get_xattr() {
    let db_path = DBPath::new("_test_put_get_xattr");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    let raw_xattr: Vec<u8> = vec![
        0x62, 0x70, 0x6C, 0x69, 0x73, 0x74, 0x30, 0x30, 0xA0, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
    ];
    let sha1sum = Checksum::sha1_from_bytes(&raw_xattr);
    datasource.insert_xattr(&sha1sum, &raw_xattr).unwrap();
    datasource.insert_xattr(&sha1sum, &raw_xattr).unwrap();
    datasource.insert_xattr(&sha1sum, &raw_xattr).unwrap();
    let option = datasource.get_xattr(&sha1sum).unwrap();
    assert!(option.is_some());
    let actual = option.unwrap();
    let new1sum = Checksum::sha1_from_bytes(&actual);
    assert_eq!(new1sum, sha1sum);
}

#[test]
fn test_put_get_snapshot() {
    let db_path = DBPath::new("_test_put_get_snapshot");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();

    let parent = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
    let tree = Checksum::SHA1(String::from("811ea7199968a119eeba4b65ace06cc7f835c497"));
    let snapshot = entities::Snapshot::new(Some(parent), tree, 1024);
    datasource.put_snapshot(&snapshot).unwrap();
    let option = datasource.get_snapshot(&snapshot.digest).unwrap();
    assert!(option.is_some());
    let actual = option.unwrap();
    assert_eq!(actual.digest, snapshot.digest);
    assert_eq!(actual.parent, snapshot.parent);
    assert_eq!(actual.start_time, snapshot.start_time);
    assert_eq!(actual.end_time, snapshot.end_time);
    assert_eq!(actual.file_count, snapshot.file_count);
    assert_eq!(actual.tree, snapshot.tree);
}

#[test]
fn test_backup_restore() {
    let db_path = DBPath::new("_test_backup_restore");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    assert!(datasource.put_computer_id("charlie", "localhost").is_ok());

    // backup the database
    let backup_path = DBPath::new("_test_backup_restore_bup");
    datasource
        .create_backup(Some(backup_path.as_ref().to_path_buf()))
        .unwrap();

    // modify the database
    assert!(datasource.put_computer_id("charlie", "remotehost").is_ok());

    // restore from backup
    datasource
        .restore_from_backup(Some(backup_path.as_ref().to_path_buf()))
        .unwrap();

    // verify contents of restored database
    match datasource.get_computer_id("charlie") {
        Ok(Some(value)) => assert_eq!(value, "localhost"),
        Ok(None) => panic!("missing computer id record"),
        Err(e) => panic!("error: {}", e),
    };
    let _ = std::fs::remove_dir_all(backup_path);
}
