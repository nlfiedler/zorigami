//
// Copyright (c) 2019 Nathan Fiedler
//
#[macro_use]
extern crate lazy_static;

use rocksdb::DB;
use std::fs;
use std::ops::Deref;
use zorigami::core::*;
use zorigami::database::*;

static DB_PATH: &str = "test/tmp/database/rocksdb";
lazy_static! {
    static ref DBASE: DB = {
        // clear the old test data, otherwise it is very confusing
        fs::remove_dir_all(DB_PATH).unwrap();
        DB::open_default(DB_PATH).unwrap()
    };
}

#[test]
fn test_insert_document() {
    assert!(insert_document(&DBASE, b"charlie", b"localhost").is_ok());
    assert!(insert_document(&DBASE, b"charlie", b"remotehost").is_ok());
    match get_document(&DBASE, b"charlie") {
        Ok(Some(value)) => assert_eq!(value.deref(), b"localhost"),
        Ok(None) => panic!("get document returned None!"),
        Err(e) => panic!("get document error: {}", e),
    }
}

#[test]
fn test_chunk_records() {
    // test no such record
    let result = get_chunk(&DBASE, "sha256-cafebabedeadbeef");
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
    // test the happy path
    let chunk = Chunk::new(
        "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1",
        0,
        40000,
    )
    .packfile("sha1-bc1a3198db79036e56b30f0ab307cee55e845907");
    assert!(chunk.packfile.is_some());
    assert!(insert_chunk(&DBASE, &chunk).is_ok());
    let result = get_chunk(&DBASE, &chunk.digest);
    assert!(result.is_ok());
    let record: Option<Chunk> = result.unwrap();
    assert!(record.is_some());
    let actual: Chunk = record.unwrap();
    assert_eq!(
        actual.digest,
        "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1"
    );
    assert_eq!(actual.offset, 0);
    assert_eq!(actual.length, 40000);
    assert!(actual.filepath.is_none());
    assert!(actual.packfile.is_some());
    assert_eq!(
        actual.packfile.unwrap(),
        "sha1-bc1a3198db79036e56b30f0ab307cee55e845907"
    );
}

#[test]
fn test_prefix_counting() {
    assert!(insert_document(&DBASE, b"punk/cafebabe", b"madoka magic").is_ok());
    assert!(insert_document(&DBASE, b"punk/deadbeef", b"made in abyss").is_ok());
    assert!(insert_document(&DBASE, b"punk/cafed00d", b"houseki no kuni").is_ok());
    assert!(insert_document(&DBASE, b"punk/1badb002", b"eureka seven").is_ok());
    assert!(insert_document(&DBASE, b"punk/abadbabe", b"last exile").is_ok());
    assert!(insert_document(&DBASE, b"kree/cafebabe", b"hibeke! euphonium").is_ok());
    assert!(insert_document(&DBASE, b"kree/deadbeef", b"flip flappers").is_ok());
    assert!(insert_document(&DBASE, b"kree/abadbabe", b"koe no katachi").is_ok());
    assert!(insert_document(&DBASE, b"kree/cafefeed", b"toradora!").is_ok());
    let result = count_prefix(&DBASE, "punk");
    assert!(result.is_ok());
    let count: usize = result.unwrap();
    assert_eq!(count, 5);
    let result = count_prefix(&DBASE, "kree");
    assert!(result.is_ok());
    let count: usize = result.unwrap();
    assert_eq!(count, 4);
}
