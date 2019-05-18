//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fs;
use std::ops::Deref;
use std::path::Path;
use std::thread;
use std::time::SystemTime;
use zorigami::core::*;
use zorigami::database::*;

static DB_PATH: &str = "tmp/test/database/rocksdb";
lazy_static! {
    static ref DBASE: Database = {
        // clear the old test data, otherwise it is very confusing
        let _ = fs::remove_dir_all(DB_PATH);
        Database::new(Path::new(DB_PATH)).unwrap()
    };
}

#[test]
fn test_insert_document() {
    assert!(DBASE.insert_document(b"charlie", b"localhost").is_ok());
    assert!(DBASE.insert_document(b"charlie", b"remotehost").is_ok());
    match DBASE.get_document(b"charlie") {
        Ok(Some(value)) => assert_eq!(value.deref(), b"localhost"),
        Ok(None) => panic!("get document returned None!"),
        Err(e) => panic!("get document error: {}", e),
    }
    // we can update a value using put_document()
    assert!(DBASE.put_document(b"charlie", b"remotehost").is_ok());
    match DBASE.get_document(b"charlie") {
        Ok(Some(value)) => assert_eq!(value.deref(), b"remotehost"),
        Ok(None) => panic!("get document returned None!"),
        Err(e) => panic!("get document error: {}", e),
    }
}

#[test]
fn test_chunk_records() {
    // test no such record
    let missingsum = Checksum::SHA1("cafebabedeadbeef".to_owned());
    let result = DBASE.get_chunk(&missingsum);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
    // test the happy path
    let chnksum = Checksum::SHA256(
        "ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1".to_owned(),
    );
    let packsum = Checksum::SHA1("bc1a3198db79036e56b30f0ab307cee55e845907".to_owned());
    let chunk = Chunk::new(chnksum, 0, 40000).packfile(packsum);
    assert!(chunk.packfile.is_some());
    assert!(DBASE.insert_chunk(&chunk).is_ok());
    let result = DBASE.get_chunk(&chunk.digest);
    assert!(result.is_ok());
    let record: Option<Chunk> = result.unwrap();
    assert!(record.is_some());
    let actual: Chunk = record.unwrap();
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
fn test_tree_records() {
    let tref1 = TreeReference::FILE(Checksum::SHA1("cafebabe".to_owned()));
    let entry1 = TreeEntry {
        name: String::from("regu"),
        fstype: EntryType::FILE,
        mode: Some(0o644),
        uid: Some(100),
        gid: Some(100),
        user: Some(String::from("user")),
        group: Some(String::from("group")),
        ctime: SystemTime::UNIX_EPOCH,
        mtime: SystemTime::UNIX_EPOCH,
        reference: tref1,
        xattrs: HashMap::new(),
    };
    let tref2 = TreeReference::FILE(Checksum::SHA1("babecafe".to_owned()));
    let entry2 = TreeEntry {
        name: String::from("riko"),
        fstype: EntryType::FILE,
        mode: Some(0o644),
        uid: Some(100),
        gid: Some(100),
        user: Some(String::from("user")),
        group: Some(String::from("group")),
        ctime: SystemTime::UNIX_EPOCH,
        mtime: SystemTime::UNIX_EPOCH,
        reference: tref2,
        xattrs: HashMap::new(),
    };
    let tref3 = TreeReference::FILE(Checksum::SHA1("babebabe".to_owned()));
    let entry3 = TreeEntry {
        name: String::from("nanachi"),
        fstype: EntryType::FILE,
        mode: Some(0o644),
        uid: Some(100),
        gid: Some(100),
        user: Some(String::from("user")),
        group: Some(String::from("group")),
        ctime: SystemTime::UNIX_EPOCH,
        mtime: SystemTime::UNIX_EPOCH,
        reference: tref3,
        xattrs: HashMap::new(),
    };
    let tree = Tree::new(vec![entry1, entry2, entry3], 3);
    let sum = tree.checksum();
    let result = DBASE.insert_tree(&sum, &tree);
    assert!(result.is_ok());
    let result = DBASE.get_tree(&sum);
    assert!(result.is_ok());
    let maybe = result.unwrap();
    assert!(maybe.is_some());
    let tree = maybe.unwrap();
    let mut entries = tree.entries.iter();
    assert_eq!(entries.next().unwrap().name, "nanachi");
    assert_eq!(entries.next().unwrap().name, "regu");
    assert_eq!(entries.next().unwrap().name, "riko");
    assert!(entries.next().is_none());
    // file count is not persisted
    assert_eq!(tree.file_count, 0);
}

#[test]
fn test_prefix_counting() {
    assert!(DBASE
        .insert_document(b"punk/cafebabe", b"madoka magic")
        .is_ok());
    assert!(DBASE
        .insert_document(b"punk/deadbeef", b"made in abyss")
        .is_ok());
    assert!(DBASE
        .insert_document(b"punk/cafed00d", b"houseki no kuni")
        .is_ok());
    assert!(DBASE
        .insert_document(b"punk/1badb002", b"eureka seven")
        .is_ok());
    assert!(DBASE
        .insert_document(b"punk/abadbabe", b"last exile")
        .is_ok());
    assert!(DBASE
        .insert_document(b"kree/cafebabe", b"hibeke! euphonium")
        .is_ok());
    assert!(DBASE
        .insert_document(b"kree/deadbeef", b"flip flappers")
        .is_ok());
    assert!(DBASE
        .insert_document(b"kree/abadbabe", b"koe no katachi")
        .is_ok());
    assert!(DBASE
        .insert_document(b"kree/cafefeed", b"toradora!")
        .is_ok());
    let result = DBASE.count_prefix("punk");
    assert!(result.is_ok());
    let count: usize = result.unwrap();
    assert_eq!(count, 5);
    let result = DBASE.count_prefix("kree");
    assert!(result.is_ok());
    let count: usize = result.unwrap();
    assert_eq!(count, 4);
}

#[test]
fn test_db_threads_uniq_paths() -> Result<(), Error> {
    let mut children = vec![];
    for ii in 0..10 {
        children.push(thread::spawn(move || {
            // create a clean database for each thread
            let db_path = format!("tmp/test/engine/thread_{}/rocksdb", ii);
            let _ = fs::remove_dir_all(&db_path);
            let dbase = Database::new(Path::new(&db_path)).unwrap();
            let key = format!("thread_test_key_{}", ii);
            let result = dbase.insert_document(key.as_bytes(), b"foo bar baz quux");
            assert!(result.is_ok());
        }));
    }
    for child in children {
        let _ = child.join();
    }
    Ok(())
}

#[test]
fn test_db_threads_one_path() -> Result<(), Error> {
    let db_path = "tmp/test/engine/threads/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let mut children = vec![];
    for ii in 0..50 {
        children.push(thread::spawn(move || {
            // create a clean database for each thread
            let dbase = Database::new(Path::new(db_path)).unwrap();
            let key = format!("thread_test_key_{}", ii);
            let result = dbase.insert_document(key.as_bytes(), b"foo bar baz quux");
            assert!(result.is_ok());
        }));
    }
    for child in children {
        let _ = child.join();
    }
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let result = dbase.count_prefix("thread_test_key_");
    assert!(result.is_ok());
    let count: usize = result.unwrap();
    assert_eq!(count, 50);
    Ok(())
}
