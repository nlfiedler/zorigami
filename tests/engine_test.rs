//
// Copyright (c) 2019 Nathan Fiedler
//
#[macro_use]
extern crate lazy_static;

use failure::Error;
use std::fs;
use std::path::{Path, PathBuf};
use zorigami::core::*;
use zorigami::database::*;
use zorigami::engine::*;

static DB_PATH: &str = "test/tmp/engine/rocksdb";
lazy_static! {
    static ref DBASE: Database = {
        // clear the old test data, otherwise it is very confusing
        let _ = fs::remove_dir_all(DB_PATH);
        Database::new(Path::new(DB_PATH)).unwrap()
    };
}

fn make_entry(pathname: &str, digest: &str) -> TreeEntry {
    let path = Path::new(pathname);
    let sha1 = Checksum::SHA1(digest.to_owned());
    let tref = TreeReference::FILE(sha1);
    TreeEntry::new(&path, tref).unwrap()
}

#[test]
fn test_tree_walker() {
    // create the a/b/c tree object
    let entry1 = make_entry(
        "./test/fixtures/lorem-ipsum.txt",
        "b14c4909c3fce2483cd54b328ada88f5ef5e8f96",
    );
    let entry2 = make_entry(
        "./test/fixtures/SekienAkashita.jpg",
        "4c009e44fe5794df0b1f828f2a8c868e66644964",
    );
    let tree = Tree::new(vec![entry1, entry2], 2);
    let tree_c = tree.checksum();
    let result = DBASE.insert_tree(&tree_c, &tree);
    assert!(result.is_ok());

    // create the a/b tree object
    let tree_path = Path::new("./test/fixtures");
    let tref_c = TreeReference::TREE(tree_c.clone());
    let result = TreeEntry::new(&tree_path, tref_c);
    let entry_c = result.unwrap();
    let entry1 = make_entry(
        "./test/fixtures/lorem-ipsum.txt",
        "b14c4909c3fce2483cd54b328ada88f5ef5e8f96",
    );
    let entry2 = make_entry(
        "./test/fixtures/SekienAkashita.jpg",
        "4c009e44fe5794df0b1f828f2a8c868e66644964",
    );
    let tree = Tree::new(vec![entry_c, entry1, entry2], 4);
    let tree_b = tree.checksum();
    let result = DBASE.insert_tree(&tree_b, &tree);
    assert!(result.is_ok());

    // create the a tree object
    let tref_b = TreeReference::TREE(tree_b.clone());
    let result = TreeEntry::new(&tree_path, tref_b);
    let entry_b = result.unwrap();
    let entry1 = make_entry(
        "./test/fixtures/lorem-ipsum.txt",
        "b14c4909c3fce2483cd54b328ada88f5ef5e8f96",
    );
    let entry2 = make_entry(
        "./test/fixtures/SekienAkashita.jpg",
        "4c009e44fe5794df0b1f828f2a8c868e66644964",
    );
    let tree = Tree::new(vec![entry_b, entry1, entry2], 6);
    let tree_a = tree.checksum();
    let result = DBASE.insert_tree(&tree_a, &tree);
    assert!(result.is_ok());

    // walk the tree starting at a
    let walker = TreeWalker::new(&DBASE, "a", tree_a);
    let collected: Vec<Result<ChangedFile, Error>> = walker.collect();
    assert_eq!(collected.len(), 6);
    assert_eq!(
        collected[0].as_ref().unwrap().path,
        PathBuf::from("a/SekienAkashita.jpg")
    );
    assert_eq!(
        collected[1].as_ref().unwrap().path,
        PathBuf::from("a/lorem-ipsum.txt")
    );
    assert_eq!(
        collected[2].as_ref().unwrap().path,
        PathBuf::from("a/fixtures/SekienAkashita.jpg")
    );
    assert_eq!(
        collected[3].as_ref().unwrap().path,
        PathBuf::from("a/fixtures/lorem-ipsum.txt")
    );
    assert_eq!(
        collected[4].as_ref().unwrap().path,
        PathBuf::from("a/fixtures/fixtures/SekienAkashita.jpg")
    );
    assert_eq!(
        collected[5].as_ref().unwrap().path,
        PathBuf::from("a/fixtures/fixtures/lorem-ipsum.txt")
    );

    // walk the tree starting at a/b
    let walker = TreeWalker::new(&DBASE, "b", tree_b);
    let collected: Vec<Result<ChangedFile, Error>> = walker.collect();
    assert_eq!(collected.len(), 4);
    assert_eq!(
        collected[0].as_ref().unwrap().path,
        PathBuf::from("b/SekienAkashita.jpg")
    );
    assert_eq!(
        collected[1].as_ref().unwrap().path,
        PathBuf::from("b/lorem-ipsum.txt")
    );
    assert_eq!(
        collected[2].as_ref().unwrap().path,
        PathBuf::from("b/fixtures/SekienAkashita.jpg")
    );
    assert_eq!(
        collected[3].as_ref().unwrap().path,
        PathBuf::from("b/fixtures/lorem-ipsum.txt")
    );

    // walk the tree starting at a/b/c
    let walker = TreeWalker::new(&DBASE, "c", tree_c);
    let collected: Vec<Result<ChangedFile, Error>> = walker.collect();
    assert_eq!(collected.len(), 2);
    assert_eq!(
        collected[0].as_ref().unwrap().path,
        PathBuf::from("c/SekienAkashita.jpg")
    );
    assert_eq!(
        collected[1].as_ref().unwrap().path,
        PathBuf::from("c/lorem-ipsum.txt")
    );
}
