//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use std::fs;
use std::path::{Path, PathBuf};
use xattr;
use zorigami::core::*;
use zorigami::database::*;
use zorigami::engine::*;

fn make_entry(pathname: &str, digest: &str) -> TreeEntry {
    let path = Path::new(pathname);
    let sha1 = Checksum::SHA1(digest.to_owned());
    let tref = TreeReference::FILE(sha1);
    TreeEntry::new(&path, tref).unwrap()
}

#[test]
fn test_tree_walker() {
    // create a clean database for each test
    let db_path = "tmp/test/engine/walker";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
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
    let result = dbase.insert_tree(&tree_c, &tree);
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
    let result = dbase.insert_tree(&tree_b, &tree);
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
    let result = dbase.insert_tree(&tree_a, &tree);
    assert!(result.is_ok());

    // walk the tree starting at a
    let walker = TreeWalker::new(&dbase, Path::new("a"), tree_a);
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
    let walker = TreeWalker::new(&dbase, Path::new("b"), tree_b);
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
    let walker = TreeWalker::new(&dbase, Path::new("c"), tree_c);
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

#[test]
fn test_basic_snapshots() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/snapshots/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let basepath = "tmp/test/engine/snapshots/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let dest: PathBuf = [basepath, "lorem-ipsum.txt"].iter().collect();
    assert!(fs::copy("test/fixtures/lorem-ipsum.txt", dest).is_ok());
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase)?;
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_count, 1);
    // make a change to the data set
    let dest: PathBuf = [basepath, "SekienAkashita.jpg"].iter().collect();
    assert!(fs::copy("test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    if xattr::SUPPORTED_PLATFORM {
        xattr::set(&dest, "me.fiedlers.test", b"foobar")?;
    }
    // take another snapshot
    let snap2_sha = take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase)?;
    let snapshot2 = dbase.get_snapshot(&snap2_sha)?.unwrap();
    assert!(snapshot2.parent.is_some());
    assert_eq!(snapshot2.parent.unwrap(), snap1_sha);
    assert_eq!(snapshot2.file_count, 2);
    assert_ne!(snap1_sha, snap2_sha);
    assert_ne!(snapshot1.tree, snapshot2.tree);
    // compute the differences
    let iter = find_changed_files(&dbase, snap1_sha, snap2_sha)?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert!(changed[0].is_ok());
    // should see new file record
    assert_eq!(
        changed[0].as_ref().unwrap().digest,
        Checksum::SHA256(String::from(
            "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        ))
    );
    // ensure extended attributes are stored in database
    if xattr::SUPPORTED_PLATFORM {
        let tree = dbase.get_tree(&snapshot2.tree)?.unwrap();
        let entries: Vec<&TreeEntry> = tree
            .entries
            .iter()
            .filter(|e| !e.xattrs.is_empty())
            .collect();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].xattrs.contains_key("me.fiedlers.test"));
        let x_value = dbase
            .get_xattr(&entries[0].xattrs["me.fiedlers.test"])?
            .unwrap();
        assert_eq!(x_value, b"foobar");
    }
    Ok(())
}

#[test]
fn test_snapshot_symlinks() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/symlinks/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let basepath = "tmp/test/engine/symlinks/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let dest: PathBuf = [basepath, "meaningless"].iter().collect();
    let target = "link_target_is_meaningless";
    // cfg! macro doesn't work for this case it seems so we have this
    // redundant use of the cfg directive instead
    {
        #[cfg(target_family = "unix")]
        use std::os::unix::fs;
        #[cfg(target_family = "windows")]
        use std::os::windows::fs;
        #[cfg(target_family = "unix")]
        fs::symlink(&target, &dest)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file(&target, &dest)?;
    }
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase)?;
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_count, 0);
    let tree = dbase.get_tree(&snapshot1.tree)?.unwrap();
    // ensure the tree has exactly one symlink entry
    let entries: Vec<&TreeEntry> = tree
        .entries
        .iter()
        .filter(|e| e.reference.is_link())
        .collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "meaningless");
    let value = entries[0].reference.symlink().unwrap();
    assert_eq!(value, "bGlua190YXJnZXRfaXNfbWVhbmluZ2xlc3M=");
    Ok(())
}

#[test]
fn test_snapshot_ordering() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/ordering/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let basepath = "tmp/test/engine/ordering/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let ccc: PathBuf = [basepath, "ccc", "ccc.txt"].iter().collect();
    let mmm: PathBuf = [basepath, "mmm", "mmm.txt"].iter().collect();
    let yyy: PathBuf = [basepath, "yyy", "yyy.txt"].iter().collect();
    fs::create_dir(ccc.parent().unwrap())?;
    fs::create_dir(mmm.parent().unwrap())?;
    fs::create_dir(yyy.parent().unwrap())?;
    fs::write(&ccc, b"crazy cat clawing chairs")?;
    fs::write(&mmm, b"morose monkey munching muffins")?;
    fs::write(&yyy, b"yellow yak yodeling")?;
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase)?;
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_count, 3);
    // add new files, change one file
    let bbb: PathBuf = [basepath, "bbb", "bbb.txt"].iter().collect();
    let nnn: PathBuf = [basepath, "nnn", "nnn.txt"].iter().collect();
    let zzz: PathBuf = [basepath, "zzz", "zzz.txt"].iter().collect();
    fs::create_dir(bbb.parent().unwrap())?;
    fs::create_dir(nnn.parent().unwrap())?;
    fs::create_dir(zzz.parent().unwrap())?;
    fs::write(&bbb, b"blue baboons bouncing balls")?;
    fs::write(&mmm, b"many mumbling mice moonlight")?;
    fs::write(&nnn, b"neat newts gnawing noodles")?;
    fs::write(&zzz, b"zebras riding on a zephyr")?;
    let snap2_sha = take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase)?;
    // compute the differences
    let iter = find_changed_files(&dbase, snap1_sha.clone(), snap2_sha.clone())?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 4);
    // The changed mmm/mmm.txt file ends up last because its tree was changed
    // and is pushed onto the queue, while the new entries are processed
    // immediately before returning to the queue.
    assert_eq!(changed[0].as_ref().unwrap().path, Path::new("./bbb/bbb.txt"));
    assert_eq!(changed[1].as_ref().unwrap().path, Path::new("./nnn/nnn.txt"));
    assert_eq!(changed[2].as_ref().unwrap().path, Path::new("./zzz/zzz.txt"));
    assert_eq!(changed[3].as_ref().unwrap().path, Path::new("./mmm/mmm.txt"));
    // remove some files, change another
    fs::remove_file(&bbb)?;
    fs::remove_file(&yyy)?;
    fs::write(&zzz, b"zippy zip ties zooming")?;
    let snap3_sha = take_snapshot(Path::new(basepath), Some(snap2_sha.clone()), &dbase)?;
    // compute the differences
    let iter = find_changed_files(&dbase, snap2_sha, snap3_sha)?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].as_ref().unwrap().path, Path::new("./zzz/zzz.txt"));
    Ok(())
}

#[test]
fn test_snapshot_types() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/types/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let basepath = "tmp/test/engine/types/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let ccc: PathBuf = [basepath, "ccc"].iter().collect();
    let mmm: PathBuf = [basepath, "mmm", "mmm.txt"].iter().collect();
    fs::create_dir(mmm.parent().unwrap())?;
    fs::write(&ccc, b"crazy cat clawing chairs")?;
    fs::write(&mmm, b"morose monkey munching muffins")?;
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase)?;
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_count, 2);
    // change files to dirs and vice versa
    fs::remove_file(&ccc)?;
    let ccc: PathBuf = [basepath, "ccc", "ccc.txt"].iter().collect();
    let mmm: PathBuf = [basepath, "mmm"].iter().collect();
    fs::create_dir(ccc.parent().unwrap())?;
    fs::remove_dir_all(&mmm)?;
    fs::write(&ccc, b"catastrophic catastrophes")?;
    fs::write(&mmm, b"many mumbling mice moonlight")?;
    let snap2_sha = take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase)?;
    // compute the differences
    let iter = find_changed_files(&dbase, snap1_sha.clone(), snap2_sha.clone())?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].as_ref().unwrap().path, Path::new("./ccc/ccc.txt"));
    assert_eq!(changed[1].as_ref().unwrap().path, Path::new("./mmm"));
    Ok(())
}

// TODO: copy each of the tests from test/engine.ts that test findChangedFiles()
