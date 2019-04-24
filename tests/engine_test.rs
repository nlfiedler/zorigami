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
    assert_eq!(
        changed[0].as_ref().unwrap().path,
        Path::new("./bbb/bbb.txt")
    );
    assert_eq!(
        changed[1].as_ref().unwrap().path,
        Path::new("./nnn/nnn.txt")
    );
    assert_eq!(
        changed[2].as_ref().unwrap().path,
        Path::new("./zzz/zzz.txt")
    );
    assert_eq!(
        changed[3].as_ref().unwrap().path,
        Path::new("./mmm/mmm.txt")
    );
    // remove some files, change another
    fs::remove_file(&bbb)?;
    fs::remove_file(&yyy)?;
    fs::write(&zzz, b"zippy zip ties zooming")?;
    let snap3_sha = take_snapshot(Path::new(basepath), Some(snap2_sha.clone()), &dbase)?;
    // compute the differences
    let iter = find_changed_files(&dbase, snap2_sha, snap3_sha)?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert_eq!(
        changed[0].as_ref().unwrap().path,
        Path::new("./zzz/zzz.txt")
    );
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
    assert_eq!(
        changed[0].as_ref().unwrap().path,
        Path::new("./ccc/ccc.txt")
    );
    assert_eq!(changed[1].as_ref().unwrap().path, Path::new("./mmm"));
    Ok(())
}

#[test]
fn test_snapshot_ignore_links() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/ignore_links/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let basepath = "tmp/test/engine/ignore_links/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let bbb: PathBuf = [basepath, "bbb"].iter().collect();
    let ccc: PathBuf = [basepath, "ccc", "ccc.txt"].iter().collect();
    fs::create_dir(ccc.parent().unwrap())?;
    fs::write(&bbb, b"bored baby baboons bathing")?;
    fs::write(&ccc, b"crazy cat clawing chairs")?;
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase)?;
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_count, 2);
    // replace the files and directories with links
    let mmm: PathBuf = [basepath, "mmm.txt"].iter().collect();
    fs::write(&mmm, b"morose monkey munching muffins")?;
    fs::remove_file(&bbb)?;
    fs::remove_dir_all(ccc.parent().unwrap())?;
    let ccc: PathBuf = [basepath, "ccc"].iter().collect();
    // cfg! macro doesn't work for this case it seems so we have this
    // redundant use of the cfg directive instead
    {
        #[cfg(target_family = "unix")]
        use std::os::unix::fs;
        #[cfg(target_family = "windows")]
        use std::os::windows::fs;
        #[cfg(target_family = "unix")]
        fs::symlink("mmm.txt", &bbb)?;
        #[cfg(target_family = "unix")]
        fs::symlink("mmm.txt", &ccc)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file("mmm.txt", &bbb)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file("mmm.txt", &ccc)?;
    }
    let snap2_sha = take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase)?;
    // compute the differences
    let iter = find_changed_files(&dbase, snap1_sha.clone(), snap2_sha.clone())?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].as_ref().unwrap().path, Path::new("./mmm.txt"));
    Ok(())
}

#[test]
fn test_snapshot_was_links() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/was_links/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let basepath = "tmp/test/engine/was_links/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let mmm: PathBuf = [basepath, "mmm.txt"].iter().collect();
    fs::write(&mmm, b"morose monkey munching muffins")?;
    let bbb: PathBuf = [basepath, "bbb"].iter().collect();
    let ccc: PathBuf = [basepath, "ccc"].iter().collect();
    // cfg! macro doesn't work for this case it seems so we have this
    // redundant use of the cfg directive instead
    {
        #[cfg(target_family = "unix")]
        use std::os::unix::fs;
        #[cfg(target_family = "windows")]
        use std::os::windows::fs;
        #[cfg(target_family = "unix")]
        fs::symlink("mmm.txt", &bbb)?;
        #[cfg(target_family = "unix")]
        fs::symlink("mmm.txt", &ccc)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file("mmm.txt", &bbb)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file("mmm.txt", &ccc)?;
    }
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase)?;
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_count, 1);
    // replace the links with files and directories
    fs::remove_file(&bbb)?;
    fs::write(&bbb, b"bored baby baboons bathing")?;
    fs::remove_file(&ccc)?;
    let ccc: PathBuf = [basepath, "ccc", "ccc.txt"].iter().collect();
    fs::create_dir(ccc.parent().unwrap())?;
    fs::write(&ccc, b"crazy cat clawing chairs")?;
    let snap2_sha = take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase)?;
    // compute the differences
    let iter = find_changed_files(&dbase, snap1_sha.clone(), snap2_sha.clone())?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].as_ref().unwrap().path, Path::new("./bbb"));
    assert_eq!(
        changed[1].as_ref().unwrap().path,
        Path::new("./ccc/ccc.txt")
    );
    Ok(())
}

#[test]
fn test_pack_builder() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/builder/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let basepath = "tmp/test/engine/builder/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let mut builder = PackBuilder::new(&dbase, 65536);
    builder = builder.chunk_size(16384);
    assert_eq!(builder.has_chunks(), false);
    assert_eq!(builder.is_full(), false);
    let lorem_path = Path::new("test/fixtures/lorem-ipsum.txt");
    let lorem_sha = checksum_file(&lorem_path)?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    let sekien_path = Path::new("test/fixtures/SekienAkashita.jpg");
    let sekien_sha = checksum_file(&sekien_path)?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    let pack_file: PathBuf = [basepath, "pack.001"].iter().collect();
    assert!(builder.has_chunks());
    assert!(builder.is_full());
    let mut pack = builder.build_pack(&pack_file, "keyboard cat")?;
    pack.record_completed(&dbase, "bucket1", "object1")?;
    // the builder should still have some chunks, but not be full either
    assert!(builder.has_chunks());
    assert_eq!(builder.is_full(), false);
    // verify records in the database match expectations
    let option = dbase.get_pack(pack.get_digest().unwrap())?;
    assert!(option.is_some());
    let saved_pack = option.unwrap();
    assert_eq!(saved_pack.bucket, "bucket1");
    assert_eq!(saved_pack.object, "object1");
    // ensure pack digest is _not_ the default
    assert_ne!(saved_pack.digest.to_string(), NULL_SHA1);
    // The large file will not have been completed yet, it is too large for the
    // pack size that we set above; can't be sure about the small file, either.
    let option = dbase.get_file(&sekien_sha)?;
    assert!(option.is_none());
    let mut pack = builder.build_pack(&pack_file, "keyboard cat")?;
    pack.record_completed(&dbase, "bucket1", "object2")?;
    // should be completely empty at this point
    assert_eq!(builder.has_chunks(), false);
    assert_eq!(builder.is_full(), false);
    builder.clear_cache();
    let option = dbase.get_pack(pack.get_digest().unwrap())?;
    assert!(option.is_some());
    let saved_pack = option.unwrap();
    assert_eq!(saved_pack.bucket, "bucket1");
    assert_eq!(saved_pack.object, "object2");
    // ensure pack digest is _not_ the default
    assert_ne!(saved_pack.digest.to_string(), NULL_SHA1);
    // the big file should be saved by now
    let option = dbase.get_file(&sekien_sha)?;
    assert!(option.is_some());
    let saved_file = option.unwrap();
    assert_eq!(saved_file.length, 109_466);
    assert_eq!(
        saved_file.digest.to_string(),
        "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
    );
    // the small file should also be saved
    let option = dbase.get_file(&lorem_sha)?;
    assert!(option.is_some());
    let saved_file = option.unwrap();
    assert_eq!(saved_file.length, 3_129);
    assert_eq!(
        saved_file.digest.to_string(),
        "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
    );
    Ok(())
}

#[test]
fn test_pack_builder_empty() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/ebuilder/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let mut builder = PackBuilder::new(&dbase, 65536);
    assert_eq!(builder.has_chunks(), false);
    assert_eq!(builder.is_full(), false);
    let pack_file = Path::new("pack.001");
    // empty builder should not create a file or have a digest
    let pack = builder.build_pack(&pack_file, "keyboard cat")?;
    assert!(pack.get_digest().is_none());
    assert_eq!(pack_file.exists(), false);
    Ok(())
}

#[test]
fn test_pack_builder_dupes() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = "tmp/test/engine/dbuilder/rocksdb";
    let _ = fs::remove_dir_all(db_path);
    let dbase = Database::new(Path::new(db_path)).unwrap();
    let mut builder = PackBuilder::new(&dbase, 131_072);
    builder = builder.chunk_size(16384);
    assert_eq!(builder.file_count(), 0);
    assert_eq!(builder.chunk_count(), 0);
    // builder should ignore attempts to add files with the same checksum as
    // have already been added to this builder prior to emptying into pack files
    let lorem_path = Path::new("test/fixtures/lorem-ipsum.txt");
    let lorem_sha = checksum_file(&lorem_path)?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    let sekien_path = Path::new("test/fixtures/SekienAkashita.jpg");
    let sekien_sha = checksum_file(&sekien_path)?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    assert_eq!(builder.file_count(), 2);
    assert_eq!(builder.chunk_count(), 7);
    Ok(())
}
