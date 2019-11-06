//
// Copyright (c) 2019 Nathan Fiedler
//
mod util;

use dotenv::dotenv;
use failure::Error;
use serde_json::json;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use util::DBPath;
use xattr;
use zorigami::core::*;
use zorigami::database::*;
use zorigami::engine::*;
use zorigami::state;
use zorigami::store::*;

#[test]
fn test_datasets() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_datasets");
    let dbase = Database::new(&db_path).unwrap();
    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dbase.put_dataset(&dataset)?;
    dataset.stores[0] = String::from("store/local/futts");
    dbase.put_dataset(&dataset)?;
    let setopt = dbase.get_dataset(&dataset.key)?;
    assert!(setopt.is_some());
    let setdata = setopt.unwrap();
    assert_eq!(setdata.key, dataset.key);
    assert_eq!(setdata.stores[0], "store/local/futts");
    Ok(())
}

#[test]
fn test_pack_chunk_sizes() {
    let db_path = DBPath::new("_test_pack_chunk_sizes");
    let dbase = Database::new(&db_path).unwrap();
    let builder = PackBuilder::new(&dbase, 65_536);
    assert_eq!(builder.chunk_size(), 16_384);
    let builder = PackBuilder::new(&dbase, 131_072);
    assert_eq!(builder.chunk_size(), 32_768);
    let builder = PackBuilder::new(&dbase, 262_144);
    assert_eq!(builder.chunk_size(), 65_536);
    let builder = PackBuilder::new(&dbase, 16_777_216);
    assert_eq!(builder.chunk_size(), 4_194_304);
    let builder = PackBuilder::new(&dbase, 33_554_432);
    assert_eq!(builder.chunk_size(), 4_194_304);
    let builder = PackBuilder::new(&dbase, 134_217_728);
    assert_eq!(builder.chunk_size(), 4_194_304);
}

#[test]
fn test_basic_snapshots() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_basic_snapshots");
    let dbase = Database::new(&db_path).unwrap();
    let basepath = "tmp/test/engine/snapshots/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let dest: PathBuf = [basepath, "lorem-ipsum.txt"].iter().collect();
    assert!(fs::copy("tests/fixtures/lorem-ipsum.txt", dest).is_ok());
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_count, 1);
    // make a change to the data set
    let dest: PathBuf = [basepath, "SekienAkashita.jpg"].iter().collect();
    assert!(fs::copy("tests/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let xattr_worked =
        xattr::SUPPORTED_PLATFORM && xattr::set(&dest, "me.fiedlers.test", b"foobar").is_ok();
    // take another snapshot
    let snap2_sha =
        take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    let snapshot2 = dbase.get_snapshot(&snap2_sha)?.unwrap();
    assert!(snapshot2.parent.is_some());
    assert_eq!(snapshot2.parent.unwrap(), snap1_sha);
    assert_eq!(snapshot2.file_count, 2);
    assert_ne!(snap1_sha, snap2_sha);
    assert_ne!(snapshot1.tree, snapshot2.tree);
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        PathBuf::from(basepath),
        snap1_sha,
        snap2_sha.clone(),
    )?;
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
    if xattr_worked {
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

    // take another snapshot, should indicate no changes
    let snap3_opt = take_snapshot(Path::new(basepath), Some(snap2_sha.clone()), &dbase, vec![])?;
    assert!(snap3_opt.is_none());
    Ok(())
}

#[test]
fn test_snapshot_symlinks() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_snapshot_symlinks");
    let dbase = Database::new(&db_path).unwrap();
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
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase, vec![])?.unwrap();
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
    let db_path = DBPath::new("_test_snapshot_ordering");
    let dbase = Database::new(&db_path).unwrap();
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
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase, vec![])?.unwrap();
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
    let snap2_sha =
        take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        PathBuf::from(basepath),
        snap1_sha.clone(),
        snap2_sha.clone(),
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 4);
    // The changed mmm/mmm.txt file ends up last because its tree was changed
    // and is pushed onto the queue, while the new entries are processed
    // immediately before returning to the queue.
    assert_eq!(changed[0].as_ref().unwrap().path, bbb);
    assert_eq!(changed[1].as_ref().unwrap().path, nnn);
    assert_eq!(changed[2].as_ref().unwrap().path, zzz);
    assert_eq!(changed[3].as_ref().unwrap().path, mmm);
    // remove some files, change another
    fs::remove_file(&bbb)?;
    fs::remove_file(&yyy)?;
    fs::write(&zzz, b"zippy zip ties zooming")?;
    let snap3_sha =
        take_snapshot(Path::new(basepath), Some(snap2_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(&dbase, PathBuf::from(basepath), snap2_sha, snap3_sha)?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].as_ref().unwrap().path, zzz);
    Ok(())
}

#[test]
fn test_snapshot_types() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_snapshot_types");
    let dbase = Database::new(&db_path).unwrap();
    let basepath = "tmp/test/engine/types/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let ccc: PathBuf = [basepath, "ccc"].iter().collect();
    let mmm: PathBuf = [basepath, "mmm", "mmm.txt"].iter().collect();
    fs::create_dir(mmm.parent().unwrap())?;
    fs::write(&ccc, b"crazy cat clawing chairs")?;
    fs::write(&mmm, b"morose monkey munching muffins")?;
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase, vec![])?.unwrap();
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
    let snap2_sha =
        take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        PathBuf::from(basepath),
        snap1_sha.clone(),
        snap2_sha.clone(),
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].as_ref().unwrap().path, ccc);
    assert_eq!(changed[1].as_ref().unwrap().path, mmm);
    Ok(())
}

#[test]
fn test_snapshot_ignore_links() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_snapshot_ignore_links");
    let dbase = Database::new(&db_path).unwrap();
    let basepath = "tmp/test/engine/ignore_links/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let bbb: PathBuf = [basepath, "bbb"].iter().collect();
    let ccc: PathBuf = [basepath, "ccc", "ccc.txt"].iter().collect();
    fs::create_dir(ccc.parent().unwrap())?;
    fs::write(&bbb, b"bored baby baboons bathing")?;
    fs::write(&ccc, b"crazy cat clawing chairs")?;
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase, vec![])?.unwrap();
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
    let snap2_sha =
        take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        PathBuf::from(basepath),
        snap1_sha.clone(),
        snap2_sha.clone(),
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].as_ref().unwrap().path, mmm);
    Ok(())
}

#[test]
fn test_snapshot_was_links() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_snapshot_was_links");
    let dbase = Database::new(&db_path).unwrap();
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
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_count, 1);
    // replace the links with files and directories
    fs::remove_file(&bbb)?;
    fs::write(&bbb, b"bored baby baboons bathing")?;
    fs::remove_file(&ccc)?;
    let ccc: PathBuf = [basepath, "ccc", "ccc.txt"].iter().collect();
    fs::create_dir(ccc.parent().unwrap())?;
    fs::write(&ccc, b"crazy cat clawing chairs")?;
    let snap2_sha =
        take_snapshot(Path::new(basepath), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        PathBuf::from(basepath),
        snap1_sha.clone(),
        snap2_sha.clone(),
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].as_ref().unwrap().path, bbb);
    assert_eq!(changed[1].as_ref().unwrap().path, ccc);
    Ok(())
}

#[test]
fn test_pack_builder() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_pack_builder");
    let dbase = Database::new(&db_path).unwrap();
    let basepath = "tmp/test/engine/builder/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let mut builder = PackBuilder::new(&dbase, 65536);
    assert_eq!(builder.has_chunks(), false);
    assert_eq!(builder.is_full(), false);
    let lorem_path = Path::new("tests/fixtures/lorem-ipsum.txt");
    let lorem_sha = checksum_file(&lorem_path)?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    let sekien_path = Path::new("tests/fixtures/SekienAkashita.jpg");
    let sekien_sha = checksum_file(&sekien_path)?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    let pack_file: PathBuf = [basepath, "pack.001"].iter().collect();
    assert!(builder.has_chunks());
    assert!(builder.is_full());
    let mut pack = builder.build_pack(&pack_file, "keyboard cat")?;
    let coords = vec![PackLocation::new("acme", "bucket1", "object1")];
    pack.record_completed_pack(&dbase, coords)?;
    pack.record_completed_files(&dbase)?;
    // the builder should still have some chunks, but not be full either
    assert!(builder.has_chunks());
    assert_eq!(builder.is_full(), false);
    // verify records in the database match expectations
    let option = dbase.get_pack(pack.get_digest().unwrap())?;
    assert!(option.is_some());
    let saved_pack = option.unwrap();
    assert_eq!(&saved_pack.locations[0].bucket, "bucket1");
    assert_eq!(&saved_pack.locations[0].object, "object1");
    // ensure pack digest is _not_ the default
    assert_ne!(saved_pack.digest.to_string(), NULL_SHA1);
    // The large file will not have been completed yet, it is too large for the
    // pack size that we set above; can't be sure about the small file, either.
    let option = dbase.get_file(&sekien_sha)?;
    assert!(option.is_none());
    let mut pack = builder.build_pack(&pack_file, "keyboard cat")?;
    let coords = vec![PackLocation::new("acme", "bucket1", "object2")];
    pack.record_completed_pack(&dbase, coords)?;
    pack.record_completed_files(&dbase)?;
    // should be completely empty at this point
    assert_eq!(builder.has_chunks(), false);
    assert_eq!(builder.is_full(), false);
    builder.clear_cache();
    let option = dbase.get_pack(pack.get_digest().unwrap())?;
    assert!(option.is_some());
    let saved_pack = option.unwrap();
    assert_eq!(saved_pack.locations[0].bucket, "bucket1");
    assert_eq!(saved_pack.locations[0].object, "object2");
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
    let db_path = DBPath::new("_test_pack_builder_empty");
    let dbase = Database::new(&db_path).unwrap();
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
    let db_path = DBPath::new("_test_pack_builder_dupes");
    let dbase = Database::new(&db_path).unwrap();
    let mut builder = PackBuilder::new(&dbase, 65536);
    assert_eq!(builder.file_count(), 0);
    assert_eq!(builder.chunk_count(), 0);
    // builder should ignore attempts to add files with the same checksum as
    // have already been added to this builder prior to emptying into pack files
    let lorem_path = Path::new("tests/fixtures/lorem-ipsum.txt");
    let lorem_sha = checksum_file(&lorem_path)?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    let sekien_path = Path::new("tests/fixtures/SekienAkashita.jpg");
    let sekien_sha = checksum_file(&sekien_path)?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    assert_eq!(builder.file_count(), 2);
    assert_eq!(builder.chunk_count(), 7);
    Ok(())
}

#[test]
fn test_perform_backup() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_perform_backup");
    let dbase = Database::new(&db_path).unwrap();
    let pack_path = "tmp/test/engine/backup/packs";
    let _ = fs::remove_dir_all(pack_path);

    // create a local store
    let config_json = json!({
        "label": "foobar",
        "basepath": pack_path,
    });
    let value = config_json.to_string();
    let mut store = local::LocalStore::new("testing");
    store.get_config_mut().from_json(&value)?;
    save_store(&dbase, &store)?;

    // create a dataset
    let basepath = "tmp/test/engine/backup/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let unique_id = generate_unique_id("charlie", "localhost");
    let store_name = store_name(&store);
    let mut dataset = Dataset::new(&unique_id, Path::new(basepath), &store_name);
    dataset.pack_size = 65536 as u64;
    dataset.key = "foobar".to_owned();

    // perform the first backup
    let dest: PathBuf = [basepath, "lorem-ipsum.txt"].iter().collect();
    assert!(fs::copy("tests/fixtures/lorem-ipsum.txt", dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // check for object(s) being present in the pack store
    let result = store.list_buckets();
    assert!(result.is_ok());
    let buckets = result.unwrap();
    // two buckets, one for the packs and one for the database
    assert_eq!(buckets.len(), 2);
    let bucket = &buckets[0];
    let result = store.list_objects(bucket);
    assert!(result.is_ok());
    let listing = result.unwrap();
    assert!(!listing.is_empty());

    // perform the second backup
    let dest: PathBuf = [basepath, "SekienAkashita.jpg"].iter().collect();
    assert!(fs::copy("tests/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // check for more buckets and objects
    let result = store.list_buckets();
    assert!(result.is_ok());
    let buckets = result.unwrap();
    assert!(buckets.len() > 1);
    let bucket = &buckets[0];
    let result = store.list_objects(bucket);
    assert!(result.is_ok());
    let listing = result.unwrap();
    assert!(!listing.is_empty());
    let bucket = &buckets[1];
    let result = store.list_objects(bucket);
    assert!(result.is_ok());
    let listing = result.unwrap();
    assert!(!listing.is_empty());

    // run the backup again with no changes, assert no new snapshot
    //
    // N.B. would like to do this, but sometimes there is a subtle difference in
    // the trees; maybe a minor change in the file metadata.
    //
    // let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    // assert!(backup_opt.is_none());

    let stated = state::get_state();
    let backups = stated.backups(&dataset.key).unwrap();
    assert!(backups.end_time().is_some());
    assert!(backups.end_time().unwrap() > backups.start_time());
    assert_eq!(backups.packs_uploaded(), 2);
    assert_eq!(backups.files_uploaded(), 1);
    Ok(())
}

#[test]
fn test_restore_file() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_restore_file");
    let dbase = Database::new(&db_path).unwrap();
    let pack_path = "tmp/test/engine/restore_file/packs";
    let _ = fs::remove_dir_all(pack_path);

    // create a local store
    let config_json = json!({
        "label": "foobar",
        "basepath": pack_path,
    });
    let value = config_json.to_string();
    let mut store = local::LocalStore::new("testing");
    store.get_config_mut().from_json(&value)?;
    save_store(&dbase, &store)?;

    // create a dataset
    let basepath = "tmp/test/engine/restore_file/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let unique_id = generate_unique_id("charlie", "localhost");
    let store_name = store_name(&store);
    let mut dataset = Dataset::new(&unique_id, Path::new(basepath), &store_name);
    dataset.pack_size = 65536 as u64;

    // perform the first backup
    let dest: PathBuf = [basepath, "lorem-ipsum.txt"].iter().collect();
    assert!(fs::copy("tests/fixtures/lorem-ipsum.txt", dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // perform the second backup
    let dest: PathBuf = [basepath, "SekienAkashita.jpg"].iter().collect();
    assert!(fs::copy("tests/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // perform the third backup
    let dest: PathBuf = [basepath, "washington-journal.txt"].iter().collect();
    assert!(fs::copy("tests/fixtures/washington-journal.txt", &dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // should be 8 chunks in database (pack size of 64kb means chunks around
    // 16kb; testing with two small files and one larger file)
    let count = dbase.count_prefix("chunk")?;
    assert_eq!(count, 8);

    // perform the fourth backup with shifted larger file
    let infile = Path::new("tests/fixtures/SekienAkashita.jpg");
    let outfile: PathBuf = [basepath, "SekienShifted.jpg"].iter().collect();
    copy_with_prefix("mary had a little lamb", &infile, &outfile)?;
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // should be one more chunk in database
    let count = dbase.count_prefix("chunk")?;
    assert_eq!(count, 9);

    // restore the file from the first snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = checksum_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the second snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = checksum_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the third snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = checksum_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the fourth snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "b2c67e90a01f5d7aca48835b8ad8f0902ef03288aa4083e742bccbd96d8590a4",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = checksum_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    Ok(())
}

///
/// Copy one file to another, prepending the result with the given text.
///
fn copy_with_prefix(header: &str, infile: &Path, outfile: &Path) -> Result<(), Error> {
    let mut reader: &[u8] = header.as_bytes();
    let mut writer = fs::File::create(outfile)?;
    io::copy(&mut reader, &mut writer)?;
    let mut reader = fs::File::open(infile)?;
    io::copy(&mut reader, &mut writer)?;
    Ok(())
}

#[test]
fn test_multiple_stores() -> Result<(), Error> {
    // set up the environment and remote connection
    dotenv().ok();
    let endp_var = env::var("MINIO_ENDPOINT");
    if endp_var.is_err() {
        // this requires using a remote store
        return Ok(());
    }

    // create a clean database for each test
    let db_path = DBPath::new("_test_multiple_stores");
    let dbase = Database::new(&db_path).unwrap();
    let pack_path = "tmp/test/engine/multi_store/packs";
    let _ = fs::remove_dir_all(pack_path);

    // create a local store
    let config_json = json!({
        "label": "foobar",
        "basepath": pack_path,
    });
    let value = config_json.to_string();
    let mut local_store = local::LocalStore::new("local_123");
    local_store.get_config_mut().from_json(&value)?;
    save_store(&dbase, &local_store)?;

    // create a remote store (minio will work)
    let endpoint = endp_var.unwrap();
    let region = env::var("MINIO_REGION").unwrap();
    let config_json = json!({
        "label": "foobar",
        "region": region,
        "endpoint": endpoint,
    });
    let mut minio_store = minio::MinioStore::new("minio_345");
    let value = config_json.to_string();
    minio_store.get_config_mut().from_json(&value)?;
    save_store(&dbase, &minio_store)?;

    // create a dataset
    let basepath = "tmp/test/engine/multi_store/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let unique_id = generate_unique_id("charlie", "localhost");
    let stor_name = store_name(&local_store);
    let mut dataset = Dataset::new(&unique_id, Path::new(basepath), &stor_name);
    let stor_name = store_name(&minio_store);
    dataset = dataset.add_store(&stor_name);
    dataset.pack_size = 65536 as u64;

    // perform the first backup
    let dest: PathBuf = [basepath, "lorem-ipsum.txt"].iter().collect();
    assert!(fs::copy("tests/fixtures/lorem-ipsum.txt", dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());

    // ensure the pack record has multiple locations
    let pack_keys = dbase.find_prefix("pack/")?;
    for key in pack_keys {
        // prefix of "pack/sha256-" is 12 characters long
        let digest = Checksum::SHA256(key[12..].to_string());
        let pack_rec = dbase.get_pack(&digest)?;
        assert!(pack_rec.is_some());
        let pack = pack_rec.unwrap();
        assert_eq!(pack.locations.len(), 2);
    }

    // restore the file from the first snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
    ));
    let outdir = tempdir().unwrap();
    let restored_file = outdir.path().join("restored.bin");
    restore_file(
        &dbase,
        &dataset,
        "keyboard cat",
        digest_expected.clone(),
        &restored_file,
    )?;
    let digest_actual = checksum_file(&restored_file)?;
    assert_eq!(digest_expected, digest_actual);

    Ok(())
}

#[test]
fn test_continue_backup() -> Result<(), Error> {
    // create a clean database for each test
    let db_path = DBPath::new("_test_continue_backup");
    let dbase = Database::new(&db_path).unwrap();
    let pack_path = "tmp/test/engine/continue/packs";
    let _ = fs::remove_dir_all(pack_path);

    // create a local store
    let config_json = json!({
        "label": "foobar",
        "basepath": pack_path,
    });
    let value = config_json.to_string();
    let mut store = local::LocalStore::new("testing");
    store.get_config_mut().from_json(&value)?;
    save_store(&dbase, &store)?;

    // create a dataset
    let basepath = "tmp/test/engine/continue/fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let unique_id = generate_unique_id("charlie", "localhost");
    let store_name = store_name(&store);
    let mut dataset = Dataset::new(&unique_id, Path::new(basepath), &store_name);
    dataset.pack_size = 65536 as u64;

    // perform the first backup
    let dest: PathBuf = [basepath, "SekienAkashita.jpg"].iter().collect();
    assert!(fs::copy("tests/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());
    let first_sha1 = backup_opt.unwrap();

    // fake an incomplete backup by resetting the end_time field
    let mut snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    snapshot.end_time = None;
    dbase.put_snapshot(&first_sha1, &snapshot)?;

    // run the backup again to make sure it is finished
    let backup_opt = perform_backup(&mut dataset, &dbase, "keyboard cat")?;
    assert!(backup_opt.is_some());
    let second_sha1 = backup_opt.unwrap();
    assert_eq!(first_sha1, second_sha1);
    let snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    assert!(snapshot.end_time.is_some());

    Ok(())
}
