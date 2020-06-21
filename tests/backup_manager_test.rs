//
// Copyright (c) 2020 Nathan Fiedler
//
mod common;

use common::DBPath;

use failure::Error;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zorigami::data::repositories::{PackRepositoryImpl, RecordRepositoryImpl};
use zorigami::data::sources::{EntityDataSourceImpl, PackSourceBuilderImpl};
use zorigami::domain::entities::{self, Checksum};
use zorigami::domain::managers::backup::*;
use zorigami::domain::repositories::{PackRepository, RecordRepository};

#[test]
fn test_basic_snapshots() -> Result<(), Error> {
    let db_path = DBPath::new("_test_basic_snapshots");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    let basepath: PathBuf = ["tmp", "test", "managers", "snapshots", "basics"]
        .iter()
        .collect();
    let _ = fs::remove_dir_all(&basepath);
    fs::create_dir_all(&basepath)?;
    let mut dest: PathBuf = basepath.clone();
    dest.push("lorem-ipsum.txt");
    assert!(fs::copy("tests/fixtures/lorem-ipsum.txt", dest).is_ok());
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(&basepath, None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_count, 1);
    // make a change to the data set
    let mut dest: PathBuf = basepath.clone();
    dest.push("SekienAkashita.jpg");
    assert!(fs::copy("tests/fixtures/SekienAkashita.jpg", &dest).is_ok());

    // take another snapshot
    let snap2_sha = take_snapshot(&basepath, Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    let snapshot2 = dbase.get_snapshot(&snap2_sha)?.unwrap();
    assert!(snapshot2.parent.is_some());
    assert_eq!(snapshot2.parent.unwrap(), snap1_sha);
    assert_eq!(snapshot2.file_count, 2);
    assert_ne!(snap1_sha, snap2_sha);
    assert_ne!(snapshot1.tree, snapshot2.tree);
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        PathBuf::from(&basepath),
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

    // take another snapshot, should indicate no changes
    let snap3_opt = take_snapshot(&basepath, Some(snap2_sha), &dbase, vec![])?;
    assert!(snap3_opt.is_none());
    Ok(())
}

#[test]
fn test_snapshots_xattrs() -> Result<(), Error> {
    let db_path = DBPath::new("_test_snapshots_xattrs");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    let basepath: PathBuf = ["tmp", "test", "managers", "snapshots", "xattrs"]
        .iter()
        .collect();
    let _ = fs::remove_dir_all(&basepath);
    fs::create_dir_all(&basepath)?;
    let mut dest: PathBuf = basepath.clone();
    dest.push("lorem-ipsum.txt");
    assert!(fs::copy("tests/fixtures/lorem-ipsum.txt", &dest).is_ok());
    #[allow(unused_mut, unused_assignments)]
    let mut xattr_worked = false;
    #[cfg(target_family = "unix")]
    {
        use xattr;
        xattr_worked =
            xattr::SUPPORTED_PLATFORM && xattr::set(&dest, "me.fiedlers.test", b"foobar").is_ok();
    }

    let snapshot_digest = take_snapshot(&basepath, None, &dbase, vec![])?.unwrap();
    let snapshot = dbase.get_snapshot(&snapshot_digest)?.unwrap();
    assert!(snapshot.parent.is_none());
    assert_eq!(snapshot.file_count, 1);

    // ensure extended attributes are stored in database
    if xattr_worked {
        let tree = dbase.get_tree(&snapshot.tree)?.unwrap();
        let entries: Vec<&entities::TreeEntry> = tree
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
    let db_path = DBPath::new("_test_snapshot_symlinks");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    #[cfg(target_family = "unix")]
    let basepath = "tmp/test/engine/symlinks/fixtures";
    #[cfg(target_family = "windows")]
    let basepath = "tmp\\test\\engine\\symlinks\\fixtures";
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

    // take a snapshot
    let snap1_sha = take_snapshot(Path::new(basepath), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_count, 0);
    let tree = dbase.get_tree(&snapshot1.tree)?.unwrap();

    // ensure the tree has exactly one symlink entry
    let entries: Vec<&entities::TreeEntry> = tree
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
    let db_path = DBPath::new("_test_snapshot_ordering");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    #[cfg(target_family = "unix")]
    let basepath = "tmp/test/engine/ordering/fixtures";
    #[cfg(target_family = "windows")]
    let basepath = "tmp\\test\\engine\\ordering\\fixtures";
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
        snap1_sha,
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
    let db_path = DBPath::new("_test_snapshot_types");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    #[cfg(target_family = "unix")]
    let basepath = "tmp/test/engine/types/fixtures";
    #[cfg(target_family = "windows")]
    let basepath = "tmp\\test\\engine\\types\\fixtures";
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
    let iter = find_changed_files(&dbase, PathBuf::from(basepath), snap1_sha, snap2_sha)?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].as_ref().unwrap().path, ccc);
    assert_eq!(changed[1].as_ref().unwrap().path, mmm);
    Ok(())
}

#[test]
fn test_snapshot_ignore_links() -> Result<(), Error> {
    let db_path = DBPath::new("_test_snapshot_ignore_links");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    #[cfg(target_family = "unix")]
    let basepath = "tmp/test/engine/ignore_links/fixtures";
    #[cfg(target_family = "windows")]
    let basepath = "tmp\\test\\engine\\ignore_links\\fixtures";
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
    let iter = find_changed_files(&dbase, PathBuf::from(basepath), snap1_sha, snap2_sha)?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].as_ref().unwrap().path, mmm);
    Ok(())
}

#[test]
fn test_snapshot_was_links() -> Result<(), Error> {
    let db_path = DBPath::new("_test_snapshot_was_links");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    #[cfg(target_family = "unix")]
    let basepath = "tmp/test/engine/was_links/fixtures";
    #[cfg(target_family = "windows")]
    let basepath = "tmp\\test\\engine\\was_links\\fixtures";
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
    let iter = find_changed_files(&dbase, PathBuf::from(basepath), snap1_sha, snap2_sha)?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].as_ref().unwrap().path, bbb);
    assert_eq!(changed[1].as_ref().unwrap().path, ccc);
    Ok(())
}

#[test]
fn test_pack_builder() -> Result<(), Error> {
    let db_path = DBPath::new("_test_pack_builder");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    #[cfg(target_family = "unix")]
    let basepath = "tmp/test/engine/builder/fixtures";
    #[cfg(target_family = "windows")]
    let basepath = "tmp\\test\\engine\\builder\\fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let mut builder = PackBuilder::new(&dbase, 65536);
    assert_eq!(builder.has_chunks(), false);
    assert_eq!(builder.is_full(), false);
    let lorem_path = Path::new("tests/fixtures/lorem-ipsum.txt");
    let lorem_sha = Checksum::sha256_from_file(&lorem_path)?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    let sekien_path = Path::new("tests/fixtures/SekienAkashita.jpg");
    let sekien_sha = Checksum::sha256_from_file(&sekien_path)?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    let pack_file: PathBuf = [basepath, "pack.001"].iter().collect();
    assert!(builder.has_chunks());
    assert!(builder.is_full());
    let mut pack = builder.build_pack(&pack_file, "keyboard cat")?;
    let coords = vec![entities::PackLocation::new("acme", "bucket1", "object1")];
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
    assert_ne!(saved_pack.digest.to_string(), entities::NULL_SHA1);
    // The large file will not have been completed yet, it is too large for the
    // pack size that we set above; can't be sure about the small file, either.
    let option = dbase.get_file(&sekien_sha)?;
    assert!(option.is_none());
    let mut pack = builder.build_pack(&pack_file, "keyboard cat")?;
    let coords = vec![entities::PackLocation::new("acme", "bucket1", "object2")];
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
    assert_ne!(saved_pack.digest.to_string(), entities::NULL_SHA1);
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
    #[cfg(target_family = "unix")]
    assert_eq!(saved_file.length, 3_129);
    #[cfg(target_family = "windows")]
    assert_eq!(saved_file.length, 3_138);
    #[cfg(target_family = "unix")]
    assert_eq!(
        saved_file.digest.to_string(),
        "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
    );
    #[cfg(target_family = "windows")]
    assert_eq!(
        saved_file.digest.to_string(),
        "sha256-1ed890fb1b875a5d7637d54856dc36195bed2e8e40fe6c155a2908b8dd00ebee"
    );
    Ok(())
}

#[test]
fn test_pack_builder_empty() -> Result<(), Error> {
    let db_path = DBPath::new("_test_pack_builder_empty");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

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
    let db_path = DBPath::new("_test_pack_builder_dupes");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    let mut builder = PackBuilder::new(&dbase, 65536);
    assert_eq!(builder.file_count(), 0);
    assert_eq!(builder.chunk_count(), 0);
    // builder should ignore attempts to add files with the same checksum as
    // have already been added to this builder prior to emptying into pack files
    let lorem_path = Path::new("tests/fixtures/lorem-ipsum.txt");
    let lorem_sha = Checksum::sha256_from_file(&lorem_path)?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha.clone())?;
    builder.add_file(lorem_path, lorem_sha)?;
    let sekien_path = Path::new("tests/fixtures/SekienAkashita.jpg");
    let sekien_sha = Checksum::sha256_from_file(&sekien_path)?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha.clone())?;
    builder.add_file(sekien_path, sekien_sha)?;
    assert_eq!(builder.file_count(), 2);
    assert_eq!(builder.chunk_count(), 7);
    Ok(())
}

#[test]
fn test_continue_backup() -> Result<(), Error> {
    let db_path = DBPath::new("_test_continue_backup");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    #[cfg(target_family = "unix")]
    let pack_path = "tmp/test/managers/backup/packs";
    #[cfg(target_family = "windows")]
    let pack_path = "tmp\\test\\managers\\backup\\packs";
    let _ = fs::remove_dir_all(pack_path);

    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert("basepath".to_owned(), pack_path.to_owned());
    let stores = vec![entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
    }];
    let store_builder = Box::new(PackSourceBuilderImpl {});
    let packs: Box<dyn PackRepository> = Box::new(PackRepositoryImpl::new(stores, store_builder)?);

    // create a dataset
    #[cfg(target_family = "unix")]
    let basepath = "tmp/test/managers/backup/fixtures";
    #[cfg(target_family = "windows")]
    let basepath = "tmp\\test\\managers\\backup\\fixtures";
    let _ = fs::remove_dir_all(basepath);
    fs::create_dir_all(basepath)?;
    let mut dataset = entities::Dataset::new(Path::new(basepath));
    dataset = dataset.add_store("local123");
    dataset.pack_size = 65536 as u64;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let dest: PathBuf = [basepath, "SekienAkashita.jpg"].iter().collect();
    assert!(fs::copy("tests/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let backup_opt = perform_backup(&mut dataset, &dbase, &packs, "keyboard cat")?;
    assert!(backup_opt.is_some());
    let first_sha1 = backup_opt.unwrap();

    // fake an incomplete backup by resetting the end_time field
    let mut snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    snapshot.end_time = None;
    dbase.put_snapshot(&snapshot)?;

    // run the backup again to make sure it is finished
    let backup_opt = perform_backup(&mut dataset, &dbase, &packs, "keyboard cat")?;
    assert!(backup_opt.is_some());
    let second_sha1 = backup_opt.unwrap();
    assert_eq!(first_sha1, second_sha1);
    let snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    assert!(snapshot.end_time.is_some());

    Ok(())
}
