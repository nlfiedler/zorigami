//
// Copyright (c) 2022 Nathan Fiedler
//
use anyhow::Error;
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::EntityDataSourceImpl;
use server::domain::entities::{self, Checksum};
use server::domain::managers::backup::{
    find_changed_files, take_snapshot, ChangedFile, OutOfTimeFailure, Performer, PerformerImpl,
    Request, TreeWalker,
};
use server::domain::managers::state::{StateStore, StateStoreImpl};
use server::domain::repositories::RecordRepository;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[test]
fn test_basic_snapshots() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    // set up dataset base directory
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;

    // take a snapshot of the dataset
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_counts.total_files(), 1);

    // take another snapshot
    let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let snap2_sha =
        take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    let snapshot2 = dbase.get_snapshot(&snap2_sha)?.unwrap();
    assert!(snapshot2.parent.is_some());
    assert_eq!(snapshot2.parent.unwrap(), snap1_sha);
    assert_eq!(snapshot2.file_counts.files_below_10k, 1);
    assert_eq!(snapshot2.file_counts.files_below_1m, 1);
    assert_eq!(snapshot2.file_counts.total_files(), 2);
    assert_ne!(snap1_sha, snap2_sha);
    assert_ne!(snapshot1.tree, snapshot2.tree);

    // compute the differences
    let iter = find_changed_files(
        &dbase,
        fixture_path.path().to_path_buf(),
        snap1_sha,
        snap2_sha.clone(),
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert!(changed[0].is_ok());
    assert_eq!(
        changed[0].as_ref().unwrap().digest,
        Checksum::SHA256(String::from(
            "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        ))
    );

    // take yet another snapshot, should find no changes
    let snap3_opt = take_snapshot(fixture_path.path(), Some(snap2_sha), &dbase, vec![])?;
    assert!(snap3_opt.is_none());
    Ok(())
}

#[test]
fn test_default_excludes() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let basepath: PathBuf = ["..", "test", "fixtures", "dataset_1"].iter().collect();
    let mut workspace = basepath.clone();
    workspace.push(".tmp");
    let excludes = vec![workspace];
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(&basepath, None, &dbase, excludes)?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_counts.total_files(), 6);
    // walk the snapshot and ensure all files were included
    let tree = snapshot1.tree;
    let iter = TreeWalker::new(&dbase, &basepath, tree);
    let mut exe_count: usize = 0;
    let mut txt_count: usize = 0;
    let mut js_count: usize = 0;
    let mut jpg_count: usize = 0;
    for result in iter {
        let path = result.unwrap().path;
        let path_str = path.to_str().unwrap();
        if path_str.ends_with(".exe") {
            exe_count += 1;
        }
        if path_str.ends_with(".txt") {
            txt_count += 1;
        }
        if path_str.ends_with(".js") {
            js_count += 1;
        }
        if path_str.ends_with(".jpg") {
            jpg_count += 1;
        }
    }
    assert_eq!(exe_count, 1);
    assert_eq!(txt_count, 2);
    assert_eq!(js_count, 1);
    assert_eq!(jpg_count, 1);
    Ok(())
}

#[test]
fn test_basic_excludes() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let mut excludes: Vec<PathBuf> = Vec::new();
    // individual files
    excludes.push(PathBuf::from("*.exe"));
    // entire directory structure by name (while technically different than the
    // pattern that ends with /**, it has the same effect for our purposes since
    // we ignore directories)
    excludes.push(PathBuf::from("**/node_modules"));
    // entire directory structure by name based at the root only
    excludes.push(PathBuf::from("workspace"));
    let basepath: PathBuf = ["..", "test", "fixtures", "dataset_1"].iter().collect();
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(&basepath, None, &dbase, excludes)?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_counts.total_files(), 3);
    // walk the snapshot and ensure excluded files are excluded
    let tree = snapshot1.tree;
    let iter = TreeWalker::new(&dbase, &basepath, tree);
    for result in iter {
        let path = result.unwrap().path;
        let path_str = path.to_str().unwrap();
        assert!(!path_str.ends_with(".exe"));
        assert!(!path_str.contains("node_modules"));
        assert!(!path_str.contains("workspace"));
    }
    Ok(())
}

#[test]
fn test_snapshots_xattrs() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", &dest).is_ok());
    #[allow(unused_mut, unused_assignments)]
    let mut xattr_worked = false;
    #[cfg(target_family = "unix")]
    {
        use xattr;
        xattr_worked =
            xattr::SUPPORTED_PLATFORM && xattr::set(&dest, "me.fiedlers.test", b"foobar").is_ok();
    }

    let snapshot_digest = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
    let snapshot = dbase.get_snapshot(&snapshot_digest)?.unwrap();
    assert!(snapshot.parent.is_none());
    assert_eq!(snapshot.file_counts.total_files(), 1);

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
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let dest: PathBuf = fixture_path.path().join("meaningless");
    let target = "link_target_is_meaningless";
    // cfg! macro only works if all paths can compile on every platform
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
    let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert!(snapshot1.parent.is_none());
    assert_eq!(snapshot1.file_counts.total_files(), 0);
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
    assert_eq!(value, target.as_bytes());
    Ok(())
}

#[test]
fn test_snapshot_ordering() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let ccc: PathBuf = fixture_path.path().join("ccc").join("ccc.txt");
    let mmm: PathBuf = fixture_path.path().join("mmm").join("mmm.txt");
    let yyy: PathBuf = fixture_path.path().join("yyy").join("yyy.txt");
    fs::create_dir(ccc.parent().unwrap())?;
    fs::create_dir(mmm.parent().unwrap())?;
    fs::create_dir(yyy.parent().unwrap())?;
    fs::write(&ccc, b"crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs")?;
    fs::write(&mmm, b"morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins")?;
    fs::write(&yyy, b"yellow yak yodeling, yellow yak yodeling, yellow yak yodeling, yellow yak yodeling, yellow yak yodeling")?;
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_counts.total_files(), 3);
    // add new files, change one file
    let bbb: PathBuf = fixture_path.path().join("bbb").join("bbb.txt");
    let nnn: PathBuf = fixture_path.path().join("nnn").join("nnn.txt");
    let zzz: PathBuf = fixture_path.path().join("zzz").join("zzz.txt");
    fs::create_dir(bbb.parent().unwrap())?;
    fs::create_dir(nnn.parent().unwrap())?;
    fs::create_dir(zzz.parent().unwrap())?;
    fs::write(&bbb, b"blue baboons bouncing balls, blue baboons bouncing balls, blue baboons bouncing balls, blue baboons bouncing balls")?;
    fs::write(&mmm, b"many mumbling mice moonlight, many mumbling mice moonlight, many mumbling mice moonlight, many mumbling mice moonlight")?;
    fs::write(&nnn, b"neat newts gnawing noodles, neat newts gnawing noodles, neat newts gnawing noodles, neat newts gnawing noodles")?;
    fs::write(&zzz, b"zebras riding on a zephyr, zebras riding on a zephyr, zebras riding on a zephyr, zebras riding on a zephyr")?;
    let snap2_sha =
        take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        fixture_path.path().to_path_buf(),
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
    fs::write(&zzz, b"zippy zip ties zooming, zippy zip ties zooming, zippy zip ties zooming, zippy zip ties zooming")?;
    let snap3_sha =
        take_snapshot(fixture_path.path(), Some(snap2_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        fixture_path.path().to_path_buf(),
        snap2_sha,
        snap3_sha,
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].as_ref().unwrap().path, zzz);
    Ok(())
}

#[test]
fn test_snapshot_types() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let ccc: PathBuf = fixture_path.path().join("ccc");
    let mmm: PathBuf = fixture_path.path().join("mmm").join("mmm.txt");
    fs::create_dir(mmm.parent().unwrap())?;
    fs::write(&ccc, b"crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs")?;
    fs::write(&mmm, b"morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins")?;
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_counts.total_files(), 2);
    // change files to dirs and vice versa
    fs::remove_file(&ccc)?;
    let ccc: PathBuf = fixture_path.path().join("ccc").join("ccc.txt");
    let mmm: PathBuf = fixture_path.path().join("mmm");
    fs::create_dir(ccc.parent().unwrap())?;
    fs::remove_dir_all(&mmm)?;
    fs::write(&ccc, b"catastrophic catastrophes, catastrophic catastrophes, catastrophic catastrophes, catastrophic catastrophes")?;
    fs::write(&mmm, b"many mumbling mice moonlight, many mumbling mice moonlight, many mumbling mice moonlight, many mumbling mice moonlight")?;
    let snap2_sha =
        take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        fixture_path.path().to_path_buf(),
        snap1_sha,
        snap2_sha,
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].as_ref().unwrap().path, ccc);
    assert_eq!(changed[1].as_ref().unwrap().path, mmm);
    Ok(())
}

#[test]
fn test_snapshot_ignore_links() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let bbb: PathBuf = fixture_path.path().join("bbb");
    let ccc: PathBuf = fixture_path.path().join("ccc").join("ccc.txt");
    fs::create_dir(ccc.parent().unwrap())?;
    fs::write(&bbb, b"bored baby baboons bathing, bored baby baboons bathing, bored baby baboons bathing, bored baby baboons bathing")?;
    fs::write(&ccc, b"crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs")?;
    // take a snapshot of the test data
    let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_counts.total_files(), 2);
    // replace the files and directories with links
    let mmm: PathBuf = fixture_path.path().join("mmm.txt");
    fs::write(&mmm, b"morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins")?;
    fs::remove_file(&bbb)?;
    fs::remove_dir_all(ccc.parent().unwrap())?;
    let ccc: PathBuf = fixture_path.path().join("ccc");
    // cfg! macro only works if all paths can compile on every platform
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
        take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        fixture_path.path().to_path_buf(),
        snap1_sha,
        snap2_sha,
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].as_ref().unwrap().path, mmm);
    Ok(())
}

#[test]
fn test_snapshot_was_links() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mmm: PathBuf = fixture_path.path().join("mmm.txt");
    fs::write(&mmm, b"morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins, morose monkey munching muffins")?;
    let bbb: PathBuf = fixture_path.path().join("bbb");
    let ccc: PathBuf = fixture_path.path().join("ccc");
    // cfg! macro only works if all paths can compile on every platform
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
    let snap1_sha = take_snapshot(fixture_path.path(), None, &dbase, vec![])?.unwrap();
    let snapshot1 = dbase.get_snapshot(&snap1_sha)?.unwrap();
    assert_eq!(snapshot1.file_counts.total_files(), 1);
    // replace the links with files and directories
    fs::remove_file(&bbb)?;
    fs::write(&bbb, b"bored baby baboons bathing, bored baby baboons bathing, bored baby baboons bathing, bored baby baboons bathing")?;
    fs::remove_file(&ccc)?;
    let ccc: PathBuf = fixture_path.path().join("ccc").join("ccc.txt");
    fs::create_dir(ccc.parent().unwrap())?;
    fs::write(&ccc, b"crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs, crazy cat clawing chairs")?;
    let snap2_sha =
        take_snapshot(fixture_path.path(), Some(snap1_sha.clone()), &dbase, vec![])?.unwrap();
    // compute the differences
    let iter = find_changed_files(
        &dbase,
        fixture_path.path().to_path_buf(),
        snap1_sha,
        snap2_sha,
    )?;
    let changed: Vec<Result<ChangedFile, Error>> = iter.collect();
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].as_ref().unwrap().path, bbb);
    assert_eq!(changed[1].as_ref().unwrap().path, ccc);
    Ok(())
}

#[test]
fn test_continue_backup() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    // set up local pack store
    let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
    fs::create_dir_all(&pack_base)?;
    let pack_path = tempfile::tempdir_in(&pack_base)?;
    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert(
        "basepath".to_owned(),
        pack_path.into_path().to_string_lossy().into(),
    );
    let store = entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset = dataset.add_store("local123");
    dataset.pack_size = 131072 as u64;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::new();
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let passphrase = String::from("keyboard cat");
    let request = Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let backup_opt = performer.backup(request)?;
    assert!(backup_opt.is_some());
    let first_sha1 = backup_opt.unwrap();

    // fake an incomplete backup by resetting the end_time field
    let mut snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    snapshot.end_time = None;
    dbase.put_snapshot(&snapshot)?;

    // run the backup again to make sure it is finished
    let request = Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let backup_opt = performer.backup(request)?;
    assert!(backup_opt.is_some());
    let second_sha1 = backup_opt.unwrap();
    assert_eq!(first_sha1, second_sha1);
    let snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    assert!(snapshot.end_time.is_some());

    // ensure the backup created the expected number of each record type
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 1);
    assert_eq!(counts.file, 1);
    assert_eq!(counts.chunk, 3);
    assert_eq!(counts.tree, 1);

    Ok(())
}

#[test]
fn test_backup_out_of_time() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
    fs::create_dir_all(&pack_base)?;
    let pack_path = tempfile::tempdir_in(&pack_base)?;
    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert(
        "basepath".to_owned(),
        pack_path.into_path().to_string_lossy().into(),
    );
    let store = entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset = dataset.add_store("local123");
    dataset.pack_size = 65536 as u64;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // start the backup manager after the stop time has already passed; should
    // return an "out of time" error
    let performer = PerformerImpl::new();
    let passphrase = String::from("keyboard cat");
    let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let stop_time = Some(chrono::Utc::now() - chrono::Duration::minutes(5));
    let request = Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        stop_time,
    );
    match performer.backup(request) {
        Ok(_) => panic!("expected backup to return an error"),
        Err(err) => assert!(err.downcast::<OutOfTimeFailure>().is_ok()),
    }
    Ok(())
}

#[test]
fn test_backup_empty_file() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
    fs::create_dir_all(&pack_base)?;
    let pack_path = tempfile::tempdir_in(&pack_base)?;
    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert(
        "basepath".to_owned(),
        pack_path.into_path().to_string_lossy().into(),
    );
    let store = entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset = dataset.add_store("local123");
    dataset.pack_size = 65536 as u64;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "hal9000");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform a backup where there is only an empty file
    let performer = PerformerImpl::new();
    let passphrase = String::from("keyboard cat");
    let dest: PathBuf = fixture_path.path().join("zero-length.txt");
    assert!(fs::write(dest, vec![]).is_ok());
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let request = Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let backup_opt = performer.backup(request)?;
    // it did not blow up, so that counts as passing
    assert!(backup_opt.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 0);
    assert_eq!(counts.file, 0);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 1);
    Ok(())
}
