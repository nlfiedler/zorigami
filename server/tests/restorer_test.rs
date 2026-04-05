//
// Copyright (c) 2020 Nathan Fiedler
//
use anyhow::Error;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use zorigami::data::repositories::RecordRepositoryImpl;
use zorigami::data::sources::EntityDataSourceImpl;
use zorigami::domain::entities::{self, Checksum, PackRetention};
use zorigami::domain::repositories::RecordRepository;
use zorigami::tasks::backup::{self, Backuper, BackuperImpl};
use zorigami::tasks::restore::{self, Restorer, RestorerImpl};

struct DummyBackupSubscriber();

impl backup::Subscriber for DummyBackupSubscriber {
    fn started(&self, _request_id: &str) {}

    fn files_changed(&self, _request_id: &str, _count: u64) {}

    fn pack_uploaded(&self, _request_id: &str) {}

    fn bytes_uploaded(&self, _request_id: &str, _addend: u64) {}

    fn files_uploaded(&self, _request_id: &str, _addend: u64) {}

    fn error(&self, _request_id: &str, _error: String) {}

    fn paused(&self, _request_id: &str) {}

    fn restarted(&self, _request_id: &str) {}

    fn finished(&self, _request_id: &str) {}
}

struct DummyRestoreSubscriber();

impl restore::Subscriber for DummyRestoreSubscriber {
    fn started(&self, _request_id: &str) -> bool {
        false
    }

    fn restored(&self, _request_id: &str, _addend: u64) -> bool {
        false
    }

    fn error(&self, _request_id: &str, _error: String) -> bool {
        false
    }

    fn finished(&self, _request_id: &str) -> bool {
        false
    }
}

#[test]
fn test_restorer_full_cycle() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(db_path.path())?;
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    // set up local pack store
    let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
    fs::create_dir_all(&pack_base)?;
    let pack_path = tempfile::tempdir_in(&pack_base)?;
    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert(
        "basepath".to_owned(),
        pack_path.keep().to_string_lossy().into(),
    );
    let store = entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
        retention: PackRetention::ALL,
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("local123");
    dataset.chunk_size = 32768;
    dataset.pack_size = 131072;
    dbase.put_dataset(&dataset)?;

    // perform the first backup
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummyBackupSubscriber());
    let backuper = BackuperImpl::new(dbase.clone(), subscriber, stopper);
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let dest: PathBuf = fixture_path.path().join("zero-length.txt");
    assert!(fs::write(dest, vec![]).is_ok());
    let passphrase = String::from("keyboard cat");
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let first_backup = backuper.backup(request)?.unwrap();
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 1);
    assert_eq!(counts.file, 1);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 1);

    // perform the second backup
    let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let second_backup = backuper.backup(request)?.unwrap();
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 2);
    assert_eq!(counts.file, 2);
    assert_eq!(counts.chunk, 2);
    assert_eq!(counts.tree, 2);

    // perform the third backup
    let dest: PathBuf = fixture_path.path().join("washington-journal.txt");
    assert!(fs::copy("../test/fixtures/washington-journal.txt", &dest).is_ok());
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let third_backup = backuper.backup(request)?.unwrap();
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 3);
    assert_eq!(counts.file, 3);
    assert_eq!(counts.chunk, 2);
    assert_eq!(counts.tree, 3);

    // perform the fourth backup with shifted larger file
    let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
    let outfile: PathBuf = fixture_path.path().join("SekienShifted.jpg");
    copy_with_prefix("mary had a little lamb", infile, &outfile)?;
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let fourth_backup = backuper.backup(request)?.unwrap();
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 4);
    assert_eq!(counts.file, 4);
    assert_eq!(counts.chunk, 3);
    assert_eq!(counts.tree, 4);

    // set up the restorer to perform restores
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummyRestoreSubscriber());
    let restorer = RestorerImpl::new(dbase.clone(), subscriber, stopper);

    // restore the file from the first snapshot
    #[cfg(target_family = "unix")]
    let digest_expected = Checksum::BLAKE3(String::from(
        "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128",
    ));
    #[cfg(target_family = "windows")]
    let digest_expected = Checksum::BLAKE3(String::from(
        "2720a91db93dae2a92ed9f74b0f7a135cfdf4d32dd069477cda457002ffc9e7a",
    ));
    let snapshot = dbase.get_snapshot(&first_backup)?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("lorem-ipsum.txt"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    let outfile: PathBuf = fixture_path.path().join("restored.bin");
    assert!(outfile.exists());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the second snapshot
    let digest_expected = Checksum::BLAKE3(String::from(
        "dba425aa7292ef1209841ab3855a93d4dfa6855658a347f85c502f2c2208cf0f",
    ));
    let snapshot = dbase.get_snapshot(&second_backup)?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("SekienAkashita.jpg"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the third snapshot
    #[cfg(target_family = "unix")]
    let digest_expected = Checksum::BLAKE3(String::from(
        "540c45803112958ab53e31daee5eec067b1442d579eb1e787cf7684657275b60",
    ));
    #[cfg(target_family = "windows")]
    let digest_expected = Checksum::BLAKE3(String::from(
        "183d52ff928be3e77cccf1b78b12b31910d5079195a637a9a2b499059f99b781",
    ));
    let snapshot = dbase.get_snapshot(&third_backup)?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("washington-journal.txt"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the fourth snapshot
    let digest_expected = Checksum::BLAKE3(String::from(
        "3153cbf8ed39aa92c6dbe17eb08b3253ec3d600aef6b0a0fc43673ac6d255427",
    ));
    let snapshot = dbase.get_snapshot(&fourth_backup)?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("SekienShifted.jpg"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the zero length file from the first snapshot
    let digest_expected = Checksum::BLAKE3(String::from(
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
    ));
    let snapshot = dbase.get_snapshot(&first_backup)?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("zero-length.txt"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    Ok(())
}

///
/// Copy one file to another, prepending the result with the given text.
///
fn copy_with_prefix(header: &str, infile: &Path, outfile: &Path) -> Result<(), Error> {
    let mut reader: &[u8] = header.as_bytes();
    let mut writer = fs::File::create(outfile)?;
    std::io::copy(&mut reader, &mut writer)?;
    let mut reader = fs::File::open(infile)?;
    std::io::copy(&mut reader, &mut writer)?;
    Ok(())
}

#[cfg(target_family = "unix")]
#[test]
fn test_restorer_backup_recover_errorred_files() -> Result<(), Error> {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;

    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(db_path.path()).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
    fs::create_dir_all(&pack_base)?;
    let pack_path = tempfile::tempdir_in(&pack_base)?;
    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert(
        "basepath".to_owned(),
        pack_path.keep().to_string_lossy().into(),
    );
    let store = entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
        retention: PackRetention::ALL,
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("local123");
    dataset.chunk_size = 16384;
    dataset.pack_size = 65536;
    dbase.put_dataset(&dataset)?;

    // perform the first backup
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummyBackupSubscriber());
    let backuper = BackuperImpl::new(dbase.clone(), subscriber, stopper);
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let passphrase = String::from("keyboard cat");
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let first_backup = backuper.backup(request)?;
    assert!(first_backup.is_some());

    // perform the second backup with a file that is not readable
    // (add two files so there is something to backup, producing a snapshot)
    let dest: PathBuf = fixture_path.path().join("short-file.txt");
    assert!(fs::write(dest, vec![102, 111, 111, 98, 97, 114]).is_ok());
    let dest: PathBuf = fixture_path.path().join("washington-journal.txt");
    assert!(fs::copy("../test/fixtures/washington-journal.txt", &dest).is_ok());
    fs::set_permissions(&dest, Permissions::from_mode(0o000))?;
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let second_backup = backuper.backup(request)?.unwrap();

    // reset the permissions and delete the file in order to try to restore it
    fs::set_permissions(&dest, Permissions::from_mode(0o644))?;
    fs::remove_file(&dest)?;

    // set up the restorer to perform restores
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummyRestoreSubscriber());
    let restorer = RestorerImpl::new(dbase.clone(), subscriber, stopper);

    // try to restore the file, it will quietly fail since it was not backed up
    let snapshot = dbase.get_snapshot(&second_backup)?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("washington-journal.txt"),
        PathBuf::from("washington-journal.txt"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    assert!(!fs::exists(&dest).unwrap());

    // produce the original file again and perform the third backup
    assert!(fs::copy("../test/fixtures/washington-journal.txt", &dest).is_ok());
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let third_backup = backuper.backup(request)?;
    assert!(third_backup.is_some());

    // delete the file and restore it from the third snapshot
    fs::remove_file(&dest)?;
    let snapshot = dbase.get_snapshot(&third_backup.unwrap())?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("washington-journal.txt"),
        PathBuf::from("washington-journal.txt"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());

    #[cfg(target_family = "unix")]
    let digest_expected = Checksum::BLAKE3(String::from(
        "540c45803112958ab53e31daee5eec067b1442d579eb1e787cf7684657275b60",
    ));
    #[cfg(target_family = "windows")]
    let digest_expected = Checksum::BLAKE3(String::from(
        "183d52ff928be3e77cccf1b78b12b31910d5079195a637a9a2b499059f99b781",
    ));
    let digest_actual = Checksum::blake3_from_file(&dest)?;
    assert_eq!(digest_expected, digest_actual);

    Ok(())
}

//
// TODO: this would be testing the efficient restore of a file that already exists
//       at the target location and its checksum matches the one being restored
//
// #[actix_rt::test]
// #[serial_test::serial]
// async fn test_restorer_backup_restore_over_existing() -> Result<(), Error> {
//     // create the database
//     let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
//     fs::create_dir_all(&db_base)?;
//     let db_path = tempfile::tempdir_in(&db_base)?;
//     let datasource = EntityDataSourceImpl::new(db_path.path()).unwrap();
//     let repo = RecordRepositoryImpl::new(Arc::new(datasource));
//     let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

//     // create a local pack store
//     let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
//     fs::create_dir_all(&pack_base)?;
//     let pack_path = tempfile::tempdir_in(&pack_base)?;
//     let mut local_props: HashMap<String, String> = HashMap::new();
//     local_props.insert(
//         "basepath".to_owned(),
//         pack_path.into_path().to_string_lossy().into(),
//     );
//     let store = entities::Store {
//         id: "local123".to_owned(),
//         store_type: entities::StoreType::LOCAL,
//         label: "my local".to_owned(),
//         properties: local_props,
//     };
//     dbase.put_store(&store)?;

//     // create a dataset
//     let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
//     fs::create_dir_all(&fixture_base)?;
//     let fixture_path = tempfile::tempdir_in(&fixture_base)?;
//     let mut dataset = entities::Dataset::new(fixture_path.path());
//     dataset = dataset.add_store("local123");
//     dbase.put_dataset(&dataset)?;
//     let computer_id = entities::Configuration::generate_unique_id("charlie", "hal9000");
//     dbase.put_computer_id(&dataset.id, &computer_id)?;

//     // perform the first backup
//     let backuper = BackuperImpl::new();
//     let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
//     assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
//     let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
//     let passphrase = String::from("keyboard cat");
//     let request = backup::Request::new(
//         dataset.clone(),
//         dbase.clone(),
//         state.clone(),
//         &passphrase,
//         None,
//     );
//     let first_backup = backuper.backup(request)?;
//     assert!(first_backup.is_some());
//     let first_backup_sum = first_backup.unwrap();

//     // try to restore the file, it should do nothing since it already exists
//     let sut = RestorerImpl::new(state.clone(), file_restorer_factory);
//     let result = sut.start(dbase.clone());
//     assert!(result.is_ok());
//     let snapshot = dbase.get_snapshot(&first_backup_sum)?.unwrap();
//     let result = sut.enqueue(restore::Request::new(
//         snapshot.tree,
//         String::from("lorem-ipsum.txt"),
//         PathBuf::from("lorem-ipsum.txt"),
//         dataset.id.to_owned(),
//         "keyboard cat".into(),
//     ));
//     assert!(result.is_ok());
//     sut.wait_for_restores();
//     let requests = sut.requests();
//     assert_eq!(requests.len(), 1);
//     let request = &requests[0];
//     assert!(request.error_msg.is_none());
//     assert_eq!(request.files_restored, 0);

//     // TODO: modify the target file
//     // fix the file permissions and perform the third backup
//     // fs::set_permissions(&dest, Permissions::from_mode(0o644))?;
//     // let request = backup::Request::new(
//     //     dataset.clone(),
//     //     dbase.clone(),
//     //     state.clone(),
//     //     &passphrase,
//     //     None,
//     // );
//     // let third_backup = backuper.backup(request)?;
//     // assert!(third_backup.is_some());

//     // TODO: restore the file from the first snapshot, should overwrite modified target
//     // sut.reset_restores();
//     // let snapshot = dbase.get_snapshot(&third_backup.unwrap())?.unwrap();
//     // let result = sut.enqueue(restore::Request::new(
//     //     snapshot.tree,
//     //     String::from("washington-journal.txt"),
//     //     PathBuf::from("washington-journal.txt"),
//     //     dataset.id.to_owned(),
//     //     "keyboard cat".into(),
//     // ));
//     // assert!(result.is_ok());
//     // // assert success
//     // sut.wait_for_restores();
//     // let requests = sut.requests();
//     // assert_eq!(requests.len(), 1);
//     // let request = &requests[0];
//     // assert!(request.error_msg.is_none());
//     // assert_eq!(request.files_restored, 1);
//     Ok(())
// }

#[test]
fn test_restorer_backup_restore_symlink() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(db_path.path())?;
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    // set up local pack store
    let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
    fs::create_dir_all(&pack_base)?;
    let pack_path = tempfile::tempdir_in(&pack_base)?;
    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert(
        "basepath".to_owned(),
        pack_path.keep().to_string_lossy().into(),
    );
    let store = entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
        retention: PackRetention::ALL,
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("local123");
    dataset.chunk_size = 32768;
    dataset.pack_size = 131072;
    dbase.put_dataset(&dataset)?;

    // perform the first backup
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummyBackupSubscriber());
    let backuper = BackuperImpl::new(dbase.clone(), subscriber, stopper);
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let real_dest: PathBuf = fixture_path.path().join("link-to-lorem.txt");
    let fake_dest = fixture_path.path().join("link-to-nothing");
    // cfg! macro will not work in this OS-specific import case
    {
        #[cfg(target_family = "unix")]
        use std::os::unix::fs;
        #[cfg(target_family = "windows")]
        use std::os::windows::fs;
        // make a normal symlink that points to a file
        #[cfg(target_family = "unix")]
        fs::symlink("lorem-ipsum.txt", &real_dest)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file("lorem-ipsum.txt", &real_dest)?;
        // make a phony symlink that points to nothing
        #[cfg(target_family = "unix")]
        fs::symlink("link-value-is-meaningless", &fake_dest)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file("link-value-is-meaningless", &fake_dest)?;
    }
    let passphrase = String::from("keyboard cat");
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let first_backup = backuper.backup(request)?;
    assert!(first_backup.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 1);
    assert_eq!(counts.file, 1);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 1);

    // perform the second backup
    let dest: PathBuf = fixture_path.path().join("link-to-lorem.txt");
    fs::remove_file(&dest).unwrap();
    let target = "target-does-not-exist";
    // cfg! macro will not work in this OS-specific import case
    {
        #[cfg(target_family = "unix")]
        use std::os::unix::fs;
        #[cfg(target_family = "windows")]
        use std::os::windows::fs;
        #[cfg(target_family = "unix")]
        fs::symlink(target, &dest)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file(&target, &dest)?;
    }
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let second_backup = backuper.backup(request)?;
    assert!(second_backup.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 1);
    assert_eq!(counts.file, 1);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 2);

    // set up the restorer to perform restores
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummyRestoreSubscriber());
    let restorer = RestorerImpl::new(dbase.clone(), subscriber, stopper);

    // restore the normal symlink from the first snapshot
    let snapshot = dbase.get_snapshot(first_backup.as_ref().unwrap())?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("link-to-lorem.txt"),
        PathBuf::from("link-to-lorem.txt"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());

    // ensure symlink contains expected contents
    let link_path: PathBuf = fixture_path.path().join("link-to-lorem.txt");
    let value_path = fs::read_link(link_path).unwrap();
    let value_str = value_path.to_str().unwrap();
    assert_eq!(value_str, "lorem-ipsum.txt");

    // restore the weird symlink from the first snapshot but also remove
    // the symlink from the dataset to ensure restore functions correctly
    fs::remove_file(&fake_dest).unwrap();
    let snapshot = dbase.get_snapshot(first_backup.as_ref().unwrap())?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("link-to-nothing"),
        PathBuf::from("link-to-nothing"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());

    // ensure symlink contains expected contents
    let link_path: PathBuf = fixture_path.path().join("link-to-nothing");
    let value_path = fs::read_link(link_path).unwrap();
    let value_str = value_path.to_str().unwrap();
    assert_eq!(value_str, "link-value-is-meaningless");

    Ok(())
}

#[test]
fn test_restorer_backup_restore_small() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(db_path.path())?;
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    // set up local pack store
    let pack_base: PathBuf = ["tmp", "test", "packs"].iter().collect();
    fs::create_dir_all(&pack_base)?;
    let pack_path = tempfile::tempdir_in(&pack_base)?;
    let mut local_props: HashMap<String, String> = HashMap::new();
    local_props.insert(
        "basepath".to_owned(),
        pack_path.keep().to_string_lossy().into(),
    );
    let store = entities::Store {
        id: "local123".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "my local".to_owned(),
        properties: local_props,
        retention: PackRetention::ALL,
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("local123");
    dataset.chunk_size = 32768;
    dataset.pack_size = 131072;
    dbase.put_dataset(&dataset)?;

    // perform the first backup
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummyBackupSubscriber());
    let backuper = BackuperImpl::new(dbase.clone(), subscriber, stopper);
    let dest: PathBuf = fixture_path.path().join("zero-length.txt");
    assert!(fs::write(dest, vec![]).is_ok());
    let dest: PathBuf = fixture_path.path().join("very-small.txt");
    let content = "keyboard cat".as_bytes().to_vec();
    assert!(fs::write(dest, content).is_ok());
    let passphrase = String::from("keyboard cat");
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let first_backup = backuper.backup(request)?;
    assert!(first_backup.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 0);
    assert_eq!(counts.file, 0);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 1);

    // perform the second backup
    let dest: PathBuf = fixture_path.path().join("zero-length.txt");
    assert!(fs::remove_file(&dest).is_ok());
    assert!(fs::metadata(&dest).is_err());
    let dest: PathBuf = fixture_path.path().join("very-small.txt");
    let content = "danger mouse".as_bytes().to_vec();
    assert!(fs::write(dest, content).is_ok());
    let request = backup::Request::new(dataset.id.clone(), &passphrase, None);
    let second_backup = backuper.backup(request)?;
    assert!(second_backup.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 0);
    assert_eq!(counts.file, 0);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 2);

    // set up the restorer to perform restores
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummyRestoreSubscriber());
    let restorer = RestorerImpl::new(dbase.clone(), subscriber, stopper);

    // restore the small file from the first snapshot
    let snapshot = dbase.get_snapshot(&first_backup.unwrap())?.unwrap();
    let result = restorer.restore_files(restore::Request::new(
        snapshot.tree,
        String::from("very-small.txt"),
        PathBuf::from("very-small.txt"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());

    // ensure small file contains expected contents
    let small_path: PathBuf = fixture_path.path().join("very-small.txt");
    let value_str = fs::read_to_string(small_path).unwrap();
    assert_eq!(value_str, "keyboard cat");

    Ok(())
}
