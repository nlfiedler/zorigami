//
// Copyright (c) 2020 Nathan Fiedler
//
use anyhow::Error;
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::EntityDataSourceImpl;
use server::domain::entities::{self, Checksum};
use server::domain::managers::backup::{self, Performer, PerformerImpl};
use server::domain::managers::restore::*;
use server::domain::managers::state::{StateStore, StateStoreImpl};
use server::domain::repositories::RecordRepository;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn file_restorer_factory(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
    Box::new(FileRestorerImpl::new(dbase))
}

#[actix_rt::test]
#[serial_test::serial]
async fn test_backup_restore() -> Result<(), Error> {
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
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("local123");
    dataset.pack_size = 131072 as u64;
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::default();
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let dest: PathBuf = fixture_path.path().join("zero-length.txt");
    assert!(fs::write(dest, vec![]).is_ok());
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let passphrase = String::from("keyboard cat");
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let first_backup = performer.backup(request)?.unwrap();
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 1);
    assert_eq!(counts.file, 1);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 1);

    // perform the second backup
    let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let second_backup = performer.backup(request)?.unwrap();
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 2);
    assert_eq!(counts.file, 2);
    assert_eq!(counts.chunk, 2);
    assert_eq!(counts.tree, 2);

    // perform the third backup
    let dest: PathBuf = fixture_path.path().join("washington-journal.txt");
    assert!(fs::copy("../test/fixtures/washington-journal.txt", &dest).is_ok());
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let third_backup = performer.backup(request)?.unwrap();
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 3);
    assert_eq!(counts.file, 3);
    assert_eq!(counts.chunk, 2);
    assert_eq!(counts.tree, 3);

    // perform the fourth backup with shifted larger file
    let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
    let outfile: PathBuf = fixture_path.path().join("SekienShifted.jpg");
    copy_with_prefix("mary had a little lamb", &infile, &outfile)?;
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let fourth_backup = performer.backup(request)?.unwrap();
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 4);
    assert_eq!(counts.file, 4);
    assert_eq!(counts.chunk, 3);
    assert_eq!(counts.tree, 4);

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
    let sut = RestorerImpl::new(state, file_restorer_factory);
    let result = sut.start(dbase.clone());
    assert!(result.is_ok());
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("lorem-ipsum.txt"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());
    let outfile: PathBuf = fixture_path.path().join("restored.bin");
    assert!(outfile.exists());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the second snapshot
    let digest_expected = Checksum::BLAKE3(String::from(
        "dba425aa7292ef1209841ab3855a93d4dfa6855658a347f85c502f2c2208cf0f",
    ));
    sut.reset_completed();
    let snapshot = dbase.get_snapshot(&second_backup)?.unwrap();
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("SekienAkashita.jpg"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());
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
    sut.reset_completed();
    let snapshot = dbase.get_snapshot(&third_backup)?.unwrap();
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("washington-journal.txt"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the fourth snapshot
    let digest_expected = Checksum::BLAKE3(String::from(
        "3153cbf8ed39aa92c6dbe17eb08b3253ec3d600aef6b0a0fc43673ac6d255427",
    ));
    sut.reset_completed();
    let snapshot = dbase.get_snapshot(&fourth_backup)?.unwrap();
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("SekienShifted.jpg"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the zero length file from the first snapshot
    let digest_expected = Checksum::BLAKE3(String::from(
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
    ));
    sut.reset_completed();
    let snapshot = dbase.get_snapshot(&first_backup)?.unwrap();
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("zero-length.txt"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // shutdown the restorer supervisor to release the database lock
    let result = sut.stop();
    assert!(result.is_ok());
    actix::System::current().stop();
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
#[actix_rt::test]
#[serial_test::serial]
async fn test_backup_recover_errorred_files() -> Result<(), Error> {
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
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("local123");
    dataset.pack_size = 65536 as u64;
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "hal9000");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::default();
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let passphrase = String::from("keyboard cat");
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let first_backup = performer.backup(request)?;
    assert!(first_backup.is_some());

    // perform the second backup with a file that is not readable
    // (add two files so there is something to backup, producing a snapshot)
    let dest: PathBuf = fixture_path.path().join("short-file.txt");
    assert!(fs::write(dest, vec![102, 111, 111, 98, 97, 114]).is_ok());
    let dest: PathBuf = fixture_path.path().join("washington-journal.txt");
    assert!(fs::copy("../test/fixtures/washington-journal.txt", &dest).is_ok());
    fs::set_permissions(&dest, Permissions::from_mode(0o000))?;
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let second_backup = performer.backup(request)?.unwrap();

    // try to restore the file, it should fail
    let sut = RestorerImpl::new(state.clone(), file_restorer_factory);
    let result = sut.start(dbase.clone());
    assert!(result.is_ok());
    let snapshot = dbase.get_snapshot(&second_backup)?.unwrap();
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("washington-journal.txt"),
        PathBuf::from("washington-journal.txt"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    // assert sort-of failure: the file was not actually successfully restored
    // because it was never really backed up
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());
    assert_eq!(request.files_restored, 0);

    // fix the file permissions and perform the third backup
    fs::set_permissions(&dest, Permissions::from_mode(0o644))?;
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let third_backup = performer.backup(request)?;
    assert!(third_backup.is_some());

    // restore the file from the third snapshot
    sut.reset_completed();
    let snapshot = dbase.get_snapshot(&third_backup.unwrap())?.unwrap();
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("washington-journal.txt"),
        PathBuf::from("washington-journal.txt"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    // assert success
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());
    assert_eq!(request.files_restored, 1);
    Ok(())
}

//
// TODO: this would be testing the efficient restore of a file that already exists
//       at the target location and its checksum matches the one being restored
//
// #[actix_rt::test]
// #[serial_test::serial]
// async fn test_backup_restore_over_existing() -> Result<(), Error> {
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
//     let performer = PerformerImpl::new();
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
//     let first_backup = performer.backup(request)?;
//     assert!(first_backup.is_some());
//     let first_backup_sum = first_backup.unwrap();

//     // try to restore the file, it should do nothing since it already exists
//     let sut = RestorerImpl::new(state.clone(), file_restorer_factory);
//     let result = sut.start(dbase.clone());
//     assert!(result.is_ok());
//     let snapshot = dbase.get_snapshot(&first_backup_sum)?.unwrap();
//     let result = sut.enqueue(Request::new(
//         snapshot.tree,
//         String::from("lorem-ipsum.txt"),
//         PathBuf::from("lorem-ipsum.txt"),
//         dataset.id.to_owned(),
//         "keyboard cat".into(),
//     ));
//     assert!(result.is_ok());
//     sut.wait_for_completed();
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
//     // let third_backup = performer.backup(request)?;
//     // assert!(third_backup.is_some());

//     // TODO: restore the file from the first snapshot, should overwrite modified target
//     // sut.reset_completed();
//     // let snapshot = dbase.get_snapshot(&third_backup.unwrap())?.unwrap();
//     // let result = sut.enqueue(Request::new(
//     //     snapshot.tree,
//     //     String::from("washington-journal.txt"),
//     //     PathBuf::from("washington-journal.txt"),
//     //     dataset.id.to_owned(),
//     //     "keyboard cat".into(),
//     // ));
//     // assert!(result.is_ok());
//     // // assert success
//     // sut.wait_for_completed();
//     // let requests = sut.requests();
//     // assert_eq!(requests.len(), 1);
//     // let request = &requests[0];
//     // assert!(request.error_msg.is_none());
//     // assert_eq!(request.files_restored, 1);
//     Ok(())
// }

#[actix_rt::test]
#[serial_test::serial]
async fn test_backup_restore_symlink() -> Result<(), Error> {
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
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("local123");
    dataset.pack_size = 131072 as u64;
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::default();
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
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let passphrase = String::from("keyboard cat");
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let first_backup = performer.backup(request)?;
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
        fs::symlink(&target, &dest)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file(&target, &dest)?;
    }
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let second_backup = performer.backup(request)?;
    assert!(second_backup.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 1);
    assert_eq!(counts.file, 1);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 2);

    // restore the normal symlink from the first snapshot
    let snapshot = dbase.get_snapshot(&first_backup.as_ref().unwrap())?.unwrap();
    let sut = RestorerImpl::new(state.clone(), file_restorer_factory);
    let result = sut.start(dbase.clone());
    assert!(result.is_ok());
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("link-to-lorem.txt"),
        PathBuf::from("link-to-lorem.txt"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());

    // ensure symlink contains expected contents
    let link_path: PathBuf = fixture_path.path().join("link-to-lorem.txt");
    let value_path = fs::read_link(link_path).unwrap();
    let value_str = value_path.to_str().unwrap();
    assert_eq!(value_str, "lorem-ipsum.txt");

    // restore the weird symlink from the first snapshot but also remove
    // the symlink from the dataset to ensure restore functions correctly
    fs::remove_file(&fake_dest).unwrap();
    let snapshot = dbase.get_snapshot(&first_backup.as_ref().unwrap())?.unwrap();
    let sut = RestorerImpl::new(state, file_restorer_factory);
    let result = sut.start(dbase);
    assert!(result.is_ok());
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("link-to-nothing"),
        PathBuf::from("link-to-nothing"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());

    // ensure symlink contains expected contents
    let link_path: PathBuf = fixture_path.path().join("link-to-nothing");
    let value_path = fs::read_link(link_path).unwrap();
    let value_str = value_path.to_str().unwrap();
    assert_eq!(value_str, "link-value-is-meaningless");

    // shutdown the restorer supervisor to release the database lock
    let result = sut.stop();
    assert!(result.is_ok());
    actix::System::current().stop();
    Ok(())
}

#[actix_rt::test]
#[serial_test::serial]
async fn test_backup_restore_small() -> Result<(), Error> {
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
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("local123");
    dataset.pack_size = 131072 as u64;
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::default();
    let dest: PathBuf = fixture_path.path().join("zero-length.txt");
    assert!(fs::write(dest, vec![]).is_ok());
    let dest: PathBuf = fixture_path.path().join("very-small.txt");
    let content = "keyboard cat".as_bytes().to_vec();
    assert!(fs::write(dest, content).is_ok());
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let passphrase = String::from("keyboard cat");
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let first_backup = performer.backup(request)?;
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
    let request = backup::Request::new(
        dataset.clone(),
        dbase.clone(),
        state.clone(),
        &passphrase,
        None,
    );
    let second_backup = performer.backup(request)?;
    assert!(second_backup.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 0);
    assert_eq!(counts.file, 0);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 2);

    // restore the small file from the first snapshot
    let snapshot = dbase.get_snapshot(&first_backup.unwrap())?.unwrap();
    let sut = RestorerImpl::new(state, file_restorer_factory);
    let result = sut.start(dbase);
    assert!(result.is_ok());
    let result = sut.enqueue(Request::new(
        snapshot.tree,
        String::from("very-small.txt"),
        PathBuf::from("very-small.txt"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    sut.wait_for_completed();
    let requests = sut.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());

    // ensure small file contains expected contents
    let small_path: PathBuf = fixture_path.path().join("very-small.txt");
    let value_str = fs::read_to_string(small_path).unwrap();
    assert_eq!(value_str, "keyboard cat");

    // shutdown the restorer supervisor to release the database lock
    let result = sut.stop();
    assert!(result.is_ok());
    actix::System::current().stop();
    Ok(())
}
