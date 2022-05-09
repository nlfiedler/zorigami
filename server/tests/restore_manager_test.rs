//
// Copyright (c) 2022 Nathan Fiedler
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

#[actix_rt::test]
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
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::new();
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
    assert_eq!(counts.chunk, 3);
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
    assert_eq!(counts.chunk, 3);
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
    assert_eq!(counts.chunk, 4);
    assert_eq!(counts.tree, 4);

    // restore the file from the first snapshot
    #[cfg(target_family = "unix")]
    let digest_expected = Checksum::SHA256(String::from(
        "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
    ));
    #[cfg(target_family = "windows")]
    let digest_expected = Checksum::SHA256(String::from(
        "1ed890fb1b875a5d7637d54856dc36195bed2e8e40fe6c155a2908b8dd00ebee",
    ));
    let snapshot = dbase.get_snapshot(&first_backup)?.unwrap();
    let sut = RestorerImpl::new();
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
    let digest_actual = Checksum::sha256_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the second snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed",
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
    let digest_actual = Checksum::sha256_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the third snapshot
    #[cfg(target_family = "unix")]
    let digest_expected = Checksum::SHA256(String::from(
        "314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05",
    ));
    #[cfg(target_family = "windows")]
    let digest_expected = Checksum::SHA256(String::from(
        "494cb077670d424f47a3d33929d6f1cbcf408a06d28be11259b2fe90666010dc",
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
    let digest_actual = Checksum::sha256_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the file from the fourth snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "b2c67e90a01f5d7aca48835b8ad8f0902ef03288aa4083e742bccbd96d8590a4",
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
    let digest_actual = Checksum::sha256_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // restore the zero length file from the first snapshot
    let digest_expected = Checksum::SHA256(String::from(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
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
    println!("request: {:?}", request);
    assert!(request.error_msg.is_none());
    let digest_actual = Checksum::sha256_from_file(&outfile)?;
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
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "hal9000");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::new();
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
    let sut = RestorerImpl::new();
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
    println!("requests: {:?}", requests);
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

#[actix_rt::test]
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
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::new();
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let dest: PathBuf = fixture_path.path().join("link-to-lorem.txt");
    let target = "lorem-ipsum.txt";
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

    // restore the symlink from the first snapshot
    let snapshot = dbase.get_snapshot(&first_backup.unwrap())?.unwrap();
    let sut = RestorerImpl::new();
    let result = sut.start(dbase);
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

    // shutdown the restorer supervisor to release the database lock
    let result = sut.stop();
    assert!(result.is_ok());
    actix::System::current().stop();
    Ok(())
}

#[actix_rt::test]
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
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "horse");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform the first backup
    let performer = PerformerImpl::new();
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
    let sut = RestorerImpl::new();
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
