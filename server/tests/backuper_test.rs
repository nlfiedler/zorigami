//
// Copyright (c) 2020 Nathan Fiedler
//
use anyhow::Error;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use zorigami::data::repositories::RecordRepositoryImpl;
use zorigami::data::sources::EntityDataSourceImpl;
use zorigami::domain::entities::{self, PackRetention};
use zorigami::domain::repositories::RecordRepository;
use zorigami::tasks::backup::{Backuper, BackuperImpl, OutOfTimeFailure, Request, Subscriber};

struct DummySubscriber();

impl Subscriber for DummySubscriber {
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
    let dataset_id = dataset.id.clone();
    dbase.put_dataset(&dataset)?;

    // perform the first backup
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummySubscriber());
    let backuper = BackuperImpl::new(dbase.clone(), subscriber, stopper);
    let passphrase = String::from("keyboard cat");
    let request = Request::new(dataset_id.clone(), &passphrase, None);
    let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let backup_opt = backuper.backup(request)?;
    assert!(backup_opt.is_some());
    let first_sha1 = backup_opt.unwrap();
    let dataset = dbase.get_dataset(&dataset_id)?.unwrap();
    assert_eq!(dataset.snapshot, Some(first_sha1.clone()));

    // fake an incomplete backup by resetting the end_time field
    let mut snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    snapshot.end_time = None;
    dbase.put_snapshot(&snapshot)?;

    // run the backup again to make sure it is finished
    let request = Request::new(dataset_id.clone(), &passphrase, None);
    let backup_opt = backuper.backup(request)?;
    assert!(backup_opt.is_some());
    let second_sha1 = backup_opt.unwrap();
    let dataset = dbase.get_dataset(&dataset_id)?.unwrap();
    assert_eq!(dataset.snapshot, Some(first_sha1.clone()));
    assert_eq!(first_sha1, second_sha1);
    let snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    assert!(snapshot.end_time.is_some());

    // ensure the backup created the expected number of each record type
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 1);
    assert_eq!(counts.file, 1);
    assert_eq!(counts.chunk, 2);
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

    // start the backup manager after the stop time has already passed; should
    // return an "out of time" error
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummySubscriber());
    let backuper = BackuperImpl::new(dbase.clone(), subscriber, stopper);
    let passphrase = String::from("keyboard cat");
    let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let stop_time = Some(chrono::Utc::now() - chrono::Duration::minutes(5));
    let request = Request::new(dataset.id.clone(), &passphrase, stop_time);
    match backuper.backup(request) {
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

    // perform a backup where there is only an empty file
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummySubscriber());
    let backuper = BackuperImpl::new(dbase.clone(), subscriber, stopper);
    let passphrase = String::from("keyboard cat");
    let dest: PathBuf = fixture_path.path().join("zero-length.txt");
    assert!(fs::write(dest, vec![]).is_ok());
    let request = Request::new(dataset.id.clone(), &passphrase, None);
    let backup_opt = backuper.backup(request)?;
    // it did not blow up, so that counts as passing
    assert!(backup_opt.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 0);
    assert_eq!(counts.file, 0);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 1);
    Ok(())
}

#[actix_rt::test]
#[serial_test::serial]
async fn test_backup_no_changes() -> Result<(), Error> {
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
    let dataset_id = dataset.id.clone();

    // perform the first backup
    let stopper = Arc::new(RwLock::new(false));
    let subscriber = Arc::new(DummySubscriber());
    let backuper = BackuperImpl::new(dbase.clone(), subscriber, stopper);
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let passphrase = String::from("keyboard cat");
    let request = Request::new(dataset_id.clone(), &passphrase, None);
    let first_backup = backuper.backup(request)?;
    assert!(first_backup.is_some());
    let counts = dbase.get_entity_counts().unwrap();
    assert_eq!(counts.pack, 1);
    assert_eq!(counts.file, 1);
    assert_eq!(counts.chunk, 0);
    assert_eq!(counts.tree, 1);

    // perform the second backup in which nothing changed; need to fetch the
    // latest dataset record in order to get the updated snapshot value
    let request = Request::new(dataset_id.clone(), &passphrase, None);
    let second_backup = backuper.backup(request)?;
    assert!(second_backup.is_none());
    Ok(())
}
