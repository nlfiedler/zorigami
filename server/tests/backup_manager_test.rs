//
// Copyright (c) 2020 Nathan Fiedler
//
use anyhow::Error;
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::EntityDataSourceImpl;
use server::domain::entities::{self, PackRetention};
use server::domain::managers::backup::{OutOfTimeFailure, Performer, PerformerImpl, Request};
use server::domain::managers::state::{StateStore, StateStoreImpl};
use server::domain::repositories::RecordRepository;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

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
    dataset.pack_size = 131072 as u64;
    let dataset_id = dataset.id.clone();

    // perform the first backup
    let performer = PerformerImpl::default();
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let passphrase = String::from("keyboard cat");
    let request = Request::new(dataset, dbase.clone(), state.clone(), &passphrase, None);
    let dest: PathBuf = fixture_path.path().join("SekienAkashita.jpg");
    assert!(fs::copy("../test/fixtures/SekienAkashita.jpg", &dest).is_ok());
    let backup_opt = performer.backup(request)?;
    assert!(backup_opt.is_some());
    let first_sha1 = backup_opt.unwrap();
    let dataset = dbase.get_dataset(&dataset_id)?.unwrap();
    assert_eq!(dataset.snapshot, Some(first_sha1.clone()));

    // fake an incomplete backup by resetting the end_time field
    let mut snapshot = dbase.get_snapshot(&first_sha1)?.unwrap();
    snapshot.end_time = None;
    dbase.put_snapshot(&snapshot)?;

    // run the backup again to make sure it is finished
    let request = Request::new(dataset, dbase.clone(), state.clone(), &passphrase, None);
    let backup_opt = performer.backup(request)?;
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
    dataset.pack_size = 65536 as u64;

    // start the backup manager after the stop time has already passed; should
    // return an "out of time" error
    let performer = PerformerImpl::default();
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
    dataset.pack_size = 65536 as u64;

    // perform a backup where there is only an empty file
    let performer = PerformerImpl::default();
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
