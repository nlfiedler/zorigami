//
// Copyright (c) 2024 Nathan Fiedler
//
use anyhow::Error;
use dotenv::dotenv;
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::EntityDataSourceImpl;
use server::domain::entities::schedule::Schedule;
use server::domain::entities::{self, Checksum};
use server::domain::managers::backup::{Performer, PerformerImpl, Scheduler, SchedulerImpl};
use server::domain::managers::restore::*;
use server::domain::managers::state::{BackupAction, StateStore, StateStoreImpl};
use server::domain::repositories::RecordRepository;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn file_restorer_factory(dbase: Arc<dyn RecordRepository>) -> Box<dyn FileRestorer> {
    Box::new(FileRestorerImpl::new(dbase))
}

//
// Test the full backup with a pack store that uses async calls.
//
#[actix_rt::test]
async fn test_process_manager_async_store() -> Result<(), Error> {
    // set up the environment and remote connection
    dotenv().ok();
    let endp_var = env::var("MINIO_ENDPOINT");
    if endp_var.is_err() {
        // bail out silently if minio is not available
        return Ok(());
    }
    let minio_endpoint = endp_var?;
    let minio_region = env::var("MINIO_REGION")?;
    let minio_access_key = env::var("MINIO_ACCESS_KEY_1")?;
    let minio_secret_key = env::var("MINIO_SECRET_KEY_1")?;

    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let db_path = tempfile::tempdir_in(&db_base)?;
    let datasource = EntityDataSourceImpl::new(db_path.path())?;
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Arc<dyn RecordRepository> = Arc::new(repo);

    // set up minio pack store
    let mut minio_props: HashMap<String, String> = HashMap::new();
    minio_props.insert("region".to_owned(), minio_region);
    minio_props.insert("endpoint".to_owned(), minio_endpoint);
    minio_props.insert("access_key".to_owned(), minio_access_key);
    minio_props.insert("secret_key".to_owned(), minio_secret_key);
    let store = entities::Store {
        id: "minio123".to_owned(),
        store_type: entities::StoreType::MINIO,
        label: "s3clone".to_owned(),
        properties: minio_props,
    };
    dbase.put_store(&store)?;

    // create a dataset
    let fixture_base: PathBuf = ["tmp", "test", "fixtures"].iter().collect();
    fs::create_dir_all(&fixture_base)?;
    let fixture_path = tempfile::tempdir_in(&fixture_base)?;
    let mut dataset = entities::Dataset::new(fixture_path.path());
    dataset.add_store("minio123");
    dataset.add_schedule(Schedule::Hourly);
    dbase.put_dataset(&dataset)?;
    let computer_id = entities::Configuration::generate_unique_id("charlie", "homebase");
    dbase.put_computer_id(&dataset.id, &computer_id)?;

    // perform a single backup
    let dest: PathBuf = fixture_path.path().join("lorem-ipsum.txt");
    assert!(fs::copy("../test/fixtures/lorem-ipsum.txt", dest).is_ok());
    let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
    let performer: Arc<dyn Performer> = Arc::new(PerformerImpl::default());
    let processor = SchedulerImpl::new(state.clone(), performer).interval(100);
    let result = processor.start(dbase.clone());
    assert!(result.is_ok());
    // n.b. If the tests seem to be hanging here, check that the store_minio
    // tests are passing, there could be an issue with the access keys; be sure
    // to define new access keys if the minio docker container is rebuilt.
    state.wait_for_backup(BackupAction::Finish(dataset.id.clone()));

    // restore a file from backup
    let maybe_snapshot = dbase.get_latest_snapshot(&dataset.id)?;
    assert!(maybe_snapshot.is_some(), "latest snapshot not available");
    let snapshot_sha1 = maybe_snapshot.unwrap();
    #[cfg(target_family = "unix")]
    let digest_expected = Checksum::BLAKE3(String::from(
        "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128",
    ));
    #[cfg(target_family = "windows")]
    let digest_expected = Checksum::BLAKE3(String::from(
        "2720a91db93dae2a92ed9f74b0f7a135cfdf4d32dd069477cda457002ffc9e7a",
    ));
    let snapshot = dbase.get_snapshot(&snapshot_sha1)?.unwrap();
    let restorer = RestorerImpl::new(state, file_restorer_factory);
    let result = restorer.start(dbase.clone());
    assert!(result.is_ok());
    let result = restorer.enqueue(Request::new(
        snapshot.tree,
        String::from("lorem-ipsum.txt"),
        PathBuf::from("restored.bin"),
        dataset.id.to_owned(),
        "keyboard cat".into(),
    ));
    assert!(result.is_ok());
    restorer.wait_for_completed();
    let requests = restorer.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.error_msg.is_none());
    let outfile: PathBuf = fixture_path.path().join("restored.bin");
    assert!(outfile.exists());
    let digest_actual = Checksum::blake3_from_file(&outfile)?;
    assert_eq!(digest_expected, digest_actual);

    // Ideally would iterate pack records in database and delete the pack files
    // from minio, but eventually the store_minio tests will run and clean up
    // everything anyway.

    // shutdown the restorer supervisor to release the database lock
    let result = restorer.stop();
    assert!(result.is_ok());
    actix::System::current().stop();
    Ok(())
}
