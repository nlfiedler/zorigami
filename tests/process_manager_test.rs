//
// Copyright (c) 2020 Nathan Fiedler
//
mod common;

use chrono::prelude::*;
use chrono::Duration;
use common::DBPath;
use failure::Error;
use std::path::Path;
use std::sync::Arc;
use zorigami::data::repositories::RecordRepositoryImpl;
use zorigami::data::sources::EntityDataSourceImpl;
use zorigami::domain::entities::{schedule, Checksum, Dataset, Snapshot};
use zorigami::domain::managers::process::*;
use zorigami::domain::managers::state::{self, Action};
use zorigami::domain::repositories::RecordRepository;

#[test]
fn test_dataset_no_schedule() -> Result<(), Error> {
    let db_path = DBPath::new("_test_dataset_no_schedule");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_first_backup_due() -> Result<(), Error> {
    let db_path = DBPath::new("_test_first_backup_due");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    // let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    // let store = "store/local/stuff";
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;

    assert_eq!(should_run(&dbase, &dataset)?, true);
    Ok(())
}

#[test]
fn test_first_backup_running() -> Result<(), Error> {
    let db_path = DBPath::new("_test_first_backup_running");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;

    // indicate that the dataset is already running a backup
    state::dispatch(Action::StartBackup(dataset.id.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_backup_not_overdue() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_not_overdue");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    // build a "latest" snapshot that finished just now
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let end_time = Utc::now();
    snapshot = snapshot.end_time(end_time);
    dbase.put_snapshot(&snapshot)?;

    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;
    dbase.put_latest_snapshot(&dataset.id, &snapshot.digest)?;

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_backup_overdue() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_overdue");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    // build a "latest" snapshot that finished a while ago
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let day_ago = Duration::hours(25);
    let end_time = Utc::now() - day_ago;
    snapshot = snapshot.end_time(end_time);
    dbase.put_snapshot(&snapshot)?;

    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;
    dbase.put_latest_snapshot(&dataset.id, &snapshot.digest)?;

    assert_eq!(should_run(&dbase, &dataset)?, true);
    Ok(())
}

#[test]
fn test_old_snapshot_recent_backup() -> Result<(), Error> {
    let db_path = DBPath::new("_test_old_snapshot_recent_backup");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    // build a "latest" snapshot that finished a while ago
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let day_ago = Duration::hours(25);
    let end_time = Utc::now() - day_ago;
    snapshot = snapshot.end_time(end_time);
    dbase.put_snapshot(&snapshot)?;

    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;
    dbase.put_latest_snapshot(&dataset.id, &snapshot.digest)?;

    // insert a backup state that finished recently, which is the case when
    // there were no file changes, but the backup "finished" nonetheless
    state::dispatch(Action::StartBackup(dataset.id.clone()));
    state::dispatch(Action::FinishBackup(dataset.id.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_backup_restarted() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_restarted");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    // build a "latest" snapshot that did not finish
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let snapshot = Snapshot::new(None, tree_sha, 0);
    dbase.put_snapshot(&snapshot)?;

    // create the dataset with a schedule
    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;
    dbase.put_latest_snapshot(&dataset.id, &snapshot.digest)?;

    // and the app restarted, so there is no state either
    assert!(state::get_state().backups(&dataset.id).is_none());

    // the backup should run, and there is now state
    assert_eq!(should_run(&dbase, &dataset)?, true);
    assert!(state::get_state().backups(&dataset.id).is_some());
    Ok(())
}

#[test]
fn test_backup_restarted_not_overdue() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_restarted_not_overdue");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    // build a "latest" snapshot that finished just now
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let end_time = Utc::now();
    snapshot = snapshot.end_time(end_time);
    dbase.put_snapshot(&snapshot)?;

    // create the dataset with a schedule
    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;
    dbase.put_latest_snapshot(&dataset.id, &snapshot.digest)?;

    // and the app restarted, so there is no state either
    assert!(state::get_state().backups(&dataset.id).is_none());

    // the backup should not run and there is still no state
    assert_eq!(should_run(&dbase, &dataset)?, false);
    assert!(state::get_state().backups(&dataset.id).is_none());
    Ok(())
}

#[test]
fn test_overdue_backup_running() -> Result<(), Error> {
    let db_path = DBPath::new("_test_overdue_backup_running");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    // build a "latest" snapshot that finished a while ago
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let day_ago = Duration::hours(25);
    let end_time = Utc::now() - day_ago;
    snapshot = snapshot.end_time(end_time);
    dbase.put_snapshot(&snapshot)?;

    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;
    dbase.put_latest_snapshot(&dataset.id, &snapshot.digest)?;

    // indicate that the dataset is already running a backup
    state::dispatch(Action::StartBackup(dataset.id.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_overdue_had_error() -> Result<(), Error> {
    let db_path = DBPath::new("_test_overdue_had_error");
    let datasource = EntityDataSourceImpl::new(&db_path).unwrap();
    let repo = RecordRepositoryImpl::new(Arc::new(datasource));
    let dbase: Box<dyn RecordRepository> = Box::new(repo);

    // build a "latest" snapshot that started just now
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let snapshot = Snapshot::new(None, tree_sha, 0);
    dbase.put_snapshot(&snapshot)?;

    let basepath = Path::new("/some/path");
    let mut dataset = Dataset::new(basepath);
    dataset = dataset.add_schedule(schedule::Schedule::Daily(None));
    dataset = dataset.add_store("local123");
    dbase.put_dataset(&dataset)?;
    dbase.put_latest_snapshot(&dataset.id, &snapshot.digest)?;

    // indicate that the backup started but then failed
    state::dispatch(Action::StartBackup(dataset.id.clone()));
    state::dispatch(Action::ErrorBackup(dataset.id.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, true);
    Ok(())
}
