//
// Copyright (c) 2020 Nathan Fiedler
//
mod util;

use chrono::prelude::*;
use chrono::Duration;
use failure::Error;
use std::path::Path;
use util::DBPath;
use zorigami::core::*;
use zorigami::database::*;
use zorigami::schedule::Schedule;
use zorigami::state::{self, Action};
use zorigami::supervisor::*;

#[test]
fn test_dataset_no_schedule() -> Result<(), Error> {
    let db_path = DBPath::new("_test_dataset_no_schedule");
    let dbase = Database::new(&db_path).unwrap();

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let dataset = Dataset::new(&unique_id, basepath, store);
    dbase.put_dataset(&dataset)?;

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_first_backup_due() -> Result<(), Error> {
    let db_path = DBPath::new("_test_first_backup_due");
    let dbase = Database::new(&db_path).unwrap();

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dbase.put_dataset(&dataset)?;

    assert_eq!(should_run(&dbase, &dataset)?, true);
    Ok(())
}

#[test]
fn test_first_backup_running() -> Result<(), Error> {
    let db_path = DBPath::new("_test_first_backup_running");
    let dbase = Database::new(&db_path).unwrap();

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dbase.put_dataset(&dataset)?;

    // indicate that the dataset is already running a backup
    state::dispatch(Action::StartBackup(dataset.key.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_backup_not_overdue() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_not_overdue");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that finished just now
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let end_time = Utc::now();
    snapshot = snapshot.end_time(end_time);
    let sha1 = snapshot.digest.clone();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dataset.latest_snapshot = Some(sha1);
    dbase.put_dataset(&dataset)?;

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_backup_overdue() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_overdue");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that finished a while ago
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let day_ago = Duration::hours(25);
    let end_time = Utc::now() - day_ago;
    snapshot = snapshot.end_time(end_time);
    let sha1 = snapshot.digest.clone();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dataset.latest_snapshot = Some(sha1);
    dbase.put_dataset(&dataset)?;

    assert_eq!(should_run(&dbase, &dataset)?, true);
    Ok(())
}

#[test]
fn test_old_snapshot_recent_backup() -> Result<(), Error> {
    let db_path = DBPath::new("_test_old_snapshot_recent_backup");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that finished a while ago
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let day_ago = Duration::hours(25);
    let end_time = Utc::now() - day_ago;
    snapshot = snapshot.end_time(end_time);
    let sha1 = snapshot.digest.clone();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dataset.latest_snapshot = Some(sha1);
    dbase.put_dataset(&dataset)?;

    // insert a backup state that finished recently, which is the case when
    // there were no file changes, but the backup "finished" nonetheless
    state::dispatch(Action::StartBackup(dataset.key.clone()));
    state::dispatch(Action::FinishBackup(dataset.key.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_backup_restarted() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_restarted");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that did not finish
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let snapshot = Snapshot::new(None, tree_sha, 0);
    let sha1 = snapshot.digest.clone();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    // create the dataset with a schedule
    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dataset.latest_snapshot = Some(sha1);
    dbase.put_dataset(&dataset)?;

    // and the app restarted, so there is no state either
    assert!(state::get_state().backups(&dataset.key).is_none());

    // the backup should run, and there is now state
    assert_eq!(should_run(&dbase, &dataset)?, true);
    assert!(state::get_state().backups(&dataset.key).is_some());
    Ok(())
}

#[test]
fn test_backup_restarted_not_overdue() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_restarted_not_overdue");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that finished just now
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let end_time = Utc::now();
    snapshot = snapshot.end_time(end_time);
    let sha1 = snapshot.digest.clone();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    // create the dataset with a schedule
    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dataset.latest_snapshot = Some(sha1);
    dbase.put_dataset(&dataset)?;

    // and the app restarted, so there is no state either
    assert!(state::get_state().backups(&dataset.key).is_none());

    // the backup should not run and there is still no state
    assert_eq!(should_run(&dbase, &dataset)?, false);
    assert!(state::get_state().backups(&dataset.key).is_none());
    Ok(())
}

#[test]
fn test_overdue_backup_running() -> Result<(), Error> {
    let db_path = DBPath::new("_test_overdue_backup_running");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that finished a while ago
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha, 0);
    let day_ago = Duration::hours(25);
    let end_time = Utc::now() - day_ago;
    snapshot = snapshot.end_time(end_time);
    let sha1 = snapshot.digest.clone();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dataset.latest_snapshot = Some(sha1);
    dbase.put_dataset(&dataset)?;

    // indicate that the dataset is already running a backup
    state::dispatch(Action::StartBackup(dataset.key.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_overdue_had_error() -> Result<(), Error> {
    let db_path = DBPath::new("_test_overdue_had_error");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that started just now
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let snapshot = Snapshot::new(None, tree_sha, 0);
    let sha1 = snapshot.digest.clone();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedules = vec![Schedule::Daily(None)];
    dataset.latest_snapshot = Some(sha1);
    dbase.put_dataset(&dataset)?;

    // indicate that the backup started but then failed
    state::dispatch(Action::StartBackup(dataset.key.clone()));
    state::dispatch(Action::ErrorBackup(dataset.key.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, true);
    Ok(())
}
