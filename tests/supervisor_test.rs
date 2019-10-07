//
// Copyright (c) 2019 Nathan Fiedler
//
mod util;

use failure::Error;
use std::path::Path;
use std::time::{Duration, SystemTime};
use util::DBPath;
use zorigami::core::*;
use zorigami::database::*;
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
    dataset.schedule = Some("@daily".to_owned());
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
    dataset.schedule = Some("@daily".to_owned());
    dbase.put_dataset(&dataset)?;

    // indicate that the dataset is already running a backup
    state::dispatch(Action::StartBackup(dataset.key.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}

#[test]
fn test_backup_not_due() -> Result<(), Error> {
    let db_path = DBPath::new("_test_backup_not_due");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that finished just now
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha);
    let end_time = SystemTime::now();
    snapshot = snapshot.end_time(end_time);
    let sha1 = snapshot.checksum();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedule = Some("@daily".to_owned());
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
    let mut snapshot = Snapshot::new(None, tree_sha);
    let day_ago = Duration::new(90_000, 0);
    let end_time = SystemTime::now() - day_ago;
    snapshot = snapshot.end_time(end_time);
    let sha1 = snapshot.checksum();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedule = Some("@daily".to_owned());
    dataset.latest_snapshot = Some(sha1);
    dbase.put_dataset(&dataset)?;

    assert_eq!(should_run(&dbase, &dataset)?, true);
    Ok(())
}

#[test]
fn test_overdue_backup_running() -> Result<(), Error> {
    let db_path = DBPath::new("_test_overdue_backup_running");
    let dbase = Database::new(&db_path).unwrap();

    // build a "latest" snapshot that finished a while ago
    let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
    let mut snapshot = Snapshot::new(None, tree_sha);
    let day_ago = Duration::new(90_000, 0);
    let end_time = SystemTime::now() - day_ago;
    snapshot = snapshot.end_time(end_time);
    let sha1 = snapshot.checksum();
    dbase.insert_snapshot(&sha1, &snapshot)?;

    let unique_id = generate_unique_id("charlie", "localhost");
    let basepath = Path::new("/some/path");
    let store = "store/local/stuff";
    let mut dataset = Dataset::new(&unique_id, basepath, store);
    dataset.schedule = Some("@daily".to_owned());
    dataset.latest_snapshot = Some(sha1.clone());
    dbase.put_dataset(&dataset)?;

    // indicate that the dataset is already running a backup
    state::dispatch(Action::StartBackup(dataset.key.clone()));

    assert_eq!(should_run(&dbase, &dataset)?, false);
    Ok(())
}