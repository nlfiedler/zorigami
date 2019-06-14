//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `supervisor` module spawns threads to perform backups, ensuring backups
//! are performed for each dataset according to a schedule.
//!
//! This module assumes that `std::time::SystemTime` is UTC, which seems to be
//! the case, but is not mentioned in the documentation.

use super::core::Snapshot;
use super::database::Database;
use super::engine;
use super::state;
use chrono::prelude::*;
use cron::Schedule;
use failure::{err_msg, Error};
use std::cmp::Ordering;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

///
/// Spawn a thread to monitor the datasets, ensuring that backups are started
/// according to the assigned schedules. If a dataset does not have a schedule
/// then it is not run automatically by this process.
///
pub fn start(db_path: PathBuf) -> Result<(), Error> {
    let dbase = Database::new(&db_path)?;
    thread::spawn(move || {
        // unexpected errors (e.g. database) here will panic the thread;
        // i.e. database needs to be working, snapshots need to exist, etc
        // let it crash and the user will debug whatever is going on
        loop {
            // look for datasets that should be running
            let datasets = dbase.get_all_datasets().unwrap();
            for set in datasets {
                // only consider those datasets that have a schedule, otherwise
                // the user is expected to manually start the backup process
                if let Some(schedule) = set.schedule.as_ref() {
                    let mut maybe_run = if let Some(checksum) = set.latest_snapshot.as_ref() {
                        let snapshot = dbase.get_snapshot(checksum).unwrap().unwrap();
                        should_run(schedule, &snapshot).unwrap()
                    } else {
                        true
                    };
                    if maybe_run {
                        // check if backup is already running
                        let redux = state::get_state();
                        if let Some(backup) = redux.backups(&set.key) {
                            if backup.end_time().is_none() {
                                maybe_run = false;
                            }
                        }
                    }
                    if maybe_run {
                        // passed all the checks, we can start this dataset
                        run_dataset(db_path.clone(), set.key).unwrap();
                    }
                }
            }
            // * spawn a thread to run the backup for that dataset
            // sleep for 5 minutes before trying again
            thread::sleep(Duration::from_millis(300_000));
        }
    });
    Ok(())
}

///
/// Determine if the snapshot finished a sufficiently long time ago to warrant
/// running a backup now.
///
#[allow(dead_code)]
fn should_run(schedule: &str, snapshot: &Snapshot) -> Result<bool, Error> {
    if snapshot.end_time.is_some() {
        let end_time = DateTime::<Utc>::from(snapshot.end_time.unwrap());
        let result = Schedule::from_str(schedule);
        if result.is_err() {
            return Err(err_msg("schedule expression could not be parsed"));
        }
        let sched = result.unwrap();
        let mut events = sched.after(&end_time);
        let utc_now = Utc::now();
        return Ok(events.next().unwrap().cmp(&utc_now) == Ordering::Less);
    }
    Ok(false)
}

///
/// Run the backup procedure for the named dataset. Takes the passphrase from
/// the environment. Any errors occurring within the spawned thread will result
/// in a panic.
///
fn run_dataset(db_path: PathBuf, set_key: String) -> Result<(), Error> {
    let dbase = Database::new(&db_path)?;
    let passphrase = env::var("PASSPHRASE").unwrap_or_else(|_| "keyboard cat".to_owned());
    thread::spawn(move || {
        let mut dataset = dbase.get_dataset(&set_key).unwrap().unwrap();
        let _ = engine::perform_backup(&mut dataset, &dbase, &passphrase).unwrap();
        // the perform_backup() has done everything, we can quietly die now
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;
    use super::*;
    use crate::core::*;

    #[test]
    fn test_should_run_hourly() {
        let expression = "@hourly";
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha);
        //
        // test with no end time for latest snapshot, should not run
        //
        let result = should_run(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        //
        // test with a time that should fire
        //
        let hour_ago = Duration::new(3600, 0);
        let end_time = SystemTime::now() - hour_ago;
        snapshot = snapshot.end_time(end_time);
        let result = should_run(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        //
        // test with a time that should not fire
        //
        let end_time = SystemTime::now();
        snapshot = snapshot.end_time(end_time);
        let result = should_run(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_should_run_daily() {
        let expression = "@daily";
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha);
        //
        // test with no end time for latest snapshot, should not run
        //
        let result = should_run(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        //
        // test with a date that should fire
        //
        let day_ago = Duration::new(90_000, 0);
        let end_time = SystemTime::now() - day_ago;
        snapshot = snapshot.end_time(end_time);
        let result = should_run(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        //
        // test with a date that should not fire
        //
        let end_time = SystemTime::now();
        snapshot = snapshot.end_time(end_time);
        let result = should_run(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }
}
