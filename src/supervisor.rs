//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `supervisor` module spawns threads to perform backups, ensuring backups
//! are performed for each dataset according to a schedule.
//!
//! This module assumes that `std::time::SystemTime` is UTC, which seems to be
//! the case, but is not mentioned in the documentation.

use super::core::{Dataset, Snapshot};
use super::database::Database;
use super::engine;
use super::state;
use chrono::prelude::*;
use cron::Schedule;
use failure::{err_msg, Error};
use log::{debug, error, info};
use std::cmp::Ordering;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread;
use std::time::{Duration, SystemTime};

///
/// Spawn a thread to monitor the datasets, ensuring that backups are started
/// according to the assigned schedules. If a dataset does not have a schedule
/// then it is not run automatically by this process.
///
pub fn start(db_path: PathBuf) -> Result<(), Error> {
    let dbase = Database::new(&db_path)?;
    thread::spawn(move || {
        loop {
            // sleep for 5 minutes before starting so it gives me a chance
            // to change the configuration without having to wipe the
            // database first
            thread::sleep(Duration::from_millis(300_000));
            // look for datasets that should be running, spawning a thread to
            // run the backup for any waiting datasets
            match dbase.get_all_datasets() {
                Ok(datasets) => {
                    for set in datasets {
                        match should_run(&dbase, &set) {
                            Ok(true) => {
                                // passed all the checks, we can start this dataset
                                if let Err(err) = run_dataset(db_path.clone(), set.key.clone()) {
                                    error!("error running backup for {}: {}", &set, err);
                                }
                            }
                            Ok(false) => (),
                            Err(err) => error!("error while checking schedule: {}", err),
                        }
                    }
                }
                Err(err) => error!("failed to retrieve datasets: {}", err),
            }
        }
    });
    Ok(())
}

///
/// Check if the given dataset should be processed now.
///
pub fn should_run(dbase: &Database, set: &Dataset) -> Result<bool, Error> {
    // public function so it can be tested from an external crate

    // only consider those datasets that have a schedule, otherwise
    // the user is expected to manually start the backup process
    if let Some(schedule) = set.schedule.as_ref() {
        let mut maybe_run = if let Some(checksum) = set.latest_snapshot.as_ref() {
            let snapshot = dbase
                .get_snapshot(checksum)?
                .ok_or_else(|| err_msg(format!("snapshot {} missing from database", &checksum)))?;
            is_overdue(schedule, &snapshot)?
        } else {
            true
        };
        if maybe_run {
            // check if a backup is already running
            let redux = state::get_state();
            if let Some(backup) = redux.backups(&set.key) {
                if backup.end_time().is_none() {
                    maybe_run = false;
                    debug!("dataset {} backup already in progress", &set.key);
                }
            }
        }
        Ok(maybe_run)
    } else {
        Ok(false)
    }
}

///
/// Determine if the snapshot finished a sufficiently long time ago to warrant
/// running a backup now.
///
fn is_overdue(schedule: &str, snapshot: &Snapshot) -> Result<bool, Error> {
    if let Some(et) = snapshot.end_time {
        let end_time = DateTime::<Utc>::from(et);
        // cannot use ? because the error type is not thread-safe
        if let Ok(sched) = Schedule::from_str(schedule) {
            let mut events = sched.after(&end_time);
            let utc_now = Utc::now();
            let next = events
                .next()
                .ok_or_else(|| err_msg("scheduled event had no 'next'?"))?;
            return Ok(next.cmp(&utc_now) == Ordering::Less);
        } else {
            return Err(err_msg("schedule expression could not be parsed"));
        }
    }
    Ok(false)
}

///
/// Run the backup procedure for the named dataset. Takes the passphrase from
/// the environment.
///
fn run_dataset(db_path: PathBuf, set_key: String) -> Result<(), Error> {
    let dbase = Database::new(&db_path)?;
    let passphrase = env::var("PASSPHRASE").unwrap_or_else(|_| "keyboard cat".to_owned());
    thread::spawn(move || {
        info!("dataset {} to be backed up", &set_key);
        match dbase.get_dataset(&set_key) {
            Ok(Some(mut dataset)) => {
                let start_time = SystemTime::now();
                match engine::perform_backup(&mut dataset, &dbase, &passphrase) {
                    Ok(Some(checksum)) => {
                        let end_time = SystemTime::now();
                        let time_diff = end_time.duration_since(start_time);
                        let pretty_time = engine::pretty_print_duration(time_diff);
                        info!("created new snapshot {}", &checksum);
                        info!("dataset {} backup complete after {}", &set_key, pretty_time);
                    }
                    Ok(None) => info!("no new snapshot required"),
                    Err(err) => error!("could not perform backup: {}", err),
                }
            }
            Ok(None) => error!("dataset {} missing from database", &set_key),
            Err(err) => error!("could not retrieve dataset {}: {}", &set_key, err),
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::*;
    use std::time::SystemTime;

    #[test]
    fn test_is_overdue_hourly() {
        let expression = "@hourly";
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha);
        //
        // test with no end time for latest snapshot, should not run
        //
        let result = is_overdue(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        //
        // test with a time that should fire
        //
        let hour_ago = Duration::new(3600, 0);
        let end_time = SystemTime::now() - hour_ago;
        snapshot = snapshot.end_time(end_time);
        let result = is_overdue(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        //
        // test with a time that should not fire
        //
        let end_time = SystemTime::now();
        snapshot = snapshot.end_time(end_time);
        let result = is_overdue(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_is_overdue_daily() {
        let expression = "@daily";
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha);
        //
        // test with no end time for latest snapshot, should not run
        //
        let result = is_overdue(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        //
        // test with a date that should fire
        //
        let day_ago = Duration::new(90_000, 0);
        let end_time = SystemTime::now() - day_ago;
        snapshot = snapshot.end_time(end_time);
        let result = is_overdue(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        //
        // test with a date that should not fire
        //
        let end_time = SystemTime::now();
        snapshot = snapshot.end_time(end_time);
        let result = is_overdue(expression, &snapshot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }
}
