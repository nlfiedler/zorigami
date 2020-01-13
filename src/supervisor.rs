//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `supervisor` module spawns threads to perform backups, ensuring backups
//! are performed for each dataset according to a schedule.

use super::core::{self, Dataset};
use super::database::Database;
use super::engine;
use super::state::{self, Action};
use actix::prelude::*;
use chrono::prelude::*;
use failure::{err_msg, Error};
use lazy_static::lazy_static;
use log::{debug, error, info, trace, warn};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, SystemTime};

#[derive(Message)]
struct Start(PathBuf);

#[derive(Default)]
struct MySupervisor {
    dbase: Option<Database>,
}

impl Actor for MySupervisor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("supervisor actor started");
        ctx.run_interval(Duration::from_millis(300_000), |this, _ctx| {
            trace!("supervisor interval fired");
            if let Some(dbase) = this.dbase.as_ref() {
                if let Err(err) = start_due_datasets(dbase) {
                    error!("failed to check datasets: {}", err);
                }
            } else {
                warn!("supervisor does not yet have a database path");
            }
        });
    }
}

impl Supervised for MySupervisor {
    fn restarting(&mut self, _ctx: &mut Context<MySupervisor>) {
        debug!("supervisor actor restarting");
    }
}

impl Handler<Start> for MySupervisor {
    type Result = ();

    fn handle(&mut self, msg: Start, _ctx: &mut Context<MySupervisor>) {
        debug!("supervisor received start message");
        //
        // Open the database and keep it open to avoid any subtle timing issues
        // with the supervisor and arbiter threads opening and closing databases
        // at the same time. Would occasionally fail to get the exclusive lock
        // when opening the database.
        //
        // Also, opening the database repeatedly seems to create many log files.
        //
        match Database::new(msg.0) {
            Ok(dbase) => self.dbase = Some(dbase),
            Err(err) => error!("could not open database: {}", err),
        }
    }
}

#[derive(Message)]
struct StartBackup {
    db_path: PathBuf,
    dataset: String,
}

struct Runner;

impl Actor for Runner {
    type Context = Context<Self>;
}

impl Handler<StartBackup> for Runner {
    type Result = ();

    fn handle(&mut self, msg: StartBackup, ctx: &mut Context<Runner>) {
        debug!("runner received backup message");
        run_dataset(msg.db_path, msg.dataset.clone());
        debug!("runner finished backup");
        ctx.stop();
    }
}

lazy_static! {
    // Address of our supervised actor. Need to keep this around lest our actor
    // be terminated prematurely by the actix runtime.
    static ref MY_SUPER: Addr<MySupervisor> = {
        actix::Supervisor::start(|_| Default::default())
    };
    // Arbiter manages the runner actors that perform the backups. This will
    // manage an event loop on a single thread. To allow for concurrent tasks
    // use the SyncArbiter, which manages a single type of actor but runs them
    // on multiple threads.
    static ref MY_RUNNER: Arbiter = { Arbiter::new() };
}

///
/// Spawn a thread to monitor the datasets, ensuring that backups are started
/// according to the assigned schedules. If a dataset does not have a schedule
/// then it is not run automatically by this process.
///
pub fn start(db_path: PathBuf) -> std::io::Result<()> {
    thread::spawn(move || {
        let system = System::new("backup-supervisor");
        // must send a message to get the system to run our actor
        if let Err(err) = MY_SUPER.try_send(Start(db_path)) {
            error!("error sending message to supervisor: {}", err);
        }
        system.run()
    });
    Ok(())
}

///
/// Begin the backup process for all datasets that are ready to run.
///
fn start_due_datasets(dbase: &Database) -> Result<(), Error> {
    let datasets = dbase.get_all_datasets()?;
    for set in datasets {
        if should_run(&dbase, &set)? {
            let msg = StartBackup {
                db_path: dbase.get_path().to_owned(),
                dataset: set.key.clone(),
            };
            let addr = Actor::start_in_arbiter(&MY_RUNNER, |_| Runner {});
            if let Err(err) = addr.try_send(msg) {
                error!("error sending message to runner: {}", err);
            }
        }
    }
    Ok(())
}

///
/// Check if the given dataset should be processed now.
///
pub fn should_run(dbase: &Database, set: &Dataset) -> Result<bool, Error> {
    // public function so it can be tested from an external crate

    // only consider those datasets that have a schedule, otherwise
    // the user is expected to manually start the backup process
    if !set.schedules.is_empty() {
        let end_time: Option<DateTime<Utc>> = if let Some(checksum) = set.latest_snapshot.as_ref() {
            let snapshot = dbase
                .get_snapshot(checksum)?
                .ok_or_else(|| err_msg(format!("snapshot {} missing from database", &checksum)))?;
            snapshot.end_time
        } else {
            None
        };
        // consider each schedule until one is found that should start now
        for schedule in set.schedules.iter() {
            // check if backup overdue
            let mut maybe_run = if let Some(et) = end_time {
                schedule.is_ready(et)
            } else {
                true
            };
            // Check if a backup is still running or had an error; if it had an
            // error, definitely try running again; if it is still running, then do
            // nothing for now.
            //
            // Also consider recently finished backups where there were no changes
            // in which case we clear the overdue flag.
            let redux = state::get_state();
            if let Some(backup) = redux.backups(&set.key) {
                if backup.had_error() {
                    maybe_run = true;
                    debug!("dataset {} backup had an error, will restart", &set.key);
                } else if let Some(et) = backup.end_time() {
                    if !schedule.is_ready(et) {
                        maybe_run = false;
                    }
                } else {
                    maybe_run = false;
                    debug!("dataset {} backup already in progress", &set.key);
                }
            } else if maybe_run {
                // kickstart the application state when it appears that our
                // application has restarted while a backup was in progress
                debug!("reset missing backup state to start");
                state::dispatch(Action::StartBackup(set.key.clone()));
            }
            if maybe_run {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

///
/// Run the backup procedure for the named dataset. Takes the passphrase from
/// the environment.
///
fn run_dataset(db_path: PathBuf, set_key: String) {
    match Database::new(&db_path) {
        Ok(dbase) => {
            let passphrase = core::get_passphrase();
            info!("dataset {} to be backed up", &set_key);
            match dbase.get_dataset(&set_key) {
                Ok(Some(mut dataset)) => {
                    let start_time = SystemTime::now();
                    // reset any error state in the backup
                    state::dispatch(Action::RestartBackup(set_key.clone()));
                    match engine::perform_backup(&mut dataset, &dbase, &passphrase) {
                        Ok(Some(checksum)) => {
                            let end_time = SystemTime::now();
                            let time_diff = end_time.duration_since(start_time);
                            let pretty_time = engine::pretty_print_duration(time_diff);
                            info!("created new snapshot {}", &checksum);
                            info!("dataset {} backup complete after {}", &set_key, pretty_time);
                        }
                        Ok(None) => info!("no new snapshot required"),
                        Err(err) => match err.downcast::<engine::OutOfTimeError>() {
                            Ok(_) => {
                                info!("backup window has reached its end");
                                // put the backup in the error state so we try again
                                state::dispatch(Action::ErrorBackup(set_key.clone()));
                            }
                            Err(err) => {
                                // here `err` is the original error
                                error!("could not perform backup: {}", err);
                                // put the backup in the error state so we try again
                                state::dispatch(Action::ErrorBackup(set_key.clone()));
                            }
                        },
                    }
                }
                Ok(None) => error!("dataset {} missing from database", &set_key),
                Err(err) => error!("could not retrieve dataset {}: {}", &set_key, err),
            }
        }
        Err(err) => {
            error!("error opening database for {}: {}", &set_key, err);
            // put the backup in the error state so we try again
            state::dispatch(Action::ErrorBackup(set_key));
        }
    }
}
