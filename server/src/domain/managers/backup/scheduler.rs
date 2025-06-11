//
// Copyright (c) 2024 Nathan Fiedler
//

//! The `scheduler` module spawns threads to perform backups, ensuring backups
//! are performed for each dataset according to a schedule.

use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::Dataset;
use crate::domain::helpers::crypto;
use crate::domain::managers::backup::{OutOfTimeFailure, Performer, Request};
use crate::domain::managers::pretty_print_duration;
use crate::domain::managers::state::{BackupAction, StateStore, SupervisorAction};
use crate::domain::repositories::RecordRepository;
use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::prelude::*;
use log::{debug, error, info, warn, trace};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

///
/// `Scheduler` manages a supervised actor which in turn spawns actors to
/// process backups according to their schedules.
///
#[cfg_attr(test, automock)]
pub trait Scheduler: Send + Sync {
    /// Start a supervisor that will run scheduled backups.
    fn start(&self, repo: Arc<dyn RecordRepository>) -> Result<(), Error>;

    /// Start the backup for a given dataset immediately.
    fn start_backup(&self, dataset: Dataset) -> Result<(), Error>;

    /// Signal the supervisor to stop and release the database reference.
    fn stop(&self) -> Result<(), Error>;
}

///
/// Concrete implementation of `Scheduler` that uses the actix actor framework
/// to spawn threads and send messages to actors to manage the backups according
/// to their schedules.
///
pub struct SchedulerImpl {
    // Arbiter manages the supervised actor that initiates backups.
    runner: Arbiter,
    // Application state to be provided to supervisor and runners.
    state: Arc<dyn StateStore>,
    // Address of the supervisor actor, if it has been started.
    super_addr: Mutex<Option<Addr<BackupSupervisor>>>,
    // Sleep interval between checks for datasets ready to run.
    interval: u64,
    // Performs the dataset backup.
    performer: Arc<dyn Performer>,
}

#[cfg(test)]
static SUPERVISOR_INTERVAL: u64 = 100;
#[cfg(not(test))]
static SUPERVISOR_INTERVAL: u64 = 300_000;

impl SchedulerImpl {
    /// Construct a new instance of SchedulerImpl.
    pub fn new(state: Arc<dyn StateStore>, performer: Arc<dyn Performer>) -> Self {
        // create an Arbiter to manage an event loop on a new thread
        Self {
            runner: Arbiter::new(),
            state: state.clone(),
            super_addr: Mutex::new(None),
            interval: SUPERVISOR_INTERVAL,
            performer: performer.clone(),
        }
    }

    /// Set the interval in seconds by which the background thread will wake up
    /// and check if any datasets are ready to run.
    pub fn interval(mut self, interval: u64) -> Self {
        self.interval = interval;
        self
    }
}

impl Scheduler for SchedulerImpl {
    fn start(&self, repo: Arc<dyn RecordRepository>) -> Result<(), Error> {
        let mut su_addr = self.super_addr.lock().unwrap();
        if su_addr.is_none() {
            // start supervisor within the arbiter created earlier
            let state = self.state.clone();
            let interval = self.interval;
            let performer = self.performer.clone();
            let addr = actix::Supervisor::start_in_arbiter(&self.runner.handle(), move |_| {
                BackupSupervisor::new(repo, state, interval, performer)
            });
            *su_addr = Some(addr);
        }
        Ok(())
    }

    fn start_backup(&self, dataset: Dataset) -> Result<(), Error> {
        fn err_convert(err: SendError<Start>) -> Error {
            anyhow!(format!("SchedulerImpl.start_backup(): {:?}", err))
        }
        let su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.deref() {
            addr.try_send(Start { dataset }).map_err(err_convert)
        } else {
            warn!("supervisor not running, cannot start backup");
            Ok(())
        }
    }

    fn stop(&self) -> Result<(), Error> {
        fn err_convert(err: SendError<Stop>) -> Error {
            anyhow!(format!("SchedulerImpl.stop(): {:?}", err))
        }
        let mut su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.take() {
            addr.try_send(Stop()).map_err(err_convert)
        } else {
            warn!("supervisor not running, cannot stop backup");
            Ok(())
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Start {
    dataset: Dataset,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Stop();

//
// Supervised actor that starts another actor to perform backups according to
// the schedules defined by the user. Uses an interval timer to wake up
// periodically to check the database.
//
struct BackupSupervisor {
    // Database connection for querying datasets.
    dbase: Arc<dyn RecordRepository>,
    // Application state for signaling changes in backup status.
    state: Arc<dyn StateStore>,
    // Arbiter manages the runner actors that perform the backups.
    runner: Arbiter,
    // Sleep interval between checks for datasets ready to run.
    interval: u64,
    // Performs the dataset backup.
    performer: Arc<dyn Performer>,
}

impl BackupSupervisor {
    fn new(
        repo: Arc<dyn RecordRepository>,
        state: Arc<dyn StateStore>,
        interval: u64,
        performer: Arc<dyn Performer>,
    ) -> Self {
        // Create an Arbiter to manage an event loop on a new thread, keeping
        // the backup runners separate from the supervisor since they manage a
        // significant workload for an extended period of time.
        Self {
            dbase: repo,
            state,
            runner: Arbiter::new(),
            interval,
            performer,
        }
    }

    /// Begin the backup process for all datasets that are ready to run.
    fn start_due_datasets(&self) -> Result<(), Error> {
        let datasets = self.dbase.get_datasets()?;
        let state = self.state.clone();
        let performer = self.performer.clone();
        for set in datasets {
            if let Some(schedule) = should_run(&self.dbase, &state, &set)? {
                let msg = StartBackup {
                    dbase: self.dbase.clone(),
                    state: state.clone(),
                    dataset: set,
                    schedule,
                    performer: performer.clone(),
                };
                let addr = Actor::start_in_arbiter(&self.runner.handle(), |_| BackupRunner {});
                if let Err(err) = addr.try_send(msg) {
                    return Err(anyhow!(format!("error sending message to runner: {}", err)));
                }
            }
        }
        Ok(())
    }

    /// Begin the backup process for the given dataset if not already running.
    fn start_dataset_now(&self, dataset: Dataset) -> Result<(), Error> {
        let state = self.state.clone();
        let performer = self.performer.clone();
        if let Some(schedule) = can_run(&state, &dataset)? {
            let msg = StartBackup {
                dbase: self.dbase.clone(),
                state: state.clone(),
                dataset,
                schedule,
                performer: performer.clone(),
            };
            let addr = Actor::start_in_arbiter(&self.runner.handle(), |_| BackupRunner {});
            if let Err(err) = addr.try_send(msg) {
                return Err(anyhow!(format!("error sending message to runner: {}", err)));
            }
        }
        Ok(())
    }
}

impl Actor for BackupSupervisor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("supervisor started");
        self.state.supervisor_event(SupervisorAction::Started);
        ctx.run_interval(Duration::from_millis(self.interval), |this, _ctx| {
            trace!("supervisor interval fired");
            if let Err(err) = this.start_due_datasets() {
                error!("failed to check datasets: {}", err);
            }
        });
    }

    fn stopping(&mut self, _ctx: &mut Context<Self>) -> Running {
        debug!("supervisor stopping");
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        debug!("supervisor stopped");
    }
}

impl Supervised for BackupSupervisor {
    fn restarting(&mut self, _ctx: &mut Context<BackupSupervisor>) {
        debug!("supervisor restarting");
    }
}

impl Handler<Start> for BackupSupervisor {
    type Result = ();

    fn handle(&mut self, msg: Start, _ctx: &mut Context<BackupSupervisor>) {
        debug!("supervisor received Start message");
        if let Err(err) = self.start_dataset_now(msg.dataset) {
            error!("failed to check datasets: {}", err);
        }
    }
}

impl Handler<Stop> for BackupSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<BackupSupervisor>) {
        debug!("supervisor received Stop message");
        self.state.supervisor_event(SupervisorAction::Stopped);
        ctx.stop();
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct StartBackup {
    dbase: Arc<dyn RecordRepository>,
    state: Arc<dyn StateStore>,
    dataset: Dataset,
    schedule: Schedule,
    performer: Arc<dyn Performer>,
}

//
// Actor that performs a single backup of a dataset and then stops.
//
struct BackupRunner;

impl Actor for BackupRunner {
    type Context = Context<Self>;
}

impl Handler<StartBackup> for BackupRunner {
    type Result = ();

    fn handle(&mut self, msg: StartBackup, ctx: &mut Context<BackupRunner>) {
        debug!("runner received StartBackup message");
        thread::spawn(move || {
            // Allow the backup process to run an async runtime of its own, if
            // necessary, possibly with a threaded future executor, by spawning
            // it to a separate thread.
            run_dataset(
                msg.dbase,
                msg.state,
                msg.dataset.clone(),
                msg.schedule.clone(),
                msg.performer.clone(),
            );
        });
        debug!("runner spawned backup process");
        ctx.stop();
    }
}

///
/// Check if dataset can be backed up now (backup not already running).
///
/// Returns Some(Schedule::Hourly) if okay to run, otherwise None.
///
fn can_run(state: &Arc<dyn StateStore>, set: &Dataset) -> Result<Option<Schedule>, Error> {
    let redux = state.get_state();
    let backup_state = redux.backups(&set.id);
    if let Some(backup) = backup_state {
        // not errored, not poused, and no end time means it is still running
        if !backup.had_error() && !backup.is_paused() && backup.end_time().is_none() {
            debug!("dataset {} still in progress", &set.id);
            return Ok(None);
        }
    } else {
        // maybe the application restarted after a crash
        debug!("reset missing state to start");
        state.backup_event(BackupAction::Start(set.id.clone()));
    }
    Ok(Some(Schedule::Hourly))
}

///
/// Check if the given dataset should be processed now.
///
/// Returns the applicative schedule for the purpose of setting the end time.
///
fn should_run(
    dbase: &Arc<dyn RecordRepository>,
    state: &Arc<dyn StateStore>,
    set: &Dataset,
) -> Result<Option<Schedule>, Error> {
    if !set.schedules.is_empty() {
        let end_time: Option<DateTime<Utc>> = if let Some(ref checksum) = set.snapshot {
            let snapshot = dbase
                .get_snapshot(checksum)?
                .ok_or_else(|| anyhow!(format!("snapshot {} missing from database", checksum)))?;
            snapshot.end_time
        } else {
            None
        };
        let redux = state.get_state();
        let backup_state = redux.backups(&set.id);
        for schedule in set.schedules.iter() {
            // consider if backup is overdue based on snapshot
            let mut maybe_run = if let Some(et) = end_time {
                schedule.is_ready(et)
            } else {
                schedule.within_range(Utc::now())
            };
            // consider how the backup state may affect the decision
            if let Some(backup) = backup_state {
                // ignore the error state, it does not override the schedule
                if !backup.had_error() {
                    if let Some(et) = backup.end_time() {
                        // a backup ran but there were no changes found
                        if !schedule.is_ready(et) {
                            maybe_run = false;
                        }
                    } else if !backup.is_paused() {
                        // not error and not paused means it is still running
                        maybe_run = false;
                        debug!("dataset {} already in progress", &set.id);
                    }
                }
            } else if maybe_run {
                // maybe the application restarted after a crash
                debug!("reset missing state to start");
                state.backup_event(BackupAction::Start(set.id.clone()));
            }
            if maybe_run {
                return Ok(Some(schedule.to_owned()));
            }
        }
    }
    Ok(None)
}

///
/// Run the backup procedure for the named dataset. Takes the passphrase from
/// the environment.
///
fn run_dataset(
    dbase: Arc<dyn RecordRepository>,
    state: Arc<dyn StateStore>,
    dataset: Dataset,
    schedule: Schedule,
    performer: Arc<dyn Performer>,
) {
    let passphrase = crypto::get_passphrase();
    info!("dataset {} to be backed up", &dataset.id);
    let start_time = SystemTime::now();
    let stop_time = schedule.stop_time(Utc::now());
    // reset any error state in the backup
    state.backup_event(BackupAction::Restart(dataset.id.clone()));
    let dataset_id = dataset.id.clone();
    let request = Request::new(dataset, dbase, state.clone(), passphrase, stop_time);
    match performer.backup(request) {
        Ok(Some(checksum)) => {
            let end_time = SystemTime::now();
            let time_diff = end_time.duration_since(start_time);
            let pretty_time = pretty_print_duration(time_diff);
            info!("created new snapshot {}", &checksum);
            info!(
                "dataset {} backup complete after {}",
                &dataset_id, pretty_time
            );
        }
        Ok(None) => info!("no new snapshot required"),
        Err(err) => match err.downcast::<OutOfTimeFailure>() {
            Ok(_) => {
                info!("backup window has reached its end");
                // put the backup in the paused state for the time being
                state.backup_event(BackupAction::Pause(dataset_id.clone()));
            }
            Err(err) => {
                // here `err` is the original error
                error!("could not perform backup: {}", err);
                // put the backup in the error state so we try again
                state.backup_event(BackupAction::Error(dataset_id.clone(), err.to_string()));
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::schedule::{Schedule, TimeRange};
    use crate::domain::entities::{Checksum, Snapshot};
    use crate::domain::managers::backup::MockPerformer;
    use crate::domain::managers::state::{StateStore, StateStoreImpl};
    use crate::domain::repositories::MockRecordRepository;
    use std::io;
    use std::path::Path;

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_scheduler_start_stop_restart() -> io::Result<()> {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let repo = Arc::new(mock);
        // start
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let performer: Arc<dyn Performer> = Arc::new(MockPerformer::new());
        let sut = SchedulerImpl::new(state.clone(), performer);
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        state.wait_for_supervisor(SupervisorAction::Started);
        // stop
        let result = sut.stop();
        assert!(result.is_ok());
        state.wait_for_supervisor(SupervisorAction::Stopped);
        // restart
        let result = sut.start(repo);
        assert!(result.is_ok());
        state.wait_for_supervisor(SupervisorAction::Started);
        Ok(())
    }

    #[test]
    fn test_can_run_empty_state() {
        // arrange
        let dataset = Dataset::new(Path::new("/some/path"));
        // act
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let result = can_run(&state, &dataset);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_can_run_backup_running() {
        // arrange
        let dataset = Dataset::new(Path::new("/some/path"));
        let dataset_id = dataset.id.clone();
        // indicate that the dataset is already running a backup
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id));
        // act
        let result = can_run(&state, &dataset);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_can_run_backup_had_error() {
        // arrange
        let dataset = Dataset::new(Path::new("/some/path"));
        // indicate that the backup started but then failed
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset.id.clone()));
        state.backup_event(BackupAction::Error(
            dataset.id.clone(),
            String::from("oh no"),
        ));
        // act
        let result = can_run(&state, &dataset);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_can_run_backup_paused() {
        // arrange
        let dataset = Dataset::new(Path::new("/some/path"));
        // indicate that the backup has been paused
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset.id.clone()));
        state.backup_event(BackupAction::Pause(dataset.id.clone()));
        // act
        let result = can_run(&state, &dataset);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_should_run_no_schedule() {
        // arrange
        let dataset = Dataset::new(Path::new("/some/path"));
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // act
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_should_run_first_backup_due() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // act
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_should_run_first_backup_running() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id_clone = dataset.id.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the dataset is already running a backup
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_clone));
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_should_run_backup_not_overdue() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        // build a "latest" snapshot that finished just now
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, Default::default());
        snapshot.set_end_time(Utc::now());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1.clone());
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // act
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_should_run_backup_overdue() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        // build a "latest" snapshot that finished a while ago
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, Default::default());
        let day_ago = chrono::Duration::hours(25);
        let end_time = Utc::now() - day_ago;
        snapshot.set_end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1.clone());
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // act
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_should_run_old_snapshot_recent_backup() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id_1 = dataset.id.clone();
        let dataset_id_2 = dataset.id.clone();
        // build a "latest" snapshot that finished a while ago
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, Default::default());
        let day_ago = chrono::Duration::hours(25);
        let end_time = Utc::now() - day_ago;
        snapshot.set_end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1.clone());
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // insert a backup state that finished recently, which is the case when
        // there were no file changes, but the backup "finished" nonetheless
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_1));
        state.backup_event(BackupAction::Finish(dataset_id_2));
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_should_run_backup_restarted() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let dataset_id = dataset.id.clone();
        // build a "latest" snapshot that did not finish
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // start with a clean state, as if the app has restarted
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        assert!(state.get_state().backups(&dataset_id).is_none());
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        // and now there is backup state showing that it restarted
        assert!(state.get_state().backups(&dataset_id).is_some());
    }

    #[test]
    fn test_should_run_backup_restarted_not_overdue() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let dataset_id = dataset.id.clone();
        // build a "latest" snapshot that finished just now
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, Default::default());
        let end_time = Utc::now();
        snapshot.set_end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // start with a clean state, as if the app has restarted
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        assert!(state.get_state().backups(&dataset_id).is_none());
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        // the backup should not run and there is still no state
        assert!(state.get_state().backups(&dataset_id).is_none());
    }

    #[test]
    fn test_should_run_overdue_backup_running() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let dataset_id = dataset.id.clone();
        // build a "latest" snapshot that finished a while ago
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, Default::default());
        let day_ago = chrono::Duration::hours(25);
        let end_time = Utc::now() - day_ago;
        snapshot.set_end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the dataset is already running a backup
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id));
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_should_run_overdue_had_error() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id_1 = dataset.id.clone();
        let dataset_id_2 = dataset.id.clone();
        // build a "latest" snapshot that started just now
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the backup started but then failed
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_1));
        state.backup_event(BackupAction::Error(
            dataset_id_2,
            String::from("oh no"),
        ));
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_should_run_time_range_and_paused() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        // schedule has a time range that already passed, so that a paused
        // backup should not be restarted
        let start_time = chrono::Utc::now() - chrono::Duration::minutes(305);
        let stop_time = chrono::Utc::now() - chrono::Duration::minutes(5);
        let range = TimeRange::new(
            start_time.hour(),
            start_time.minute(),
            stop_time.hour(),
            stop_time.minute(),
        );
        dataset.add_schedule(Schedule::Daily(Some(range)));
        let dataset_id_1 = dataset.id.clone();
        let dataset_id_2 = dataset.id.clone();
        // build a "latest" snapshot that started recently
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the backup has been paused
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_1));
        state.backup_event(BackupAction::Pause(dataset_id_2));
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_should_run_time_range_had_error() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        // schedule has a time range that already passed, so even with an error
        // condition, the backup should not be restarted
        let start_time = chrono::Utc::now() - chrono::Duration::minutes(305);
        let stop_time = chrono::Utc::now() - chrono::Duration::minutes(5);
        let range = TimeRange::new(
            start_time.hour(),
            start_time.minute(),
            stop_time.hour(),
            stop_time.minute(),
        );
        dataset.add_schedule(Schedule::Daily(Some(range)));
        let dataset_id_1 = dataset.id.clone();
        let dataset_id_2 = dataset.id.clone();
        // build a "latest" snapshot that started recently
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let dataset_clone = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the backup started but then failed
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_1));
        state.backup_event(BackupAction::Error(
            dataset_id_2,
            String::from("oh no"),
        ));
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_scheduler_start_backup() -> io::Result<()> {
        // arrange
        let dataset = Dataset::new(Path::new("/some/path"));
        let dataset_copy1 = dataset.clone();
        let dataset_copy2 = dataset.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        //
        // The expectations here are being called on another thread, so any
        // failures there will go unnoticed, hence we call checkpoint() to make
        // sure the mock was invoked as expected.
        //
        let mut perf = MockPerformer::new();
        // make sure the mock was invoked exactly two times
        perf.expect_backup().times(2).returning(|request| {
            // indicate backup completed so it can be run more than once
            request
                .state
                .backup_event(BackupAction::Finish(request.dataset.id.clone()));
            Ok(None)
        });
        // act, assert
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let performer: Arc<MockPerformer> = Arc::new(perf);
        // make sure the performer reference in the supervisor is dropped
        {
            let sut = SchedulerImpl::new(state.clone(), performer.clone());
            let result = sut.start(Arc::new(mock));
            assert!(result.is_ok());
            thread::sleep(Duration::new(1, 0));
            let result = sut.start_backup(dataset_copy1);
            assert!(result.is_ok());
            thread::sleep(Duration::new(1, 0));
            let result = sut.start_backup(dataset_copy2);
            assert!(result.is_ok());
            thread::sleep(Duration::new(1, 0));
            // shutdown the supervisor to release the performer reference
            let result = sut.stop();
            assert!(result.is_ok());
            thread::sleep(Duration::new(1, 0));
        }
        // Do bad things with the arc so we can call checkpoint() without
        // relying on clone(), which MockPerformer does not implement.
        let mut perf = Arc::try_unwrap(performer).unwrap();
        perf.checkpoint();
        Ok(())
    }
}
