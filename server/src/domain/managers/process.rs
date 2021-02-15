//
// Copyright (c) 2020 Nathan Fiedler
//

//! The `process` module spawns threads to perform backups, ensuring backups are
//! performed for each dataset according to a schedule.

use super::state::{BackupAction, StateStore, SupervisorAction};
use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::Dataset;
use crate::domain::repositories::RecordRepository;
use actix::prelude::*;
use chrono::prelude::*;
use failure::{err_msg, Error};
use log::{debug, error, info, trace};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

///
/// `Processor` manages a supervised actor which in turn spawns actors to
/// process backups according to their schedules.
///
#[cfg_attr(test, automock)]
pub trait Processor: Send + Sync {
    /// Start a supervisor that will run scheduled backups.
    fn start(&self, repo: Arc<dyn RecordRepository>) -> Result<(), Error>;

    /// Signal the supervisor to stop and release the database reference.
    fn stop(&self) -> Result<(), Error>;
}

///
/// Concrete implementation of `Processor` that uses the actix actor framework
/// to spawn threads and send messages to actors to manage the backups according
/// to their schedules.
///
pub struct ProcessorImpl {
    // Arbiter manages the supervised actor that initiates backups.
    runner: Arbiter,
    // Application state to be provided to supervisor and runners.
    state: Arc<dyn StateStore>,
    // Address of the supervisor actor, if it has been started.
    super_addr: Mutex<Option<Addr<BackupSupervisor>>>,
}

impl ProcessorImpl {
    /// Construct a new instance of ProcessorImpl.
    pub fn new(state: Arc<dyn StateStore>) -> Self {
        // create an Arbiter to manage an event loop on a new thread
        Self {
            runner: Arbiter::new(),
            state: state.clone(),
            super_addr: Mutex::new(None),
        }
    }
}

impl Processor for ProcessorImpl {
    fn start(&self, repo: Arc<dyn RecordRepository>) -> Result<(), Error> {
        let mut su_addr = self.super_addr.lock().unwrap();
        if su_addr.is_none() {
            // start supervisor within the arbiter created earlier
            let state = self.state.clone();
            let addr = actix::Supervisor::start_in_arbiter(&self.runner, move |_| {
                BackupSupervisor::new(repo, state)
            });
            *su_addr = Some(addr);
        }
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
        fn err_convert(err: SendError<Stop>) -> Error {
            err_msg(format!("ProcessorImpl.stop(): {:?}", err))
        }
        let mut su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.take() {
            addr.try_send(Stop()).map_err(err_convert)
        } else {
            Ok(())
        }
    }
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
}

impl BackupSupervisor {
    fn new(repo: Arc<dyn RecordRepository>, state: Arc<dyn StateStore>) -> Self {
        // Create an Arbiter to manage an event loop on a new thread, keeping
        // the backup runners separate from the supervisor since they manage a
        // significant workload for an extended period of time.
        Self {
            dbase: repo,
            state,
            runner: Arbiter::new(),
        }
    }

    /// Begin the backup process for all datasets that are ready to run.
    fn start_due_datasets(&self) -> Result<(), Error> {
        let datasets = self.dbase.get_datasets()?;
        let state = self.state.clone();
        for set in datasets {
            if let Some(schedule) = should_run(&self.dbase, &state, &set)? {
                let msg = StartBackup {
                    dbase: self.dbase.clone(),
                    state: state.clone(),
                    dataset: set,
                    schedule,
                };
                let addr = Actor::start_in_arbiter(&self.runner, |_| BackupRunner {});
                if let Err(err) = addr.try_send(msg) {
                    return Err(err_msg(format!("error sending message to runner: {}", err)));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
static SUPERVISOR_INTERVAL: u64 = 100;
#[cfg(not(test))]
static SUPERVISOR_INTERVAL: u64 = 300_000;

impl Actor for BackupSupervisor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("supervisor started");
        self.state.supervisor_event(SupervisorAction::Started);
        ctx.run_interval(Duration::from_millis(SUPERVISOR_INTERVAL), |this, _ctx| {
            trace!("supervisor interval fired");
            if let Err(err) = this.start_due_datasets() {
                error!("failed to check datasets: {}", err);
            }
        });
    }
}

impl Supervised for BackupSupervisor {
    fn restarting(&mut self, _ctx: &mut Context<BackupSupervisor>) {
        debug!("supervisor restarting");
    }
}

impl Handler<Stop> for BackupSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<BackupSupervisor>) {
        debug!("supervisor received stop message");
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
        debug!("runner received start backup message");
        thread::spawn(|| {
            // Allow the backup process to run an async runtime of its own, if
            // necessary, possibly with a threaded future executor, by spawning
            // it to a separate thread. This helps with some pack stores that
            // call into libraries that return their results as a Future.
            run_dataset(
                msg.dbase,
                msg.state,
                msg.dataset.clone(),
                msg.schedule.clone(),
            );
        });
        debug!("runner spawned backup process");
        ctx.stop();
    }
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
        let latest_snapshot = dbase.get_latest_snapshot(&set.id)?;
        let end_time: Option<DateTime<Utc>> = if let Some(checksum) = latest_snapshot {
            let snapshot = dbase
                .get_snapshot(&checksum)?
                .ok_or_else(|| err_msg(format!("snapshot {} missing from database", &checksum)))?;
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
                        debug!("dataset {} backup already in progress", &set.id);
                    }
                }
            } else if maybe_run {
                // maybe the application restarted after a crash
                debug!("reset missing backup state to start");
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
) {
    let passphrase = super::get_passphrase();
    info!("dataset {} to be backed up", &dataset.id);
    let start_time = SystemTime::now();
    let stop_time = schedule.stop_time(Utc::now());
    // reset any error state in the backup
    state.backup_event(BackupAction::Restart(dataset.id.clone()));
    match super::backup::perform_backup(&dataset, &dbase, &state, &passphrase, stop_time) {
        Ok(Some(checksum)) => {
            let end_time = SystemTime::now();
            let time_diff = end_time.duration_since(start_time);
            let pretty_time = super::pretty_print_duration(time_diff);
            info!("created new snapshot {}", &checksum);
            info!(
                "dataset {} backup complete after {}",
                &dataset.id, pretty_time
            );
        }
        Ok(None) => info!("no new snapshot required"),
        Err(err) => match err.downcast::<super::backup::OutOfTimeFailure>() {
            Ok(_) => {
                info!("backup window has reached its end");
                // put the backup in the paused state for the time being
                state.backup_event(BackupAction::Pause(dataset.id.clone()));
            }
            Err(err) => {
                // here `err` is the original error
                error!("could not perform backup: {}", err);
                // put the backup in the error state so we try again
                state.backup_event(BackupAction::Error(dataset.id.clone(), err.to_string()));
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::schedule::{Schedule, TimeRange};
    use crate::domain::entities::{Checksum, Snapshot};
    use crate::domain::managers::state::{StateStore, StateStoreImpl};
    use crate::domain::repositories::MockRecordRepository;
    use std::io;
    use std::path::Path;
    use tempfile::tempdir;

    #[actix_rt::test]
    async fn test_process_start_stop_restart() -> io::Result<()> {
        // arrange
        let outdir = tempdir()?;
        // need a real path so the backup manager can create a workspace
        let mut dataset = Dataset::new(outdir.path());
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_id = dataset.id.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(|_| Ok(None));
        mock.expect_get_excludes().returning(move || Vec::new());
        // Once the backup process starts inserting trees into the database, we
        // know that the supervisor and runner have done their part, so give an
        // error to cause the backup to stop.
        mock.expect_insert_tree()
            .returning(|_| Err(err_msg("oh no")));
        let repo = Arc::new(mock);
        // act (start)
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let sut = ProcessorImpl::new(state.clone());
        let result = sut.start(repo.clone());
        thread::sleep(Duration::new(1, 0));
        // assert (start)
        assert!(result.is_ok());
        // act (stop)
        let result = sut.stop();
        // assert (stop)
        assert!(result.is_ok());
        // act (start again)
        let result = sut.start(repo);
        // assert (start again)
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_should_run_no_schedule() {
        // arrange
        let dataset = Dataset::new(Path::new("/some/path"));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(|_| Ok(None));
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
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(|_| Ok(None));
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
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let dataset_id_clone = dataset.id.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(|_| Ok(None));
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
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that finished just now
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, 0);
        let end_time = Utc::now();
        snapshot = snapshot.end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
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
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that finished a while ago
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, 0);
        let day_ago = chrono::Duration::hours(25);
        let end_time = Utc::now() - day_ago;
        snapshot = snapshot.end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
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
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let dataset_id_clone1 = dataset.id.clone();
        let dataset_id_clone2 = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that finished a while ago
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, 0);
        let day_ago = chrono::Duration::hours(25);
        let end_time = Utc::now() - day_ago;
        snapshot = snapshot.end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // insert a backup state that finished recently, which is the case when
        // there were no file changes, but the backup "finished" nonetheless
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_clone1));
        state.backup_event(BackupAction::Finish(dataset_id_clone2));
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
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let dataset_id_clone = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that did not finish
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, 0);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // start with a clean state, as if the app has restarted
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        assert!(state.get_state().backups(&dataset_id_clone).is_none());
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        // and now there is backup state showing that it restarted
        assert!(state.get_state().backups(&dataset_id_clone).is_some());
    }

    #[test]
    fn test_should_run_backup_restarted_not_overdue() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let dataset_id_clone = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that finished just now
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, 0);
        let end_time = Utc::now();
        snapshot = snapshot.end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // start with a clean state, as if the app has restarted
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        assert!(state.get_state().backups(&dataset_id_clone).is_none());
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        // the backup should not run and there is still no state
        assert!(state.get_state().backups(&dataset_id_clone).is_none());
    }

    #[test]
    fn test_should_run_overdue_backup_running() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let dataset_id_clone1 = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that finished a while ago
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, 0);
        let day_ago = chrono::Duration::hours(25);
        let end_time = Utc::now() - day_ago;
        snapshot = snapshot.end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the dataset is already running a backup
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_clone1));
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
        dataset = dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let dataset_id_clone1 = dataset.id.clone();
        let dataset_id_clone2 = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that started just now
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, 0);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the backup started but then failed
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_clone1));
        state.backup_event(BackupAction::Error(
            dataset_id_clone2,
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
        dataset = dataset.add_schedule(Schedule::Daily(Some(range)));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let dataset_id_clone1 = dataset.id.clone();
        let dataset_id_clone2 = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that started recently
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, 0);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the backup has been paused
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_clone1));
        state.backup_event(BackupAction::Pause(dataset_id_clone2));
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
        dataset = dataset.add_schedule(Schedule::Daily(Some(range)));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let dataset_id_clone1 = dataset.id.clone();
        let dataset_id_clone2 = dataset.id.clone();
        let datasets = vec![dataset];
        // build a "latest" snapshot that started recently
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, 0);
        let snapshot_sha1 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_latest_snapshot()
            .withf(move |id| id == dataset_id)
            .returning(move |_| Ok(Some(snapshot_sha1.clone())));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the backup started but then failed
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        state.backup_event(BackupAction::Start(dataset_id_clone1));
        state.backup_event(BackupAction::Error(
            dataset_id_clone2,
            String::from("oh no"),
        ));
        // act
        let result = should_run(&repo, &state, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
