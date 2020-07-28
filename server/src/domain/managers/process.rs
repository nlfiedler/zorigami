//
// Copyright (c) 2020 Nathan Fiedler
//

//! The `process` module spawns threads to perform backups, ensuring backups are
//! performed for each dataset according to a schedule.

use super::state::{self, Action};
use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::Dataset;
use crate::domain::repositories::RecordRepository;
use actix::prelude::*;
use chrono::prelude::*;
use failure::{err_msg, Error};
use lazy_static::lazy_static;
use log::{debug, error, info, trace, warn};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

#[derive(Message)]
#[rtype(result = "()")]
struct Start(Arc<dyn RecordRepository>);

#[derive(Default)]
struct MySupervisor {
    dbase: Option<Arc<dyn RecordRepository>>,
}

#[cfg(test)]
static SUPERVISOR_INTERVAL: u64 = 100;
#[cfg(not(test))]
static SUPERVISOR_INTERVAL: u64 = 300_000;

impl Actor for MySupervisor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("supervisor actor started");
        ctx.run_interval(Duration::from_millis(SUPERVISOR_INTERVAL), |this, _ctx| {
            trace!("supervisor interval fired");
            if let Some(dbase) = this.dbase.as_ref() {
                if let Err(err) = start_due_datasets(dbase.clone()) {
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
        // Previously this code was managing the database instance, but then
        // everything was changed for Clean Architecture.
        //
        // It is worth noting that opening and closing the database led to
        // subtle timing issues with the supervisor and arbiter threads opening
        // and closing databases at the same time. Would occasionally fail to
        // get the exclusive lock when opening the database.
        //
        // Also, opening the database repeatedly seems to create many log files.
        //
        self.dbase = Some(msg.0);
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct StartBackup {
    dbase: Arc<dyn RecordRepository>,
    dataset: Dataset,
    schedule: Schedule,
}

struct Runner;

impl Actor for Runner {
    type Context = Context<Self>;
}

impl Handler<StartBackup> for Runner {
    type Result = ();

    fn handle(&mut self, msg: StartBackup, ctx: &mut Context<Runner>) {
        debug!("runner received backup message");
        thread::spawn(|| {
            // Allow the backup process to run an async runtime of its own, if
            // necessary, possibly with a threaded future executor, by spawning
            // it to a separate thread. This helps with some pack stores that
            // call into libraries that return their results as a Future.
            run_dataset(msg.dbase, msg.dataset.clone(), msg.schedule.clone());
        });
        debug!("runner spawned backup process");
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
    static ref MY_RUNNER: Arbiter = Arbiter::new();
}

///
/// Spawn a thread to monitor the datasets, ensuring that backups are started
/// according to the assigned schedules. If a dataset does not have a schedule
/// then it is not run automatically by this process.
///
pub fn start(repo: Arc<dyn RecordRepository>) -> std::io::Result<()> {
    thread::spawn(move || {
        let system = System::new("backup-supervisor");
        // must send a message to get the system to run our actor
        if let Err(err) = MY_SUPER.try_send(Start(repo)) {
            error!("error sending message to supervisor: {}", err);
        }
        system.run()
    });
    Ok(())
}

///
/// Begin the backup process for all datasets that are ready to run.
///
fn start_due_datasets(dbase: Arc<dyn RecordRepository>) -> Result<(), Error> {
    let datasets = dbase.get_datasets()?;
    for set in datasets {
        if let Some(schedule) = should_run(&dbase, &set)? {
            let msg = StartBackup {
                dbase: dbase.clone(),
                dataset: set,
                schedule,
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
/// Returns the applicative schedule for the same of setting the end time.
///
fn should_run(dbase: &Arc<dyn RecordRepository>, set: &Dataset) -> Result<Option<Schedule>, Error> {
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
        let redux = state::get_state();
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
                if backup.had_error() {
                    maybe_run = true;
                    debug!("dataset {} backup had an error, will restart", &set.id);
                } else if let Some(et) = backup.end_time() {
                    // a backup ran but there were no changes found
                    if !schedule.is_ready(et) {
                        maybe_run = false;
                    }
                } else if !backup.is_paused() {
                    maybe_run = false;
                    debug!("dataset {} backup already in progress", &set.id);
                }
            } else if maybe_run {
                // maybe the application restarted after a crash
                debug!("reset missing backup state to start");
                state::dispatch(Action::StartBackup(set.id.clone()));
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
fn run_dataset(dbase: Arc<dyn RecordRepository>, dataset: Dataset, schedule: Schedule) {
    let passphrase = super::get_passphrase();
    info!("dataset {} to be backed up", &dataset.id);
    let start_time = SystemTime::now();
    let stop_time = schedule.stop_time(Utc::now());
    // reset any error state in the backup
    state::dispatch(Action::RestartBackup(dataset.id.clone()));
    match super::backup::perform_backup(&dataset, &dbase, &passphrase, stop_time) {
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
                state::dispatch(Action::PauseBackup(dataset.id.clone()));
            }
            Err(err) => {
                // here `err` is the original error
                error!("could not perform backup: {}", err);
                // put the backup in the error state so we try again
                state::dispatch(Action::ErrorBackup(dataset.id.clone()));
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::schedule::{Schedule, TimeRange};
    use crate::domain::entities::{Checksum, Snapshot};
    use crate::domain::repositories::MockRecordRepository;
    use std::io;
    use std::path::Path;
    use tempfile::tempdir;

    #[actix_rt::test]
    async fn test_process_start() -> io::Result<()> {
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
        // The backup manager has started doing its thing, so we can cause it to
        // exit on an error, we are done with the test at this point as we have
        // effectively demonstrated that the supervisor and runner actors are
        // working as expected.
        mock.expect_get_excludes().returning(move || Vec::new());
        mock.expect_insert_tree()
            .returning(|_| Err(err_msg("oh no")));
        let repo = Arc::new(mock);
        // act
        let result = start(repo);
        thread::sleep(Duration::new(1, 0));
        // assert
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
        let result = should_run(&repo, &dataset_clone);
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
        let result = should_run(&repo, &dataset_clone);
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
        state::dispatch(Action::StartBackup(dataset_id_clone));
        // act
        let result = should_run(&repo, &dataset_clone);
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
        let result = should_run(&repo, &dataset_clone);
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
        let result = should_run(&repo, &dataset_clone);
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
        state::dispatch(Action::StartBackup(dataset_id_clone1));
        state::dispatch(Action::FinishBackup(dataset_id_clone2));
        // act
        let result = should_run(&repo, &dataset_clone);
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
        // the app has restarted, so ensure there is no state
        assert!(state::get_state().backups(&dataset_id_clone).is_none());
        // act
        let result = should_run(&repo, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        // and now there is backup state showing that it restarted
        assert!(state::get_state().backups(&dataset_id_clone).is_some());
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
        // the app has restarted, so ensure there is no state
        assert!(state::get_state().backups(&dataset_id_clone).is_none());
        // act
        let result = should_run(&repo, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        // the backup should not run and there is still no state
        assert!(state::get_state().backups(&dataset_id_clone).is_none());
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
        state::dispatch(Action::StartBackup(dataset_id_clone1));
        // act
        let result = should_run(&repo, &dataset_clone);
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
        state::dispatch(Action::StartBackup(dataset_id_clone1));
        state::dispatch(Action::ErrorBackup(dataset_id_clone2));
        // act
        let result = should_run(&repo, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_should_run_time_range_and_paused() {
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
        // indicate that the backup has been paused
        state::dispatch(Action::StartBackup(dataset_id_clone1));
        state::dispatch(Action::PauseBackup(dataset_id_clone2));
        // act
        let result = should_run(&repo, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
