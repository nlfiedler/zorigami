//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::Dataset;
use crate::domain::entities::schedule::Schedule;
use crate::domain::repositories::RecordRepository;
use crate::shared::packs;
use crate::shared::state::{SchedulerAction, StateStore};
use crate::tasks::backup;
use crate::tasks::leader::RingLeader;
use crate::tasks::prune;
use actix::prelude::*;
use anyhow::{Error, anyhow};
use chrono::prelude::*;
use log::{debug, error, trace, warn};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::sync::{Arc, Mutex};
use std::time::Duration;

///
/// `Scheduler` manages a supervised actor which fires backup requests at the
/// appropriate time for each dataset, according to its schedule.
///
#[cfg_attr(test, automock)]
pub trait Scheduler: Send + Sync {
    /// Start a supervisor that will manage an interval timer to run backups.
    fn start(&self, repo: Arc<dyn RecordRepository>) -> Result<(), Error>;

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
    // Application state to be provided to supervisor.
    state: Arc<dyn StateStore>,
    // Backup requests are sent to the leader.
    leader: Arc<dyn RingLeader>,
    // Address of the supervisor actor, if it has been started.
    super_addr: Mutex<Option<Addr<ScheduleSupervisor>>>,
    // Sleep interval in milliseconds between checks for datasets ready to run.
    interval: u64,
}

impl SchedulerImpl {
    /// Construct a new instance of SchedulerImpl.
    pub fn new(state: Arc<dyn StateStore>, leader: Arc<dyn RingLeader>, interval: u64) -> Self {
        // create an Arbiter to manage an event loop on a new thread
        Self {
            runner: Arbiter::new(),
            state: state.clone(),
            leader: leader.clone(),
            super_addr: Mutex::new(None),
            interval,
        }
    }

    /// Set the interval in milliseconds by which the background thread will
    /// wake up and check if any datasets are ready to run.
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
            let leader = self.leader.clone();
            let interval = self.interval;
            let addr = actix::Supervisor::start_in_arbiter(&self.runner.handle(), move |_| {
                ScheduleSupervisor::new(repo, state, leader, interval)
            });
            *su_addr = Some(addr);
        }
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
        fn err_convert(err: SendError<Stop>) -> Error {
            anyhow!(format!("SchedulerImpl.stop(): {:?}", err))
        }
        let mut su_addr = self.super_addr.lock().unwrap();
        match su_addr.take() {
            Some(addr) => addr.try_send(Stop()).map_err(err_convert),
            _ => {
                warn!("supervisor not running, cannot stop backup");
                Ok(())
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Stop();

//
// Supervised actor that enqueues requests to perform backups according to the
// schedules defined in the dataset. Uses an interval timer to wake up
// periodically to check for datasets that are ready to backup.
//
struct ScheduleSupervisor {
    // Database connection for querying datasets.
    dbase: Arc<dyn RecordRepository>,
    // Application state for signaling changes in backup status.
    state: Arc<dyn StateStore>,
    // Backup requests are sent to the leader.
    leader: Arc<dyn RingLeader>,
    // Sleep interval (milliseconds) between checks for datasets ready to run.
    interval: u64,
}

impl ScheduleSupervisor {
    fn new(
        repo: Arc<dyn RecordRepository>,
        state: Arc<dyn StateStore>,
        leader: Arc<dyn RingLeader>,
        interval: u64,
    ) -> Self {
        Self {
            dbase: repo,
            state,
            leader,
            interval,
        }
    }

    /// Begin the backup process for all datasets that are ready to run.
    fn start_due_datasets(&self) -> Result<(), Error> {
        let datasets = self.dbase.get_datasets()?;
        for set in datasets {
            if let Some(schedule) = should_run(&self.dbase, self.leader.clone(), &set)? {
                let passphrase = packs::get_passphrase();
                let stop_time = schedule.stop_time(Utc::now());
                let request = backup::Request::new(set.id, passphrase, stop_time);
                self.leader.backup(request)?;
            }
        }
        Ok(())
    }

    /// Begin the prune process for all datasets.
    fn prune_all_datasets(&self) -> Result<(), Error> {
        let datasets = self.dbase.get_datasets()?;
        for set in datasets {
            let request = prune::Request::new(set.id);
            self.leader.prune(request)?;
        }
        Ok(())
    }
}

impl Actor for ScheduleSupervisor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("schedule supervisor started");
        self.state.scheduler_event(SchedulerAction::Started);

        // periodically check for datasets that need a backup
        ctx.run_interval(Duration::from_millis(self.interval), |this, _ctx| {
            trace!("schedule backup interval fired");
            if let Err(err) = this.start_due_datasets() {
                error!("failed to backup datasets: {}", err);
            }
        });

        // every day have all datasets prune old snapshots
        let prune_interval_hours = std::env::var("PRUNE_INTERVAL_HOURS")
            .map(|s| s.parse::<u64>().unwrap_or(24))
            .unwrap_or(24);
        ctx.run_interval(Duration::from_hours(prune_interval_hours), |this, _ctx| {
            trace!("schedule prune interval fired");
            if let Err(err) = this.prune_all_datasets() {
                error!("failed to prune datasets: {}", err);
            }
        });

        // periodically exercise the restore path on a random file to catch
        // store or encryption regressions before a user actually needs them
        let restore_test_days = std::env::var("RESTORE_TEST_INTERVAL_DAYS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| n.clamp(1, 30))
            .unwrap_or(7);
        ctx.run_interval(
            Duration::from_hours(restore_test_days * 24),
            |this, _ctx| {
                trace!("schedule restore-test interval fired");
                let passphrase = packs::get_passphrase();
                if let Err(err) = this.leader.restore_test(passphrase) {
                    error!("failed to schedule restore test: {}", err);
                }
            },
        );

        // periodically scan the database for unreachable or unreadable records
        let scrub_interval_days = std::env::var("DATABASE_SCRUB_INTERVAL_DAYS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| n.clamp(1, 30))
            .unwrap_or(7);
        ctx.run_interval(
            Duration::from_hours(scrub_interval_days * 24),
            |this, _ctx| {
                trace!("schedule database-scrub interval fired");
                if let Err(err) = this.leader.database_scrub() {
                    error!("failed to schedule database scrub: {}", err);
                }
            },
        );

        // periodically delete unreachable pack files and old database archives
        let pack_prune_interval_days = std::env::var("PACK_PRUNE_INTERVAL_DAYS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| n.clamp(1, 180))
            .unwrap_or(30);
        ctx.run_interval(
            Duration::from_hours(pack_prune_interval_days * 24),
            |this, _ctx| {
                trace!("schedule pack-prune interval fired");
                if let Err(err) = this.leader.prune_packs() {
                    error!("failed to schedule pack prune: {}", err);
                }
            },
        );
    }

    fn stopping(&mut self, _ctx: &mut Context<Self>) -> Running {
        debug!("schedule supervisor stopping");
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        debug!("schedule supervisor stopped");
    }
}

impl Supervised for ScheduleSupervisor {
    fn restarting(&mut self, _ctx: &mut Context<ScheduleSupervisor>) {
        warn!("schedule supervisor restarting");
    }
}

impl Handler<Stop> for ScheduleSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<ScheduleSupervisor>) {
        debug!("schedule supervisor received Stop message");
        self.state.scheduler_event(SchedulerAction::Stopped);
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
    leader: Arc<dyn RingLeader>,
    dataset: &Dataset,
) -> Result<Option<Schedule>, Error> {
    if !dataset.schedules.is_empty() {
        let end_time: Option<DateTime<Utc>> = if let Some(ref checksum) = dataset.snapshot {
            let snapshot = dbase
                .get_snapshot(checksum)?
                .ok_or_else(|| anyhow!(format!("snapshot {} missing from database", checksum)))?;
            snapshot.end_time
        } else {
            None
        };
        let backup = if let Some(bup) = leader.get_backup_by_dataset(&dataset.id) {
            bup
        } else {
            backup::Request::new(dataset.id.clone(), "tiger", None)
        };
        for schedule in dataset.schedules.iter() {
            // consider if backup is overdue based on snapshot
            let mut maybe_run = if let Some(et) = end_time {
                schedule.is_ready(et)
            } else {
                schedule.within_range(Utc::now())
            };
            // consider how the backup state may affect the decision
            if backup.started.is_some() {
                // ignore failed backups, they do not override the schedule
                if backup.errors.is_empty() {
                    if let Some(et) = backup.finished {
                        // a backup ran but there were no changes found
                        if !schedule.is_ready(et) {
                            maybe_run = false;
                        }
                    } else if backup.status != backup::Status::PAUSED {
                        // not error and not paused means it is still running
                        maybe_run = false;
                        debug!("dataset {} already in progress", &dataset.id);
                    }
                }
            }
            if maybe_run {
                return Ok(Some(schedule.to_owned()));
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::schedule::{Schedule, TimeRange};
    use crate::domain::entities::{Checksum, Snapshot};
    use crate::domain::repositories::MockRecordRepository;
    use crate::shared::state::{StateStore, StateStoreImpl};
    use crate::tasks::leader::MockRingLeader;
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
        let leader: Arc<dyn RingLeader> = Arc::new(MockRingLeader::new());
        let sut = SchedulerImpl::new(state.clone(), leader.clone(), 60000);
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        state.wait_for_scheduler(SchedulerAction::Started);
        // stop
        let result = sut.stop();
        assert!(result.is_ok());
        state.wait_for_scheduler(SchedulerAction::Stopped);
        // restart
        let result = sut.start(repo);
        assert!(result.is_ok());
        state.wait_for_scheduler(SchedulerAction::Started);
        Ok(())
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_scheduler_submit_backup() -> io::Result<()> {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Hourly);
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let repo = Arc::new(mock);
        // start
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let mut leader = MockRingLeader::new();
        leader.expect_get_backup_by_dataset().returning(|_| None);
        leader.expect_backup().returning(|_| Ok(()));
        let sut = SchedulerImpl::new(state.clone(), Arc::new(leader), 5);
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        state.wait_for_scheduler(SchedulerAction::Started);
        std::thread::sleep(std::time::Duration::from_secs(1));
        // stop
        let result = sut.stop();
        assert!(result.is_ok());
        state.wait_for_scheduler(SchedulerAction::Stopped);
        Ok(())
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
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(|_| None);
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
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
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(|_| None);
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
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
        let dataset_id = dataset.id.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the dataset is already running a backup
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(move |_| {
                let mut req = backup::Request::new(dataset_id.clone(), "tiger", None);
                req.started = Some(Utc::now());
                Some(req)
            });
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
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
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(|_| None);
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
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
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(|_| None);
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
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
        let dataset_id = dataset.id.clone();
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
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(move |_| {
                let mut req = backup::Request::new(dataset_id.clone(), "tiger", None);
                req.started = Some(Utc::now());
                req.finished = Some(Utc::now());
                Some(req)
            });
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_should_run_incomplete_backup_restarted() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
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
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(|_| None);
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_should_run_overdue_backup_running() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/some/path"));
        dataset.add_schedule(Schedule::Daily(None));
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        // build a "latest" snapshot that finished a while ago
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut snapshot = Snapshot::new(None, tree_sha, Default::default());
        let day_ago = chrono::Duration::hours(25);
        let end_time = Utc::now() - day_ago;
        snapshot.set_end_time(end_time);
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the dataset is already running a backup
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(move |_| {
                let mut req = backup::Request::new(dataset_id.clone(), "tiger", None);
                req.started = Some(Utc::now());
                Some(req)
            });
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
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
        let dataset_id = dataset.id.clone();
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
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(move |_| {
                let mut req = backup::Request::new(dataset_id.clone(), "tiger", None);
                req.started = Some(Utc::now());
                req.errors.push("oh no".into());
                Some(req)
            });
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
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
        // build a "latest" snapshot that started recently
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the backup has been paused
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(move |_| {
                let mut req = backup::Request::new(dataset_id.clone(), "tiger", None);
                req.started = Some(Utc::now());
                req.status = backup::Status::PAUSED;
                Some(req)
            });
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
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
        // build a "latest" snapshot that started recently
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1);
        let dataset_clone = dataset.clone();
        let dataset_id = dataset.id.clone();
        let datasets = vec![dataset];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        mock.expect_get_snapshot()
            .returning(move |_| Ok(Some(snapshot.clone())));
        let repo: Arc<dyn RecordRepository> = Arc::new(mock);
        // indicate that the backup started but then failed
        let mut mock_leader = MockRingLeader::new();
        mock_leader
            .expect_get_backup_by_dataset()
            .returning(move |_| {
                let mut req = backup::Request::new(dataset_id.clone(), "tiger", None);
                req.started = Some(Utc::now());
                req.errors.push("oh no".into());
                Some(req)
            });
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
