//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::BackgroundOperation;
use crate::domain::entities::Dataset;
use crate::domain::entities::schedule::Schedule;
use crate::domain::repositories::{RecordRepository, StatusRepository};
use crate::shared::packs;
use crate::shared::state::{SchedulerAction, StateStore};
use crate::tasks::backup;
use crate::tasks::leader::RingLeader;
use crate::tasks::prune;
use actix::prelude::*;
use anyhow::{Error, anyhow};
use chrono::prelude::*;
use chrono_tz::Tz;
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
    ///
    /// The status repository supplies the last-run times used to decide which
    /// periodic tasks are overdue at startup.
    fn start(
        &self,
        repo: Arc<dyn RecordRepository>,
        status: Arc<dyn StatusRepository>,
    ) -> Result<(), Error>;

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
    fn start(
        &self,
        repo: Arc<dyn RecordRepository>,
        status: Arc<dyn StatusRepository>,
    ) -> Result<(), Error> {
        let mut su_addr = self.super_addr.lock().unwrap();
        if su_addr.is_none() {
            // start supervisor within the arbiter created earlier
            let state = self.state.clone();
            let leader = self.leader.clone();
            let interval = self.interval;
            let addr = actix::Supervisor::start_in_arbiter(&self.runner.handle(), move |_| {
                ScheduleSupervisor::new(repo, state, leader, status, interval)
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

/// Delay between the supervisor starting and the catch-up pass that runs
/// overdue tasks, so the catch-up does not compete with the rest of the server
/// coming up.
const STARTUP_CATCHUP_DELAY: Duration = Duration::from_secs(60);

/// How often each periodic task should run, from the environment.
#[derive(Clone, Copy)]
struct TaskIntervals {
    prune: Duration,
    restore_test: Duration,
    database_scrub: Duration,
    pack_prune: Duration,
    workspace_cleanup: Duration,
}

impl TaskIntervals {
    fn from_env() -> Self {
        // Every interval is clamped to at least one unit; a zero-length
        // actix interval would spin.
        fn clamped(name: &str, default: u64, lo: u64, hi: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .map(|n| n.clamp(lo, hi))
                .unwrap_or(default)
        }
        fn days(name: &str, default: u64, lo: u64, hi: u64) -> Duration {
            Duration::from_hours(clamped(name, default, lo, hi) * 24)
        }
        Self {
            prune: Duration::from_hours(clamped("PRUNE_INTERVAL_HOURS", 24, 1, 8760)),
            restore_test: days("RESTORE_TEST_INTERVAL_DAYS", 7, 1, 30),
            database_scrub: days("DATABASE_SCRUB_INTERVAL_DAYS", 7, 1, 30),
            pack_prune: days("PACK_PRUNE_INTERVAL_DAYS", 7, 1, 180),
            workspace_cleanup: Duration::from_hours(clamped(
                "WORKSPACE_CLEANUP_INTERVAL_HOURS",
                24,
                1,
                720,
            )),
        }
    }
}

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
    // Source of the last-run times for the periodic tasks.
    status: Arc<dyn StatusRepository>,
    // Sleep interval (milliseconds) between checks for datasets ready to run.
    interval: u64,
}

impl ScheduleSupervisor {
    fn new(
        repo: Arc<dyn RecordRepository>,
        state: Arc<dyn StateStore>,
        leader: Arc<dyn RingLeader>,
        status: Arc<dyn StatusRepository>,
        interval: u64,
    ) -> Self {
        Self {
            dbase: repo,
            state,
            leader,
            status,
            interval,
        }
    }

    /// Begin the backup process for all datasets that are ready to run.
    fn start_due_datasets(&self) -> Result<(), Error> {
        let tz = self.dbase.get_configuration()?.resolved_tz();
        let datasets = self.dbase.get_datasets()?;
        for set in datasets {
            if let Some(schedule) = should_run(&self.dbase, self.leader.clone(), &set, tz)? {
                let passphrase = packs::get_passphrase();
                let stop_time = schedule.stop_time(Utc::now(), tz);
                let request = backup::Request::new(set.id, passphrase, stop_time);
                self.leader.backup(request)?;
            }
        }
        Ok(())
    }

    /// Run the restore self-test, which needs the passphrase.
    fn run_restore_test(&self) {
        let passphrase = packs::get_passphrase();
        if let Err(err) = self.leader.restore_test(passphrase) {
            error!("failed to schedule restore test: {}", err);
        }
    }

    /// Start every periodic task whose interval has already elapsed since its
    /// last recorded run, or that has never run at all.
    ///
    /// The leader serializes these, so triggering several at once queues them
    /// rather than running them concurrently.
    fn run_overdue_tasks(&self, intervals: &TaskIntervals) {
        let runs = match self.status.list_runs() {
            Ok(runs) => runs,
            Err(err) => {
                // Without the run records there is no way to tell what is
                // overdue. The armed intervals still apply.
                warn!("cannot determine overdue tasks: {}", err);
                return;
            }
        };
        // A task may have several rows, one per dataset, so the last run of
        // the task as a whole is the most recent of them.
        let last_run = |operation: BackgroundOperation| -> Option<DateTime<Utc>> {
            runs.iter()
                .filter(|r| r.operation == operation)
                .map(|r| r.finished_at)
                .max()
        };
        let overdue = |operation: BackgroundOperation, interval: Duration| -> bool {
            match last_run(operation) {
                Some(finished) => match chrono::Duration::from_std(interval) {
                    Ok(interval) => Utc::now() - finished >= interval,
                    Err(_) => false,
                },
                // never run, which is exactly the case this exists to fix
                None => true,
            }
        };

        if overdue(BackgroundOperation::Prune, intervals.prune) {
            debug!("startup catch-up: pruning snapshots");
            if let Err(err) = self.prune_all_datasets() {
                error!("failed to prune datasets: {}", err);
            }
        }
        if overdue(BackgroundOperation::RestoreTest, intervals.restore_test) {
            debug!("startup catch-up: restore test");
            self.run_restore_test();
        }
        if overdue(BackgroundOperation::DatabaseScrub, intervals.database_scrub) {
            debug!("startup catch-up: database scrub");
            if let Err(err) = self.leader.database_scrub() {
                error!("failed to schedule database scrub: {}", err);
            }
        }
        if overdue(BackgroundOperation::PackPrune, intervals.pack_prune) {
            debug!("startup catch-up: pack prune");
            if let Err(err) = self.leader.prune_packs() {
                error!("failed to schedule pack prune: {}", err);
            }
        }
        if overdue(
            BackgroundOperation::WorkspaceCleanup,
            intervals.workspace_cleanup,
        ) {
            debug!("startup catch-up: workspace cleanup");
            if let Err(err) = self.leader.cleanup_workspaces() {
                error!("failed to schedule workspace cleanup: {}", err);
            }
        }
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

        let intervals = TaskIntervals::from_env();

        // every day have all datasets prune old snapshots
        ctx.run_interval(intervals.prune, |this, _ctx| {
            trace!("schedule prune interval fired");
            if let Err(err) = this.prune_all_datasets() {
                error!("failed to prune datasets: {}", err);
            }
        });

        // periodically exercise the restore path on a random file to catch
        // store or encryption regressions before a user actually needs them
        ctx.run_interval(intervals.restore_test, |this, _ctx| {
            trace!("schedule restore-test interval fired");
            this.run_restore_test();
        });

        // periodically scan the database for unreachable or unreadable records
        ctx.run_interval(intervals.database_scrub, |this, _ctx| {
            trace!("schedule database-scrub interval fired");
            if let Err(err) = this.leader.database_scrub() {
                error!("failed to schedule database scrub: {}", err);
            }
        });

        // periodically delete unreachable pack files and old database archives
        ctx.run_interval(intervals.pack_prune, |this, _ctx| {
            trace!("schedule pack-prune interval fired");
            if let Err(err) = this.leader.prune_packs() {
                error!("failed to schedule pack prune: {}", err);
            }
        });

        // periodically wipe leftover temporary pack files from each dataset's
        // workspace; killed backups/restores can leave files behind otherwise
        ctx.run_interval(intervals.workspace_cleanup, |this, _ctx| {
            trace!("schedule workspace-cleanup interval fired");
            if let Err(err) = this.leader.cleanup_workspaces() {
                error!("failed to schedule workspace cleanup: {}", err);
            }
        });

        // The intervals above are armed from zero and fire only after a full
        // period has elapsed, so a server restarted more often than the
        // interval would never run these tasks at all. Consult the recorded
        // run times and start anything already overdue. Delayed a little so
        // the catch-up does not compete with the rest of the server coming up.
        ctx.run_later(STARTUP_CATCHUP_DELAY, move |this, _ctx| {
            this.run_overdue_tasks(&intervals);
        });
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
    tz: Tz,
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
                schedule.is_ready(et, tz)
            } else {
                schedule.within_range(Utc::now(), tz)
            };
            // consider how the backup state may affect the decision
            if backup.started.is_some() {
                // ignore failed backups, they do not override the schedule;
                // non-fatal warnings collected in `errors` do not count as
                // failure here — only the FAILED status does
                if backup.status != backup::Status::FAILED {
                    if let Some(et) = backup.finished {
                        // a backup ran but there were no changes found
                        if !schedule.is_ready(et, tz) {
                            maybe_run = false;
                        }
                    } else if backup.status != backup::Status::PAUSED {
                        // not error and not paused means it is still running
                        maybe_run = false;
                        debug!("dataset {} already in progress", dataset.id);
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
    use crate::domain::entities::{Checksum, Snapshot, TaskRun};
    use crate::domain::repositories::{MockRecordRepository, MockStatusRepository};
    use crate::shared::state::{StateStore, StateStoreImpl};
    use crate::tasks::leader::MockRingLeader;
    use std::io;
    use std::path::Path;

    /// A status repository reporting no recorded runs, so the startup
    /// catch-up would consider everything overdue. The tests below never wait
    /// long enough for that pass to fire.
    fn mock_status_repo() -> Arc<dyn StatusRepository> {
        let mut mock = MockStatusRepository::new();
        mock.expect_list_runs().returning(|| Ok(vec![]));
        Arc::new(mock)
    }

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
        let result = sut.start(repo.clone(), mock_status_repo());
        assert!(result.is_ok());
        state.wait_for_scheduler(SchedulerAction::Started);
        // stop
        let result = sut.stop();
        assert!(result.is_ok());
        state.wait_for_scheduler(SchedulerAction::Stopped);
        // restart
        let result = sut.start(repo, mock_status_repo());
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
        mock.expect_get_configuration()
            .returning(|| Ok(crate::domain::entities::Configuration::default()));
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let repo = Arc::new(mock);
        // start
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let mut leader = MockRingLeader::new();
        leader.expect_get_backup_by_dataset().returning(|_| None);
        leader.expect_backup().returning(|_| Ok(()));
        let sut = SchedulerImpl::new(state.clone(), Arc::new(leader), 5);
        let result = sut.start(repo.clone(), mock_status_repo());
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
                req.status = backup::Status::FAILED;
                Some(req)
            });
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
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
                req.status = backup::Status::FAILED;
                Some(req)
            });
        let leader: Arc<dyn RingLeader> = Arc::new(mock_leader);
        // act
        let result = should_run(&repo, leader, &dataset_clone, Tz::UTC);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    /// Build a supervisor whose leader records which tasks were triggered.
    fn overdue_fixture(runs: Vec<TaskRun>) -> (ScheduleSupervisor, Arc<Mutex<Vec<&'static str>>>) {
        let fired: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let mut leader = MockRingLeader::new();
        let sink = fired.clone();
        leader.expect_restore_test().returning(move |_| {
            sink.lock().unwrap().push("restore_test");
            Ok(())
        });
        let sink = fired.clone();
        leader.expect_database_scrub().returning(move || {
            sink.lock().unwrap().push("database_scrub");
            Ok(())
        });
        let sink = fired.clone();
        leader.expect_prune_packs().returning(move || {
            sink.lock().unwrap().push("pack_prune");
            Ok(())
        });
        let sink = fired.clone();
        leader.expect_cleanup_workspaces().returning(move || {
            sink.lock().unwrap().push("workspace_cleanup");
            Ok(())
        });

        let mut dbase = MockRecordRepository::new();
        // prune_all_datasets walks the datasets; an empty list is enough to
        // tell that it was invoked without needing a real prune
        let sink = fired.clone();
        dbase.expect_get_datasets().returning(move || {
            sink.lock().unwrap().push("prune");
            Ok(vec![])
        });

        let mut status = MockStatusRepository::new();
        status
            .expect_list_runs()
            .returning(move || Ok(runs.clone()));

        let supervisor = ScheduleSupervisor::new(
            Arc::new(dbase),
            Arc::new(StateStoreImpl::new()),
            Arc::new(leader),
            Arc::new(status),
            60000,
        );
        (supervisor, fired)
    }

    fn run_finished_ago(operation: BackgroundOperation, hours: i64) -> TaskRun {
        let finished_at = Utc::now() - chrono::Duration::hours(hours);
        TaskRun {
            operation,
            dataset_id: None,
            started_at: finished_at - chrono::Duration::seconds(1),
            finished_at,
            outcome: crate::domain::entities::TaskOutcome::Success,
            issue_count: 0,
            summary: "done".into(),
        }
    }

    #[test]
    #[serial_test::serial]
    fn test_overdue_runs_everything_when_never_run() {
        // This is the case the catch-up exists for: a server restarted more
        // often than the interval never reaches the armed timer, so with no
        // recorded run every task must be treated as due.
        let (supervisor, fired) = overdue_fixture(vec![]);
        supervisor.run_overdue_tasks(&TaskIntervals::from_env());
        let fired = fired.lock().unwrap();
        assert!(fired.contains(&"prune"));
        assert!(fired.contains(&"restore_test"));
        assert!(fired.contains(&"database_scrub"));
        assert!(fired.contains(&"pack_prune"));
        assert!(fired.contains(&"workspace_cleanup"));
    }

    #[test]
    #[serial_test::serial]
    fn test_overdue_skips_recently_run_tasks() {
        // Defaults: prune and workspace cleanup every 24 hours, the rest
        // every 7 days. Everything here ran an hour ago, so nothing is due.
        let runs = vec![
            run_finished_ago(BackgroundOperation::Prune, 1),
            run_finished_ago(BackgroundOperation::RestoreTest, 1),
            run_finished_ago(BackgroundOperation::DatabaseScrub, 1),
            run_finished_ago(BackgroundOperation::PackPrune, 1),
            run_finished_ago(BackgroundOperation::WorkspaceCleanup, 1),
        ];
        let (supervisor, fired) = overdue_fixture(runs);
        supervisor.run_overdue_tasks(&TaskIntervals::from_env());
        assert!(fired.lock().unwrap().is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn test_overdue_runs_only_elapsed_tasks() {
        // Two days since each ran: the daily tasks are due, the weekly ones
        // are not.
        let runs = vec![
            run_finished_ago(BackgroundOperation::Prune, 48),
            run_finished_ago(BackgroundOperation::RestoreTest, 48),
            run_finished_ago(BackgroundOperation::DatabaseScrub, 48),
            run_finished_ago(BackgroundOperation::PackPrune, 48),
            run_finished_ago(BackgroundOperation::WorkspaceCleanup, 48),
        ];
        let (supervisor, fired) = overdue_fixture(runs);
        supervisor.run_overdue_tasks(&TaskIntervals::from_env());
        let fired = fired.lock().unwrap();
        assert!(fired.contains(&"prune"));
        assert!(fired.contains(&"workspace_cleanup"));
        assert!(!fired.contains(&"restore_test"));
        assert!(!fired.contains(&"database_scrub"));
        assert!(!fired.contains(&"pack_prune"));
    }

    #[test]
    #[serial_test::serial]
    fn test_overdue_uses_most_recent_run_of_a_task() {
        // Snapshot pruning records a row per dataset. The task as a whole is
        // only overdue if even the most recent of them has aged out.
        let mut old = run_finished_ago(BackgroundOperation::Prune, 100);
        old.dataset_id = Some("ds1".into());
        let mut recent = run_finished_ago(BackgroundOperation::Prune, 1);
        recent.dataset_id = Some("ds2".into());
        let (supervisor, fired) = overdue_fixture(vec![old, recent]);
        supervisor.run_overdue_tasks(&TaskIntervals::from_env());
        assert!(!fired.lock().unwrap().contains(&"prune"));
    }
}
