//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::repositories::RecordRepository;
use crate::shared::state;
use crate::tasks::backup;
use crate::tasks::prune;
use crate::tasks::restore;
use actix::prelude::*;
use anyhow::{Error, anyhow};
use chrono::Utc;
use log::{debug, error, info, warn};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, RwLock};

///
/// `RingLeader` receives requests for backups, restores, and prunes. The
/// requests are doled out to the various sibling modules that process each of
/// these types of requests. Requests are processed asynchronously by the leader
/// supervisor, however all requests are processed sequentially to avoid making
/// conflicting changes in the database (such as pruning while backing up).
///
/// All *restore* requests are processed first, then *backup* requests, and then
/// *pruning* requests are processed only when there is nothing else to do.
///
#[cfg_attr(test, automock)]
pub trait RingLeader: Send + Sync {
    /// Start a supervisor that will process all of the requests.
    fn start(&self, dbase: Arc<dyn RecordRepository>) -> Result<(), Error>;

    /// Signal the supervisor to stop.
    fn stop(&self) -> Result<(), Error>;

    /// Add the given restore request to the queue to be processed.
    fn restore(&self, request: restore::Request) -> Result<(), Error>;

    /// Return all pending, processing, and recently completed restore requests.
    fn restores(&self) -> Vec<restore::Request>;

    /// Cancel the pending restore request.
    ///
    /// Does not cancel the request if it has already begun the restoration
    /// process. Returns true if successfully cancelled.
    fn cancel_restore(&self, request_id: String) -> bool;

    /// Add the given backup request to the queue to be processed.
    fn backup(&self, request: backup::Request) -> Result<(), Error>;

    /// Retrieve the most recent backup request for a given dataset, if any.
    fn get_backup_by_dataset(&self, dataset_id: &str) -> Option<backup::Request>;

    /// Send a stop request to the running backup.
    fn cancel_backup(&self, request_id: String) -> Result<(), Error>;

    /// Add the given prune request to the queue to be processed.
    fn prune(&self, request: prune::Request) -> Result<(), Error>;

    /// Restore the database from the most recent snapshot on the given store.
    fn restore_database(&self, store_id: String, passphrase: String) -> Result<(), Error>;

    /// Run a self-test of the restore path on a random backed-up file.
    fn restore_test(&self, passphrase: String) -> Result<(), Error>;
}

///
/// Concrete implementation of `RingLeader` that uses the Actix actor framework
/// to spawn threads and send messages to actors to process requests.
///
pub struct RingLeaderImpl {
    // State to which events regarding the ringer leader are sent.
    state: Arc<dyn state::StateStore>,
    // Arbiter manages the supervised actor that initiates restores.
    runner: Arbiter,
    // Address of the supervisor actor, if it has been started.
    super_addr: Mutex<Option<Addr<LeaderSupervisor>>>,
    // Holder of the shared leader state.
    context: Arc<LeaderContext>,
}

impl RingLeaderImpl {
    /// Construct a new ring leader.
    pub fn new(state: Arc<dyn state::StateStore>) -> Self {
        // Create a single-threaded arbiter to run the supervised actor which
        // will process all of the various requests sequentially.
        Self {
            state,
            runner: Arbiter::new(),
            super_addr: Mutex::new(None),
            context: Arc::new(LeaderContext::default()),
        }
    }

    /// Construct a ring leader with various factory methods.
    pub fn with_factories(
        state: Arc<dyn state::StateStore>,
        restorer: Option<RestorerFactory>,
        backuper: Option<BackuperFactory>,
        pruner: Option<PrunerFactory>,
    ) -> Self {
        let context = LeaderContext {
            restorer_factory: restorer,
            backuper_factory: backuper,
            pruner_factory: pruner,
            ..Default::default()
        };
        // Create a single-threaded arbiter to run the supervised actor which
        // will process all of the various requests sequentially.
        Self {
            state,
            runner: Arbiter::new(),
            super_addr: Mutex::new(None),
            context: Arc::new(context),
        }
    }

    /// Send the `Process` message to the supervisor.
    fn start_processing(&self) -> Result<(), Error> {
        let su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.as_ref() {
            fn err_convert(err: SendError<Process>) -> Error {
                anyhow!(format!("RingLeaderImpl::restore(): {:?}", err))
            }
            addr.try_send(Process()).map_err(err_convert)
        } else {
            error!("must call start() first");
            Err(anyhow!("must call start() first"))
        }
    }

    /// Clear the completed restore requests list, for testing.
    pub fn reset_restores(&self) {
        let pair = self.context.completed.clone();
        let (lock, _cvar) = &*pair;
        let mut completed = lock.lock().unwrap();
        completed.clear();
    }

    /// Wait for at least one restore request to be completed, for testing.
    pub fn wait_for_restores(&self) {
        let pair = self.context.completed.clone();
        let (lock, cvar) = &*pair;
        let mut completed = lock.lock().unwrap();
        while completed.is_empty() {
            completed = cvar.wait(completed).unwrap();
        }
    }

    /// Clear the backups collection, for testing.
    pub fn reset_backups(&self) {
        let pair = self.context.backups.clone();
        let (lock, _cvar) = &*pair;
        let mut backups = lock.lock().unwrap();
        backups.clear();
    }

    /// Wait for at least one backup request to be completed, for testing.
    pub fn wait_for_backup(&self) {
        let pair = self.context.backups.clone();
        let (lock, cvar) = &*pair;
        let mut backups = lock.lock().unwrap();
        while !backups.values().any(|b| b.finished.is_some()) {
            backups = cvar.wait(backups).unwrap();
        }
    }

    /// Wait for all backup requests to be completed, for testing.
    ///
    /// The `expected` value assures that at least that many requests finish.
    pub fn wait_for_all_backups(&self, expected: usize) {
        let pair = self.context.backups.clone();
        let (lock, cvar) = &*pair;
        let mut backups = lock.lock().unwrap();
        while backups.len() < expected || !backups.values().all(|b| b.finished.is_some()) {
            backups = cvar.wait(backups).unwrap();
        }
    }

    /// Wait for at least one backup request to be paused, for testing.
    pub fn wait_for_paused_backup(&self) {
        let pair = self.context.backups.clone();
        let (lock, cvar) = &*pair;
        let mut backups = lock.lock().unwrap();
        while !backups.values().any(|b| b.status == backup::Status::PAUSED) {
            backups = cvar.wait(backups).unwrap();
        }
    }

    /// Wait for at least one backup request to have failed, for testing.
    pub fn wait_for_failed_backup(&self) {
        let pair = self.context.backups.clone();
        let (lock, cvar) = &*pair;
        let mut backups = lock.lock().unwrap();
        while !backups.values().any(|b| b.status == backup::Status::FAILED) {
            backups = cvar.wait(backups).unwrap();
        }
    }

    /// Wait for at least one prune request to be completed, for testing.
    pub fn wait_for_prunes(&self) {
        let pair = self.context.prunes.clone();
        let (lock, cvar) = &*pair;
        let mut prunes = lock.lock().unwrap();
        while !prunes.iter().any(|b| b.finished.is_some()) {
            prunes = cvar.wait(prunes).unwrap();
        }
    }

    /// Return recently processed prune requests that may still be running.
    pub fn prunes(&self) -> Vec<prune::Request> {
        let mut requests: Vec<prune::Request> = Vec::new();
        let pair = self.context.prunes.clone();
        let (lock, _cvar) = &*pair;
        let prunes = lock.lock().unwrap();
        let slices = prunes.as_slices();
        requests.extend_from_slice(slices.0);
        requests.extend_from_slice(slices.1);
        requests
    }
}

impl RingLeader for RingLeaderImpl {
    fn start(&self, dbase: Arc<dyn RecordRepository>) -> Result<(), Error> {
        let mut su_addr = self.super_addr.lock().unwrap();
        if su_addr.is_none() {
            // start supervisor within the arbiter created earlier
            let state = self.state.clone();
            let context = self.context.clone();
            let addr = actix::Supervisor::start_in_arbiter(&self.runner.handle(), move |_| {
                LeaderSupervisor::new(dbase, state, context)
            });
            *su_addr = Some(addr);
        }
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
        fn err_convert(err: SendError<Stop>) -> Error {
            anyhow!(format!("RingLeaderImpl::stop(): {:?}", err))
        }
        let mut su_addr = self.super_addr.lock().unwrap();
        match su_addr.take() {
            Some(addr) => addr.try_send(Stop()).map_err(err_convert),
            _ => Ok(()),
        }
    }

    fn restore(&self, request: restore::Request) -> Result<(), Error> {
        info!(
            "enqueue restore for {} into {}",
            request.entry,
            request.filepath.display()
        );
        let mut queue = self.context.incoming_restores.lock().unwrap();
        queue.push_back(request);
        self.start_processing()
    }

    fn restores(&self) -> Vec<restore::Request> {
        let mut requests: Vec<restore::Request> = Vec::new();
        let queue = self.context.incoming_restores.lock().unwrap();
        let slices = queue.as_slices();
        requests.extend_from_slice(slices.0);
        requests.extend_from_slice(slices.1);
        if let Some(ref req) = *self.context.processing.lock().unwrap() {
            requests.push(req.clone());
        }
        let pair = self.context.completed.clone();
        let (lock, _cvar) = &*pair;
        let completed = lock.lock().unwrap();
        let slices = completed.as_slices();
        requests.extend_from_slice(slices.0);
        requests.extend_from_slice(slices.1);
        requests
    }

    fn cancel_restore(&self, request_id: String) -> bool {
        let mut queue = self.context.incoming_restores.lock().unwrap();
        let position = queue.iter().position(|r| r.id == request_id);
        if let Some(idx) = position {
            if let Some(mut req) = queue.remove(idx) {
                info!("cancelled restore for {}/{}", req.tree, req.entry);
                req.status = restore::Status::CANCELLED;
                self.context.push_completed_restore(req);
            }
            return true;
        } else {
            warn!("cancel, restore request not found");
        }
        false
    }

    fn backup(&self, request: backup::Request) -> Result<(), Error> {
        info!("enqueue backup for {}", request.dataset);
        let mut map = self.context.incoming_backups.lock().unwrap();
        map.push_back(request);
        self.start_processing()
    }

    fn get_backup_by_dataset(&self, dataset_id: &str) -> Option<backup::Request> {
        let pair = self.context.backups.clone();
        let (lock, _cvar) = &*pair;
        let map = lock.lock().unwrap();
        map.get(dataset_id).map(|r| r.to_owned())
    }

    fn cancel_backup(&self, request_id: String) -> Result<(), Error> {
        // search for the backup in the queue and remove it
        let mut queue = self.context.incoming_backups.lock().unwrap();
        let position = queue.iter().position(|r| r.id == request_id);
        if let Some(idx) = position {
            if let Some(req) = queue.remove(idx) {
                info!("cancelled backup for {}", req.dataset);
            }
            return Ok(());
        }

        // if the request is not in the queue, maybe it is running, in which
        // case look for the request among the collection of backups and if
        // a match is found, signal the backup process to stop and change the
        // request status to reflect the change
        let pair = self.context.backups.clone();
        let (lock, _cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id && request.status == backup::Status::RUNNING {
                info!("running backup {} will stop soon", request_id);
                let mut stopper = self.context.backup_stopper.write().unwrap();
                *stopper = true;
                request.status = backup::Status::STOPPING;
            }
        }
        Ok(())
    }

    fn prune(&self, request: prune::Request) -> Result<(), Error> {
        info!("enqueue prune for {}", request.dataset);
        let mut map = self.context.incoming_prunes.lock().unwrap();
        map.push_back(request);
        self.start_processing()
    }

    fn restore_database(&self, store_id: String, passphrase: String) -> Result<(), Error> {
        info!("enqueue database restore from {}", store_id);
        let su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.as_ref() {
            fn err_convert(err: SendError<RestoreDatabase>) -> Error {
                anyhow!(format!("RingLeaderImpl::restore_database(): {:?}", err))
            }
            addr.try_send(RestoreDatabase {
                store_id,
                passphrase,
            })
            .map_err(err_convert)
        } else {
            error!("must call start() first");
            Err(anyhow!("must call start() first"))
        }
    }

    fn restore_test(&self, passphrase: String) -> Result<(), Error> {
        info!("enqueue restore test");
        let su_addr = self.super_addr.lock().unwrap();
        if let Some(addr) = su_addr.as_ref() {
            fn err_convert(err: SendError<RestoreTest>) -> Error {
                anyhow!(format!("RingLeaderImpl::restore_test(): {:?}", err))
            }
            addr.try_send(RestoreTest { passphrase })
                .map_err(err_convert)
        } else {
            error!("must call start() first");
            Err(anyhow!("must call start() first"))
        }
    }
}

//
// Supervised actor that processes requests in sequential order.
//
struct LeaderSupervisor {
    // Database connection needed by all of the request processors.
    dbase: Arc<dyn RecordRepository>,
    // State to which events regarding the ringer leader are sent.
    state: Arc<dyn state::StateStore>,
    // Holder of the shared leader state.
    context: Arc<LeaderContext>,
}

impl LeaderSupervisor {
    fn new(
        dbase: Arc<dyn RecordRepository>,
        state: Arc<dyn state::StateStore>,
        context: Arc<LeaderContext>,
    ) -> Self {
        Self {
            dbase,
            state,
            context,
        }
    }

    /// Process all of the requests in the various queues.
    ///
    /// Returns `true` if a request was processed, or `false` if there were no requests.
    fn process_queues(&self) -> bool {
        let mut did_something = false;
        while self.process_restore() || self.process_backup() || self.process_prune() {
            // Process a single request on each iteration, doing either a
            // restore, or a backup, or a prune request on each pass. With this
            // logic, all restore operations will be processed before anything
            // else, followed by backups, and then finally prunes.
            did_something = true;
        }
        did_something
    }

    /// Process a single restore request from the queue.
    ///
    /// Returns `true` if a request was processed, or `false` if there were no requests.
    fn process_restore(&self) -> bool {
        if std::env::var("RESTORE_ALWAYS_PENDING").is_ok() {
            // pretend there are no restore requests to be processed
            return false;
        }
        if std::env::var("RESTORE_ALWAYS_PROCESSING").is_ok() {
            // if a request is already processing, pretend we're busy
            let working = self.context.processing.lock().unwrap();
            if working.is_some() {
                return false;
            }
        }

        let mut queue = self.context.incoming_restores.lock().unwrap();
        if let Some(request) = queue.pop_front() {
            drop(queue);
            let mut stopper = self.context.restore_stopper.write().unwrap();
            *stopper = false;
            drop(stopper);
            let restorer = if let Some(factory) = self.context.restorer_factory {
                factory(
                    self.dbase.clone(),
                    self.context.clone(),
                    self.context.restore_stopper.clone(),
                )
            } else {
                Box::new(restore::RestorerImpl::new(
                    self.dbase.clone(),
                    self.context.clone(),
                    self.context.restore_stopper.clone(),
                ))
            };
            let mut working = self.context.processing.lock().unwrap();
            *working = Some(request.clone());
            drop(working);
            if let Err(err) = restorer.restore_files(request) {
                error!("leader supervisor restore error: {}", err);
            }
            if std::env::var("RESTORE_ALWAYS_PROCESSING").is_err() {
                let mut working = self.context.processing.lock().unwrap();
                let request = working.take().unwrap();
                self.context.push_completed_restore(request);
            }
            true
        } else {
            false
        }
    }

    /// Process a single backup request from the queue.
    ///
    /// Returns `true` if a request was processed, or `false` if there were no requests.
    fn process_backup(&self) -> bool {
        let mut queue = self.context.incoming_backups.lock().unwrap();
        if let Some(request) = queue.pop_front() {
            drop(queue);
            let mut stopper = self.context.backup_stopper.write().unwrap();
            *stopper = false;
            drop(stopper);
            let backuper = if let Some(factory) = self.context.backuper_factory {
                factory(
                    self.dbase.clone(),
                    self.context.clone(),
                    self.context.backup_stopper.clone(),
                )
            } else {
                Box::new(backup::BackuperImpl::new(
                    self.dbase.clone(),
                    self.context.clone(),
                    self.context.backup_stopper.clone(),
                ))
            };
            self.context.insert_started_backup(request.clone());
            if let Err(err) = backuper.backup(request) {
                error!("leader supervisor backup error: {}", err);
            }
            true
        } else {
            false
        }
    }

    /// Process a single prune request from the queue.
    ///
    /// Returns `true` if a request was processed, or `false` if there were no requests.
    fn process_prune(&self) -> bool {
        let mut queue = self.context.incoming_prunes.lock().unwrap();
        if let Some(request) = queue.pop_front() {
            drop(queue);
            let mut stopper = self.context.prune_stopper.write().unwrap();
            *stopper = false;
            drop(stopper);
            let pruner = if let Some(factory) = self.context.pruner_factory {
                factory(
                    self.dbase.clone(),
                    self.context.clone(),
                    self.context.prune_stopper.clone(),
                )
            } else {
                Box::new(prune::PrunerImpl::new(
                    self.dbase.clone(),
                    self.context.clone(),
                    self.context.prune_stopper.clone(),
                ))
            };
            self.context.push_started_prune(request.clone());
            if let Err(err) = pruner.prune_snapshots(request) {
                error!("leader supervisor prune error: {}", err);
            }
            true
        } else {
            false
        }
    }

    /// Restore the database from the most recent snapshot on the pack store.
    fn restore_database(&self, store_id: String, passphrase: String) -> Result<(), Error> {
        // because the actor is running inside a single-threaded arbiter, we can
        // safely clear all of the queues and colletions
        {
            let mut coll = self.context.incoming_backups.lock().unwrap();
            coll.clear();
            let mut coll = self.context.incoming_restores.lock().unwrap();
            coll.clear();
            let mut coll = self.context.incoming_prunes.lock().unwrap();
            coll.clear();
            let pair = self.context.backups.clone();
            let (lock, _cvar) = &*pair;
            let mut coll = lock.lock().unwrap();
            coll.clear();
            let pair = self.context.completed.clone();
            let (lock, _cvar) = &*pair;
            let mut coll = lock.lock().unwrap();
            coll.clear();
        }

        let mut stopper = self.context.restore_stopper.write().unwrap();
        *stopper = false;
        drop(stopper);
        let restorer = if let Some(factory) = self.context.restorer_factory {
            factory(
                self.dbase.clone(),
                self.context.clone(),
                self.context.restore_stopper.clone(),
            )
        } else {
            Box::new(restore::RestorerImpl::new(
                self.dbase.clone(),
                self.context.clone(),
                self.context.restore_stopper.clone(),
            ))
        };
        if let Err(err) = restorer.restore_database(&store_id, &passphrase) {
            error!("leader supervisor database restore error: {}", err);
        }
        Ok(())
    }

    /// Run a restore self-test. Unlike `restore_database`, this does not clear
    /// the other queues; the single-threaded arbiter serializes this handler
    /// with the normal `Process` handling so user work is preserved.
    fn restore_test(&self, passphrase: String) -> Result<(), Error> {
        let mut stopper = self.context.restore_stopper.write().unwrap();
        *stopper = false;
        drop(stopper);
        let restorer = if let Some(factory) = self.context.restorer_factory {
            factory(
                self.dbase.clone(),
                self.context.clone(),
                self.context.restore_stopper.clone(),
            )
        } else {
            Box::new(restore::RestorerImpl::new(
                self.dbase.clone(),
                self.context.clone(),
                self.context.restore_stopper.clone(),
            ))
        };
        if let Err(err) = restorer.restore_test(&passphrase) {
            error!("leader supervisor restore test error: {}", err);
        }
        Ok(())
    }
}

impl Actor for LeaderSupervisor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        debug!("leader supervisor started");
        self.state.leader_event(state::LeaderAction::Started);
    }

    fn stopping(&mut self, _ctx: &mut Context<Self>) -> Running {
        debug!("leader supervisor stopping");
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        debug!("leader supervisor stopped");
    }
}

impl Supervised for LeaderSupervisor {
    fn restarting(&mut self, _ctx: &mut Context<LeaderSupervisor>) {
        warn!("leader supervisor restarting");
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Stop();

impl Handler<Stop> for LeaderSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<LeaderSupervisor>) {
        debug!("leader supervisor received Stop message");
        self.state.leader_event(state::LeaderAction::Stopped);
        ctx.stop();
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Process();

impl Handler<Process> for LeaderSupervisor {
    type Result = ();

    fn handle(&mut self, _msg: Process, _ctx: &mut Context<LeaderSupervisor>) {
        debug!("leader supervisor received Process message");
        self.process_queues();
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct RestoreDatabase {
    store_id: String,
    passphrase: String,
}

impl Handler<RestoreDatabase> for LeaderSupervisor {
    type Result = ();

    fn handle(&mut self, msg: RestoreDatabase, _ctx: &mut Context<LeaderSupervisor>) {
        debug!("leader supervisor received RestoreDatabase message");
        if let Err(err) = self.restore_database(msg.store_id, msg.passphrase) {
            error!("RestoreDatabase error: {}", err);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct RestoreTest {
    passphrase: String,
}

impl Handler<RestoreTest> for LeaderSupervisor {
    type Result = ();

    fn handle(&mut self, msg: RestoreTest, _ctx: &mut Context<LeaderSupervisor>) {
        debug!("leader supervisor received RestoreTest message");
        if let Err(err) = self.restore_test(msg.passphrase) {
            error!("RestoreTest error: {}", err);
        }
    }
}

type BackuperFactory = fn(
    dbase: Arc<dyn RecordRepository>,
    subscriber: Arc<dyn backup::Subscriber>,
    stop_requested: Arc<RwLock<bool>>,
) -> Box<dyn backup::Backuper>;

type RestorerFactory = fn(
    dbase: Arc<dyn RecordRepository>,
    subscriber: Arc<dyn restore::Subscriber>,
    stop_requested: Arc<RwLock<bool>>,
) -> Box<dyn restore::Restorer>;

type PrunerFactory = fn(
    dbase: Arc<dyn RecordRepository>,
    subscriber: Arc<dyn prune::Subscriber>,
    stop_requested: Arc<RwLock<bool>>,
) -> Box<dyn prune::Pruner>;

/// Holds the many constituents of the leader state that are shared between the
/// ring leader implementation and the supervised actor that processes requests.
#[derive(Clone, Default)]
struct LeaderContext {
    // Queue of incoming backup requests waiting to be processed.
    incoming_backups: Arc<Mutex<VecDeque<backup::Request>>>,
    // Queue of incoming restore requests waiting to be processed.
    incoming_restores: Arc<Mutex<VecDeque<restore::Request>>>,
    // Queue of incoming prune requests waiting to be processed.
    incoming_prunes: Arc<Mutex<VecDeque<prune::Request>>>,
    // Most recent backup request for each dataset, keyed by dataset identifier.
    backups: Arc<(Mutex<HashMap<String, backup::Request>>, Condvar)>,
    // Restore request actively being processed.
    processing: Arc<Mutex<Option<restore::Request>>>,
    // Limited number of recently completed restore requests.
    completed: Arc<(Mutex<VecDeque<restore::Request>>, Condvar)>,
    // Limited number of recently completed prune requests.
    prunes: Arc<(Mutex<VecDeque<prune::Request>>, Condvar)>,
    // Factory method for building a restorer implementation.
    restorer_factory: Option<RestorerFactory>,
    // Factory method for building a backuper implementation.
    backuper_factory: Option<BackuperFactory>,
    // Factory method for building a pruner implementation.
    pruner_factory: Option<PrunerFactory>,
    // Set to true to stop a restore from running.
    restore_stopper: Arc<RwLock<bool>>,
    // Set to true to stop a backup from running.
    backup_stopper: Arc<RwLock<bool>>,
    // Set to true to stop a prune from running.
    prune_stopper: Arc<RwLock<bool>>,
}

impl LeaderContext {
    /// Put the restore request into the completed set, trim the set to size.
    fn push_completed_restore(&self, request: restore::Request) {
        let pair = self.completed.clone();
        let (lock, cvar) = &*pair;
        let mut completed = lock.lock().unwrap();
        // Push the completed request to the front of the list and truncate the
        // older items to keep the list from growing indefinitely.
        completed.push_front(request);
        completed.truncate(32);
        // the condvar is for testing
        cvar.notify_all();
    }

    /// Insert the backup request into the collection to allow the subscriber to
    /// update the progress of the request.
    fn insert_started_backup(&self, request: backup::Request) {
        let pair = self.backups.clone();
        let (lock, _cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        map.insert(request.dataset.clone(), request);
    }

    /// Put the prune request into the collection, trim the set to size.
    fn push_started_prune(&self, request: prune::Request) {
        let pair = self.prunes.clone();
        let (lock, cvar) = &*pair;
        let mut prunes = lock.lock().unwrap();
        // Push the request to the front of the list and truncate the older
        // items to keep the list from growing indefinitely.
        prunes.push_front(request);
        prunes.truncate(32);
        // the condvar is for testing
        cvar.notify_all();
    }
}

impl backup::Subscriber for LeaderContext {
    fn started(&self, request_id: &str) -> bool {
        let pair = self.backups.clone();
        let (lock, cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.started = Some(Utc::now());
                request.status = backup::Status::RUNNING;
                break;
            }
        }
        // notify the waiting tests
        cvar.notify_all();
        true
    }

    fn files_changed(&self, request_id: &str, count: u64) -> bool {
        let pair = self.backups.clone();
        let (lock, _cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.changed_files = count;
                break;
            }
        }
        true
    }

    fn pack_uploaded(&self, request_id: &str) -> bool {
        let pair = self.backups.clone();
        let (lock, _cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.packs_uploaded += 1;
                break;
            }
        }
        true
    }

    fn bytes_uploaded(&self, request_id: &str, addend: u64) -> bool {
        let pair = self.backups.clone();
        let (lock, _cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.bytes_uploaded += addend;
                break;
            }
        }
        true
    }

    fn files_uploaded(&self, request_id: &str, addend: u64) -> bool {
        let pair = self.backups.clone();
        let (lock, _cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.files_uploaded += addend;
                break;
            }
        }
        true
    }

    fn error(&self, request_id: &str, error: String) -> bool {
        let pair = self.backups.clone();
        let (lock, cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.status = backup::Status::FAILED;
                request.errors.push(error);
                break;
            }
        }
        // notify the waiting tests
        cvar.notify_all();
        true
    }

    fn paused(&self, request_id: &str) -> bool {
        let pair = self.backups.clone();
        let (lock, cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.status = backup::Status::PAUSED;
                break;
            }
        }
        // notify the waiting tests
        cvar.notify_all();
        true
    }

    fn restarted(&self, request_id: &str) -> bool {
        let pair = self.backups.clone();
        let (lock, cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.status = backup::Status::RUNNING;
                request.finished = None;
                break;
            }
        }
        // notify the waiting tests
        cvar.notify_all();
        true
    }

    fn finished(&self, request_id: &str) -> bool {
        let pair = self.backups.clone();
        let (lock, cvar) = &*pair;
        let mut map = lock.lock().unwrap();
        for request in map.values_mut() {
            if request.id == request_id {
                request.status = backup::Status::COMPLETED;
                request.finished = Some(Utc::now());
                break;
            }
        }
        // notify the waiting tests
        cvar.notify_all();
        true
    }
}

impl restore::Subscriber for LeaderContext {
    fn started(&self, _request_id: &str) -> bool {
        if let Some(req) = self.processing.lock().unwrap().as_mut() {
            req.started = Some(Utc::now());
            req.status = restore::Status::RUNNING;
        }
        false
    }

    fn restored(&self, _request_id: &str, addend: u64) -> bool {
        if let Some(req) = self.processing.lock().unwrap().as_mut() {
            req.files_restored += addend;
        }
        false
    }

    fn error(&self, _request_id: &str, error: String) -> bool {
        if let Some(req) = self.processing.lock().unwrap().as_mut() {
            // avoid collecting so many errors that we run out of memory
            if req.errors.len() < 100 {
                req.errors.push(error);
            }
        }
        false
    }

    fn finished(&self, _request_id: &str) -> bool {
        if let Some(req) = self.processing.lock().unwrap().as_mut() {
            req.finished = Some(Utc::now());
            req.status = restore::Status::COMPLETED;
        }
        false
    }
}

impl prune::Subscriber for LeaderContext {
    fn started(&self, request_id: &str) -> bool {
        let pair = self.prunes.clone();
        let (lock, cvar) = &*pair;
        let mut coll = lock.lock().unwrap();
        for request in coll.iter_mut() {
            if request.id == request_id {
                request.started = Some(Utc::now());
                request.status = prune::Status::RUNNING;
                break;
            }
        }
        // notify the waiting tests
        cvar.notify_all();
        true
    }

    fn finished(&self, request_id: &str) -> bool {
        let pair = self.prunes.clone();
        let (lock, cvar) = &*pair;
        let mut coll = lock.lock().unwrap();
        for request in coll.iter_mut() {
            if request.id == request_id {
                request.status = prune::Status::COMPLETED;
                request.finished = Some(Utc::now());
                break;
            }
        }
        // notify the waiting tests
        cvar.notify_all();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::Checksum;
    use crate::domain::repositories::MockRecordRepository;
    use crate::shared::state::{StateStore, StateStoreImpl};
    use std::path::PathBuf;

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_start_stop_restart() -> std::io::Result<()> {
        // arrange
        let repo = Arc::new(MockRecordRepository::new());
        // start
        let state: Arc<dyn StateStore> = Arc::new(StateStoreImpl::new());
        let sut = RingLeaderImpl::new(state.clone());
        let result = sut.start(repo.clone());
        assert!(result.is_ok());
        state.wait_for_leader(state::LeaderAction::Started);
        // stop
        let result = sut.stop();
        assert!(result.is_ok());
        state.wait_for_leader(state::LeaderAction::Stopped);
        // restart
        let result = sut.start(repo);
        assert!(result.is_ok());
        state.wait_for_leader(state::LeaderAction::Started);
        Ok(())
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_backup_order() {
        fn b_factory(
            _dbase: Arc<dyn RecordRepository>,
            subscriber: Arc<dyn backup::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn backup::Backuper> {
            let subscriber = subscriber.clone();
            let mut backuper = backup::MockBackuper::new();
            backuper.expect_backup().return_once(move |request| {
                // go through the complete lifecycle and add enough time so that
                // the finished date-time will be sufficiently different to
                // assert the order of completion
                subscriber.started(&request.id);
                subscriber.files_changed(&request.id, 42);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.files_uploaded(&request.id, 21);
                subscriber.bytes_uploaded(&request.id, 524288);
                subscriber.pack_uploaded(&request.id);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.files_uploaded(&request.id, 21);
                subscriber.bytes_uploaded(&request.id, 524288);
                subscriber.pack_uploaded(&request.id);
                subscriber.finished(&request.id);
                Ok(None)
            });
            Box::new(backuper)
        }
        let state = Arc::new(state::StateStoreImpl::new());
        let sut = RingLeaderImpl::with_factories(state, None, Some(b_factory), None);
        let mock = MockRecordRepository::new();
        assert!(sut.start(Arc::new(mock)).is_ok());

        let dataset_1_id = xid::new().to_string();
        let input = backup::Request::new(dataset_1_id.clone(), "tiger", None);
        assert!(sut.backup(input).is_ok());

        let dataset_2_id = xid::new().to_string();
        let input = backup::Request::new(dataset_2_id.clone(), "tiger", None);
        assert!(sut.backup(input).is_ok());

        let dataset_3_id = xid::new().to_string();
        let input = backup::Request::new(dataset_3_id.clone(), "tiger", None);
        assert!(sut.backup(input).is_ok());

        // wait for all backups to complete then compare their running times to
        // ensure that the requests are processed in the order they are received
        // and that they do not overlap
        sut.wait_for_all_backups(3);
        let request_1 = sut.get_backup_by_dataset(&dataset_1_id).unwrap();
        assert_eq!(request_1.status, backup::Status::COMPLETED);
        assert_eq!(request_1.changed_files, 42);
        assert_eq!(request_1.files_uploaded, 42);
        assert_eq!(request_1.bytes_uploaded, 1048576);
        assert_eq!(request_1.packs_uploaded, 2);
        let request_2 = sut.get_backup_by_dataset(&dataset_2_id).unwrap();
        assert_eq!(request_1.status, backup::Status::COMPLETED);
        let request_3 = sut.get_backup_by_dataset(&dataset_3_id).unwrap();
        assert_eq!(request_1.status, backup::Status::COMPLETED);
        assert!(request_1.finished.unwrap() <= request_2.started.unwrap());
        assert!(request_2.finished.unwrap() <= request_3.started.unwrap());
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_backup_paused() {
        fn b_factory(
            _dbase: Arc<dyn RecordRepository>,
            subscriber: Arc<dyn backup::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn backup::Backuper> {
            let subscriber = subscriber.clone();
            let mut backuper = backup::MockBackuper::new();
            backuper.expect_backup().return_once(move |request| {
                subscriber.started(&request.id);
                subscriber.files_changed(&request.id, 42);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.files_uploaded(&request.id, 21);
                subscriber.bytes_uploaded(&request.id, 524288);
                subscriber.pack_uploaded(&request.id);
                subscriber.paused(&request.id);
                Err(Error::from(backup::OutOfTimeFailure))
            });
            Box::new(backuper)
        }
        let state = Arc::new(state::StateStoreImpl::new());
        let sut = RingLeaderImpl::with_factories(state, None, Some(b_factory), None);
        let mock = MockRecordRepository::new();
        assert!(sut.start(Arc::new(mock)).is_ok());

        let dataset_1_id = xid::new().to_string();
        let input = backup::Request::new(dataset_1_id.clone(), "tiger", None);
        assert!(sut.backup(input).is_ok());

        sut.wait_for_paused_backup();
        let request_1 = sut.get_backup_by_dataset(&dataset_1_id).unwrap();
        assert_eq!(request_1.status, backup::Status::PAUSED);
        assert_eq!(request_1.changed_files, 42);
        assert_eq!(request_1.files_uploaded, 21);
        assert_eq!(request_1.bytes_uploaded, 524288);
        assert_eq!(request_1.packs_uploaded, 1);
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_backup_failed() {
        fn b_factory(
            _dbase: Arc<dyn RecordRepository>,
            subscriber: Arc<dyn backup::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn backup::Backuper> {
            let subscriber = subscriber.clone();
            let mut backuper = backup::MockBackuper::new();
            backuper.expect_backup().return_once(move |request| {
                subscriber.started(&request.id);
                subscriber.files_changed(&request.id, 42);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.files_uploaded(&request.id, 21);
                subscriber.bytes_uploaded(&request.id, 524288);
                subscriber.pack_uploaded(&request.id);
                subscriber.error(&request.id, "oh no".into());
                Err(anyhow!("oh no"))
            });
            Box::new(backuper)
        }
        let state = Arc::new(state::StateStoreImpl::new());
        let sut = RingLeaderImpl::with_factories(state, None, Some(b_factory), None);
        let mock = MockRecordRepository::new();
        assert!(sut.start(Arc::new(mock)).is_ok());

        let dataset_1_id = xid::new().to_string();
        let input = backup::Request::new(dataset_1_id.clone(), "tiger", None);
        assert!(sut.backup(input).is_ok());

        sut.wait_for_failed_backup();
        let request_1 = sut.get_backup_by_dataset(&dataset_1_id).unwrap();
        assert_eq!(request_1.status, backup::Status::FAILED);
        assert_eq!(request_1.changed_files, 42);
        assert_eq!(request_1.files_uploaded, 21);
        assert_eq!(request_1.bytes_uploaded, 524288);
        assert_eq!(request_1.packs_uploaded, 1);
        assert_eq!(request_1.errors[0], "oh no");
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_backup_cancel() {
        fn b_factory(
            _dbase: Arc<dyn RecordRepository>,
            subscriber: Arc<dyn backup::Subscriber>,
            stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn backup::Backuper> {
            let subscriber = subscriber.clone();
            let mut backuper = backup::MockBackuper::new();
            backuper.expect_backup().return_once(move |request| {
                subscriber.started(&request.id);
                subscriber.files_changed(&request.id, 42);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.files_uploaded(&request.id, 21);
                subscriber.bytes_uploaded(&request.id, 524288);
                subscriber.pack_uploaded(&request.id);
                // sleep-wait for the stopper to become true, then exit
                loop {
                    let should_stop = stop_requested.read().unwrap();
                    if *should_stop {
                        subscriber.paused(&request.id);
                        return Err(Error::from(backup::OutOfTimeFailure {}));
                    }
                    drop(should_stop);
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            });
            Box::new(backuper)
        }
        let state = Arc::new(state::StateStoreImpl::new());
        let sut = RingLeaderImpl::with_factories(state, None, Some(b_factory), None);
        let mock = MockRecordRepository::new();
        assert!(sut.start(Arc::new(mock)).is_ok());

        let dataset_1_id = xid::new().to_string();
        let request_1 = backup::Request::new(dataset_1_id.clone(), "tiger", None);
        let request_1_id = request_1.id.clone();
        assert!(sut.backup(request_1).is_ok());

        let dataset_2_id = xid::new().to_string();
        let request_2 = backup::Request::new(dataset_2_id.clone(), "tiger", None);
        let request_2_id = request_2.id.clone();
        assert!(sut.backup(request_2).is_ok());

        // second one should be pending
        assert!(sut.cancel_backup(request_2_id).is_ok());
        // give the first backup request a chance to start running
        actix_rt::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(sut.cancel_backup(request_1_id).is_ok());

        sut.wait_for_paused_backup();
        let request_1 = sut.get_backup_by_dataset(&dataset_1_id).unwrap();
        // cancelling a running backup is the same as running out of time
        assert_eq!(request_1.status, backup::Status::PAUSED);
        // a cancelled pending request disappears completely
        assert!(sut.get_backup_by_dataset(&dataset_2_id).is_none());
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_request_priority() {
        fn r_factory(
            _dbase: Arc<dyn RecordRepository>,
            subscriber: Arc<dyn restore::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn restore::Restorer> {
            let subscriber = subscriber.clone();
            let mut restorer = restore::MockRestorer::new();
            restorer.expect_restore_files().return_once(move |request| {
                // go through the complete lifecycle and add enough time so that
                // the finished date-time will be sufficiently different to
                // assert the order of completion
                subscriber.started(&request.id);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.finished(&request.id);
                Ok(())
            });
            Box::new(restorer)
        }

        fn b_factory(
            _dbase: Arc<dyn RecordRepository>,
            subscriber: Arc<dyn backup::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn backup::Backuper> {
            let subscriber = subscriber.clone();
            let mut backuper = backup::MockBackuper::new();
            backuper.expect_backup().return_once(move |request| {
                // go through the complete lifecycle and add enough time so that
                // the finished date-time will be sufficiently different to
                // assert the order of completion
                subscriber.started(&request.id);
                subscriber.files_changed(&request.id, 42);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.files_uploaded(&request.id, 21);
                subscriber.bytes_uploaded(&request.id, 524288);
                subscriber.pack_uploaded(&request.id);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.files_uploaded(&request.id, 21);
                subscriber.bytes_uploaded(&request.id, 524288);
                subscriber.pack_uploaded(&request.id);
                subscriber.finished(&request.id);
                Ok(None)
            });
            Box::new(backuper)
        }

        fn p_factory(
            _dbase: Arc<dyn RecordRepository>,
            subscriber: Arc<dyn prune::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn prune::Pruner> {
            let subscriber = subscriber.clone();
            let mut pruner = prune::MockPruner::new();
            pruner.expect_prune_snapshots().return_once(move |request| {
                subscriber.started(&request.id);
                std::thread::sleep(std::time::Duration::from_secs(1));
                subscriber.finished(&request.id);
                Ok(1)
            });
            Box::new(pruner)
        }

        let state = Arc::new(state::StateStoreImpl::new());
        let sut = RingLeaderImpl::with_factories(
            state,
            Some(r_factory),
            Some(b_factory),
            Some(p_factory),
        );
        let mock = MockRecordRepository::new();
        assert!(sut.start(Arc::new(mock)).is_ok());

        // submit a backup, prune, another backup, and then a restore all while
        // the first backup may still be processing; once the first backup
        // finishes, the other requests should be processed in priority order

        let dataset_1_id = xid::new().to_string();
        let input = backup::Request::new(dataset_1_id.clone(), "tiger", None);
        assert!(sut.backup(input).is_ok());

        let input = prune::Request::new(dataset_1_id.clone());
        assert!(sut.prune(input).is_ok());

        let dataset_2_id = xid::new().to_string();
        let input = backup::Request::new(dataset_2_id.clone(), "tiger", None);
        assert!(sut.backup(input).is_ok());

        // give the first backup request a chance to start running
        actix_rt::time::sleep(std::time::Duration::from_millis(100)).await;

        let input = restore::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
            PathBuf::from("lorem-ipsum.txt"),
            "dataset1".into(),
            "password".into(),
        );
        assert!(sut.restore(input).is_ok());

        // wait for the prune to finish, which should be processed last due to
        // the priority of the restore, backup, and prune requests
        sut.wait_for_prunes();

        // assert that all of the requests have completed and that they were
        // processed in priority order
        let restores = sut.restores();
        assert_eq!(restores.len(), 1);
        assert_eq!(restores[0].status, restore::Status::COMPLETED);
        let prunes = sut.prunes();
        assert_eq!(prunes.len(), 1);
        assert_eq!(prunes[0].status, prune::Status::COMPLETED);
        let backup_1 = sut.get_backup_by_dataset(&dataset_1_id).unwrap();
        assert_eq!(backup_1.status, backup::Status::COMPLETED);
        let backup_2 = sut.get_backup_by_dataset(&dataset_2_id).unwrap();
        assert_eq!(backup_2.status, backup::Status::COMPLETED);
        assert!(backup_1.finished.unwrap() <= restores[0].started.unwrap());
        assert!(restores[0].finished.unwrap() <= backup_2.started.unwrap());
        assert!(backup_2.finished.unwrap() <= prunes[0].started.unwrap());
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_restore_requests() {
        fn r_factory(
            _dbase: Arc<dyn RecordRepository>,
            subscriber: Arc<dyn restore::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn restore::Restorer> {
            let subscriber = subscriber.clone();
            let mut restorer = restore::MockRestorer::new();
            restorer.expect_restore_files().return_once(move |request| {
                // go through the complete lifecycle and add enough time so that
                // the finished date-time will be sufficiently different to
                // assert the order of completion
                subscriber.started(&request.id);
                std::thread::sleep(std::time::Duration::from_secs(2));
                subscriber.restored(&request.id, 42);
                subscriber.error(&request.id, "oh no".into());
                subscriber.finished(&request.id);
                Ok(())
            });
            Box::new(restorer)
        }

        let state = Arc::new(state::StateStoreImpl::new());
        let sut = RingLeaderImpl::with_factories(state, Some(r_factory), None, None);
        let mock = MockRecordRepository::new();
        assert!(sut.start(Arc::new(mock)).is_ok());

        // enqueue 4 restore requests and immediately cancel the last one
        let input = restore::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
            PathBuf::from("lorem-ipsum.txt"),
            "dataset1".into(),
            "password".into(),
        );
        assert!(sut.restore(input).is_ok());
        let input = restore::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
            PathBuf::from("lorem-ipsum.txt"),
            "dataset1".into(),
            "password".into(),
        );
        assert!(sut.restore(input).is_ok());
        let input = restore::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
            PathBuf::from("lorem-ipsum.txt"),
            "dataset1".into(),
            "password".into(),
        );
        assert!(sut.restore(input).is_ok());
        let input = restore::Request::new(
            Checksum::SHA1("cafebabe".into()),
            String::from("lorem-ipsum.txt"),
            PathBuf::from("lorem-ipsum.txt"),
            "dataset1".into(),
            "password".into(),
        );
        let input_id = input.id.clone();
        assert!(sut.restore(input).is_ok());

        // give the first request a chance to start running
        actix_rt::time::sleep(std::time::Duration::from_millis(100)).await;

        // wait for one restore to finish, cancel the last one, then collect a
        // snapshot of the state of the restores
        sut.wait_for_restores();
        assert!(sut.cancel_restore(input_id));
        let restores = sut.restores();
        assert_eq!(restores.len(), 4);
        assert_eq!(restores[0].status, restore::Status::PENDING);
        // due to timing of the test, the second request may have started
        assert!(
            restores[0].status == restore::Status::PENDING
                || restores[0].status == restore::Status::RUNNING
        );
        assert_eq!(restores[2].status, restore::Status::CANCELLED);
        assert_eq!(restores[3].status, restore::Status::COMPLETED);
        assert_eq!(restores[3].files_restored, 42);
        assert_eq!(restores[3].errors.len(), 1);
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_restore_database() {
        fn r_factory(
            _dbase: Arc<dyn RecordRepository>,
            _subscriber: Arc<dyn restore::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn restore::Restorer> {
            let mut restorer = restore::MockRestorer::new();
            restorer.expect_restore_database().return_once(move |_, _| {
                std::thread::sleep(std::time::Duration::from_secs(1));
                Ok(())
            });
            Box::new(restorer)
        }

        let state = Arc::new(state::StateStoreImpl::new());
        let sut = RingLeaderImpl::with_factories(state, Some(r_factory), None, None);
        let mock = MockRecordRepository::new();
        assert!(sut.start(Arc::new(mock)).is_ok());

        assert!(
            sut.restore_database("store_id".into(), "passphrase".into())
                .is_ok()
        );

        // give the first request a chance to start running
        actix_rt::time::sleep(std::time::Duration::from_millis(100)).await;

        // unsure how to confirm the database restore happened
    }

    #[actix_rt::test]
    #[serial_test::serial]
    async fn test_ring_leader_restore_test_preserves_queues() {
        // The RestoreTest handler MUST NOT clear the other queues (unlike
        // RestoreDatabase). A pending backup enqueued before restore_test is
        // called must still be present afterward.
        fn r_factory(
            _dbase: Arc<dyn RecordRepository>,
            _subscriber: Arc<dyn restore::Subscriber>,
            _stop_requested: Arc<RwLock<bool>>,
        ) -> Box<dyn restore::Restorer> {
            let mut restorer = restore::MockRestorer::new();
            restorer.expect_restore_test().return_once(|_| Ok(()));
            Box::new(restorer)
        }

        let state = Arc::new(state::StateStoreImpl::new());
        let sut = RingLeaderImpl::with_factories(state, Some(r_factory), None, None);
        let mock = MockRecordRepository::new();
        assert!(sut.start(Arc::new(mock)).is_ok());

        // manually stage a pending backup on the queue without triggering
        // process_queues (no real backup factory is registered)
        {
            let mut queue = sut.context.incoming_backups.lock().unwrap();
            queue.push_back(backup::Request::new("dataset-1".into(), "pw", None));
        }

        assert!(sut.restore_test("passphrase".into()).is_ok());

        // give the handler a chance to run
        actix_rt::time::sleep(std::time::Duration::from_millis(200)).await;

        let queue = sut.context.incoming_backups.lock().unwrap();
        assert_eq!(queue.len(), 1, "backup queue was cleared by restore_test");
    }
}
