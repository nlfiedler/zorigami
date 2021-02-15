//
// Copyright (c) 2020 Nathan Fiedler
//

//! The `state` module manages the application state.

use chrono::prelude::*;
#[cfg(test)]
use mockall::{automock, predicate::*};
use reducer::{Dispatcher, Reactor, Reducer, Store};
use std::collections::{hash_map, HashMap};
use std::sync::{Arc, Condvar, Mutex};

///
/// Function signature for a subscription to state changes.
///
pub type Subscription<State> = fn(&State, Option<&State>);

///
/// A `StateStore` receives events which are used to modify the application
/// state in a controlled and predictable fashion. Listeners can subscribe to be
/// informed of state changes.
///
#[cfg_attr(test, automock)]
pub trait StateStore: Send + Sync {
    /// Dispatch a backup related action to the store.
    fn backup_event(&self, action: BackupAction);

    /// Dispatch a supervisor related action to the store.
    fn supervisor_event(&self, action: SupervisorAction);

    /// Wait for the given action to have been received.
    ///
    /// This function will block the current **thread** of execution.
    fn wait_for_supervisor(&self, action: SupervisorAction);

    /// Get a copy of the current state.
    fn get_state(&self) -> State;

    /// Add the given callback as a state listener to the store.
    fn subscribe(&self, key: &str, listener: Subscription<State>);

    /// Remove the callback associated with the given key.
    fn unsubscribe(&self, key: &str);
}

///
/// Concrete implementation of `StateStore` that uses the `reducer` crate to
/// manage the application state and notify listeners of changes.
///
pub struct StateStoreImpl {
    // Mutable shared reference to the redux store.
    store: Mutex<Store<State, Display>>,
    // Used to allow callers to wait for supervisor events.
    super_var: Arc<(Mutex<SupervisorAction>, Condvar)>,
}

impl StateStoreImpl {
    /// Construct a new instance of StateStoreImpl.
    pub fn new() -> Self {
        let store = Store::new(State::default(), Display::default());
        let pair = Arc::new((Mutex::new(SupervisorAction::Stopped), Condvar::new()));
        Self {
            store: Mutex::new(store),
            super_var: pair,
        }
    }
}

impl StateStore for StateStoreImpl {
    fn backup_event(&self, action: BackupAction) {
        let mut store = self.store.lock().unwrap();
        let _ = store.dispatch(action);
    }

    fn supervisor_event(&self, action: SupervisorAction) {
        let mut store = self.store.lock().unwrap();
        let _ = store.dispatch(action.clone());
        drop(store);
        let pair = self.super_var.clone();
        let (lock, cvar) = &*pair;
        let mut actual = lock.lock().unwrap();
        *actual = action;
        cvar.notify_all();
    }

    fn wait_for_supervisor(&self, action: SupervisorAction) {
        let pair = self.super_var.clone();
        let (lock, cvar) = &*pair;
        let mut actual = lock.lock().unwrap();
        while *actual != action {
            actual = cvar.wait(actual).unwrap();
        }
    }

    fn get_state(&self) -> State {
        let store = self.store.lock().unwrap();
        store.clone()
    }

    fn subscribe(&self, key: &str, listener: Subscription<State>) {
        let action = SubscriberAction::Add(key.to_owned(), listener);
        let mut store = self.store.lock().unwrap();
        let _ = store.dispatch(action);
    }

    fn unsubscribe(&self, key: &str) {
        let action = SubscriberAction::Remove(key.to_owned());
        let mut store = self.store.lock().unwrap();
        let _ = store.dispatch(action);
    }
}

// Internal actions for managing the subscriber list.
#[derive(Clone)]
enum SubscriberAction {
    // Add a subscriber by the given key.
    Add(String, Subscription<State>),
    // Remove the subscriber with the given key.
    Remove(String),
}

///
/// Actions related to the state of a backup.
///
#[derive(Clone, Debug)]
pub enum BackupAction {
    /// Reset the counters for the backup of a given dataset.
    Start(String),
    /// Increment the pack upload count for a dataset.
    UploadPack(String),
    /// Increase the file upload count for a dataset by the given amount.
    UploadFiles(String, u64),
    /// Set the completion time for the backup of a given dataset.
    Finish(String),
    /// Sets the backup in the "error" state (dataset key and error message).
    Error(String, String),
    /// Sets the backup in the "paused" state.
    Pause(String),
    /// Clear the error state and end time to indicate a restart.
    Restart(String),
}

///
/// Actions, both imperative and informative, related to the supervisor that
/// manages the backup processes.
///
#[derive(Clone, Debug, PartialEq)]
pub enum SupervisorAction {
    /// Signal subscribers that the supervisor should be started.
    Start,
    /// Indicates that the supervisor has in fact started.
    Started,
    /// Signal subscribers that the supervisor should be stopped.
    Stop,
    /// Indicates that the supervisor has in fact stopped.
    Stopped,
}

///
/// The state of the backup process for a particular dataset.
///
#[derive(Clone)]
pub struct BackupState {
    start_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    packs_uploaded: u64,
    files_uploaded: u64,
    error_msg: Option<String>,
    paused: bool,
}

impl Default for BackupState {
    fn default() -> Self {
        Self {
            start_time: Utc::now(),
            end_time: None,
            packs_uploaded: 0,
            files_uploaded: 0,
            error_msg: None,
            paused: false,
        }
    }
}

impl BackupState {
    /// Return the start time for the backup of this dataset.
    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// Return the completion time for the backup of this dataset.
    pub fn end_time(&self) -> Option<DateTime<Utc>> {
        self.end_time
    }

    /// Return the number of packs uploaded for this dataset.
    pub fn packs_uploaded(&self) -> u64 {
        self.packs_uploaded
    }

    /// Return the number of files uploaded for this dataset.
    pub fn files_uploaded(&self) -> u64 {
        self.files_uploaded
    }

    /// Return the state of the error flag.
    pub fn had_error(&self) -> bool {
        self.error_msg.is_some()
    }

    /// Return the textual error message, if any.
    pub fn error_message(&self) -> Option<String> {
        self.error_msg.clone()
    }
    /// Return the state of the paused flag.
    pub fn is_paused(&self) -> bool {
        self.paused
    }
}

///
/// State of the supervisor process that manages backups.
///
#[derive(Clone, Debug, PartialEq)]
pub enum SupervisorState {
    /// Supervisor is in the process of starting.
    Starting,
    /// Supervisor has been instructed to start.
    Started,
    /// Supervisor is in the process of stopping.
    Stopping,
    /// Supervisor has not been started or was stopped.
    Stopped,
}

///
/// The entire state of the application.
///
pub struct State {
    /// Backup progress is tracked by the dataset identifier.
    backups: HashMap<String, BackupState>,
    /// Requested state of the supervisor process.
    pub supervisor: SupervisorState,
    /// Collection of subscribers to the application state.
    subscribers: HashMap<String, Subscription<State>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            backups: HashMap::new(),
            supervisor: SupervisorState::Stopped,
            subscribers: HashMap::new(),
        }
    }
}

impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            backups: self.backups.clone(),
            supervisor: self.supervisor.clone(),
            subscribers: self.subscribers.clone(),
        }
    }
}

impl Reducer<SubscriberAction> for State {
    fn reduce(&mut self, action: SubscriberAction) {
        match action {
            SubscriberAction::Add(key, listener) => {
                self.subscribers.insert(key, listener);
            }
            SubscriberAction::Remove(key) => {
                self.subscribers.remove(&key);
            }
        }
    }
}

impl Reducer<BackupAction> for State {
    fn reduce(&mut self, action: BackupAction) {
        match action {
            BackupAction::Start(key) => {
                self.backups.insert(key, BackupState::default());
            }
            BackupAction::UploadPack(key) => {
                if let Some(record) = self.backups.get_mut(&key) {
                    record.packs_uploaded += 1;
                }
            }
            BackupAction::UploadFiles(key, inc) => {
                if let Some(record) = self.backups.get_mut(&key) {
                    record.files_uploaded += inc;
                }
            }
            BackupAction::Finish(key) => {
                if let Some(record) = self.backups.get_mut(&key) {
                    record.end_time = Some(Utc::now());
                }
            }
            BackupAction::Error(key, msg) => {
                if let Some(record) = self.backups.get_mut(&key) {
                    record.error_msg = Some(msg);
                }
            }
            BackupAction::Pause(key) => {
                if let Some(record) = self.backups.get_mut(&key) {
                    record.paused = true;
                }
            }
            BackupAction::Restart(key) => {
                if let Some(record) = self.backups.get_mut(&key) {
                    record.error_msg = None;
                    record.paused = false;
                    record.end_time = None;
                }
            }
        }
    }
}

impl Reducer<SupervisorAction> for State {
    fn reduce(&mut self, action: SupervisorAction) {
        match action {
            SupervisorAction::Start => {
                self.supervisor = SupervisorState::Starting;
            }
            SupervisorAction::Started => {
                self.supervisor = SupervisorState::Started;
            }
            SupervisorAction::Stop => {
                self.supervisor = SupervisorState::Stopping;
            }
            SupervisorAction::Stopped => {
                self.supervisor = SupervisorState::Stopped;
            }
        }
    }
}

impl State {
    /// Return all of the datasets currently in the backups collection.
    pub fn active_datasets(&self) -> hash_map::Iter<String, BackupState> {
        self.backups.iter()
    }

    /// Retrieve the backup state for the named dataset.
    pub fn backups(&self, dataset: &str) -> Option<&BackupState> {
        if self.backups.contains_key(dataset) {
            Some(&self.backups[dataset])
        } else {
            None
        }
    }
}

///
/// Implementation of the `Reactor` trait that invokes all registered listeners
/// with the state whenever an action is dispatched.
///
struct Display {
    // copy of the previous application state
    previous: Option<State>,
}

impl Default for Display {
    fn default() -> Self {
        Self { previous: None }
    }
}

impl Reactor<State> for Display {
    type Error = std::io::Error;

    fn react(&mut self, state: &State) -> std::io::Result<()> {
        // get a copy of the registered listeners to allow for a listener to
        // remove listeners from the list during the event dispatch
        let listeners: Vec<Subscription<State>> =
            state.subscribers.values().map(|e| e.to_owned()).collect();
        for entry in listeners {
            entry(state, self.previous.as_ref());
        }
        self.previous = Some(state.to_owned());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_stats() {
        let key = "dataset0";
        let sut = StateStoreImpl::new();
        sut.backup_event(BackupAction::Start(key.to_owned()));
        sut.backup_event(BackupAction::UploadPack(key.to_owned()));
        sut.backup_event(BackupAction::UploadPack(key.to_owned()));
        sut.backup_event(BackupAction::UploadFiles(key.to_owned(), 2));
        sut.backup_event(BackupAction::UploadFiles(key.to_owned(), 3));
        let state = sut.get_state();
        let backup = state.backups(key).unwrap();
        assert_eq!(backup.packs_uploaded(), 2);
        assert_eq!(backup.files_uploaded(), 5);
        assert!(sut.get_state().backups("foobar").is_none());
    }

    #[test]
    fn test_errored_backup() {
        let key = "dataset1";
        let sut = StateStoreImpl::new();
        sut.backup_event(BackupAction::Start(key.to_owned()));
        sut.backup_event(BackupAction::Error(key.to_owned(), String::from("oh no")));
        let state = sut.get_state();
        let backup = state.backups(key).unwrap();
        assert_eq!(backup.had_error(), true);
        assert_eq!(backup.error_message(), Some(String::from("oh no")));
        assert_eq!(backup.is_paused(), false);
    }

    #[test]
    fn test_paused_backup() {
        let key = "dataset2";
        let sut = StateStoreImpl::new();
        sut.backup_event(BackupAction::Start(key.to_owned()));
        sut.backup_event(BackupAction::Pause(key.to_owned()));
        let state = sut.get_state();
        let backup = state.backups(key).unwrap();
        assert_eq!(backup.had_error(), false);
        assert_eq!(backup.is_paused(), true);
    }

    #[test]
    fn test_finished_backup() {
        let key = "dataset3";
        let sut = StateStoreImpl::new();
        sut.backup_event(BackupAction::Start(key.to_owned()));
        sut.backup_event(BackupAction::Finish(key.to_owned()));
        let state = sut.get_state();
        let backup = state.backups(key).unwrap();
        assert!(backup.end_time().is_some());
    }

    #[test]
    fn test_restarted_backup() {
        let key = "dataset4";
        let sut = StateStoreImpl::new();
        sut.backup_event(BackupAction::Start(key.to_owned()));
        sut.backup_event(BackupAction::Error(key.to_owned(), String::from("oh no")));
        sut.backup_event(BackupAction::Pause(key.to_owned()));
        sut.backup_event(BackupAction::Finish(key.to_owned()));
        let state = sut.get_state();
        let backup = state.backups(key).unwrap();
        assert_eq!(backup.had_error(), true);
        assert_eq!(backup.is_paused(), true);
        assert!(backup.end_time().is_some());
        sut.backup_event(BackupAction::Restart(key.to_owned()));
        let state = sut.get_state();
        let backup = state.backups(key).unwrap();
        assert_eq!(backup.had_error(), false);
        assert_eq!(backup.is_paused(), false);
        assert!(backup.end_time().is_none());
    }

    #[test]
    fn test_supervisor_start_stop() {
        let sut = StateStoreImpl::new();
        // assert initial state is "stopped"
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Stopped);
        sut.supervisor_event(SupervisorAction::Start);
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Starting);
        sut.supervisor_event(SupervisorAction::Started);
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Started);
        sut.supervisor_event(SupervisorAction::Stop);
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Stopping);
        sut.supervisor_event(SupervisorAction::Stopped);
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Stopped);
    }

    #[test]
    fn test_wait_for_supervisor() {
        let sut = Arc::new(StateStoreImpl::new());
        // assert initial state is "stopped"
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Stopped);
        // change supervisor to starting
        sut.supervisor_event(SupervisorAction::Start);
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Starting);
        // spawn a thread that sends the started event later
        let sut_clone = sut.clone();
        std::thread::spawn(move || {
            let delay = std::time::Duration::from_millis(250);
            std::thread::sleep(delay);
            sut_clone.supervisor_event(SupervisorAction::Started);
        });
        // wait on the current thread for the action to arrive
        sut.wait_for_supervisor(SupervisorAction::Started);
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Started);
        // wait for an action that has already arrived
        sut.supervisor_event(SupervisorAction::Stopped);
        sut.wait_for_supervisor(SupervisorAction::Stopped);
        let state = sut.get_state();
        assert_eq!(state.supervisor, SupervisorState::Stopped);
    }
}
