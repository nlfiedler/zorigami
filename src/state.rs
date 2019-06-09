//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `state` module manages the application state.

use lazy_static::lazy_static;
use reducer::{Dispatcher, Reactor, Reducer, Store};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

lazy_static! {
    /// Set of registered state change listeners.
    static ref REACTORS: Mutex<HashMap<String, Subscription<State>>> = {
        let listeners: HashMap<String, Subscription<State>> = HashMap::new();
        Mutex::new(listeners)
    };
    /// Mutable shared reference to the redux store.
    static ref STORE: Mutex<Store<State, Display>> =
        Mutex::new(Store::new(State::default(), Display));
}

///
/// Dispatch the given action to the store.
///
pub fn dispatch(action: Action) {
    let mut store = STORE.lock().unwrap();
    store.dispatch(action);
}

///
/// Get a copy of the current state.
///
pub fn get_state() -> State {
    let store = STORE.lock().unwrap();
    store.clone()
}

///
/// Actions are dispatched to the store to update the application state.
///
#[derive(Clone, Debug)]
pub enum Action {
    /// Reset the counters for the backup of a given dataset.
    StartBackup(String),
    /// Increment the pack upload count for a dataset.
    UploadPack(String),
    /// Increase the file upload count for a dataset by the given amount.
    UploadFiles(String, u64),
    /// Set the completion time for the backup of a given dataset.
    FinishBackup(String),
}

///
/// The state of the backup process for a particular dataset.
///
#[derive(Clone)]
pub struct BackupState {
    start_time: SystemTime,
    end_time: Option<SystemTime>,
    packs_uploaded: u64,
    files_uploaded: u64,
}

impl Default for BackupState {
    fn default() -> Self {
        Self {
            start_time: SystemTime::now(),
            end_time: None,
            packs_uploaded: 0,
            files_uploaded: 0,
        }
    }
}

impl BackupState {
    ///
    /// Return the start time for the backup of this dataset.
    ///
    pub fn start_time(&self) -> SystemTime {
        self.start_time
    }

    ///
    /// Return the completion time for the backup of this dataset.
    ///
    pub fn end_time(&self) -> Option<SystemTime> {
        self.end_time
    }

    ///
    /// Return the number of packs uploaded for this dataset.
    ///
    pub fn packs_uploaded(&self) -> u64 {
        self.packs_uploaded
    }

    ///
    /// Return the number of files uploaded for this dataset.
    ///
    pub fn files_uploaded(&self) -> u64 {
        self.files_uploaded
    }
}

///
/// The state of the application. All changes are effected by dispatching
/// actions via the `dispatch()` function.
///
pub struct State {
    /// Backup progress is tracked by the dataset identifier.
    backups: HashMap<String, BackupState>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            backups: HashMap::new(),
        }
    }
}

impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            backups: self.backups.clone(),
        }
    }
}

impl Reducer<Action> for State {
    fn reduce(&mut self, action: Action) {
        match action {
            Action::StartBackup(key) => {
                self.backups.insert(key, BackupState::default());
            }
            Action::UploadPack(key) => {
                let record = self.backups.get_mut(&key).unwrap();
                record.packs_uploaded += 1;
            }
            Action::UploadFiles(key, inc) => {
                let record = self.backups.get_mut(&key).unwrap();
                record.files_uploaded += inc;
            }
            Action::FinishBackup(key) => {
                let record = self.backups.get_mut(&key).unwrap();
                record.end_time = Some(SystemTime::now());
            }
        }
    }
}

impl State {
    ///
    /// Return all of the datasets currently in the backups collection. Use
    /// the `backups()` accessor to access individual entries.
    ///
    pub fn active_datasets(&self) -> Vec<String> {
        self.backups.keys().cloned().collect()
    }

    ///
    /// Retrieve the backup state for the named dataset.
    ///
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
struct Display;

impl Reactor<State> for Display {
    type Output = ();
    fn react(&self, state: &State) -> Self::Output {
        // get a copy of the registered listeners to allow for a listener to
        // remove listeners from the list during the event dispatch
        let listeners: Vec<Subscription<State>> = {
            let reactor = REACTORS.lock().unwrap();
            reactor.values().map(|e| e.to_owned()).collect()
        };
        for entry in listeners {
            entry(state);
        }
    }
}

///
/// Function signature for a subscription.
///
pub type Subscription<State> = fn(&State);

///
/// Add the given callback as an action listener to the store.
///
pub fn subscribe(key: &str, listener: Subscription<State>) {
    let mut reactor = REACTORS.lock().unwrap();
    reactor.insert(key.to_owned(), listener);
}

///
/// Remove the callback associated with the given key.
///
pub fn unsubscribe(key: &str) {
    let mut reactor = REACTORS.lock().unwrap();
    reactor.remove(key);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_state() {
        let key = "mydataset";
        dispatch(Action::StartBackup(key.to_owned()));
        dispatch(Action::UploadPack(key.to_owned()));
        dispatch(Action::UploadPack(key.to_owned()));
        dispatch(Action::UploadFiles(key.to_owned(), 2));
        dispatch(Action::UploadFiles(key.to_owned(), 3));
        let state = get_state();
        let backup = state.backups("mydataset").unwrap();
        assert_eq!(backup.packs_uploaded(), 2);
        assert_eq!(backup.files_uploaded(), 5);
        assert!(get_state().backups("foobar").is_none());
    }
}
