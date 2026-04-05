//
// Copyright (c) 2019 Nathan Fiedler
//
#[cfg(test)]
use mockall::{automock, predicate::*};
use reducer::{Dispatcher, Reactor, Reducer, Store};
use std::collections::HashMap;
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
    /// Dispatch a scheduler related action to the store.
    fn scheduler_event(&self, action: SchedulerAction);

    /// Wait for the given scheduler action to have been received.
    ///
    /// This function will block the current thread of execution.
    fn wait_for_scheduler(&self, action: SchedulerAction);

    /// Dispatch a leader related action to the store.
    fn leader_event(&self, action: LeaderAction);

    /// Wait for the given leader action to have been received.
    ///
    /// This function will block the current thread of execution.
    fn wait_for_leader(&self, action: LeaderAction);

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
    // Used to allow callers to wait for scheduler events.
    scheduler_var: Arc<(Mutex<SchedulerAction>, Condvar)>,
    // Used to allow callers to wait for restorer events.
    leader_var: Arc<(Mutex<LeaderAction>, Condvar)>,
}

impl StateStoreImpl {
    /// Construct a new instance of StateStoreImpl.
    pub fn new() -> Self {
        let store = Store::new(State::default(), Display::default());
        let scheduler_var = Arc::new((Mutex::new(SchedulerAction::Stopped), Condvar::new()));
        let leader_var = Arc::new((Mutex::new(LeaderAction::Stopped), Condvar::new()));
        Self {
            store: Mutex::new(store),
            scheduler_var,
            leader_var,
        }
    }
}

impl StateStore for StateStoreImpl {
    fn scheduler_event(&self, action: SchedulerAction) {
        let mut store = self.store.lock().unwrap();
        let _ = store.dispatch(action.clone());
        drop(store);
        let pair = self.scheduler_var.clone();
        let (lock, cvar) = &*pair;
        let mut actual = lock.lock().unwrap();
        *actual = action;
        cvar.notify_all();
    }

    fn wait_for_scheduler(&self, action: SchedulerAction) {
        let pair = self.scheduler_var.clone();
        let (lock, cvar) = &*pair;
        let mut actual = lock.lock().unwrap();
        while *actual != action {
            actual = cvar.wait(actual).unwrap();
        }
    }

    fn leader_event(&self, action: LeaderAction) {
        let mut store = self.store.lock().unwrap();
        let _ = store.dispatch(action.clone());
        drop(store);
        let pair = self.leader_var.clone();
        let (lock, cvar) = &*pair;
        let mut actual = lock.lock().unwrap();
        *actual = action;
        cvar.notify_all();
    }

    fn wait_for_leader(&self, action: LeaderAction) {
        let pair = self.leader_var.clone();
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

impl Default for StateStoreImpl {
    fn default() -> Self {
        StateStoreImpl::new()
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
/// Actions, both imperative and informative, related to the supervisor that
/// manages the backup processes.
///
#[derive(Clone, Debug, PartialEq)]
pub enum SchedulerAction {
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
/// Actions, both imperative and informative, related to the supervisor that
/// manages the file restore processes.
///
#[derive(Clone, Debug, PartialEq)]
pub enum LeaderAction {
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
/// State of the supervisor process that manages backups.
///
#[derive(Clone, Debug, PartialEq)]
pub enum SchedulerState {
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
/// State of the supervisor process that manages file restores.
///
#[derive(Clone, Debug, PartialEq)]
pub enum LeaderState {
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
    /// Requested state of the scheduler supervised actor.
    pub scheduler: SchedulerState,
    /// Requested state of the ring leader supervised actor.
    pub leader: LeaderState,
    /// Collection of subscribers to the application state.
    subscribers: HashMap<String, Subscription<State>>,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "scheduler: {:?}, leader: {:?}",
            self.scheduler, self.leader
        )
    }
}

impl Default for State {
    fn default() -> Self {
        Self {
            scheduler: SchedulerState::Stopped,
            leader: LeaderState::Stopped,
            subscribers: HashMap::new(),
        }
    }
}

impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            scheduler: self.scheduler.clone(),
            leader: self.leader.clone(),
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

impl Reducer<SchedulerAction> for State {
    fn reduce(&mut self, action: SchedulerAction) {
        match action {
            SchedulerAction::Start => {
                self.scheduler = SchedulerState::Starting;
            }
            SchedulerAction::Started => {
                self.scheduler = SchedulerState::Started;
            }
            SchedulerAction::Stop => {
                self.scheduler = SchedulerState::Stopping;
            }
            SchedulerAction::Stopped => {
                self.scheduler = SchedulerState::Stopped;
            }
        }
    }
}

impl Reducer<LeaderAction> for State {
    fn reduce(&mut self, action: LeaderAction) {
        match action {
            LeaderAction::Start => {
                self.leader = LeaderState::Starting;
            }
            LeaderAction::Started => {
                self.leader = LeaderState::Started;
            }
            LeaderAction::Stop => {
                self.leader = LeaderState::Stopping;
            }
            LeaderAction::Stopped => {
                self.leader = LeaderState::Stopped;
            }
        }
    }
}

///
/// Implementation of the `Reactor` trait that invokes all registered listeners
/// with the state whenever an action is dispatched.
///
#[derive(Default)]
struct Display {
    // copy of the previous application state
    previous: Option<State>,
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
    fn test_wait_for_scheduler() {
        let sut = Arc::new(StateStoreImpl::new());
        // assert initial state is "stopped"
        let state = sut.get_state();
        assert_eq!(state.scheduler, SchedulerState::Stopped);
        // change supervisor to starting
        sut.scheduler_event(SchedulerAction::Start);
        let state = sut.get_state();
        assert_eq!(state.scheduler, SchedulerState::Starting);
        // spawn a thread that sends the started event later
        let sut_clone = sut.clone();
        std::thread::spawn(move || {
            let delay = std::time::Duration::from_millis(250);
            std::thread::sleep(delay);
            sut_clone.scheduler_event(SchedulerAction::Started);
        });
        // wait on the current thread for the action to arrive
        sut.wait_for_scheduler(SchedulerAction::Started);
        let state = sut.get_state();
        assert_eq!(state.scheduler, SchedulerState::Started);
        // wait for an action that has already arrived
        sut.scheduler_event(SchedulerAction::Stopped);
        sut.wait_for_scheduler(SchedulerAction::Stopped);
        let state = sut.get_state();
        assert_eq!(state.scheduler, SchedulerState::Stopped);
    }

    #[test]
    fn test_wait_for_leader() {
        let sut = Arc::new(StateStoreImpl::new());
        // assert initial state is "stopped"
        let state = sut.get_state();
        assert_eq!(state.leader, LeaderState::Stopped);
        // change restorer to starting
        sut.leader_event(LeaderAction::Start);
        let state = sut.get_state();
        assert_eq!(state.leader, LeaderState::Starting);
        // spawn a thread that sends the started event later
        let sut_clone = sut.clone();
        std::thread::spawn(move || {
            let delay = std::time::Duration::from_millis(250);
            std::thread::sleep(delay);
            sut_clone.leader_event(LeaderAction::Started);
        });
        // wait on the current thread for the action to arrive
        sut.wait_for_leader(LeaderAction::Started);
        let state = sut.get_state();
        assert_eq!(state.leader, LeaderState::Started);
        // wait for an action that has already arrived
        sut.leader_event(LeaderAction::Stopped);
        sut.wait_for_leader(LeaderAction::Stopped);
        let state = sut.get_state();
        assert_eq!(state.leader, LeaderState::Stopped);
    }
}
