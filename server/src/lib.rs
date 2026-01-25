//
// Copyright (c) 2020 Nathan Fiedler
//
extern crate thiserror;
use crate::domain::managers::state::{StateStore, StateStoreImpl};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

pub mod data;
pub mod domain;
pub mod preso;

// When running in test mode, the cwd is the server directory.
#[cfg(test)]
static DEFAULT_DB_PATH: &str = "../tmp/test/database";

// Running in debug/release mode we assume cwd is root directory.
#[cfg(not(test))]
static DEFAULT_DB_PATH: &str = "./tmp/database";

// Path to the database files.
pub static DB_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    let path = std::env::var("DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_owned());
    PathBuf::from(path)
});

// Application state store.
pub static STATE_STORE: LazyLock<Arc<dyn StateStore>> =
    LazyLock::new(|| Arc::new(StateStoreImpl::new()));
