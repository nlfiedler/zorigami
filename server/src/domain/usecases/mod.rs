//
// Copyright (c) 2020 Nathan Fiedler
//
use anyhow::Error;
use std::cmp;
use std::fmt;

pub mod cancel_restore;
pub mod delete_dataset;
pub mod delete_store;
pub mod find_missing;
pub mod get_counts;
pub mod get_datasets;
pub mod get_pack;
pub mod get_snapshot;
pub mod get_stores;
pub mod get_tree;
pub mod new_dataset;
pub mod new_store;
pub mod prune_extra;
pub mod query_restores;
pub mod reassign_packs;
pub mod restore_database;
pub mod restore_files;
pub mod restore_missing;
pub mod start_backup;
pub mod stop_backup;
pub mod test_store;
pub mod update_dataset;
pub mod update_store;
pub mod verify_snapshot;

/// `UseCase` is the interface by which all use cases are invoked.
pub trait UseCase<Type, Params> {
    fn call(&self, params: Params) -> Result<Type, Error>;
}

/// `NoParams` is the type for use cases that do not take arguments.
pub struct NoParams {}

impl fmt::Display for NoParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NoParams()")
    }
}

impl cmp::PartialEq for NoParams {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl cmp::Eq for NoParams {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noparams_equality() {
        let np1 = NoParams {};
        let np2 = NoParams {};
        assert!(np1 == np2);
        assert!(np2 == np1);
    }

    #[test]
    fn test_noparams_stringify() {
        let np = NoParams {};
        assert_eq!(np.to_string(), "NoParams()");
    }
}
