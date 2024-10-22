//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Error};
use log::info;
use std::cmp;
use std::fmt;

///
/// Make snapshots beyond a certain number, or number of days, disappear and
/// remove anything that is no longer reachable from the remaining snapshots.
///
pub struct PruneSnapshots {
    repo: Box<dyn RecordRepository>,
}

impl PruneSnapshots {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<(), Params> for PruneSnapshots {
    fn call(&self, _params: Params) -> Result<(), Error> {
        // TODO: find the latest snapshot for the given dataset
        // TODO: walk back in the snapshot history to find the desired end
        // TODO: clear the parent reference on the new last snapshot
        // TODO: make everything unreachable disappear
        Ok(())
    }
}

pub struct Params {
    /// Identifier of the dataset containing the snapshot.
    dataset: String,
}

impl Params {
    pub fn new(dataset: String) -> Self {
        Self { dataset }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.dataset)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.dataset == other.dataset
    }
}

impl cmp::Eq for Params {}
