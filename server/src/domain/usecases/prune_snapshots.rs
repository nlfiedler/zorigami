//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::tasks::leader::RingLeader;
use crate::tasks::prune::Request;
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::sync::Arc;

///
/// Make snapshots beyond a certain number, or number of days, disappear and
/// remove anything that is no longer reachable from the remaining snapshots.
///
pub struct PruneSnapshots {
    leader: Arc<dyn RingLeader>,
}

impl PruneSnapshots {
    pub fn new(leader: Arc<dyn RingLeader>) -> Self {
        Self { leader }
    }
}

impl super::UseCase<(), Params> for PruneSnapshots {
    fn call(&self, params: Params) -> Result<(), Error> {
        let request = Request::new(params.dataset);
        self.leader.prune(request)
    }
}

pub struct Params {
    /// Identifier of the dataset whose snapshots will be pruned.
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

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::tasks::leader::MockRingLeader;
    use anyhow::anyhow;

    #[test]
    fn test_prune_snapshots_ok() {
        // arrange
        let dataset_id = xid::new().to_string();
        let mut leader = MockRingLeader::new();
        leader.expect_prune().returning(move |_| Ok(()));
        // act
        let usecase = PruneSnapshots::new(Arc::new(leader));
        let params = Params::new(dataset_id);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_prune_snapshots_err() {
        // arrange
        let dataset_id = xid::new().to_string();
        let mut leader = MockRingLeader::new();
        leader
            .expect_prune()
            .returning(move |_| Err(anyhow!("oh no")));
        // act
        let usecase = PruneSnapshots::new(Arc::new(leader));
        let params = Params::new(dataset_id);
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
