//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::tasks::leader::RingLeader;
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::sync::Arc;

pub struct CancelRestore {
    leader: Arc<dyn RingLeader>,
}

impl CancelRestore {
    pub fn new(leader: Arc<dyn RingLeader>) -> Self {
        Self { leader }
    }
}

impl super::UseCase<bool, Params> for CancelRestore {
    fn call(&self, params: Params) -> Result<bool, Error> {
        Ok(self.leader.cancel_restore(params.id))
    }
}

pub struct Params {
    /// Unique identifier of the request to be cancelled.
    id: String,
}

impl Params {
    pub fn new(id: String) -> Self {
        Self { id }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.id)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::tasks::leader::MockRingLeader;

    #[test]
    fn test_cancel_restore_ok() {
        // arrange
        let mut mock = MockRingLeader::new();
        mock.expect_cancel_restore().returning(|_| true);
        // act
        let usecase = CancelRestore::new(Arc::new(mock));
        let params = Params::new("abc123".into());
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(value);
    }
}
