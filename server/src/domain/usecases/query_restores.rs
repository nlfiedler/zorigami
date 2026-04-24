//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::usecases::NoParams;
use crate::tasks::leader::RingLeader;
use crate::tasks::restore::Request;
use anyhow::Error;
use std::sync::Arc;

pub struct QueryRestores {
    leader: Arc<dyn RingLeader>,
}

impl QueryRestores {
    pub fn new(leader: Arc<dyn RingLeader>) -> Self {
        Self { leader }
    }
}

impl super::UseCase<Vec<Request>, NoParams> for QueryRestores {
    fn call(&self, _params: NoParams) -> Result<Vec<Request>, Error> {
        Ok(self.leader.restores())
    }
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::tasks::leader::MockRingLeader;

    #[test]
    fn test_restore_files_ok() {
        // arrange
        let mut mock = MockRingLeader::new();
        mock.expect_restores().returning(Vec::new);
        // act
        let usecase = QueryRestores::new(Arc::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let requests = result.unwrap();
        assert!(requests.is_empty());
    }
}
