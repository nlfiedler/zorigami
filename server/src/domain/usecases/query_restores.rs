//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::managers::restore::{Request, Restorer};
use crate::domain::usecases::NoParams;
use anyhow::Error;
use std::sync::Arc;

pub struct QueryRestores {
    restorer: Arc<dyn Restorer>,
}

impl QueryRestores {
    pub fn new(restorer: Arc<dyn Restorer>) -> Self {
        Self { restorer }
    }
}

impl super::UseCase<Vec<Request>, NoParams> for QueryRestores {
    fn call(&self, _params: NoParams) -> Result<Vec<Request>, Error> {
        Ok(self.restorer.requests())
    }
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::managers::restore::MockRestorer;

    #[test]
    fn test_restore_files_ok() {
        // arrange
        let mut mock = MockRestorer::new();
        mock.expect_requests().returning(|| Vec::new());
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
