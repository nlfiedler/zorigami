//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::repositories::RecordRepository;
use failure::Error;
use std::cmp;
use std::fmt;

pub struct DeleteStore {
    repo: Box<dyn RecordRepository>,
}

impl DeleteStore {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<(), Params> for DeleteStore {
    fn call(&self, params: Params) -> Result<(), Error> {
        self.repo.delete_store(&params.store_id)
    }
}

pub struct Params {
    /// Unique identifier of the store.
    store_id: String,
}

impl Params {
    pub fn new(store_id: String) -> Self {
        Self { store_id }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.store_id)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.store_id == other.store_id
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::repositories::MockRecordRepository;
    use failure::err_msg;

    #[test]
    fn test_delete_store_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_delete_store().returning(|_| Ok(()));
        // act
        let usecase = DeleteStore::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_store_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_delete_store()
            .returning(|_| Err(err_msg("oh no")));
        // act
        let usecase = DeleteStore::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
