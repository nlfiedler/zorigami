//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::repositories::RecordRepository;
use anyhow::{Error, anyhow};
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
        let datasets = self.repo.get_datasets()?;
        let in_use: Vec<String> = datasets
            .into_iter()
            .filter(|d| d.stores.iter().any(|s| s == &params.store_id))
            .map(|d| d.id)
            .collect();
        if !in_use.is_empty() {
            return Err(anyhow!(
                "store is still in use by dataset(s): {}",
                in_use.join(", ")
            ));
        }
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
    use crate::domain::entities::Dataset;
    use crate::domain::repositories::MockRecordRepository;
    use std::path::Path;

    #[test]
    fn test_delete_store_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets().returning(|| Ok(vec![]));
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
    fn test_delete_store_ok_other_dataset() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        dataset.stores = vec!["someother".to_owned()];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(vec![dataset.clone()]));
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
        mock.expect_get_datasets().returning(|| Ok(vec![]));
        mock.expect_delete_store()
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = DeleteStore::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_store_in_use() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        dataset.stores = vec!["cafebabe".to_owned()];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(vec![dataset.clone()]));
        // delete_store must not be called when a dataset still references it
        mock.expect_delete_store().never();
        // act
        let usecase = DeleteStore::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("still in use"));
    }

    #[test]
    fn test_delete_store_get_datasets_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(|| Err(anyhow!("oh no")));
        mock.expect_delete_store().never();
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
