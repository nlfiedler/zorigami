//
// Copyright (c) 2021 Nathan Fiedler
//
use crate::domain::entities::{Store, StoreType};
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use std::cmp;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

pub struct TestStore {
    repo: Box<dyn RecordRepository>,
}

impl TestStore {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<(), Params> for TestStore {
    fn call(&self, params: Params) -> Result<(), Error> {
        let store_type = StoreType::from_str(&params.type_name)?;
        let store = Store {
            id: params.store_id,
            store_type,
            label: params.label,
            properties: params.properties,
        };
        let pack_repo = self.repo.build_pack_repo(&store)?;
        pack_repo.test_store(&store.id)
    }
}

pub struct Params {
    /// Unique identifier of the store.
    store_id: String,
    /// The kind of store (e.g. "local", "minio").
    type_name: String,
    /// User-defined label for the store.
    label: String,
    /// Name/value pairs that make up this store configuration.
    properties: HashMap<String, String>,
}

impl Params {
    pub fn new(
        store_id: String,
        type_name: String,
        label: String,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            store_id,
            type_name,
            label,
            properties,
        }
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
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use anyhow::anyhow;

    #[test]
    fn test_test_store_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store.expect_test_store().returning(move |_| Ok(()));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = TestStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "minio".to_owned(),
            label: "pretend S3".to_owned(),
            properties,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_test_store_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_test_store()
                .returning(move |_| Err(anyhow!("oh no")));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = TestStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "minio".to_owned(),
            label: "pretend S3".to_owned(),
            properties,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
