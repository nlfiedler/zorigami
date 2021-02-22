//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::{Store, StoreType};
use crate::domain::repositories::RecordRepository;
use failure::Error;
use rusty_ulid::generate_ulid_string;
use std::cmp;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

pub struct NewStore {
    repo: Box<dyn RecordRepository>,
}

impl NewStore {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Store, Params> for NewStore {
    fn call(&self, params: Params) -> Result<Store, Error> {
        let store_type = StoreType::from_str(&params.type_name)?;
        let id = generate_ulid_string().to_lowercase();
        let store = Store {
            id,
            store_type,
            label: params.label,
            properties: params.properties,
        };
        self.repo.put_store(&store)?;
        Ok(store)
    }
}

pub struct Params {
    /// The kind of store (e.g. "local", "minio").
    type_name: String,
    /// User-defined label for the store.
    label: String,
    /// Name/value pairs that make up this store configuration.
    properties: HashMap<String, String>,
}

impl Params {
    pub fn new(type_name: String, label: String, properties: HashMap<String, String>) -> Self {
        Self {
            type_name,
            label,
            properties,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.type_name)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.type_name == other.type_name && self.label == other.label
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
    fn test_new_store_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_store().returning(|_| Ok(()));
        // act
        let usecase = NewStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        let params = Params {
            type_name: "minio".to_owned(),
            label: "pretend S3".to_owned(),
            properties,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.store_type, StoreType::MINIO);
        assert_eq!(actual.label, "pretend S3");
        assert!(actual.properties.contains_key("endpoint"));
    }

    #[test]
    fn test_new_store_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_store().returning(|_| Err(err_msg("oh no")));
        // act
        let usecase = NewStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        let params = Params {
            type_name: "minio".to_owned(),
            label: "pretend S3".to_owned(),
            properties,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
