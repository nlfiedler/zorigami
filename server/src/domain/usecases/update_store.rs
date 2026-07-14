//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::{PackRetention, Store, StoreType};
use crate::domain::repositories::RecordRepository;
use anyhow::{Error, anyhow};
use std::cmp;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

pub struct UpdateStore {
    repo: Box<dyn RecordRepository>,
}

impl UpdateStore {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Store, Params> for UpdateStore {
    fn call(&self, params: Params) -> Result<Store, Error> {
        let store_type = StoreType::from_str(&params.type_name)?;
        let store = Store {
            id: params.store_id,
            store_type,
            label: params.label,
            properties: params.properties,
            retention: params.retention,
        };
        store.validate()?;
        // Reject a silent reduction of an existing object-lock window. lock_days
        // lives in the free-form properties map, so an update that simply omits
        // it (e.g. a relabel that does not round-trip every property) would
        // otherwise drop immutability for future packs with no warning. Objects
        // already written keep their original lock regardless; a shorter window
        // going forward must be a deliberate, explicit change.
        if let Some(existing) = self.repo.get_store(&store.id)? {
            let old_lock = store_core::lock_days_from_props(&existing.properties);
            let new_lock = store_core::lock_days_from_props(&store.properties);
            if old_lock > 0 && new_lock < old_lock {
                return Err(anyhow!(
                    "cannot reduce lock_days from {} to {} on an object-locked store; \
                     set lock_days explicitly to keep the existing window (see doc/DEPLOY.md)",
                    old_lock,
                    new_lock
                ));
            }
        }
        self.repo.put_store(&store)?;
        Ok(store)
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
    /// Pack retention policy.
    retention: PackRetention,
}

impl Params {
    pub fn new(
        store_id: String,
        type_name: String,
        label: String,
        properties: HashMap<String, String>,
        retention: PackRetention,
    ) -> Self {
        Self {
            store_id,
            type_name,
            label,
            properties,
            retention,
        }
    }
}

impl From<Store> for Params {
    fn from(val: Store) -> Self {
        Params::new(
            val.id,
            val.store_type.to_string(),
            val.label,
            val.properties,
            val.retention,
        )
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
    use anyhow::anyhow;

    #[test]
    fn test_update_store_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store().returning(|_| Ok(None));
        mock.expect_put_store().returning(|_| Ok(()));
        // act
        let usecase = UpdateStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "minio".to_owned(),
            label: "pretend S3".to_owned(),
            properties,
            retention: PackRetention::ALL,
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
    fn test_update_store_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store().returning(|_| Ok(None));
        mock.expect_put_store().returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = UpdateStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "minio".to_owned(),
            label: "pretend S3".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_update_store_lock_shorter_than_retention_rejected() {
        // A lock window shorter than the pack retention would wedge pruning; it
        // must be rejected before the store is ever persisted.
        let mut mock = MockRecordRepository::new();
        mock.expect_put_store().never();
        let usecase = UpdateStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), "us-west-2".to_owned());
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        properties.insert("lock_days".to_owned(), "10".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "minio".to_owned(),
            label: "locked minio".to_owned(),
            properties,
            retention: PackRetention::DAYS(30),
        };
        let result = usecase.call(params);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("lock_days"), "unexpected error: {}", msg);
    }

    #[test]
    fn test_update_store_lock_on_local_rejected() {
        // Object lock has no meaning on the local backend and must be rejected
        // rather than silently ignored.
        let mut mock = MockRecordRepository::new();
        mock.expect_put_store().never();
        let usecase = UpdateStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/tmp/store".to_owned());
        properties.insert("lock_days".to_owned(), "7".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "local".to_owned(),
            label: "locked local".to_owned(),
            properties,
            retention: PackRetention::DAYS(1),
        };
        let result = usecase.call(params);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("not supported"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn test_update_store_lock_meets_retention_ok() {
        // A lock window >= retention on a supported backend is accepted.
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store().returning(|_| Ok(None));
        mock.expect_put_store().returning(|_| Ok(()));
        let usecase = UpdateStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), "us-west-2".to_owned());
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        properties.insert("lock_days".to_owned(), "30".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "minio".to_owned(),
            label: "locked minio".to_owned(),
            properties,
            retention: PackRetention::DAYS(30),
        };
        let result = usecase.call(params);
        assert!(result.is_ok(), "unexpected error: {:?}", result.err());
    }

    #[test]
    fn test_update_store_lock_on_azure_ok() {
        // Azure supports object lock (version-level immutability), so a
        // lock_days >= retention config on an azure store is accepted.
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store().returning(|_| Ok(None));
        mock.expect_put_store().returning(|_| Ok(()));
        let usecase = UpdateStore::new(Box::new(mock));
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("account".to_owned(), "zorigami-test".to_owned());
        properties.insert("lock_days".to_owned(), "14".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "azure".to_owned(),
            label: "locked azure".to_owned(),
            properties,
            retention: PackRetention::DAYS(7),
        };
        let result = usecase.call(params);
        assert!(result.is_ok(), "unexpected error: {:?}", result.err());
    }

    #[test]
    fn test_update_store_lock_reduction_rejected() {
        // The existing store is locked at 30 days; an update that drops the
        // window to 7 (still >= retention, so validate() passes) must be
        // rejected as a silent downgrade, and the store must not be persisted.
        let mut existing_props: HashMap<String, String> = HashMap::new();
        existing_props.insert("region".to_owned(), "us-west-2".to_owned());
        existing_props.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        existing_props.insert("lock_days".to_owned(), "30".to_owned());
        let existing = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::MINIO,
            label: "locked minio".to_owned(),
            properties: existing_props,
            retention: PackRetention::DAYS(1),
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .returning(move |_| Ok(Some(existing.clone())));
        mock.expect_put_store().never();
        let usecase = UpdateStore::new(Box::new(mock));

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), "us-west-2".to_owned());
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        properties.insert("lock_days".to_owned(), "7".to_owned());
        let params = Params {
            store_id: "cafebabe".to_owned(),
            type_name: "minio".to_owned(),
            label: "locked minio".to_owned(),
            properties,
            retention: PackRetention::DAYS(1),
        };
        let result = usecase.call(params);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("cannot reduce lock_days"),
            "unexpected error: {}",
            msg
        );
    }
}
