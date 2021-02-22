//
// Copyright (c) 2021 Nathan Fiedler
//
use crate::domain::repositories::RecordRepository;
use failure::{err_msg, Error};
use log::info;
use std::cmp;
use std::fmt;

pub struct PruneExtraPacks {
    repo: Box<dyn RecordRepository>,
}

impl PruneExtraPacks {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<u32, Params> for PruneExtraPacks {
    fn call(&self, params: Params) -> Result<u32, Error> {
        if let Some(store) = self.repo.get_store(&params.store_id)? {
            let all_packs = self.repo.get_packs(&store.id)?;
            info!(
                "PruneExtra expecting {} packs in store {}",
                all_packs.len(),
                store.id
            );
            let pack_repo = self.repo.build_pack_repo(&store)?;
            let count = pack_repo.prune_extra(&store.id, &all_packs)?;
            info!("PruneExtra removed {} packs from store {}", count, store.id);
            Ok(count)
        } else {
            Err(err_msg(format!("no such store: {}", params.store_id)))
        }
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
    use crate::domain::entities::{Checksum, Pack, PackLocation, Store, StoreType};
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use mockall::predicate::*;
    use std::collections::HashMap;

    #[test]
    fn test_prune_extra_not_any_packs() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(store.clone())));
        mock.expect_get_packs().returning(|_| Ok(Vec::new()));
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store.expect_prune_extra().returning(|_, _| Ok(0));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = PruneExtraPacks::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_prune_extra_no_extra_packs() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(store.clone())));
        mock.expect_get_packs().returning(|_| {
            let digest = Checksum::SHA1(String::from("bf24db8ccd274daad5fe73a71b95cd00ffa56a37"));
            let coords = vec![PackLocation::new("store1", "bucket1", "object1")];
            let pack1 = Pack::new(digest.clone(), coords);
            let digest = Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
            let coords = vec![PackLocation::new("store1", "bucket1", "object2")];
            let pack2 = Pack::new(digest.clone(), coords);
            let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
            let coords = vec![PackLocation::new("store1", "bucket1", "object3")];
            let pack3 = Pack::new(digest.clone(), coords);
            Ok(vec![pack1, pack2, pack3])
        });
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store.expect_prune_extra().returning(|_, _| Ok(0));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = PruneExtraPacks::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_prune_extra_some_extra_packs() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(store.clone())));
        mock.expect_get_packs().returning(|_| {
            let digest = Checksum::SHA1(String::from("bf24db8ccd274daad5fe73a71b95cd00ffa56a37"));
            let coords = vec![PackLocation::new("store1", "bucket1", "object1")];
            let pack1 = Pack::new(digest.clone(), coords);
            let digest = Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
            let coords = vec![PackLocation::new("store1", "bucket1", "object2")];
            let pack2 = Pack::new(digest.clone(), coords);
            let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
            let coords = vec![PackLocation::new("store1", "bucket1", "object3")];
            let pack3 = Pack::new(digest.clone(), coords);
            Ok(vec![pack1, pack2, pack3])
        });
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store.expect_prune_extra().returning(|_, _| Ok(42));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = PruneExtraPacks::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_prune_extra_no_store() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store().returning(|_| Ok(None));
        // act
        let usecase = PruneExtraPacks::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no such store: cafebabe"));
    }
}
