//
// Copyright (c) 2021 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Pack};
use crate::domain::repositories::RecordRepository;
use failure::{err_msg, Error};
use log::info;
use std::cmp;
use std::collections::HashSet;
use std::fmt;

pub struct FindMissingPacks {
    repo: Box<dyn RecordRepository>,
}

impl FindMissingPacks {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Vec<Pack>, Params> for FindMissingPacks {
    fn call(&self, params: Params) -> Result<Vec<Pack>, Error> {
        if let Some(store) = self.repo.get_store(&params.store_id)? {
            let all_packs = self.repo.get_packs(&store.id)?;
            info!(
                "FindMissing scanning {} possible packs for store {}",
                all_packs.len(),
                store.id
            );
            let pack_repo = self.repo.build_pack_repo(&store)?;
            let mut missing_vec = pack_repo.find_missing(&store.id, &all_packs)?;
            // convert missing vector to a hash set for easy seeking
            let mut missing_set: HashSet<Checksum> = HashSet::new();
            for value in missing_vec.drain(..) {
                missing_set.insert(value);
            }
            // retain those packs whose checksum is in the missing set
            let missing_packs: Vec<Pack> = all_packs
                .into_iter()
                .filter(|p| missing_set.contains(&p.digest))
                .collect();
            info!(
                "FindMissing found {} packs missing from store {}",
                missing_packs.len(),
                store.id
            );
            Ok(missing_packs)
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
    use crate::domain::entities::{Pack, PackLocation, Store, StoreType};
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use mockall::predicate::*;
    use std::collections::HashMap;

    #[test]
    fn test_find_missing_not_any_packs() {
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
            mock_store
                .expect_find_missing()
                .returning(|_, _| Ok(Vec::new()));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = FindMissingPacks::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let missing = result.unwrap();
        assert_eq!(missing.len(), 0);
    }

    #[test]
    fn test_find_missing_no_missing_packs() {
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
            mock_store
                .expect_find_missing()
                .returning(|_, _| Ok(Vec::new()));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = FindMissingPacks::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let missing = result.unwrap();
        assert_eq!(missing.len(), 0);
    }

    #[test]
    fn test_find_missing_some_missing_packs() {
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
            mock_store.expect_find_missing().returning(|_, _| {
                let digest =
                    Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
                Ok(vec![digest])
            });
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = FindMissingPacks::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let missing = result.unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].locations[0].object, "object2");
    }

    #[test]
    fn test_find_missing_all_missing_packs() {
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
            mock_store.expect_find_missing().returning(|_, _| {
                Ok(vec![
                    Checksum::SHA1(String::from("bf24db8ccd274daad5fe73a71b95cd00ffa56a37")),
                    Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a")),
                    Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b")),
                ])
            });
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = FindMissingPacks::new(Box::new(mock));
        let params = Params {
            store_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let missing = result.unwrap();
        assert_eq!(missing.len(), 3);
        // packs should come back in the order in which they were presented
        assert_eq!(missing[0].locations[0].object, "object1");
        assert_eq!(missing[1].locations[0].object, "object2");
        assert_eq!(missing[2].locations[0].object, "object3");
    }

    #[test]
    fn test_find_missing_no_store() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store().returning(|_| Ok(None));
        // act
        let usecase = FindMissingPacks::new(Box::new(mock));
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
