//
// Copyright (c) 2023 Nathan Fiedler
//
use crate::domain::entities::Pack;
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Error};
use log::info;
use std::cmp;
use std::fmt;

///
/// Change the store identifier from `source` to `target`, updating all pack
/// records that have a matching location entry.
/// 
/// Note that this action may result in pack records that refer to locations
/// that do not really exist. For simple pack stores like the local or sftp
/// stores, this should be safe. However, pack stores that rely on cloud storage
/// systems are prone to having buckets and or objects renamed. As such, simply
/// changing the store identifier may result in seemingly missing packs.
///
pub struct ReassignPacks {
    repo: Box<dyn RecordRepository>,
}

impl ReassignPacks {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<u64, Params> for ReassignPacks {
    fn call(&self, params: Params) -> Result<u64, Error> {
        // purely for error-checking, ensure new store exists
        let _ = self
            .repo
            .get_store(&params.new_store_id)?
            .ok_or_else(|| anyhow!(format!("no such store: {}", params.new_store_id)))?;
        let matching_packs = self.repo.get_packs(&params.old_store_id)?;
        info!(
            "ReassignPacks: will update {} packs with store {}",
            matching_packs.len(),
            params.new_store_id
        );
        for pack_record in matching_packs.iter() {
            info!(
                "ReassignPacks: changing store for pack {}",
                &pack_record.digest
            );
            let patched = patch_location(pack_record, &params.old_store_id, &params.new_store_id);
            self.repo.put_pack(&patched)?;
        }
        Ok(matching_packs.len() as u64)
    }
}

pub struct Params {
    /// Identifier of the store that will be replaced by the new one.
    old_store_id: String,
    /// Idenfifier of the store which will replace the old one.
    new_store_id: String,
}

impl Params {
    pub fn new(old_store_id: String, new_store_id: String) -> Self {
        Self {
            old_store_id,
            new_store_id,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({} -> {})", self.old_store_id, self.new_store_id)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.old_store_id == other.old_store_id && self.new_store_id == other.new_store_id
    }
}

impl cmp::Eq for Params {}

// Update the pack location for the matching store in a copy of the Pack.
fn patch_location(record: &Pack, old_store_id: &str, new_store_id: &str) -> Pack {
    let mut patched = record.clone();
    for location in patched.locations.iter_mut() {
        if location.store == old_store_id {
            location.store = new_store_id.to_owned();
        }
    }
    patched
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Checksum, Pack, PackLocation, Store, StoreType};
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use mockall::predicate::*;
    use std::collections::HashMap;

    #[test]
    fn test_reassign_packs_not_any_packs() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let source_store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "source_store".to_owned(),
            properties,
        };
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/users/lucile".to_owned());
        let target_store = Store {
            id: "deadbeef".to_owned(),
            store_type: StoreType::LOCAL,
            label: "target_store".to_owned(),
            properties,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(source_store.clone())));
        mock.expect_get_store()
            .with(eq("deadbeef"))
            .returning(move |_| Ok(Some(target_store.clone())));
        mock.expect_get_packs().returning(|_| Ok(Vec::new()));
        mock.expect_get_databases().returning(|| Ok(Vec::new()));
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_find_missing()
                .returning(|_, _| Ok(Vec::new()));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = ReassignPacks::new(Box::new(mock));
        let params = Params {
            old_store_id: "cafebabe".to_owned(),
            new_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let updated = result.unwrap();
        assert_eq!(updated, 0);
    }

    #[test]
    fn test_reassign_packs_update_packs() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let target_store = Store {
            id: "deadbeef".to_owned(),
            store_type: StoreType::LOCAL,
            label: "target_store".to_owned(),
            properties,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("deadbeef"))
            .returning(move |_| Ok(Some(target_store.clone())));
        mock.expect_get_packs().returning(|_| {
            let digest = Checksum::SHA1(String::from("bf24db8ccd274daad5fe73a71b95cd00ffa56a37"));
            let coords = vec![PackLocation::new("cafebabe", "bucket1", "object1")];
            let pack1 = Pack::new(digest.clone(), coords);
            let digest = Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
            let coords = vec![PackLocation::new("cafebabe", "bucket1", "object2")];
            let pack2 = Pack::new(digest.clone(), coords);
            let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
            let coords = vec![PackLocation::new("cafebabe", "bucket1", "object3")];
            let pack3 = Pack::new(digest.clone(), coords);
            Ok(vec![pack1, pack2, pack3])
        });
        // ensure pack record is updated with new coordinates
        mock.expect_put_pack()
            .withf(|p| p.locations[0].store == "deadbeef")
            .returning(|_| Ok(()));
        // act
        let usecase = ReassignPacks::new(Box::new(mock));
        let params = Params {
            old_store_id: "cafebabe".to_owned(),
            new_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let updated = result.unwrap();
        assert_eq!(updated, 3);
    }

    #[test]
    fn test_reassign_packs_no_target_store() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("deadbeef"))
            .returning(|_| Ok(None));
        // act
        let usecase = ReassignPacks::new(Box::new(mock));
        let params = Params {
            old_store_id: "cafebabe".to_owned(),
            new_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no such store: deadbeef"));
    }
}
