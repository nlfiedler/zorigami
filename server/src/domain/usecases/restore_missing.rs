//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Pack, PackLocation};
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Error};
use log::{error, info};
use std::cmp;
use std::collections::HashSet;
use std::fmt;

pub struct RestoreMissingPacks {
    repo: Box<dyn RecordRepository>,
}

impl RestoreMissingPacks {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Vec<Pack>, Params> for RestoreMissingPacks {
    fn call(&self, params: Params) -> Result<Vec<Pack>, Error> {
        let source_store = self
            .repo
            .get_store(&params.source_store_id)?
            .ok_or_else(|| anyhow!(format!("no such store: {}", params.source_store_id)))?;
        let target_store = self
            .repo
            .get_store(&params.target_store_id)?
            .ok_or_else(|| anyhow!(format!("no such store: {}", params.target_store_id)))?;
        // Find all packs and database snapshot packs.
        let mut all_packs = self.repo.get_packs(&target_store.id)?;
        let mut databases = self.repo.get_databases()?;
        all_packs.append(&mut databases);
        info!(
            "RestoreMissing: scanning {} possible packs for store {}",
            all_packs.len(),
            target_store.id
        );
        let target_pack_repo = self.repo.build_pack_repo(&target_store)?;
        let mut missing_vec = target_pack_repo.find_missing(&target_store.id, &all_packs)?;
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
            "RestoreMissing: found {} packs missing from store {}",
            missing_packs.len(),
            target_store.id
        );
        // retrieve the missing packs from the source and upload to target
        let source_pack_repo = self.repo.build_pack_repo(&source_store)?;
        let mut really_missing_packs: Vec<Pack> = Vec::new();
        for missing in missing_packs {
            let pack_file = tempfile::Builder::new().suffix(".pack").tempfile()?;
            info!(
                "RestoreMissing: retrieving pack {} from source",
                &missing.digest
            );
            let result = source_pack_repo.retrieve_pack(&missing.locations, pack_file.path());
            if result.is_err() {
                error!(
                    "RestoreMissing: unable to retrieve pack {}: {:?}",
                    &missing.digest, result
                );
                really_missing_packs.push(missing);
                continue;
            }
            // Attempt to use the same bucket and object for the target store.
            // Some stores may end up using new coordinates anyway.
            let location = find_location(&missing.locations, &params.target_store_id);
            info!("RestoreMissing: storing pack {} to target", &missing.digest);
            let locations = target_pack_repo.store_pack(
                pack_file.path(),
                &location.bucket,
                &location.object,
            )?;
            // Find the pack location associated with the target store and
            // replace the previous bucket/object tuple with the updated
            // coordinates, which may have been changed by the store.
            let patched = patch_location(&missing, &locations, &params.target_store_id);
            self.repo.put_pack(&patched)?;
        }
        if really_missing_packs.is_empty() {
            info!("RestoreMissing: all missing packs restored");
        } else {
            info!(
                "RestoreMissing: unable to restore {} packs",
                really_missing_packs.len()
            );
        }
        Ok(really_missing_packs)
    }
}

pub struct Params {
    /// Identifier of the store from which packs will be restored.
    source_store_id: String,
    /// Idenfifier of the store that has missing packs to be restored.
    target_store_id: String,
}

impl Params {
    pub fn new(source_store_id: String, target_store_id: String) -> Self {
        Self {
            source_store_id,
            target_store_id,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Params({} -> {})",
            self.source_store_id, self.target_store_id
        )
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.source_store_id == other.source_store_id
            && self.target_store_id == other.target_store_id
    }
}

impl cmp::Eq for Params {}

// Clone the pack location that matches the given store.
fn find_location(locations: &[PackLocation], store_id: &str) -> PackLocation {
    for location in locations {
        if location.store == store_id {
            return location.to_owned();
        }
    }
    // this is unexpected, fall back to the first entry
    locations[0].to_owned()
}

// Update the pack location for the given store in a copy of the Pack.
fn patch_location(missing: &Pack, locations: &[PackLocation], store_id: &str) -> Pack {
    let mut patched = missing.clone();
    let new_location = find_location(locations, store_id);
    for location in patched.locations.iter_mut() {
        if location.store == store_id {
            location.bucket = new_location.bucket.clone();
            location.object = new_location.object.clone();
        }
    }
    patched
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Pack, PackLocation, PackRetention, Store, StoreType};
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use mockall::predicate::*;
    use std::collections::HashMap;

    #[test]
    fn test_restore_missing_not_any_packs() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let source_store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "source_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/users/lucile".to_owned());
        let target_store = Store {
            id: "deadbeef".to_owned(),
            store_type: StoreType::LOCAL,
            label: "target_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
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
        let usecase = RestoreMissingPacks::new(Box::new(mock));
        let params = Params {
            source_store_id: "cafebabe".to_owned(),
            target_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let missing = result.unwrap();
        assert_eq!(missing.len(), 0);
    }

    #[test]
    fn test_restore_missing_no_missing_packs() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let source_store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "source_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/users/lucile".to_owned());
        let target_store = Store {
            id: "deadbeef".to_owned(),
            store_type: StoreType::LOCAL,
            label: "target_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(source_store.clone())));
        mock.expect_get_store()
            .with(eq("deadbeef"))
            .returning(move |_| Ok(Some(target_store.clone())));
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
        mock.expect_get_databases().returning(|| {
            let digest = Checksum::SHA1(String::from("e449af1b9c5561b424b8c199be502bbe06b84af9"));
            let coords = vec![PackLocation::new(
                "store1",
                "9819f08f363c5d58ac4a5b54f7a0cc25",
                "01EE40MSWC12YYVG67GN9XSQEA",
            )];
            let pack1 = Pack::new(digest.clone(), coords);
            Ok(vec![pack1])
        });
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_find_missing()
                .returning(|_, _| Ok(Vec::new()));
            Ok(Box::new(mock_store))
        });
        // act
        let usecase = RestoreMissingPacks::new(Box::new(mock));
        let params = Params {
            source_store_id: "cafebabe".to_owned(),
            target_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let missing = result.unwrap();
        assert_eq!(missing.len(), 0);
    }

    #[test]
    fn test_restore_missing_retrieve_pack_err() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let source_store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "source_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/users/lucile".to_owned());
        let target_store = Store {
            id: "deadbeef".to_owned(),
            store_type: StoreType::LOCAL,
            label: "target_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(source_store.clone())));
        mock.expect_get_store()
            .with(eq("deadbeef"))
            .returning(move |_| Ok(Some(target_store.clone())));
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
        mock.expect_get_databases().returning(|| {
            let digest = Checksum::SHA1(String::from("e449af1b9c5561b424b8c199be502bbe06b84af9"));
            let coords = vec![PackLocation::new(
                "store1",
                "9819f08f363c5d58ac4a5b54f7a0cc25",
                "01EE40MSWC12YYVG67GN9XSQEA",
            )];
            let pack1 = Pack::new(digest.clone(), coords);
            Ok(vec![pack1])
        });
        mock.expect_build_pack_repo()
            .withf(|s| s.id == "deadbeef")
            .returning(move |_| {
                let mut mock_store = MockPackRepository::new();
                mock_store.expect_find_missing().returning(|_, _| {
                    let digest =
                        Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
                    Ok(vec![digest])
                });
                Ok(Box::new(mock_store))
            });
        mock.expect_build_pack_repo()
            .withf(|s| s.id == "cafebabe")
            .returning(move |_| {
                let mut mock_store = MockPackRepository::new();
                mock_store
                    .expect_retrieve_pack()
                    .returning(|_, _| Err(anyhow!("oh no")));
                Ok(Box::new(mock_store))
            });
        // act
        let usecase = RestoreMissingPacks::new(Box::new(mock));
        let params = Params {
            source_store_id: "cafebabe".to_owned(),
            target_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let missing = result.unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].locations[0].object, "object2");
    }

    #[test]
    fn test_restore_missing_store_pack_err() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let source_store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "source_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/users/lucile".to_owned());
        let target_store = Store {
            id: "deadbeef".to_owned(),
            store_type: StoreType::LOCAL,
            label: "target_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(source_store.clone())));
        mock.expect_get_store()
            .with(eq("deadbeef"))
            .returning(move |_| Ok(Some(target_store.clone())));
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
        mock.expect_get_databases().returning(|| {
            let digest = Checksum::SHA1(String::from("e449af1b9c5561b424b8c199be502bbe06b84af9"));
            let coords = vec![PackLocation::new(
                "store1",
                "9819f08f363c5d58ac4a5b54f7a0cc25",
                "01EE40MSWC12YYVG67GN9XSQEA",
            )];
            let pack1 = Pack::new(digest.clone(), coords);
            Ok(vec![pack1])
        });
        mock.expect_build_pack_repo()
            .withf(|s| s.id == "deadbeef")
            .returning(move |_| {
                let mut mock_store = MockPackRepository::new();
                mock_store.expect_find_missing().returning(|_, _| {
                    let digest =
                        Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
                    Ok(vec![digest])
                });
                mock_store
                    .expect_store_pack()
                    .with(always(), eq("bucket1"), eq("object2"))
                    .returning(|_, _, _| Err(anyhow!("oh no")));
                Ok(Box::new(mock_store))
            });
        mock.expect_build_pack_repo()
            .withf(|s| s.id == "cafebabe")
            .returning(move |_| {
                let mut mock_store = MockPackRepository::new();
                mock_store.expect_retrieve_pack().returning(|_, _| Ok(()));
                Ok(Box::new(mock_store))
            });
        // act
        let usecase = RestoreMissingPacks::new(Box::new(mock));
        let params = Params {
            source_store_id: "cafebabe".to_owned(),
            target_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("oh no"));
    }

    #[test]
    fn test_restore_missing_one_missing_pack() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let source_store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "source_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/users/lucile".to_owned());
        let target_store = Store {
            id: "deadbeef".to_owned(),
            store_type: StoreType::LOCAL,
            label: "target_store".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(source_store.clone())));
        mock.expect_get_store()
            .with(eq("deadbeef"))
            .returning(move |_| Ok(Some(target_store.clone())));
        mock.expect_get_packs().returning(|_| {
            let digest = Checksum::SHA1(String::from("bf24db8ccd274daad5fe73a71b95cd00ffa56a37"));
            let coords = vec![
                PackLocation::new("cafebabe", "bucket1", "object1"),
                PackLocation::new("deadbeef", "bucket1", "object1"),
            ];
            let pack1 = Pack::new(digest.clone(), coords);
            let digest = Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
            let coords = vec![
                PackLocation::new("cafebabe", "bucket1", "object2"),
                PackLocation::new("deadbeef", "bucket1", "object2"),
            ];
            let pack2 = Pack::new(digest.clone(), coords);
            let digest = Checksum::SHA1(String::from("ed841695851abdcfe6a50ce3d01d770eb053356b"));
            let coords = vec![
                PackLocation::new("cafebabe", "bucket1", "object3"),
                PackLocation::new("deadbeef", "bucket1", "object3"),
            ];
            let pack3 = Pack::new(digest.clone(), coords);
            Ok(vec![pack1, pack2, pack3])
        });
        mock.expect_get_databases().returning(|| {
            let digest = Checksum::SHA1(String::from("e449af1b9c5561b424b8c199be502bbe06b84af9"));
            let coords = vec![PackLocation::new(
                "store1",
                "9819f08f363c5d58ac4a5b54f7a0cc25",
                "01EE40MSWC12YYVG67GN9XSQEA",
            )];
            let pack1 = Pack::new(digest.clone(), coords);
            Ok(vec![pack1])
        });
        mock.expect_build_pack_repo()
            .withf(|s| s.id == "deadbeef")
            .returning(move |_| {
                let mut mock_store = MockPackRepository::new();
                mock_store.expect_find_missing().returning(|_, _| {
                    let digest =
                        Checksum::SHA1(String::from("4a285c30855fde0a195f3bdbd5e2663338f7510a"));
                    Ok(vec![digest])
                });
                // return different pack coordinates on the subsequent store to
                // make sure the pack gets updated in the database
                mock_store
                    .expect_store_pack()
                    .with(always(), eq("bucket1"), eq("object2"))
                    .returning(|_, _, _| {
                        Ok(vec![PackLocation::new("deadbeef", "madoka", "magica")])
                    });
                Ok(Box::new(mock_store))
            });
        mock.expect_build_pack_repo()
            .withf(|s| s.id == "cafebabe")
            .returning(move |_| {
                let mut mock_store = MockPackRepository::new();
                mock_store.expect_retrieve_pack().returning(|_, _| Ok(()));
                Ok(Box::new(mock_store))
            });
        // ensure pack record is updated with new coordinates
        mock.expect_put_pack()
            .withf(|p| p.locations[1].bucket == "madoka" && p.locations[1].object == "magica")
            .returning(|_| Ok(()));
        // act
        let usecase = RestoreMissingPacks::new(Box::new(mock));
        let params = Params {
            source_store_id: "cafebabe".to_owned(),
            target_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let missing = result.unwrap();
        assert_eq!(missing.len(), 0);
    }

    #[test]
    fn test_restore_missing_no_source_store() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(|_| Ok(None));
        // act
        let usecase = RestoreMissingPacks::new(Box::new(mock));
        let params = Params {
            source_store_id: "cafebabe".to_owned(),
            target_store_id: "deedbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no such store: cafebabe"));
    }

    #[test]
    fn test_restore_missing_no_target_store() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(store.clone())));
        mock.expect_get_store()
            .with(eq("deadbeef"))
            .returning(|_| Ok(None));
        // act
        let usecase = RestoreMissingPacks::new(Box::new(mock));
        let params = Params {
            source_store_id: "cafebabe".to_owned(),
            target_store_id: "deadbeef".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no such store: deadbeef"));
    }
}
