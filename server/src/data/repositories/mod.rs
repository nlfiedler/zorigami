//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::data::sources::{
    EntityDataSource, PackDataSource, PackSourceBuilder, PackSourceBuilderImpl,
};
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, PackLocation, Snapshot, Store, Tree,
};
use crate::domain::repositories::{PackRepository, RecordRepository};
use failure::{err_msg, Error};
use log::{error, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Use an `Arc` to hold the data source to make cloning easy for the caller. If
// using a `Box` instead, cloning it would involve adding fake clone operations
// to the data source trait, which works, but is ugly. It gets even uglier when
// mocking the calls on the data source, which gets cloned during the test.
pub struct RecordRepositoryImpl {
    datasource: Arc<dyn EntityDataSource>,
}

impl RecordRepositoryImpl {
    pub fn new(datasource: Arc<dyn EntityDataSource>) -> Self {
        Self { datasource }
    }
}

impl RecordRepository for RecordRepositoryImpl {
    fn get_configuration(&self) -> Result<Configuration, Error> {
        if let Some(conf) = self.datasource.get_configuration()? {
            return Ok(conf);
        }
        let config: Configuration = Default::default();
        self.datasource.put_configuration(&config)?;
        Ok(config)
    }

    fn get_excludes(&self) -> Vec<PathBuf> {
        let path = self.datasource.get_db_path();
        vec![path.to_path_buf()]
    }

    fn put_computer_id(&self, dataset: &str, computer_id: &str) -> Result<(), Error> {
        self.datasource.put_computer_id(dataset, computer_id)
    }

    fn get_computer_id(&self, dataset: &str) -> Result<Option<String>, Error> {
        self.datasource.get_computer_id(dataset)
    }

    fn delete_computer_id(&self, dataset: &str) -> Result<(), Error> {
        self.datasource.delete_computer_id(dataset)
    }

    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error> {
        self.datasource.put_latest_snapshot(dataset, latest)
    }

    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error> {
        self.datasource.get_latest_snapshot(dataset)
    }

    fn delete_latest_snapshot(&self, dataset: &str) -> Result<(), Error> {
        self.datasource.delete_latest_snapshot(dataset)
    }

    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error> {
        self.datasource.insert_chunk(chunk)
    }

    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error> {
        self.datasource.get_chunk(digest)
    }

    fn insert_pack(&self, pack: &Pack) -> Result<(), Error> {
        self.datasource.insert_pack(pack)
    }

    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        self.datasource.get_pack(digest)
    }

    fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error> {
        self.datasource.insert_xattr(digest, xattr)
    }

    fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error> {
        self.datasource.get_xattr(digest)
    }

    fn insert_file(&self, file: &File) -> Result<(), Error> {
        self.datasource.insert_file(file)
    }

    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error> {
        self.datasource.get_file(digest)
    }

    fn insert_tree(&self, tree: &Tree) -> Result<(), Error> {
        self.datasource.insert_tree(tree)
    }

    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error> {
        self.datasource.get_tree(digest)
    }

    fn put_store(&self, store: &Store) -> Result<(), Error> {
        // validate the store configuration
        let builder = PackSourceBuilderImpl {};
        builder.build_source(store)?;
        self.datasource.put_store(store)
    }

    fn get_stores(&self) -> Result<Vec<Store>, Error> {
        self.datasource.get_stores()
    }

    fn get_store(&self, id: &str) -> Result<Option<Store>, Error> {
        self.datasource.get_store(id)
    }

    fn delete_store(&self, id: &str) -> Result<(), Error> {
        self.datasource.delete_store(id)
    }

    fn load_dataset_stores(&self, dataset: &Dataset) -> Result<Box<dyn PackRepository>, Error> {
        let stores: Vec<Store> = dataset
            .stores
            .iter()
            .map(|store_id| self.get_store(store_id))
            .filter_map(|s| s.ok())
            .filter_map(|s| s)
            .collect();
        if stores.is_empty() {
            return Err(err_msg(format!(
                "no stores found for dataset {}",
                dataset.id
            )));
        }
        let store_builder = Box::new(PackSourceBuilderImpl {});
        let packs: Box<dyn PackRepository> =
            Box::new(PackRepositoryImpl::new(stores, store_builder)?);
        Ok(packs)
    }

    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error> {
        self.datasource.put_dataset(dataset)
    }

    fn get_datasets(&self) -> Result<Vec<Dataset>, Error> {
        self.datasource.get_datasets()
    }

    fn get_dataset(&self, id: &str) -> Result<Option<Dataset>, Error> {
        self.datasource.get_dataset(id)
    }

    fn delete_dataset(&self, id: &str) -> Result<(), Error> {
        self.datasource.delete_dataset(id)
    }

    fn put_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error> {
        self.datasource.put_snapshot(snapshot)
    }

    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error> {
        self.datasource.get_snapshot(digest)
    }

    fn create_backup(&self, path: Option<PathBuf>) -> Result<PathBuf, Error> {
        self.datasource.create_backup(path)
    }
}

pub struct PackRepositoryImpl {
    sources: HashMap<Store, Box<dyn PackDataSource>>,
}

impl PackRepositoryImpl {
    /// Construct a pack repository that will delegate to the given stores.
    ///
    /// Defers to the provided builder to construct the pack sources.
    pub fn new(stores: Vec<Store>, builder: Box<dyn PackSourceBuilder>) -> Result<Self, Error> {
        let mut sources: HashMap<Store, Box<dyn PackDataSource>> = HashMap::new();
        for store in stores {
            let source = builder.build_source(&store)?;
            sources.insert(store, source);
        }
        Ok(Self { sources })
    }
}

impl PackRepository for PackRepositoryImpl {
    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Vec<PackLocation>, Error> {
        let mut results: Vec<PackLocation> = Vec::new();
        for (store, source) in self.sources.iter() {
            match store_pack_retry(source, packfile, bucket, object) {
                Ok(loc) => results.push(loc.into()),
                Err(err) => {
                    error!("pack store {} failed for {}/{}", store.id, bucket, object);
                    return Err(err);
                }
            }
        }
        Ok(results)
    }

    fn retrieve_pack(&self, locations: &[PackLocation], outfile: &Path) -> Result<(), Error> {
        // find a local store, if available
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id && source.is_local() {
                    let result = source.retrieve_pack(&loc, outfile);
                    if result.is_ok() {
                        return result;
                    }
                    warn!(
                        "pack retrieval failed, will try another source: {:?}",
                        result
                    );
                }
            }
        }

        // find a store that is not slow, if available
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id && !source.is_slow() {
                    let result = source.retrieve_pack(&loc, outfile);
                    if result.is_ok() {
                        return result;
                    }
                    warn!(
                        "pack retrieval failed, will try another source: {:?}",
                        result
                    );
                }
            }
        }

        // find any matching store
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id {
                    let result = source.retrieve_pack(&loc, outfile);
                    if result.is_ok() {
                        return result;
                    }
                    warn!(
                        "pack retrieval failed, will try another source: {:?}",
                        result
                    );
                }
            }
        }

        Err(err_msg("unable to retrieve pack file"))
    }
}

// Try to store the pack file up to three times before giving up.
fn store_pack_retry(
    source: &Box<dyn PackDataSource>,
    packfile: &Path,
    bucket: &str,
    object: &str,
) -> Result<PackLocation, Error> {
    let mut retries = 0;
    loop {
        let result = source.store_pack(packfile, bucket, object);
        if result.is_ok() {
            return result;
        }
        retries += 1;
        if retries == 3 {
            return result;
        }
        warn!("pack store failed, will retry: {:?}", result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::sources::{MockEntityDataSource, MockPackDataSource, MockPackSourceBuilder};
    use crate::domain::entities::StoreType;
    use mockall::predicate::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    #[test]
    fn test_get_configuration() {
        // arrange
        let config: Configuration = Default::default();
        let mut mock = MockEntityDataSource::new();
        let mut call_count = 0;
        mock.expect_get_configuration().times(2).returning(move || {
            call_count += 1;
            if call_count > 1 {
                Ok(Some(config.clone()))
            } else {
                Ok(None)
            }
        });
        mock.expect_put_configuration()
            .times(1)
            .with(always())
            .returning(|_| Ok(()));
        // act & assert
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_configuration();
        assert!(result.is_ok());
        let result = repo.get_configuration();
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_dataset_stores_ok() {
        // arrange
        let mut local_props: HashMap<String, String> = HashMap::new();
        local_props.insert("basepath".to_owned(), "/data/packs".to_owned());
        let store = Store {
            id: "local123".to_owned(),
            store_type: StoreType::LOCAL,
            label: "my local".to_owned(),
            properties: local_props,
        };
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_store()
            .with(eq("local123"))
            .returning(move |_| Ok(Some(store.clone())));
        // act
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        dataset = dataset.add_store("local123");
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.load_dataset_stores(&dataset);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_dataset_stores_none() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_store()
            .with(eq("local123"))
            .returning(move |_| Ok(None));
        // act
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        dataset = dataset.add_store("local123");
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.load_dataset_stores(&dataset);
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("no stores found for dataset"));
    }

    #[test]
    fn test_store_pack_single_source() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().with(always()).returning(|_| {
            let mut source = MockPackDataSource::new();
            source
                .expect_store_pack()
                .with(always(), eq("bucket1"), eq("object1"))
                .returning(|_, bucket, object| Ok(PackLocation::new("store", bucket, object)));
            Ok(Box::new(source))
        });
        let stores = vec![Store {
            id: "localtmp".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.store_pack(&input_file, "bucket1", "object1");
        // assert
        assert!(result.is_ok());
        let locations = result.unwrap();
        assert_eq!(locations.len(), 1);
    }

    #[test]
    fn test_store_pack_multiple_sources() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder.expect_build_source().with(always()).returning(|_| {
            let mut source = MockPackDataSource::new();
            source
                .expect_store_pack()
                .with(always(), eq("bucket1"), eq("object1"))
                .returning(|_, bucket, object| Ok(PackLocation::new("store", bucket, object)));
            Ok(Box::new(source))
        });
        let stores = vec![
            Store {
                id: "localtmp".to_owned(),
                store_type: StoreType::LOCAL,
                label: "temporary".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "minio".to_owned(),
                store_type: StoreType::MINIO,
                label: "server".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "secureftp".to_owned(),
                store_type: StoreType::SFTP,
                label: "other_server".to_owned(),
                properties: HashMap::new(),
            },
        ];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let input_file = PathBuf::from("/home/planet/important.txt");
        let result = repo.store_pack(&input_file, "bucket1", "object1");
        // assert
        assert!(result.is_ok());
        let locations = result.unwrap();
        assert_eq!(locations.len(), 3);
    }

    #[test]
    fn test_retrieve_pack_multiple_local() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder
            .expect_build_source()
            .with(always())
            .times(3)
            .returning(|store| {
                if store.store_type == StoreType::LOCAL {
                    let mut source = MockPackDataSource::new();
                    source.expect_is_local().returning(|| true);
                    source
                        .expect_retrieve_pack()
                        .withf(|location, _| location.store == "local123")
                        .returning(|_, _| Ok(()));
                    Ok(Box::new(source))
                } else if store.store_type == StoreType::MINIO {
                    let mut source = MockPackDataSource::new();
                    source.expect_is_local().times(0..2).returning(|| false);
                    source.expect_is_slow().times(0..2).returning(|| false);
                    Ok(Box::new(source))
                } else {
                    // lump all other store types into this clause
                    let mut source = MockPackDataSource::new();
                    source.expect_is_local().times(0..2).returning(|| false);
                    source.expect_is_slow().times(0..2).returning(|| true);
                    Ok(Box::new(source))
                }
            });
        let stores = vec![
            Store {
                id: "local123".to_owned(),
                store_type: StoreType::LOCAL,
                label: "temporary".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "minio123".to_owned(),
                store_type: StoreType::MINIO,
                label: "server".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "sftp123".to_owned(),
                store_type: StoreType::SFTP,
                label: "other_server".to_owned(),
                properties: HashMap::new(),
            },
        ];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        let locations = vec![
            PackLocation::new("local123", "bucket1", "object1"),
            PackLocation::new("minio123", "bucket1", "object1"),
            PackLocation::new("sftp123", "bucket1", "object1"),
        ];
        let output_file = PathBuf::from("/home/planet/restored.txt");
        let result = repo.retrieve_pack(&locations, &output_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_retrieve_pack_multiple_fast() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder
            .expect_build_source()
            .with(always())
            .times(2)
            .returning(|store| {
                if store.store_type == StoreType::MINIO {
                    let mut source = MockPackDataSource::new();
                    source.expect_is_local().times(0..2).returning(|| false);
                    source.expect_is_slow().times(0..2).returning(|| false);
                    source
                        .expect_retrieve_pack()
                        .withf(|location, _| location.store == "minio123")
                        .returning(|_, _| Ok(()));
                    Ok(Box::new(source))
                } else {
                    // lump all other store types into this clause
                    let mut source = MockPackDataSource::new();
                    source.expect_is_local().times(0..2).returning(|| false);
                    source.expect_is_slow().times(0..2).returning(|| true);
                    Ok(Box::new(source))
                }
            });
        let stores = vec![
            Store {
                id: "minio123".to_owned(),
                store_type: StoreType::MINIO,
                label: "server".to_owned(),
                properties: HashMap::new(),
            },
            Store {
                id: "sftp123".to_owned(),
                store_type: StoreType::SFTP,
                label: "other_server".to_owned(),
                properties: HashMap::new(),
            },
        ];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        // pass more locations than defined stores just because
        let locations = vec![
            PackLocation::new("local123", "bucket1", "object1"),
            PackLocation::new("minio123", "bucket1", "object1"),
            PackLocation::new("sftp123", "bucket1", "object1"),
        ];
        let output_file = PathBuf::from("/home/planet/restored.txt");
        let result = repo.retrieve_pack(&locations, &output_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_retrieve_pack_multiple_any() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder
            .expect_build_source()
            .with(always())
            .times(1)
            .returning(|_| {
                let mut source = MockPackDataSource::new();
                source.expect_is_local().times(1).returning(|| false);
                source.expect_is_slow().times(1).returning(|| true);
                source
                    .expect_retrieve_pack()
                    .withf(|location, _| location.store == "sftp123")
                    .returning(|_, _| Ok(()));
                Ok(Box::new(source))
            });
        let stores = vec![Store {
            id: "sftp123".to_owned(),
            store_type: StoreType::SFTP,
            label: "other_server".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        // pass more locations than defined stores just because
        let locations = vec![
            PackLocation::new("local123", "bucket1", "object1"),
            PackLocation::new("minio123", "bucket1", "object1"),
            PackLocation::new("sftp123", "bucket1", "object1"),
        ];
        let output_file = PathBuf::from("/home/planet/restored.txt");
        let result = repo.retrieve_pack(&locations, &output_file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_retrieve_pack_multiple_err() {
        // arrange
        let mut builder = MockPackSourceBuilder::new();
        builder
            .expect_build_source()
            .with(always())
            .times(1)
            .returning(|_| Ok(Box::new(MockPackDataSource::new())));
        let stores = vec![Store {
            id: "sftp123".to_owned(),
            store_type: StoreType::SFTP,
            label: "other_server".to_owned(),
            properties: HashMap::new(),
        }];
        // act
        let result = PackRepositoryImpl::new(stores, Box::new(builder));
        assert!(result.is_ok());
        let repo = result.unwrap();
        // none of the locations match any of the defined stores
        let locations = vec![
            PackLocation::new("local123", "bucket1", "object1"),
            PackLocation::new("minio123", "bucket1", "object1"),
        ];
        let output_file = PathBuf::from("/home/planet/restored.txt");
        let result = repo.retrieve_pack(&locations, &output_file);
        // assert
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("unable to retrieve pack file"));
    }
}
