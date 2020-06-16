//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::data::sources::{EntityDataSource, PackDataSource, PackSourceBuilder};
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, PackLocation, Snapshot, Store, Tree,
};
use crate::domain::repositories::{PackRepository, RecordRepository};
use failure::{err_msg, Error};
use std::collections::HashMap;
use std::path::Path;
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
        self.datasource.put_store(store)
    }

    fn get_stores(&self) -> Result<Vec<Store>, Error> {
        self.datasource.get_stores()
    }

    fn delete_store(&self, id: &str) -> Result<(), Error> {
        self.datasource.delete_store(id)
    }

    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error> {
        self.datasource.put_dataset(dataset)
    }

    fn get_datasets(&self) -> Result<Vec<Dataset>, Error> {
        self.datasource.get_datasets()
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
        for source in self.sources.values() {
            let loc = source.store_pack(packfile, bucket, object)?;
            results.push(loc);
        }
        Ok(results)
    }

    fn retrieve_pack(&self, locations: &[PackLocation], outfile: &Path) -> Result<(), Error> {
        // find a local store, if available
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id && source.is_local() {
                    return source.retrieve_pack(loc, outfile);
                }
            }
        }

        // find a store that is not slow, if available
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id && !source.is_slow() {
                    return source.retrieve_pack(loc, outfile);
                }
            }
        }

        // find any matching store
        for loc in locations.iter() {
            for (store, source) in self.sources.iter() {
                if loc.store == store.id {
                    return source.retrieve_pack(loc, outfile);
                }
            }
        }

        Err(err_msg("cannot find any store for pack"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::sources::{MockEntityDataSource, MockPackDataSource, MockPackSourceBuilder};
    use crate::domain::entities::{StoreType, TreeEntry, TreeReference};
    use failure::err_msg;
    use mockall::predicate::*;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

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
    fn test_put_computer_id_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_computer_id()
            .with(eq("cafebabe"), eq("charlietuna"))
            .returning(|_, _| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.put_computer_id("cafebabe", "charlietuna");
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_put_computer_id_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_computer_id()
            .with(eq("cafebabe"), eq("charlietuna"))
            .returning(|_, _| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.put_computer_id("cafebabe", "charlietuna");
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_computer_id_some() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_computer_id()
            .with(eq("cafebabe"))
            .returning(|_| Ok(Some(String::from("charlietuna"))));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_computer_id("cafebabe");
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let value = option.unwrap();
        assert_eq!(value, "charlietuna");
    }

    #[test]
    fn test_get_computer_id_none() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_computer_id()
            .with(eq("cafebabe"))
            .returning(|_| Ok(None));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_computer_id("cafebabe");
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_none());
    }

    #[test]
    fn test_get_computer_id_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_computer_id()
            .with(eq("cafebabe"))
            .returning(|_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_computer_id("cafebabe");
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_computer_id_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_computer_id()
            .with(eq("cafebabe"))
            .returning(|_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.delete_computer_id("cafebabe");
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_computer_id_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_computer_id()
            .with(eq("cafebabe"))
            .returning(|_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.delete_computer_id("cafebabe");
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_put_latest_snapshot_ok() {
        // arrange
        let digest = Checksum::SHA1("e1c3cc593da3c696ddc3200ad137ef79681c8052".to_owned());
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_latest_snapshot()
            .with(eq("cafebabe"), eq(digest.clone()))
            .returning(|_, _| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.put_latest_snapshot("cafebabe", &digest);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_put_latest_snapshot_err() {
        // arrange
        let digest = Checksum::SHA1("e1c3cc593da3c696ddc3200ad137ef79681c8052".to_owned());
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_latest_snapshot()
            .with(eq("cafebabe"), eq(digest.clone()))
            .returning(|_, _| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.put_latest_snapshot("cafebabe", &digest);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_latest_snapshot_some() {
        // arrange
        let digest = Checksum::SHA1("e1c3cc593da3c696ddc3200ad137ef79681c8052".to_owned());
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_latest_snapshot()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(digest.clone())));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_latest_snapshot("cafebabe");
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let value = option.unwrap();
        assert_eq!(
            value.to_string(),
            "sha1-e1c3cc593da3c696ddc3200ad137ef79681c8052"
        );
    }

    #[test]
    fn test_get_latest_snapshot_none() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_latest_snapshot()
            .with(eq("cafebabe"))
            .returning(|_| Ok(None));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_latest_snapshot("cafebabe");
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_none());
    }

    #[test]
    fn test_get_latest_snapshot_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_latest_snapshot()
            .with(eq("cafebabe"))
            .returning(|_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_latest_snapshot("cafebabe");
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_latest_snapshot_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_latest_snapshot()
            .with(eq("cafebabe"))
            .returning(|_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.delete_latest_snapshot("cafebabe");
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_latest_snapshot_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_latest_snapshot()
            .with(eq("cafebabe"))
            .returning(|_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.delete_latest_snapshot("cafebabe");
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_chunk_ok() {
        // arrange
        let digest1 = Checksum::SHA1(String::from("cafebabe"));
        let chunk1 = Chunk {
            digest: digest1.clone(),
            offset: 0,
            length: 65536,
            filepath: None,
            packfile: Some(Checksum::SHA1(String::from("deadbeef"))),
        };
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_chunk()
            .withf(move |chunk| chunk.digest == digest1)
            .returning(|_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_chunk(&chunk1);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_chunk_err() {
        // arrange
        let digest1 = Checksum::SHA1(String::from("cafebabe"));
        let chunk1 = Chunk {
            digest: digest1.clone(),
            offset: 0,
            length: 65536,
            filepath: None,
            packfile: Some(Checksum::SHA1(String::from("deadbeef"))),
        };
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_chunk()
            .withf(move |chunk| chunk.digest == digest1)
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_chunk(&chunk1);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_chunk_ok() {
        // arrange
        let digest1 = Checksum::SHA1(String::from("cafebabe"));
        let chunk1 = Chunk {
            digest: digest1.clone(),
            offset: 0,
            length: 65536,
            filepath: None,
            packfile: Some(Checksum::SHA1(String::from("deadbeef"))),
        };
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_chunk()
            .with(eq(digest1))
            .returning(move |_| Ok(Some(chunk1.clone())));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let digest1 = Checksum::SHA1(String::from("cafebabe"));
        let result = repo.get_chunk(&digest1);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let chunk = option.unwrap();
        assert_eq!(chunk.length, 65536);
    }

    #[test]
    fn test_get_chunk_err() {
        // arrange
        let digest1 = Checksum::SHA1(String::from("cafebabe"));
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_chunk()
            .with(eq(digest1))
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let digest1 = Checksum::SHA1(String::from("cafebabe"));
        let result = repo.get_chunk(&digest1);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_pack_ok() {
        // arrange
        let digest = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let digest_clone = digest.clone();
        let coords = vec![PackLocation::new("store1", "bucket1", "object1")];
        let pack = Pack::new(digest, coords);
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_pack()
            .withf(move |pack| pack.digest == digest_clone)
            .returning(|_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_pack(&pack);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_pack_err() {
        // arrange
        let digest = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let digest_clone = digest.clone();
        let coords = vec![PackLocation::new("store1", "bucket1", "object1")];
        let pack = Pack::new(digest, coords);
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_pack()
            .withf(move |pack| pack.digest == digest_clone)
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_pack(&pack);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_pack_ok() {
        // arrange
        let digest = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let digest_clone2 = digest.clone();
        let digest_clone3 = digest.clone();
        let coords = vec![PackLocation::new("store1", "bucket1", "object1")];
        let pack = Pack::new(digest, coords);
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_pack()
            .with(eq(digest_clone3))
            .returning(move |_| Ok(Some(pack.clone())));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_pack(&digest_clone2);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let actual = option.unwrap();
        assert_eq!(actual.locations.len(), 1);
    }

    #[test]
    fn test_get_pack_err() {
        // arrange
        let digest1 = Checksum::SHA1(String::from("cafebabe"));
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_pack()
            .with(eq(digest1))
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let digest1 = Checksum::SHA1(String::from("cafebabe"));
        let result = repo.get_pack(&digest1);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_xattr_ok() {
        // arrange
        let raw_xattr: Vec<u8> = vec![
            0x62, 0x70, 0x6C, 0x69, 0x73, 0x74, 0x30, 0x30, 0xA0, 0x08, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
        ];
        let sha1sum = Checksum::sha1_from_bytes(&raw_xattr);
        let sha1copy = sha1sum.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_xattr()
            .with(eq(sha1copy), always())
            .returning(|_, _| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_xattr(&sha1sum, &raw_xattr);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_xattr_err() {
        // arrange
        let raw_xattr: Vec<u8> = vec![
            0x62, 0x70, 0x6C, 0x69, 0x73, 0x74, 0x30, 0x30, 0xA0, 0x08, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
        ];
        let sha1sum = Checksum::sha1_from_bytes(&raw_xattr);
        let sha1copy = sha1sum.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_xattr()
            .with(eq(sha1copy), always())
            .returning(|_, _| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_xattr(&sha1sum, &raw_xattr);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_xattr_ok() {
        // arrange
        let raw_xattr: Vec<u8> = vec![
            0x62, 0x70, 0x6C, 0x69, 0x73, 0x74, 0x30, 0x30, 0xA0, 0x08, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
        ];
        let sha1sum = Checksum::sha1_from_bytes(&raw_xattr);
        let sha1copy = sha1sum.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_xattr()
            .with(eq(sha1copy))
            .returning(move |_| Ok(Some(raw_xattr.clone())));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_xattr(&sha1sum);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let actual = option.unwrap();
        let new1sum = Checksum::sha1_from_bytes(&actual);
        assert_eq!(new1sum, sha1sum);
    }

    #[test]
    fn test_get_xattr_err() {
        // arrange
        let sha1sum = "136792f4174fe829652ee94803d6db13a0ad1698";
        let digest = Checksum::SHA1(String::from(sha1sum));
        let digest_clone = digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_xattr()
            .with(eq(digest_clone))
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_xattr(&digest);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_file_ok() {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let chunks = vec![(0, file_digest.clone())];
        let file = File::new(file_digest.clone(), 3129, chunks);
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_file()
            .withf(move |file| file.digest == file_digest)
            .returning(|_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_file(&file);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_file_err() {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let chunks = vec![(0, file_digest.clone())];
        let file = File::new(file_digest.clone(), 3129, chunks);
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_file()
            .withf(move |file| file.digest == file_digest)
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_file(&file);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_file_ok() {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let digest_clone = file_digest.clone();
        let chunks = vec![(0, file_digest.clone())];
        let file = File::new(file_digest.clone(), 3129, chunks);
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_file()
            .with(eq(file_digest))
            .returning(move |_| Ok(Some(file.clone())));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_file(&digest_clone);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let actual = option.unwrap();
        assert_eq!(actual.length, 3129);
    }

    #[test]
    fn test_get_file_err() {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let digest_clone = file_digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_file()
            .with(eq(digest_clone))
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_file(&file_digest);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_tree_ok() {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let reference = TreeReference::FILE(file_digest);
        let filepath = Path::new("./tests/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(filepath, reference);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_tree()
            .withf(move |tree| tree.digest == tree_digest)
            .returning(|_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_tree(&tree);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_tree_err() {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let reference = TreeReference::FILE(file_digest);
        let filepath = Path::new("./tests/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(filepath, reference);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_insert_tree()
            .withf(move |tree| tree.digest == tree_digest)
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.insert_tree(&tree);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_tree_ok() {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let reference = TreeReference::FILE(file_digest);
        let filepath = Path::new("./tests/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(filepath, reference);
        let tree = Tree::new(vec![entry], 1);
        let tree_digest = tree.digest.clone();
        let digest_clone = tree_digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_tree()
            .with(eq(digest_clone))
            .returning(move |_| Ok(Some(tree.clone())));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_tree(&tree_digest);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let actual = option.unwrap();
        assert_eq!(actual.entries.len(), 1);
        assert_eq!(actual.entries[0].name, "lorem-ipsum.txt");
    }

    #[test]
    fn test_get_tree_err() {
        // arrange
        let sha1sum = "33078530a30953d3705095e63b159c5abf588de7";
        let tree_digest = Checksum::SHA1(String::from(sha1sum));
        let digest_clone = tree_digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_tree()
            .with(eq(digest_clone))
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_tree(&tree_digest);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_put_store_ok() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        };
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_store().returning(move |_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.put_store(&store);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_put_store_err() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        };
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_store()
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.put_store(&store);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_stores_ok() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let stores = vec![Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        }];
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_stores()
            .returning(move || Ok(stores.clone()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_stores();
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].id, "cafebabe");
    }

    #[test]
    fn test_get_stores_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_stores()
            .returning(move || Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_stores();
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_store_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_store()
            .with(eq("abc123"))
            .returning(move |_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.delete_store("abc123");
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_store_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_store()
            .with(eq("abc123"))
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.delete_store("abc123");
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_put_dataset_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_dataset()
            .withf(|d| d.basepath.to_string_lossy() == "/home/planet")
            .returning(move |_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let dataset = Dataset::new(Path::new("/home/planet"));
        let result = repo.put_dataset(&dataset);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_put_dataset_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_dataset()
            .withf(|d| d.basepath.to_string_lossy() == "/home/planet")
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let dataset = Dataset::new(Path::new("/home/planet"));
        let result = repo.put_dataset(&dataset);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_dataset_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_dataset()
            .with(eq("abc123"))
            .returning(move |_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.delete_dataset("abc123");
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_dataset_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_dataset()
            .with(eq("abc123"))
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.delete_dataset("abc123");
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_put_snapshot_ok() {
        // arrange
        let parent = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let tree = Checksum::SHA1(String::from("811ea7199968a119eeba4b65ace06cc7f835c497"));
        let snapshot = Snapshot::new(Some(parent), tree, 1024);
        let digest = snapshot.digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_snapshot()
            .withf(move |s| s.digest == digest)
            .returning(move |_| Ok(()));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.put_snapshot(&snapshot);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_put_snapshot_err() {
        // arrange
        let parent = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let tree = Checksum::SHA1(String::from("811ea7199968a119eeba4b65ace06cc7f835c497"));
        let snapshot = Snapshot::new(Some(parent), tree, 1024);
        let digest = snapshot.digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_snapshot()
            .withf(move |s| s.digest == digest)
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.put_snapshot(&snapshot);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_snapshot_some() {
        // arrange
        let tree = Checksum::SHA1("0e944139579d8af563786cceabc4524f97c69ba8".to_owned());
        let digest = Checksum::SHA1("e1c3cc593da3c696ddc3200ad137ef79681c8052".to_owned());
        let snapshot = Snapshot::new(None, tree, 1024);
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_snapshot()
            .with(eq(digest.clone()))
            .returning(move |_| Ok(Some(snapshot.clone())));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_snapshot(&digest);
        // assert
        assert!(result.is_ok());
        let opt = result.unwrap();
        assert!(opt.is_some());
        assert_eq!(opt.unwrap().file_count, 1024);
    }

    #[test]
    fn test_get_snapshot_none() {
        // arrange
        let digest = Checksum::SHA1("e1c3cc593da3c696ddc3200ad137ef79681c8052".to_owned());
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_snapshot()
            .with(eq(digest.clone()))
            .returning(|_| Ok(None));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_snapshot(&digest);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_get_snapshot_err() {
        // arrange
        let digest = Checksum::SHA1("e1c3cc593da3c696ddc3200ad137ef79681c8052".to_owned());
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_snapshot()
            .with(eq(digest.clone()))
            .returning(move |_| Err(err_msg("oh no")));
        // act
        let repo = RecordRepositoryImpl::new(Arc::new(mock));
        let result = repo.get_snapshot(&digest);
        // assert
        assert!(result.is_err());
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
        assert!(err_string.contains("cannot find any store for pack"));
    }
}
