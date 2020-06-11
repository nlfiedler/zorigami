//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::data::sources::EntityDataSource;
use crate::domain::entities::{Checksum, Chunk, Configuration, Dataset, Snapshot, Store};
use crate::domain::repositories::RecordRepository;
use failure::Error;
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

    fn put_latest_snapshot(&self, dataset: &str, latest: &Checksum) -> Result<(), Error> {
        self.datasource.put_latest_snapshot(dataset, latest)
    }

    fn get_latest_snapshot(&self, dataset: &str) -> Result<Option<Checksum>, Error> {
        self.datasource.get_latest_snapshot(dataset)
    }

    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error> {
        self.datasource.insert_chunk(chunk)
    }

    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error> {
        self.datasource.get_chunk(digest)
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

    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error> {
        self.datasource.get_snapshot(digest)
    }
}

// pub struct BlobRepositoryImpl {
//     basepath: PathBuf,
// }

// impl BlobRepositoryImpl {
//     pub fn new(basepath: &Path) -> Self {
//         Self {
//             basepath: basepath.to_path_buf(),
//         }
//     }
// }

// impl BlobRepository for BlobRepositoryImpl {
//     fn store_blob(&self, filepath: &Path, asset: &Asset) -> Result<(), Error> {
//         let dest_path = self.blob_path(&asset.key)?;
//         // do not overwrite existing asset blobs
//         if !dest_path.exists() {
//             let parent = dest_path
//                 .parent()
//                 .ok_or_else(|| err_msg(format!("no parent for {:?}", dest_path)))?;
//             std::fs::create_dir_all(parent)?;
//             // Use file copy to handle crossing file systems, then remove the
//             // original afterward.
//             //
//             // N.B. Store the asset as-is, do not make any modifications. Any
//             // changes that are needed will be made later, and likely not
//             // committed back to disk unless requested by the user.
//             std::fs::copy(filepath, dest_path)?;
//         }
//         std::fs::remove_file(filepath)?;
//         Ok(())
//     }

//     fn blob_path(&self, asset_id: &str) -> Result<PathBuf, Error> {
//         let decoded = base64::decode(asset_id)?;
//         let as_string = str::from_utf8(&decoded)?;
//         let rel_path = Path::new(&as_string);
//         let mut full_path = self.basepath.clone();
//         full_path.push(rel_path);
//         Ok(full_path)
//     }

//     fn rename_blob(&self, old_id: &str, new_id: &str) -> Result<(), Error> {
//         let old_path = self.blob_path(old_id)?;
//         let new_path = self.blob_path(new_id)?;
//         std::fs::rename(old_path, new_path)?;
//         Ok(())
//     }

//     fn thumbnail(&self, width: u32, height: u32, asset_id: &str) -> Result<Vec<u8>, Error> {
//         let filepath = self.blob_path(asset_id)?;
//         create_thumbnail(&filepath, width, height)
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::sources::MockEntityDataSource;
    use crate::domain::entities::StoreType;
    use failure::err_msg;
    use mockall::predicate::*;
    use std::collections::HashMap;
    use std::path::Path;

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

    //     #[test]
    //     fn test_store_blob_ok() {
    //         // arrange
    //         let import_date = Utc.ymd(2018, 5, 31).and_hms(21, 10, 11);
    //         let id_path = "2018/05/31/2100/01bx5zzkbkactav9wevgemmvrz.jpg";
    //         let id = base64::encode(id_path);
    //         let digest = "sha256-82084759e4c766e94bb91d8cf9ed9edc1d4480025205f5109ec39a806509ee09";
    //         let asset1 = Asset {
    //             key: id,
    //             checksum: digest.to_owned(),
    //             filename: "fighting_kittens.jpg".to_owned(),
    //             byte_length: 39932,
    //             media_type: "image/jpeg".to_owned(),
    //             tags: vec!["kittens".to_owned()],
    //             caption: None,
    //             import_date,
    //             location: None,
    //             user_date: None,
    //             original_date: None,
    //             dimensions: None,
    //         };
    //         let tmpdir = tempdir().unwrap();
    //         let basepath = tmpdir.path().join("blobs");
    //         // copy test file to temporary path as it will be (re)moved
    //         let original = PathBuf::from("./tests/fixtures/fighting_kittens.jpg");
    //         let copy = tmpdir.path().join("fighting_kittens.jpg");
    //         std::fs::copy(original, &copy).unwrap();
    //         // act
    //         let repo = BlobRepositoryImpl::new(basepath.as_path());
    //         let result = repo.store_blob(copy.as_path(), &asset1);
    //         // assert
    //         assert!(result.is_ok());
    //         let mut dest_path = basepath.clone();
    //         dest_path.push(id_path);
    //         assert!(dest_path.exists());
    //         std::fs::remove_dir_all(basepath).unwrap();
    //     }

    //     #[test]
    //     fn test_blob_path_ok() {
    //         // arrange
    //         let import_date = Utc.ymd(2018, 5, 31).and_hms(21, 10, 11);
    //         let id_path = "2018/05/31/2100/01bx5zzkbkactav9wevgemmvrz.jpg";
    //         let id = base64::encode(id_path);
    //         let digest = "sha256-82084759e4c766e94bb91d8cf9ed9edc1d4480025205f5109ec39a806509ee09";
    //         let asset1 = Asset {
    //             key: id,
    //             checksum: digest.to_owned(),
    //             filename: "fighting_kittens.jpg".to_owned(),
    //             byte_length: 39932,
    //             media_type: "image/jpeg".to_owned(),
    //             tags: vec!["kittens".to_owned()],
    //             caption: None,
    //             import_date,
    //             location: None,
    //             user_date: None,
    //             original_date: None,
    //             dimensions: None,
    //         };
    //         // act
    //         let repo = BlobRepositoryImpl::new(Path::new("foobar/blobs"));
    //         let result = repo.blob_path(&asset1.key);
    //         // assert
    //         assert!(result.is_ok());
    //         let mut blob_path = PathBuf::from("foobar/blobs");
    //         blob_path.push(id_path);
    //         assert_eq!(result.unwrap(), blob_path.as_path());
    //     }
}
