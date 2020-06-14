//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::data::sources::PackDataSource;
use crate::domain::entities::{PackLocation, Store};
use failure::{err_msg, Error};
use std::fs;
use std::path::{Path, PathBuf};

///
/// A `PackDataSource` implementation in which pack files are stored on a
/// locally accessible file system.
///
#[derive(Debug)]
pub struct LocalStore {
    store_id: String,
    basepath: String,
}

impl LocalStore {
    /// Validate the given store and construct a local pack source.
    pub fn new(store: &Store) -> Result<Self, Error> {
        let basepath = store
            .properties
            .get("basepath")
            .ok_or_else(|| err_msg("missing basepath property"))?;
        Ok(Self {
            store_id: store.id.clone(),
            basepath: basepath.to_owned(),
        })
    }
}

impl PackDataSource for LocalStore {
    fn is_local(&self) -> bool {
        true
    }

    fn is_slow(&self) -> bool {
        false
    }

    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error> {
        let mut path: PathBuf = [&self.basepath, bucket].iter().collect();
        fs::create_dir_all(&path)?;
        path.push(object);
        fs::copy(packfile, &path)?;
        let loc = PackLocation::new(&self.store_id, bucket, object);
        Ok(loc)
    }

    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        let path: PathBuf = [&self.basepath, &location.bucket, &location.object]
            .iter()
            .collect();
        fs::copy(&path, outfile)?;
        Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let mut results = Vec::new();
        for entry in fs::read_dir(&self.basepath)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = super::get_file_name(&path) {
                    results.push(name);
                }
            }
        }
        Ok(results)
    }

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let path: PathBuf = [&self.basepath, bucket].iter().collect();
        let mut results = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = super::get_file_name(&path) {
                    results.push(name);
                }
            }
        }
        Ok(results)
    }

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let path: PathBuf = [&self.basepath, bucket, object].iter().collect();
        fs::remove_file(path)?;
        Ok(())
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let path: PathBuf = [&self.basepath, bucket].iter().collect();
        fs::remove_dir(path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{Checksum, StoreType};
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn test_new_local_store_basepath() {
        let store = Store {
            id: "local123".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties: HashMap::new(),
        };
        let result = LocalStore::new(&store);
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing basepath property"));
    }

    #[test]
    fn test_new_local_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/tmp".to_owned());
        let store = Store {
            id: "local123".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties,
        };
        let result = LocalStore::new(&store);
        assert!(result.is_ok());
        let source = result.unwrap();
        assert!(source.is_local());
        assert!(!source.is_slow());
    }

    #[test]
    fn test_local_store_roundtrip() {
        // arrange
        let basepath = "./tmp/test/local_store".to_owned();
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), basepath);
        let store = Store {
            id: "localone".to_owned(),
            store_type: StoreType::LOCAL,
            label: "temporary".to_owned(),
            properties,
        };
        let result = LocalStore::new(&store);
        assert!(result.is_ok());
        let source = result.unwrap();

        // store an object
        let bucket = "747267d56e7057118a9aa40c24c1730f".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("./tests/fixtures/lorem-ipsum.txt");
        let result = source.store_pack(packfile, &bucket, &object);
        assert!(result.is_ok());
        let location = result.unwrap();
        assert_eq!(location.store, "localone");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // check for bucket(s) being present
        let result = source.list_buckets();
        assert!(result.is_ok());
        let buckets = result.unwrap();
        assert!(!buckets.is_empty());
        assert!(buckets.contains(&bucket));

        // check for object(s) being present
        let result = source.list_objects(&bucket);
        assert!(result.is_ok());
        let listing = result.unwrap();
        assert!(!listing.is_empty());
        assert!(listing.contains(&object));

        // retrieve the file and verify by checksum
        let outdir = tempdir().unwrap();
        let outfile = outdir.path().join("restored.txt");
        let result = source.retrieve_pack(&location, &outfile);
        assert!(result.is_ok());
        let sha256 = Checksum::sha256_from_file(&outfile).unwrap();
        #[cfg(target_family = "unix")]
        assert_eq!(
            sha256.to_string(),
            "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
        );
        #[cfg(target_family = "windows")]
        assert_eq!(
            // this checksum is wrong and will need to be fixed
            sha256.to_string(),
            "sha256-b917dfd10f50d2f6eee14f822df5bcca89c0d02d29ed5db372c32c97a41ba837"
        );

        // remove all objects from all buckets, and the buckets, too
        for bucket in buckets {
            let result = source.list_objects(&bucket);
            assert!(result.is_ok());
            let objects = result.unwrap();
            for obj in objects {
                source.delete_object(&bucket, &obj).unwrap();
            }
            source.delete_bucket(&bucket).unwrap();
        }
    }
}
