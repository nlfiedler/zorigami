//
// Copyright (c) 2020 Nathan Fiedler
//
use failure::{err_msg, Error};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use store_core::Coordinates;

///
/// A pack store implementation in which pack files are stored on a locally
/// accessible file system.
///
#[derive(Debug)]
pub struct LocalStore {
    store_id: String,
    basepath: String,
}

impl LocalStore {
    /// Validate the given store and construct a local pack source.
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let basepath = props
            .get("basepath")
            .ok_or_else(|| err_msg("missing basepath property"))?;
        Ok(Self {
            store_id: store_id.to_owned(),
            basepath: basepath.to_owned(),
        })
    }

    pub fn is_local(&self) -> bool {
        true
    }

    pub fn is_slow(&self) -> bool {
        false
    }

    pub fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        let mut path: PathBuf = [&self.basepath, bucket].iter().collect();
        fs::create_dir_all(&path)?;
        path.push(object);
        fs::copy(packfile, &path)?;
        let loc = Coordinates::new(&self.store_id, bucket, object);
        Ok(loc)
    }

    pub fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        let path: PathBuf = [&self.basepath, &location.bucket, &location.object]
            .iter()
            .collect();
        fs::copy(&path, outfile)?;
        Ok(())
    }

    pub fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let mut results = Vec::new();
        for entry in fs::read_dir(&self.basepath)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = store_core::get_file_name(&path) {
                    // ignore folders that are definitely not our buckets
                    if !name.starts_with(".") {
                        results.push(name);
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let path: PathBuf = [&self.basepath, bucket].iter().collect();
        let mut results = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = store_core::get_file_name(&path) {
                    // ignore files that are definitely not our buckets
                    if !name.starts_with(".") {
                        results.push(name);
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let path: PathBuf = [&self.basepath, bucket, object].iter().collect();
        fs::remove_file(path)?;
        Ok(())
    }

    pub fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let path: PathBuf = [&self.basepath, bucket].iter().collect();
        fs::remove_dir(path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn test_new_local_store_basepath() {
        let props = HashMap::new();
        let result = LocalStore::new("local123", &props);
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing basepath property"));
    }

    #[test]
    fn test_new_local_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/tmp".to_owned());
        let result = LocalStore::new("local123", &properties);
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
        let result = LocalStore::new("localone", &properties);
        assert!(result.is_ok());
        let source = result.unwrap();

        // store an object
        let bucket = "747267d56e7057118a9aa40c24c1730f".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
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
        let md5sum = store_core::md5sum_file(&outfile).unwrap();
        #[cfg(target_family = "unix")]
        assert_eq!(md5sum, "40756e6058736e2485119410c2014380");
        #[cfg(target_family = "windows")]
        assert_eq!(
            // this checksum is wrong and will need to be fixed
            md5sum,
            "40756e6058736e2485119410c2014380"
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
