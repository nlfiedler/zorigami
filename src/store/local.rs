//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use std::fs;
use std::path::{Path, PathBuf};

///
/// A `Store` implementation in which pack files are stored on a locally
/// accessible file sytem.
///
pub struct LocalStore {
    basepath: String,
}

impl LocalStore {
    ///
    /// Create an instance of `LocalStore` with the given base path.
    ///
    pub fn new(basepath: &str) -> Self {
        Self {
            basepath: basepath.to_owned()
        }
    }
}

impl super::Store for LocalStore {
    fn store_pack(&self, packfile: &Path, bucket: &str, object: &str) -> Result<(), Error> {
        let mut path: PathBuf = [&self.basepath, bucket].iter().collect();
        fs::create_dir_all(&path)?;
        path.push(object);
        fs::copy(packfile, &path)?;
        fs::remove_file(packfile)?;
        Ok(())
    }

    fn retrieve_pack(&self, bucket: &str, object: &str, outfile: &Path) -> Result<(), Error> {
        let path: PathBuf = [&self.basepath, bucket, object].iter().collect();
        fs::copy(&path, outfile)?;
        Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let mut results = Vec::new();
        for entry in fs::read_dir(&self.basepath)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                results.push(path.to_str().unwrap().to_owned());
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
                results.push(path.to_str().unwrap().to_owned());
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
