//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

///
/// Configuration for the LocalStore implementation.
///
#[derive(Serialize, Deserialize, Debug)]
struct LocalConfig {
    name: String,
    basepath: String,
}

impl super::Config for LocalConfig {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn from_json(&mut self, data: &str) -> Result<(), Error> {
        let conf: LocalConfig = serde_json::from_str(data)?;
        self.name = conf.name;
        self.basepath = conf.basepath;
        Ok(())
    }

    fn to_json(&self) -> Result<String, Error> {
        let j = serde_json::to_string(&self)?;
        Ok(j)
    }
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            name: String::from("default"),
            basepath: String::from("."),
        }
    }
}

///
/// A `Store` implementation in which pack files are stored on a locally
/// accessible file sytem.
///
pub struct LocalStore {
    config: LocalConfig,
}

impl Default for LocalStore {
    fn default() -> Self {
        Self {
            config: Default::default(),
        }
    }
}

impl super::Store for LocalStore {
    fn get_type(&self) -> super::StoreType {
        super::StoreType::LOCAL
    }

    fn get_config(&self) -> &super::Config {
        &self.config
    }

    fn get_config_mut(&mut self) -> &mut super::Config {
        &mut self.config
    }

    fn store_pack(&self, packfile: &Path, bucket: &str, object: &str) -> Result<String, Error> {
        let mut path: PathBuf = [&self.config.basepath, bucket].iter().collect();
        fs::create_dir_all(&path)?;
        path.push(object);
        fs::copy(packfile, &path)?;
        fs::remove_file(packfile)?;
        Ok(object.to_owned())
    }

    fn retrieve_pack(&self, bucket: &str, object: &str, outfile: &Path) -> Result<(), Error> {
        let path: PathBuf = [&self.config.basepath, bucket, object].iter().collect();
        fs::copy(&path, outfile)?;
        Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let mut results = Vec::new();
        for entry in fs::read_dir(&self.config.basepath)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                results.push(entry.file_name().to_str().unwrap().to_owned());
            }
        }
        Ok(results)
    }

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let path: PathBuf = [&self.config.basepath, bucket].iter().collect();
        let mut results = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                results.push(entry.file_name().to_str().unwrap().to_owned());
            }
        }
        Ok(results)
    }

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let path: PathBuf = [&self.config.basepath, bucket, object].iter().collect();
        fs::remove_file(path)?;
        Ok(())
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let path: PathBuf = [&self.config.basepath, bucket].iter().collect();
        fs::remove_dir(path)?;
        Ok(())
    }
}
