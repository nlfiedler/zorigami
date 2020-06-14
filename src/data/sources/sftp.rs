//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::data::sources::PackDataSource;
use crate::domain::entities::{PackLocation, Store};
use failure::{err_msg, Error};
use ssh2::{FileStat, Session};
use std::fs::File;
use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};

///
/// A `PackDataSource` implementation that operates over SSH2/SFTP to store pack
/// files on a remote system.
///
#[derive(Debug)]
pub struct SftpStore {
    store_id: String,
    remote_addr: String,
    username: String,
    password: Option<String>,
    basepath: Option<String>,
    // private_key: Option<String>,
    // passphrase: Option<String>,
}

impl SftpStore {
    /// Validate the given store and construct a secure FTP pack source.
    pub fn new(store: &Store) -> Result<Self, Error> {
        let remote_addr = store
            .properties
            .get("remote_addr")
            .ok_or_else(|| err_msg("missing remote_addr property"))?;
        let username = store
            .properties
            .get("username")
            .ok_or_else(|| err_msg("missing username property"))?;
        let password = store.properties.get("password").map(|s| s.to_owned());
        let basepath = store.properties.get("basepath").map(|s| s.to_owned());
        Ok(Self {
            store_id: store.id.clone(),
            remote_addr: remote_addr.to_owned(),
            username: username.to_owned(),
            password,
            basepath,
        })
    }

    /// Connect to the SFTP server using an SSH connection. The caller must
    /// instantiate the Sftp instance using the Session in connection.
    fn connect(&self) -> Result<Session, Error> {
        // Simply build a new session and connection every time. Trying to reuse
        // the session though a combination of Rc and RefCell does not improve
        // the run time in the slightest.
        let tcp = TcpStream::connect(&self.remote_addr)?;
        let mut sess = Session::new()?;
        sess.set_tcp_stream(tcp);
        sess.handshake()?;
        sess.userauth_password(&self.username, self.password.as_ref().unwrap())?;
        Ok(sess)
    }
}

impl PackDataSource for SftpStore {
    fn is_local(&self) -> bool {
        false
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
        let sess = self.connect()?;
        let sftp = sess.sftp()?;
        let mut path: PathBuf = match &self.basepath {
            Some(bp) => [bp, bucket].iter().collect(),
            None => PathBuf::from(bucket),
        };
        // mkdir will fail if directory already exists, let's just ignore all
        // errors for mkdir and hope that it was not a real issue
        let _ = sftp.mkdir(&path, 0o755);
        path.push(object);
        let mut remote = sftp.create(&path)?;
        let mut local = File::open(packfile)?;
        io::copy(&mut local, &mut remote)?;
        let loc = PackLocation::new(&self.store_id, bucket, object);
        Ok(loc)
    }

    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        let sess = self.connect()?;
        let sftp = sess.sftp()?;
        let object_path: PathBuf = match &self.basepath {
            Some(bp) => [bp, &location.bucket, &location.object].iter().collect(),
            None => [&location.bucket, &location.object].iter().collect(),
        };
        let mut remote = sftp.open(&object_path)?;
        let mut local = File::create(outfile)?;
        io::copy(&mut remote, &mut local)?;
        Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let sess = self.connect()?;
        let sftp = sess.sftp()?;
        // Default the directory to something, it cannot be blank or ~ as that
        // results in a "no such file" error. Regardless, it is discarded when
        // we produce the results so it matters not.
        let dirname: &Path = match &self.basepath {
            Some(bp) => Path::new(bp),
            None => Path::new("."),
        };
        let listing: Vec<(PathBuf, FileStat)> = sftp.readdir(dirname)?;
        let mut results = Vec::new();
        for (path, stat) in listing {
            if stat.is_dir() {
                if let Some(name) = super::get_file_name(&path) {
                    results.push(name);
                }
            }
        }
        Ok(results)
    }

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let sess = self.connect()?;
        let sftp = sess.sftp()?;
        let bucket_path: PathBuf = match &self.basepath {
            Some(bp) => [bp, bucket].iter().collect(),
            None => PathBuf::from(bucket),
        };
        let listing: Vec<(PathBuf, FileStat)> = sftp.readdir(&bucket_path)?;
        let mut results = Vec::new();
        for (path, stat) in listing {
            if stat.is_file() {
                if let Some(name) = super::get_file_name(&path) {
                    results.push(name);
                }
            }
        }
        Ok(results)
    }

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let sess = self.connect()?;
        let sftp = sess.sftp()?;
        let object_path: PathBuf = match &self.basepath {
            Some(bp) => [bp, bucket, object].iter().collect(),
            None => [bucket, object].iter().collect(),
        };
        sftp.unlink(&object_path)?;
        Ok(())
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let sess = self.connect()?;
        let sftp = sess.sftp()?;
        let bucket_path: PathBuf = match &self.basepath {
            Some(bp) => [bp, bucket].iter().collect(),
            None => PathBuf::from(bucket),
        };
        sftp.rmdir(&bucket_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{Checksum, StoreType};
    use dotenv::dotenv;
    use std::collections::HashMap;
    use std::env;
    use tempfile::tempdir;

    #[test]
    fn test_new_sftp_store_region() {
        let store = Store {
            id: "sftp123".to_owned(),
            store_type: StoreType::SFTP,
            label: "s3clone".to_owned(),
            properties: HashMap::new(),
        };
        let result = SftpStore::new(&store);
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing remote_addr property"));
        // could check all of the others, I guess?
    }

    #[test]
    fn test_new_sftp_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("remote_addr".to_owned(), "localhost:22".to_owned());
        properties.insert("username".to_owned(), "charlie".to_owned());
        let store = Store {
            id: "sftp123".to_owned(),
            store_type: StoreType::SFTP,
            label: "s3clone".to_owned(),
            properties,
        };
        let result = SftpStore::new(&store);
        assert!(result.is_ok());
        let source = result.unwrap();
        assert!(!source.is_local());
        assert!(!source.is_slow());
    }

    #[test]
    fn test_sftp_store_roundtrip() {
        // set up the environment and remote connection
        dotenv().ok();
        let addr_var = env::var("SFTP_ADDR");
        if addr_var.is_err() {
            return;
        }
        let address = addr_var.unwrap();
        let username = env::var("SFTP_USER").unwrap();
        let password = env::var("SFTP_PASSWORD").unwrap();
        let basepath = env::var("SFTP_BASEPATH").unwrap();

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("remote_addr".to_owned(), address);
        properties.insert("username".to_owned(), username);
        properties.insert("password".to_owned(), password);
        properties.insert("basepath".to_owned(), basepath);
        let store = Store {
            id: "sftpone".to_owned(),
            store_type: StoreType::SFTP,
            label: "s3clone".to_owned(),
            properties,
        };
        let result = SftpStore::new(&store);
        assert!(result.is_ok());
        let source = result.unwrap();

        // store an object
        let bucket = "747267d56e7057118a9aa40c24c1730f".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("./tests/fixtures/lorem-ipsum.txt");
        let result = source.store_pack(packfile, &bucket, &object);
        assert!(result.is_ok());
        let location = result.unwrap();
        assert_eq!(location.store, "sftpone");
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
