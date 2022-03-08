//
// Copyright (c) 2020 Nathan Fiedler
//
use anyhow::{anyhow, Error};
use ssh2::{FileStat, Session};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use store_core::Coordinates;

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
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let remote_addr = props
            .get("remote_addr")
            .ok_or_else(|| anyhow!("missing remote_addr property"))?;
        let username = props
            .get("username")
            .ok_or_else(|| anyhow!("missing username property"))?;
        let password = props.get("password").map(|s| s.to_owned());
        let basepath = props.get("basepath").map(|s| s.to_owned());
        Ok(Self {
            store_id: store_id.to_owned(),
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

    pub fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
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
        let loc = Coordinates::new(&self.store_id, bucket, object);
        Ok(loc)
    }

    pub fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
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

    pub fn list_buckets(&self) -> Result<Vec<String>, Error> {
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
                if let Some(name) = store_core::get_file_name(&path) {
                    results.push(name);
                }
            }
        }
        Ok(results)
    }

    pub fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
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
                if let Some(name) = store_core::get_file_name(&path) {
                    results.push(name);
                }
            }
        }
        Ok(results)
    }

    pub fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let sess = self.connect()?;
        let sftp = sess.sftp()?;
        let object_path: PathBuf = match &self.basepath {
            Some(bp) => [bp, bucket, object].iter().collect(),
            None => [bucket, object].iter().collect(),
        };
        sftp.unlink(&object_path)?;
        Ok(())
    }

    pub fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
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
    use dotenv::dotenv;
    use std::env;
    use tempfile::tempdir;

    #[test]
    fn test_new_sftp_store_region() {
        let properties = HashMap::new();
        let result = SftpStore::new("sftp123", &properties);
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
        let result = SftpStore::new("sftp123", &properties);
        assert!(result.is_ok());
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
        let result = SftpStore::new("sftpone", &properties);
        assert!(result.is_ok());
        let source = result.unwrap();

        // store an object
        let bucket = "747267d56e7057118a9aa40c24c1730f".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
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
