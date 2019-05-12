//
// Copyright (c) 2019 Nathan Fiedler
//
use crate::core::PackLocation;
use failure::Error;
use serde::{Deserialize, Serialize};
use ssh2::{FileStat, Session};
use std::fs::File;
use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};

///
/// Configuration for the SftpStore implementation.
///
#[derive(Serialize, Deserialize, Debug)]
struct SftpConfig {
    /// Host and port of the SFTP server (e.g. "127.0.0.1:22")
    remote_addr: String,
    /// Name of the user account on the SFTP server.
    username: String,
    /// Password for the user account on the SFTP server.
    password: Option<String>,
    /// Path on the SFTP server where buckets are stored.
    basepath: Option<String>,
    // privateKey: Buffer | string
    // passphrase: string
}

impl super::Config for SftpConfig {
    fn from_json(&mut self, data: &str) -> Result<(), Error> {
        let conf: SftpConfig = serde_json::from_str(data)?;
        self.remote_addr = conf.remote_addr;
        self.username = conf.username;
        self.password = conf.password;
        self.basepath = conf.basepath;
        Ok(())
    }

    fn to_json(&self) -> Result<String, Error> {
        let j = serde_json::to_string(&self)?;
        Ok(j)
    }
}

impl Default for SftpConfig {
    fn default() -> Self {
        Self {
            remote_addr: String::from("127.0.0.1:22"),
            username: String::from("charlie"),
            password: None,
            basepath: None,
        }
    }
}

///
/// A `Store` implementation that operates over SSH2/SFTP to store pack files on
/// a remote system. Use `new()` and the builder functions to prepare an
/// instance to connect to a system using various credentials.
///
pub struct SftpStore {
    unique_id: String,
    config: SftpConfig,
}

impl SftpStore {
    /// Construct a new instance of SftpStore with the given identifier.
    pub fn new(uuid: &str) -> Self {
        Self {
            unique_id: uuid.to_owned(),
            config: Default::default(),
        }
    }
}

impl SftpStore {
    ///
    /// Connect to the SFTP server using an SSH connection. The caller must
    /// instantiate the Sftp instance using the Session in connection.
    ///
    fn connect(&self) -> Result<Connection, Error> {
        let tcp = TcpStream::connect(&self.config.remote_addr)?;
        let mut sess = Session::new().unwrap();
        sess.handshake(&tcp)?;
        sess.userauth_password(
            &self.config.username,
            self.config.password.as_ref().unwrap(),
        )?;
        Ok(Connection {
            _stream: tcp,
            session: sess,
        })
    }
}

///
/// Holds the TCP stream and SFTP session in one place because the stream is
/// merely referenced and may be dropped prematurely (see the ssh2 docs). When
/// the connection is dropped, so will the session and stream.
///
struct Connection {
    _stream: TcpStream,
    session: Session,
}

impl super::Store for SftpStore {
    fn get_id(&self) -> &str {
        &self.unique_id
    }

    fn get_type(&self) -> super::StoreType {
        super::StoreType::SFTP
    }

    fn get_speed(&self) -> super::StoreSpeed {
        super::StoreSpeed::FAST
    }

    fn get_config(&self) -> &super::Config {
        &self.config
    }

    fn get_config_mut(&mut self) -> &mut super::Config {
        &mut self.config
    }

    fn store_pack(&self, packfile: &Path, bucket: &str, object: &str) -> Result<PackLocation, Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
        let mut path: PathBuf = match &self.config.basepath {
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
        let loc = PackLocation::new(&self.unique_id, bucket, object);
        Ok(loc)
    }

    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
        let object_path: PathBuf = match &self.config.basepath {
            Some(bp) => [bp, &location.bucket, &location.object].iter().collect(),
            None => [&location.bucket, &location.object].iter().collect(),
        };
        let mut remote = sftp.open(&object_path)?;
        let mut local = File::create(outfile)?;
        io::copy(&mut remote, &mut local)?;
        Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
        // Default the directory to something, it cannot be blank or ~ as that
        // results in a "no such file" error. Regardless, it is discarded when
        // we produce the results so it matters not.
        let dirname: &Path = match &self.config.basepath {
            Some(bp) => Path::new(bp),
            None => Path::new("."),
        };
        let listing: Vec<(PathBuf, FileStat)> = sftp.readdir(dirname)?;
        let mut results = Vec::new();
        for (path, stat) in listing {
            if stat.is_dir() {
                let fno = get_file_name(&path);
                if fno.is_some() {
                    results.push(fno.unwrap());
                }
            }
        }
        Ok(results)
    }

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
        let bucket_path: PathBuf = match &self.config.basepath {
            Some(bp) => [bp, bucket].iter().collect(),
            None => PathBuf::from(bucket),
        };
        let listing: Vec<(PathBuf, FileStat)> = sftp.readdir(&bucket_path)?;
        let mut results = Vec::new();
        for (path, stat) in listing {
            if stat.is_file() {
                let fno = get_file_name(&path);
                if fno.is_some() {
                    results.push(fno.unwrap());
                }
            }
        }
        Ok(results)
    }

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
        let object_path: PathBuf = match &self.config.basepath {
            Some(bp) => [bp, bucket, object].iter().collect(),
            None => [bucket, object].iter().collect(),
        };
        sftp.unlink(&object_path)?;
        Ok(())
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
        let bucket_path: PathBuf = match &self.config.basepath {
            Some(bp) => [bp, bucket].iter().collect(),
            None => PathBuf::from(bucket),
        };
        sftp.rmdir(&bucket_path)?;
        Ok(())
    }
}

///
/// Return the last part of the path, converting to a String.
///
fn get_file_name(path: &Path) -> Option<String> {
    let p = path.file_name();
    // ignore any paths that end in '..'
    if p.is_some() {
        let pp = p.unwrap().to_str();
        // ignore any paths that failed UTF-8 translation
        if pp.is_some() {
            return Some(pp.unwrap().to_owned());
        }
    }
    None
}
