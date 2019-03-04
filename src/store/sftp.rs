//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use ssh2::{FileStat, Session};
use std::fs::File;
use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};

///
/// A `Store` implementation that operates over SSH2/SFTP to store pack files on
/// a remote system. Use `new()` and the builder functions to prepare an
/// instance to connect to a system using various credentials.
///
pub struct SftpStore {
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

impl SftpStore {
    ///
    /// Create an instance of `SftpStore` to connect to the given system using
    /// the named user account. Set up the password or other means of
    /// authentication using the builder functions.
    ///
    pub fn new(addr: &str, username: &str) -> Self {
        Self {
            remote_addr: addr.to_owned(),
            username: username.to_owned(),
            password: None,
            basepath: None,
        }
    }

    ///
    /// Add the password property.
    ///
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_owned());
        self
    }

    ///
    /// Add the basepath property.
    ///
    pub fn basepath(mut self, basepath: &str) -> Self {
        self.basepath = Some(basepath.to_owned());
        self
    }

    ///
    /// Connect to the SFTP server using an SSH connection. The caller must
    /// instantiate the Sftp instance using the Session in connection.
    ///
    fn connect(&self) -> Result<Connection, Error> {
        let tcp = TcpStream::connect(&self.remote_addr)?;
        let mut sess = Session::new().unwrap();
        sess.handshake(&tcp)?;
        sess.userauth_password(&self.username, self.password.as_ref().unwrap())?;
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
    fn store_pack(&self, packfile: &Path, bucket: &str, object: &str) -> Result<(), Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
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
        Ok(())
    }

    fn retrieve_pack(&self, bucket: &str, object: &str, outfile: &Path) -> Result<(), Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
        let object_path: PathBuf = match &self.basepath {
            Some(bp) => [bp, bucket, object].iter().collect(),
            None => [bucket, object].iter().collect(),
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
        let dirname: &Path = match &self.basepath {
            Some(bp) => Path::new(bp),
            None => Path::new(".")
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
        let bucket_path: PathBuf = match &self.basepath {
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
        let object_path: PathBuf = match &self.basepath {
            Some(bp) => [bp, bucket, object].iter().collect(),
            None => [bucket, object].iter().collect(),
        };
        sftp.unlink(&object_path)?;
        Ok(())
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let conn = self.connect()?;
        let sftp = conn.session.sftp()?;
        let bucket_path: PathBuf = match &self.basepath {
            Some(bp) => [bp, bucket].iter().collect(),
            None => PathBuf::from(bucket),
        };
        sftp.rmdir(&bucket_path)?;
        Ok(())
    }
}

impl Default for SftpStore {
    fn default() -> Self {
        Self {
            remote_addr: String::from(""),
            username: String::from(""),
            password: None,
            basepath: None,
        }
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

#[cfg(test)]
mod tests {
    use crate::core;
    use crate::store::Store;
    use dotenv::dotenv;
    use std::env;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn test_sftp_roundtrip() {
        // set up the environment and remote connection
        dotenv().ok();
        let addr_var = env::var("SFTP_ADDR");
        if addr_var.is_err() {
            return
        }
        let address = addr_var.unwrap();
        let username = env::var("SFTP_USER").unwrap();
        let password = env::var("SFTP_PASSWORD").unwrap();
        let basepath = env::var("SFTP_BASEPATH").unwrap();
        let mut store = super::SftpStore::new(&address, &username);
        store = store.password(&password);
        store = store.basepath(&basepath);
        let unique_id = core::generate_unique_id("charlie", "localhost");
        let bucket = core::generate_bucket_name(&unique_id);

        // create a pack file with a checksum name
        let chunks = [core::Chunk::new(
            "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f",
            0,
            3129,
        )
        .filepath(Path::new("./test/fixtures/lorem-ipsum.txt"))];
        let outdir = tempdir().unwrap();
        let ptmpfile = outdir.path().join("pack.tar");
        let digest = core::pack_chunks(&chunks[..], &ptmpfile).unwrap();
        let packfile = outdir.path().join(&digest);
        std::fs::rename(&ptmpfile, &packfile).unwrap();

        // store the pack file on the remote side
        let result = store.store_pack(&packfile, &bucket, &digest);
        assert!(result.is_ok());

        // check for bucket(s) being present; may be more from previous runs
        let result = store.list_buckets();
        assert!(result.is_ok());
        let buckets = result.unwrap();
        assert!(!buckets.is_empty());
        assert!(buckets.contains(&bucket));

        // check for object(s) being present
        let result = store.list_objects(&bucket);
        assert!(result.is_ok());
        let listing = result.unwrap();
        assert!(!listing.is_empty());
        assert!(listing.contains(&digest));

        // retrieve the file and verify by checksum
        let result = store.retrieve_pack(&bucket, &digest, &ptmpfile);
        assert!(result.is_ok());
        let sha256 = core::checksum_file(&ptmpfile);
        assert_eq!(
            sha256.unwrap(),
            "sha256-9fd73dfe8b3815ebbf9b0932816306526104336017d9ba308e37e48bce5ab150"
        );

        // remove all objects from all buckets, and the buckets, too
        for buckette in buckets {
            let result = store.list_objects(&buckette);
            assert!(result.is_ok());
            let objects = result.unwrap();
            for obj in objects {
                store.delete_object(&buckette, &obj).unwrap();
            }
            store.delete_bucket(&buckette).unwrap();
        }
    }
}
