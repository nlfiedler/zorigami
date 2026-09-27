//
// Copyright (c) 2020 Nathan Fiedler
//
use anyhow::{Error, anyhow};
use russh::client::{self, Handle};
use russh::keys::PublicKeyOrCertificate;
use russh_sftp::client::SftpSession;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use store_core::Coordinates;
use tokio::io::AsyncWriteExt;

// TCP connect and SSH handshake deadline so a blackholed remote (e.g. firewall
// drop) fails fast rather than hanging the backup task. This also covers DNS
// resolution, which happens inside the connect future.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
// If nothing at all is received from the server for this long, russh closes
// the connection. This is the primary defense against a half-open connection
// silently stalling a pack transfer.
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(120);
// Send an SSH keepalive after this much quiet time, so that an idle but
// healthy connection is not mistaken for a dead one.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
// Maximum time to wait for the response to any single SFTP request.
const SFTP_REQUEST_TIMEOUT_SECS: u64 = 120;

/// SSH client event handler; the only event of interest is host key checking.
struct Client;

impl client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        // Host keys are not verified, matching the behavior of the previous
        // libssh2 implementation which never consulted known_hosts.
        Ok(true)
    }
}

/// An open SFTP session along with the SSH connection that carries it. The
/// connection handle must outlive the SFTP session.
struct Connection {
    ssh: Handle<Client>,
    sftp: SftpSession,
}

impl Connection {
    /// Close the SFTP channel and disconnect the SSH session. Errors are
    /// ignored since the operation of interest has already completed.
    async fn close(self) {
        let _ = self.sftp.close().await;
        let _ = self
            .ssh
            .disconnect(russh::Disconnect::ByApplication, "", "")
            .await;
    }
}

///
/// A `PackDataSource` implementation that operates over SSH2/SFTP to store pack
/// files on a remote system.
///
#[derive(Debug)]
pub struct SftpStore {
    store_id: String,
    address: String,
    username: String,
    password: Option<String>,
    basepath: Option<String>,
    // private_key: Option<String>,
    // passphrase: Option<String>,
}

impl SftpStore {
    /// Validate the given store and construct a secure FTP pack source.
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let address = props
            .get("address")
            .ok_or_else(|| anyhow!("missing address property"))?;
        let username = props
            .get("username")
            .ok_or_else(|| anyhow!("missing username property"))?;
        let password = props.get("password").map(|s| s.to_owned());
        let basepath = props.get("basepath").map(|s| s.to_owned());
        Ok(Self {
            store_id: store_id.to_owned(),
            address: address.to_owned(),
            username: username.to_owned(),
            password,
            basepath,
        })
    }

    /// Connect to the SFTP server using an SSH connection.
    async fn connect(&self) -> Result<Connection, Error> {
        // Simply build a new session and connection every time. Trying to reuse
        // the session does not improve the run time in the slightest.
        let config = client::Config {
            inactivity_timeout: Some(INACTIVITY_TIMEOUT),
            keepalive_interval: Some(KEEPALIVE_INTERVAL),
            ..Default::default()
        };
        let mut ssh = tokio::time::timeout(
            CONNECT_TIMEOUT,
            client::connect(Arc::new(config), self.address.as_str(), Client),
        )
        .await
        .map_err(|_| anyhow!("timed out connecting to {}", self.address))??;
        let password = self.password.clone().unwrap_or_default();
        let auth = ssh.authenticate_password(&self.username, password).await?;
        if !auth.success() {
            return Err(anyhow!("Authentication failed for user {}", self.username));
        }
        let channel = ssh.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let sftp = SftpSession::new(channel.into_stream()).await?;
        sftp.set_timeout(SFTP_REQUEST_TIMEOUT_SECS);
        Ok(Connection { ssh, sftp })
    }

    /// Join the base path (if any) with the given components. SFTP paths
    /// always use forward slashes, regardless of the local platform.
    fn remote_path(&self, parts: &[&str]) -> String {
        let mut path = self.basepath.clone().unwrap_or_default();
        for part in parts {
            if !path.is_empty() && !path.ends_with('/') {
                path.push('/');
            }
            path.push_str(part);
        }
        path
    }

    pub fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        block_on(self.store_pack_async(packfile, bucket, object))?
    }

    async fn store_pack_async(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        let conn = self.connect().await?;
        // mkdir will fail if directory already exists, let's just ignore all
        // errors for mkdir and hope that it was not a real issue
        let _ = conn.sftp.create_dir(self.remote_path(&[bucket])).await;
        let mut remote = conn
            .sftp
            .create(self.remote_path(&[bucket, object]))
            .await?;
        let mut local = tokio::fs::File::open(packfile).await?;
        tokio::io::copy(&mut local, &mut remote).await?;
        remote.shutdown().await?;
        conn.close().await;
        Ok(Coordinates::new(&self.store_id, bucket, object))
    }

    pub fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        block_on(self.retrieve_pack_async(location, outfile))?
    }

    async fn retrieve_pack_async(
        &self,
        location: &Coordinates,
        outfile: &Path,
    ) -> Result<(), Error> {
        let conn = self.connect().await?;
        let object_path = self.remote_path(&[&location.bucket, &location.object]);
        let mut remote = conn.sftp.open(object_path).await?;
        let mut local = tokio::fs::File::create(outfile).await?;
        tokio::io::copy(&mut remote, &mut local).await?;
        local.flush().await?;
        remote.shutdown().await?;
        conn.close().await;
        Ok(())
    }

    pub fn list_buckets(&self) -> Result<Vec<String>, Error> {
        block_on(async {
            let conn = self.connect().await?;
            // Default the directory to something, it cannot be blank or ~ as
            // that results in a "no such file" error.
            let dirname = self.basepath.clone().unwrap_or_else(|| ".".into());
            let listing = conn.sftp.read_dir(dirname).await?;
            let results = listing
                .filter(|entry| entry.file_type().is_dir())
                .filter_map(|entry| store_core::get_file_name(Path::new(&entry.file_name())))
                .collect();
            conn.close().await;
            Ok(results)
        })?
    }

    pub fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        block_on(async {
            let conn = self.connect().await?;
            let listing = conn.sftp.read_dir(self.remote_path(&[bucket])).await?;
            let results = listing
                .filter(|entry| entry.file_type().is_file())
                .filter_map(|entry| store_core::get_file_name(Path::new(&entry.file_name())))
                .collect();
            conn.close().await;
            Ok(results)
        })?
    }

    pub fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        block_on(async {
            let conn = self.connect().await?;
            conn.sftp
                .remove_file(self.remote_path(&[bucket, object]))
                .await?;
            conn.close().await;
            Ok(())
        })?
    }

    pub fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        block_on(async {
            let conn = self.connect().await?;
            conn.sftp.remove_dir(self.remote_path(&[bucket])).await?;
            conn.close().await;
            Ok(())
        })?
    }

    pub fn store_database(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        self.store_pack(packfile, bucket, object)
    }

    pub fn retrieve_database(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        self.retrieve_pack(location, outfile)
    }

    pub fn list_databases(&self, bucket: &str) -> Result<Vec<String>, Error> {
        self.list_objects(bucket)
    }
}

/// Run the future to completion on a new, single-threaded runtime.
fn block_on<F: Future>(future: F) -> Result<F::Output, Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(anyhow!("cannot call block_on inside a runtime"));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    Ok(runtime.block_on(future))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy::dotenv;
    use std::env;
    use tempfile::tempdir;

    #[test]
    fn test_new_sftp_store_region() {
        let properties = HashMap::new();
        let result = SftpStore::new("sftp123", &properties);
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing address property"));
        // could check all of the others, I guess?
    }

    #[test]
    fn test_new_sftp_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("address".to_owned(), "localhost:22".to_owned());
        properties.insert("username".to_owned(), "charlie".to_owned());
        let result = SftpStore::new("sftp123", &properties);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sftp_wrong_account() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let addr_var = env::var("SFTP_ADDR");
        if addr_var.is_err() {
            // bail out silently if sftp is not configured
            return Ok(());
        }
        let address = addr_var.unwrap();
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("address".to_owned(), address);
        properties.insert("username".to_owned(), "charlie".into());
        properties.insert("password".to_owned(), "secret123".into());
        properties.insert("basepath".to_owned(), "/".into());
        let source = SftpStore::new("sftpone", &properties)?;
        // act
        let result = source.list_buckets();
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("Authentication failed"));
        Ok(())
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
        properties.insert("address".to_owned(), address);
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
        assert_eq!(md5sum, "8aed508af644bc58db20c9b73c5b67ad");

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
