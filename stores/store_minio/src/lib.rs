//
// Copyright (c) 2025 Nathan Fiedler
//
use anyhow::{Error, anyhow};
use aws_config::stalled_stream_protection::StalledStreamProtectionConfig;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ObjectLockEnabled, ObjectLockMode};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use store_core::{CollisionError, Coordinates};

// Per-attempt ceiling for a single S3 request (excluding SDK retry
// attempts). Generous enough that a multi-hundred-MB database archive
// can complete over a slow uplink, but bounded so a wedged operation
// eventually surfaces as an error. Stalled-stream protection (configured
// below) is the primary defense against half-open sockets during a
// transfer; this constant is the hard upper bound.
const OPERATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60 * 60);
// Time the SDK waits for the first byte of the response after a request
// has been sent. NOT a per-read inactivity timeout on a streaming body —
// that case is handled by stalled-stream protection.
const READ_TIMEOUT: Duration = Duration::from_secs(120);
// TCP connect deadline.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

///
/// A pack store implementation that uses the Amazon S3 protocol to connect to a
/// Minio storage server.
///
#[derive(Clone, Debug)]
pub struct MinioStore {
    store_id: String,
    // Object-lock (WORM) window in days. Zero means no lock, preserving the
    // pre-immutability behavior. When positive, buckets are created with S3
    // Object Lock enabled and every uploaded object is put under a
    // compliance-mode retention of this many days.
    lock_days: u16,
    s3: aws_sdk_s3::Client,
}

impl MinioStore {
    /// Validate the given store and construct a minio pack source.
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let region = props
            .get("region")
            .ok_or_else(|| anyhow!("missing region property"))?;
        let endpoint = props
            .get("endpoint")
            .ok_or_else(|| anyhow!("missing endpoint property"))?;
        let access_key = props
            .get("access_key")
            .ok_or_else(|| anyhow!("missing access_key property"))?;
        let secret_key = props
            .get("secret_key")
            .ok_or_else(|| anyhow!("missing secret_key property"))?;

        // aws-sdk requires a full URL (scheme + host). Historically we accepted
        // bare host[:port] forms; preserve that for clearly local endpoints
        // (where http is the obvious intent) but require an explicit scheme
        // for anything else, so we don't silently downgrade production traffic
        // to cleartext.
        let endpoint_url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.clone()
        } else {
            let host = endpoint.split(':').next().unwrap_or("");
            if host == "localhost" || host == "127.0.0.1" || host == "[::1]" {
                format!("http://{}", endpoint)
            } else {
                return Err(anyhow!(
                    "endpoint must include scheme (http:// or https://): {}",
                    endpoint
                ));
            }
        };

        let creds = Credentials::new(
            access_key.clone(),
            secret_key.clone(),
            None,
            None,
            "zorigami-static",
        );
        // Bound each request so a stalled HTTP stream surfaces as an error
        // and the outer retry loop in `data::repositories` can try again,
        // rather than hanging the backup task forever. Note: `put_object`
        // issues a single-shot upload (we do not invoke the higher-level
        // `TransferManager`), so the entire pack/database transfer counts
        // against one `operation_attempt_timeout` — hence the generous cap.
        let timeouts = TimeoutConfig::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .operation_attempt_timeout(OPERATION_ATTEMPT_TIMEOUT)
            .build();
        // Detect stalled body streams in both directions (uploads and
        // downloads). Without this, a TCP socket that goes half-open mid
        // transfer would only be caught by `operation_attempt_timeout`,
        // which can be a long wait for a large transfer.
        let stalled = StalledStreamProtectionConfig::enabled().build();
        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .region(Region::new(region.clone()))
            .endpoint_url(endpoint_url)
            .force_path_style(true)
            .timeout_config(timeouts)
            .stalled_stream_protection(stalled)
            .build();
        let s3 = aws_sdk_s3::Client::from_conf(s3_config);

        Ok(Self {
            store_id: store_id.to_owned(),
            lock_days: store_core::lock_days_from_props(props),
            s3,
        })
    }

    pub fn store_pack_sync(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        // use and_then(std::convert::identity) until Result.flatten() is stable
        block_on(self.store_pack(packfile, bucket, object)).and_then(std::convert::identity)
    }

    pub async fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        // the bucket must exist before receiving objects; the bucket may be
        // renamed if the chosen name collides with an existing bucket
        let bucket = try_create_bucket(&self.s3, bucket, self.lock_days > 0).await?;
        let body = ByteStream::from_path(packfile)
            .await
            .map_err(anyhow::Error::new)?;
        let mut request = self.s3.put_object().bucket(&bucket).key(object).body(body);
        // When object lock is configured, place this object under a
        // compliance-mode retention. No principal can delete or overwrite it
        // until the retain-until date passes; the pruner defers its deletes
        // until the same window has elapsed.
        if self.lock_days > 0 {
            let retain_until =
                aws_sdk_s3::primitives::DateTime::from(store_core::lock_retain_until(self.lock_days));
            request = request
                .object_lock_mode(ObjectLockMode::Compliance)
                .object_lock_retain_until_date(retain_until);
        }
        let result = request.send().await?;
        if let Some(etag) = result.e_tag() {
            // compute MD5 of file and compare to returned e_tag
            let md5 = store_core::md5sum_file(packfile)?;
            // AWS S3 quotes the etag values for some reason
            let stripped_etag = etag.trim_matches('"');
            if !md5.eq(stripped_etag) {
                return Err(anyhow!("returned e_tag does not match MD5 of pack file"));
            }
        }
        let loc = Coordinates::new(&self.store_id, &bucket, object);
        Ok(loc)
    }

    pub fn retrieve_pack_sync(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        block_on(self.retrieve_pack(location, outfile)).and_then(std::convert::identity)
    }

    pub async fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        let result = self
            .s3
            .get_object()
            .bucket(&location.bucket)
            .key(&location.object)
            .send()
            .await?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(outfile)
            .await?;
        let mut body = result.body.into_async_read();
        tokio::io::copy(&mut body, &mut file).await?;
        Ok(())
    }

    pub fn list_buckets_sync(&self) -> Result<Vec<String>, Error> {
        block_on(self.list_buckets()).and_then(std::convert::identity)
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let result = self.s3.list_buckets().send().await?;
        let results: Vec<String> = result
            .buckets()
            .iter()
            .filter_map(|b| b.name().map(|s| s.to_string()))
            .collect();
        Ok(results)
    }

    pub fn list_objects_sync(&self, bucket: &str) -> Result<Vec<String>, Error> {
        block_on(self.list_objects(bucket)).and_then(std::convert::identity)
    }

    pub async fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let mut paginator = self
            .s3
            .list_objects_v2()
            .bucket(bucket)
            .into_paginator()
            .send();
        let mut results = Vec::new();
        while let Some(page) = paginator.next().await {
            let page = page?;
            for entry in page.contents() {
                if let Some(key) = entry.key() {
                    results.push(key.to_string());
                }
            }
        }
        Ok(results)
    }

    pub fn delete_object_sync(&self, bucket: &str, object: &str) -> Result<(), Error> {
        block_on(self.delete_object(bucket, object)).and_then(std::convert::identity)
    }

    pub async fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        self.s3
            .delete_object()
            .bucket(bucket)
            .key(object)
            .send()
            .await?;
        Ok(())
    }

    pub fn delete_bucket_sync(&self, bucket: &str) -> Result<(), Error> {
        block_on(self.delete_bucket(bucket)).and_then(std::convert::identity)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        match self.s3.delete_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                // NoSuchBucket is benign during cleanup; everything else
                // (auth, network, BucketNotEmpty, AccessDenied) should surface.
                if e.code() == Some("NoSuchBucket") {
                    Ok(())
                } else {
                    Err(anyhow::Error::new(e.into_service_error()))
                }
            }
        }
    }

    pub fn store_database_sync(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        self.store_pack_sync(packfile, bucket, object)
    }

    pub fn retrieve_database_sync(
        &self,
        location: &Coordinates,
        outfile: &Path,
    ) -> Result<(), Error> {
        self.retrieve_pack_sync(location, outfile)
    }

    pub fn list_databases_sync(&self, bucket: &str) -> Result<Vec<String>, Error> {
        self.list_objects_sync(bucket)
    }
}

/// Ensure a bucket exists, generating a new name on collision.
///
/// If the given bucket name already exists and belongs to a different account,
/// generate a new random bucket name and retry. Returns the name of the bucket
/// that was successfully created or already owned by this account.
async fn try_create_bucket(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    object_lock: bool,
) -> Result<String, Error> {
    let mut bucket_name = bucket.to_owned();
    loop {
        match create_bucket(client, &bucket_name, object_lock).await {
            Ok(()) => return Ok(bucket_name),
            Err(err) => match err.downcast::<CollisionError>() {
                Ok(_) => {
                    bucket_name = uuid::Uuid::new_v4().to_string();
                }
                Err(err) => return Err(err),
            },
        }
    }
}

/// Ensure the named bucket exists.
///
/// When `object_lock` is set, the bucket is created with S3 Object Lock enabled
/// (which also enables versioning). Object Lock can only be turned on at bucket
/// creation time, so this is the sole opportunity to do so.
async fn create_bucket(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    object_lock: bool,
) -> Result<(), Error> {
    let mut req = client.create_bucket().bucket(bucket);
    if object_lock {
        req = req.object_lock_enabled_for_bucket(true);
    }
    match req.send().await {
        Ok(_) => Ok(()),
        Err(e) => match e.into_service_error() {
            CreateBucketError::BucketAlreadyExists(_) => Err(Error::from(CollisionError {})),
            CreateBucketError::BucketAlreadyOwnedByYou(_) => {
                if object_lock {
                    ensure_object_lock_enabled(client, bucket).await
                } else {
                    Ok(())
                }
            }
            other => Err(anyhow::Error::new(other)),
        },
    }
}

/// Verify a pre-existing bucket actually has Object Lock enabled.
///
/// Object Lock can only be turned on at bucket creation, so a store that has
/// `lock_days` set but points at a bucket created without it would otherwise
/// silently attempt locked uploads that the server rejects with an opaque
/// error. Fail fast with an actionable message instead.
async fn ensure_object_lock_enabled(
    client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<(), Error> {
    let not_enabled = || {
        anyhow!(
            "bucket {} exists without Object Lock enabled; a store with lock_days \
             requires buckets created with Object Lock (see doc/DEPLOY.md)",
            bucket
        )
    };
    match client
        .get_object_lock_configuration()
        .bucket(bucket)
        .send()
        .await
    {
        Ok(output) => {
            let enabled = output
                .object_lock_configuration()
                .and_then(|c| c.object_lock_enabled())
                .map(|e| e == &ObjectLockEnabled::Enabled)
                .unwrap_or(false);
            if enabled { Ok(()) } else { Err(not_enabled()) }
        }
        // A bucket with no lock configuration reports this specific code; treat
        // it as "not enabled". Anything else (auth, network) surfaces unchanged.
        Err(e) if e.code() == Some("ObjectLockConfigurationNotFoundError") => Err(not_enabled()),
        Err(e) => Err(anyhow::Error::new(e.into_service_error())),
    }
}

/// Run the given future on a newly created single-threaded runtime if possible,
/// otherwise raise an error if this thread already has a runtime.
fn block_on<F: std::future::Future>(future: F) -> Result<F::Output, Error> {
    match tokio::runtime::Handle::try_current() {
        Ok(_handle) => Err(anyhow!("cannot call block_on inside a runtime")),
        _ => {
            // Build the simplest and lightest runtime we can, while still enabling
            // us to wait for this future (and everything it spawns) to complete
            // synchronously. Must enable the io and time features otherwise the
            // runtime does not really start.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            Ok(runtime.block_on(future))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy::dotenv;
    use std::env;
    use tempfile::tempdir;

    #[test]
    fn test_new_minio_store_region() {
        let props = HashMap::new();
        let result = MinioStore::new("minio123", &props);
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing region property"));
        // could check all of the others, I guess?
    }

    #[test]
    fn test_new_minio_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), "us-west2".to_owned());
        properties.insert("endpoint".to_owned(), "localhost:9000".to_owned());
        properties.insert("access_key".to_owned(), "minio".to_owned());
        properties.insert("secret_key".to_owned(), "shminio".to_owned());
        let result = MinioStore::new("minio123", &properties);
        assert!(result.is_ok());
    }

    #[test]
    fn test_minio_wrong_account() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let endp_var = env::var("MINIO_ENDPOINT");
        if endp_var.is_err() {
            // bail out silently if minio is not available
            return Ok(());
        }
        let endpoint = endp_var?;
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), "us-west-2".into());
        properties.insert("endpoint".to_owned(), endpoint);
        properties.insert("access_key".to_owned(), "not_access_key".into());
        properties.insert("secret_key".to_owned(), "not_secret_key".into());
        let source = MinioStore::new("minio2", &properties)?;
        // act
        let result = source.list_buckets_sync();
        // assert
        assert!(result.is_err());
        // aws-sdk wraps the original error code in the cause chain rather than
        // the top-level Display, so use the Debug formatter to inspect it.
        let err_string = format!("{:?}", result.err().unwrap());
        // MinIO and RustFS give slightly different error messages
        assert!(
            err_string.contains("InvalidAccessKeyId") || err_string.contains("UnauthorizedAccess")
        );
        Ok(())
    }

    #[test]
    fn test_minio_collision_error() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let endp_var = env::var("MINIO_ENDPOINT");
        if endp_var.is_err() {
            // bail out silently if minio is not available
            return Ok(());
        }
        let endpoint = endp_var?;
        let region = env::var("MINIO_REGION")?;
        let access_key_1 = env::var("MINIO_ACCESS_KEY_1")?;
        let secret_key_1 = env::var("MINIO_SECRET_KEY_1")?;

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), region);
        properties.insert("endpoint".to_owned(), endpoint);
        properties.insert("access_key".to_owned(), access_key_1);
        properties.insert("secret_key".to_owned(), secret_key_1);
        let source1 = MinioStore::new("minioone", &properties)?;

        // store an object
        let bucket = xid::new().to_string();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let location = source1.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "minioone");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // store another object to the same bucket but as a different user
        //
        // apparently this is not an issue out of the box, as minio allows
        // different access keys to modify any buckets and objects; the
        // bucket create operation returns "already owned by you" error
        let access_key_2 = env::var("MINIO_ACCESS_KEY_2")?;
        let secret_key_2 = env::var("MINIO_SECRET_KEY_2")?;
        properties.insert("access_key".to_owned(), access_key_2);
        properties.insert("secret_key".to_owned(), secret_key_2);
        let source2 = MinioStore::new("miniotwo", &properties)?;
        let object = "489492a49220c814f49487efb12adfbc372aa3f8".to_owned();
        let packfile = Path::new("../../test/fixtures/washington-journal.txt");
        let location = source2.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "miniotwo");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // do _NOT_ remove all buckets and objects, other test needs them;
        // tests are run concurrently, only one can clean up everything
        Ok(())
    }

    #[test]
    fn test_minio_store_roundtrip() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let endp_var = env::var("MINIO_ENDPOINT");
        if endp_var.is_err() {
            // bail out silently if minio is not available
            return Ok(());
        }
        let endpoint = endp_var?;
        let region = env::var("MINIO_REGION")?;
        let access_key = env::var("MINIO_ACCESS_KEY_1")?;
        let secret_key = env::var("MINIO_SECRET_KEY_1")?;

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), region);
        properties.insert("endpoint".to_owned(), endpoint);
        properties.insert("access_key".to_owned(), access_key);
        properties.insert("secret_key".to_owned(), secret_key);
        let source = MinioStore::new("minioone", &properties)?;

        // store an object
        let bucket = xid::new().to_string();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "minioone");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // store another object to ensure create bucket works repeatedly
        let object = "489492a49220c814f49487efb12adfbc372aa3f8".to_owned();
        let packfile = Path::new("../../test/fixtures/washington-journal.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "minioone");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // check for bucket(s) being present
        let buckets = source.list_buckets_sync()?;
        assert!(!buckets.is_empty());
        assert!(buckets.contains(&bucket));

        // check for object(s) being present
        let listing = source.list_objects_sync(&bucket)?;
        assert!(!listing.is_empty());
        assert!(listing.contains(&object));

        // retrieve the file and verify by checksum
        let outdir = tempdir()?;
        let outfile = outdir.path().join("restored.txt");
        let result = source.retrieve_pack_sync(&location, &outfile);
        assert!(result.is_ok());
        let md5sum = store_core::md5sum_file(&outfile)?;
        #[cfg(target_family = "unix")]
        assert_eq!(md5sum, "4b9772cf2c623ad529900f0ffe4e8ded");
        #[cfg(target_family = "windows")]
        assert_eq!(md5sum, "f143ccda41d1e2a553ef214e4549cd6e");

        // remove all objects from all buckets, and the buckets, too
        for bucket in buckets {
            let objects = source.list_objects_sync(&bucket)?;
            for obj in objects {
                source.delete_object_sync(&bucket, &obj)?;
            }
            source.delete_bucket_sync(&bucket)?;
        }
        Ok(())
    }

    #[test]
    fn test_minio_object_lock() -> Result<(), Error> {
        // Opt-in only: enabling Object Lock also enables bucket versioning, so
        // the locked object version cannot be reclaimed until its retention
        // expires (an app-level `delete_object` merely writes a delete marker,
        // which is why this test does NOT assert on delete). The test thus
        // leaves an object and its bucket behind for the lock window. Run it
        // against a throwaway MinIO/S3 endpoint that honors Object Lock by
        // setting MINIO_OBJECT_LOCK=1 in addition to the usual MINIO_* vars.
        dotenv().ok();
        if env::var("MINIO_OBJECT_LOCK").is_err() {
            return Ok(());
        }
        let endp_var = env::var("MINIO_ENDPOINT");
        if endp_var.is_err() {
            return Ok(());
        }
        let endpoint = endp_var?;
        let region = env::var("MINIO_REGION")?;
        let access_key = env::var("MINIO_ACCESS_KEY_1")?;
        let secret_key = env::var("MINIO_SECRET_KEY_1")?;

        // arrange: a store configured with a one-day compliance lock
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), region);
        properties.insert("endpoint".to_owned(), endpoint);
        properties.insert("access_key".to_owned(), access_key);
        properties.insert("secret_key".to_owned(), secret_key);
        properties.insert("lock_days".to_owned(), "1".to_owned());
        let source = MinioStore::new("miniolock", &properties)?;

        // storing a pack must create a lock-enabled bucket and attach the
        // per-object compliance retention without error
        let bucket = xid::new().to_string();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // the object is present in the lock-enabled bucket
        let listing = source.list_objects_sync(&bucket)?;
        assert!(listing.contains(&object));

        // a second store_pack against the now-existing lock-enabled bucket must
        // still succeed: the ensure-object-lock-enabled check on the
        // already-owned bucket path must pass for a genuinely locked bucket
        let object2 = "489492a49220c814f49487efb12adfbc372aa3f8".to_owned();
        let packfile2 = Path::new("../../test/fixtures/washington-journal.txt");
        source.store_pack_sync(packfile2, &bucket, &object2)?;
        Ok(())
    }
}
