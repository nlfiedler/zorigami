//
// Copyright (c) 2023 Nathan Fiedler
//
use anyhow::{anyhow, Error};
use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use lazy_static::lazy_static;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    CreateBucketConfiguration, CreateBucketError, CreateBucketRequest, DeleteBucketRequest,
    DeleteObjectRequest, GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client,
    StreamingBody, S3,
};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Mutex;
use store_core::{CollisionError, Coordinates};

lazy_static! {
    // Names of all existing S3 buckets. Populated and used only when too many
    // buckets have been created.
    static ref BUCKET_NAMES: Mutex<Vec<String>> = Mutex::new(Vec::new());
}

///
/// Raised when S3 indicates the account has too many buckets.
///
#[derive(thiserror::Error, Debug)]
pub struct TooManyBucketsError;

impl fmt::Display for TooManyBucketsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "too many buckets")
    }
}

///
/// A pack store implementation for Amazon S3/Glacier.
///
#[derive(Clone, Debug)]
pub struct AmazonStore {
    store_id: String,
    region: String,
    storage: String,
    access_key: String,
    secret_key: String,
}

impl AmazonStore {
    /// Validate the given store and construct an s3 pack source.
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let region = props
            .get("region")
            .ok_or_else(|| anyhow!("missing region property"))?;
        let storage = props
            .get("storage")
            .ok_or_else(|| anyhow!("missing storage property"))?;
        let access_key = props
            .get("access_key")
            .ok_or_else(|| anyhow!("missing access_key property"))?;
        let secret_key = props
            .get("secret_key")
            .ok_or_else(|| anyhow!("missing secret_key property"))?;
        Ok(Self {
            store_id: store_id.to_owned(),
            region: region.to_owned(),
            storage: storage.to_owned(),
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
        })
    }

    fn connect(&self) -> S3Client {
        //
        // Credentials are picked up in a variety of ways, see the rusoto docs:
        // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        //
        use std::str::FromStr;
        let region = Region::from_str(&self.region).unwrap_or(Region::default());
        let client = rusoto_core::request::HttpClient::new().unwrap();
        let creds = rusoto_credential::StaticProvider::new(
            self.access_key.clone(),
            self.secret_key.clone(),
            None,
            None,
        );
        S3Client::new_with(client, creds, region)
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

    // Try to create the named bucket.
    //
    // If that fails due to too many buckets, select one of the existing buckets
    // at random and use that instead.
    //
    // Returns the name of the bucket that was created or selected.
    async fn try_create_bucket(&self, client: &S3Client, bucket: &str) -> Result<String, Error> {
        match create_bucket(&client, bucket, &self.region).await {
            Err(err) => match err.downcast::<TooManyBucketsError>() {
                Ok(_) => {
                    let mut names = BUCKET_NAMES.lock().unwrap();
                    if names.is_empty() {
                        // The mutex is held during the time of requesting the
                        // list of buckets, which is acceptable. Note also that
                        // the list of buckets is never updated again, as that
                        // is assumed to be acceptable as well (normally never
                        // remove any buckets so the list remains static).
                        *names = self.list_buckets().await?;
                    }
                    use rand::{thread_rng, Rng};
                    let mut rng = thread_rng();
                    let idx = rng.gen_range(0..names.len());
                    Ok(names[idx].to_owned())
                }
                Err(err) => Err(err),
            },
            Ok(()) => Ok(bucket.to_owned()),
        }
    }

    pub async fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        let client = self.connect();
        // a bucket must exist before receiving objects; note that the bucket
        // may be renamed if there are too many buckets already
        let bucket_name = self.try_create_bucket(&client, bucket).await?;
        //
        // An alternative to streaming the entire file is to use a multi-part
        // upload and upload the large file in chunks.
        //
        let meta = std::fs::metadata(packfile)?;
        let read_stream = tokio::fs::read(packfile.to_owned())
            .into_stream()
            .map_ok(|b| Bytes::from(b));
        let req = PutObjectRequest {
            bucket: bucket_name.clone(),
            storage_class: Some(self.storage.clone()),
            key: object.to_owned(),
            content_length: Some(meta.len() as i64),
            body: Some(StreamingBody::new(read_stream)),
            ..Default::default()
        };
        // wait for the future(s) to complete
        let result = client.put_object(req).await?;
        if let Some(ref etag) = result.e_tag {
            // compute MD5 of file and compare to returned e_tag
            let md5 = store_core::md5sum_file(packfile)?;
            // AWS S3 quotes the etag values for some reason
            let stripped_etag = etag.trim_matches('"');
            if !md5.eq(stripped_etag) {
                return Err(anyhow!("returned e_tag does not match MD5 of pack file"));
            }
        }
        Ok(Coordinates::new(&self.store_id, &bucket_name, object))
    }

    pub fn retrieve_pack_sync(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        block_on(self.retrieve_pack(location, outfile)).and_then(std::convert::identity)
    }

    pub async fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        let client = self.connect();
        let request = GetObjectRequest {
            bucket: location.bucket.clone(),
            key: location.object.clone(),
            ..Default::default()
        };
        // wait for the future(s) to complete
        let result = client.get_object(request).await?;
        let stream = result.body.ok_or_else(|| {
            anyhow!(format!(
                "failed to retrieve object {} from bucket {}",
                location.object.clone(),
                location.bucket.clone()
            ))
        })?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(outfile)
            .await?;
        let mut body = stream.into_async_read();
        tokio::io::copy(&mut body, &mut file).await?;
        Ok(())
    }

    pub fn list_buckets_sync(&self) -> Result<Vec<String>, Error> {
        block_on(self.list_buckets()).and_then(std::convert::identity)
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let client = self.connect();
        // wait for the future(s) to complete
        let result = client.list_buckets().await?;
        let mut results = Vec::new();
        if let Some(buckets) = result.buckets {
            for bucket in buckets {
                if let Some(name) = bucket.name {
                    results.push(name);
                }
            }
        }
        Ok(results)
    }

    pub fn list_objects_sync(&self, bucket: &str) -> Result<Vec<String>, Error> {
        block_on(self.list_objects(bucket)).and_then(std::convert::identity)
    }

    pub async fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let client = self.connect();
        // default AWS S3 max-keys is 1,000
        let mut request = ListObjectsV2Request {
            bucket: bucket.to_owned(),
            ..Default::default()
        };
        let mut results = Vec::new();
        loop {
            // we will be re-using the request, so clone it each time
            // wait for the future(s) to complete
            let result = client.list_objects_v2(request.clone()).await?;
            if let Some(contents) = result.contents {
                for entry in contents {
                    if let Some(key) = entry.key {
                        results.push(key);
                    }
                }
            }
            // check if there are more results to be fetched
            if result.next_continuation_token.is_none() {
                break;
            }
            request.continuation_token = result.next_continuation_token;
        }
        Ok(results)
    }

    pub fn delete_object_sync(&self, bucket: &str, object: &str) -> Result<(), Error> {
        block_on(self.delete_object(bucket, object)).and_then(std::convert::identity)
    }

    pub async fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let client = self.connect();
        let request = DeleteObjectRequest {
            bucket: bucket.to_owned(),
            key: object.to_owned(),
            ..Default::default()
        };
        // wait for the future(s) to complete
        client.delete_object(request).await?;
        Ok(())
    }

    pub fn delete_bucket_sync(&self, bucket: &str) -> Result<(), Error> {
        block_on(self.delete_bucket(bucket)).and_then(std::convert::identity)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let client = self.connect();
        let request = DeleteBucketRequest {
            bucket: bucket.to_owned(),
            expected_bucket_owner: None,
        };
        // wait for the future(s) to complete
        let result = client.delete_bucket(request).await;
        // certain error conditions are okay
        match result {
            Err(e) => match e {
                RusotoError::Unknown(_) => Ok(()),
                _ => Err(anyhow!(format!("{}", e))),
            },
            Ok(_) => Ok(()),
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

    // pub async fn store_database(
    //     &self,
    //     packfile: &Path,
    //     bucket: &str,
    //     object: &str,
    // ) -> Result<Coordinates, Error> {
    //     self.store_pack(packfile, bucket, object)
    // }

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

/// Ensure the named bucket exists.
async fn create_bucket(client: &S3Client, bucket: &str, region: &str) -> Result<(), Error> {
    let config = CreateBucketConfiguration {
        location_constraint: Some(region.to_owned()),
    };
    let request = CreateBucketRequest {
        bucket: bucket.to_owned(),
        create_bucket_configuration: Some(config),
        ..Default::default()
    };
    // wait for the future(s) to complete
    let result = client.create_bucket(request).await;
    // certain error conditions are okay while others need to be detected and
    // converted to discrete error types
    match result {
        Err(e) => match e {
            RusotoError::Service(ref se) => match se {
                CreateBucketError::BucketAlreadyExists(_) => Err(Error::from(CollisionError {})),
                CreateBucketError::BucketAlreadyOwnedByYou(_) => Ok(()),
            },
            RusotoError::Unknown(ref bhr) => {
                // rusoto_s3 does not recognize many errors very well
                if bhr.status.as_u16() == 400 && bhr.body_as_str().contains("TooManyBuckets") {
                    Err(Error::from(TooManyBucketsError {}))
                } else {
                    Err(e.into())
                }
            }
            _ => Err(e.into()),
        },
        Ok(_) => Ok(()),
    }
}

/// Run the given future on a newly created single-threaded runtime if possible,
/// otherwise raise an error if this thread already has a runtime.
fn block_on<F: std::future::Future>(future: F) -> Result<F::Output, Error> {
    if let Ok(_handle) = tokio::runtime::Handle::try_current() {
        Err(anyhow!("cannot call block_on inside a runtime"))
    } else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use std::env;
    use tempfile::tempdir;

    #[test]
    fn test_new_amazon_store_region() {
        let props = HashMap::new();
        let result = AmazonStore::new("amazon123", &props);
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing region property"));
        // could check all of the others, I guess?
    }

    #[test]
    fn test_new_amazon_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), "us-west2".to_owned());
        properties.insert("storage".to_owned(), "STANDARD_IA".to_owned());
        properties.insert("access_key".to_owned(), "amazon".to_owned());
        properties.insert("secret_key".to_owned(), "shamazon".to_owned());
        let result = AmazonStore::new("amazon123", &properties);
        assert!(result.is_ok());
    }

    #[test]
    fn test_amazon_bucket_collision() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let region_var = env::var("AWS_REGION");
        if region_var.is_err() {
            // bail out silently if amazon is not configured
            return Ok(());
        }
        let region = region_var?;
        let access_key = env::var("AWS_ACCESS_KEY")?;
        let secret_key = env::var("AWS_SECRET_KEY")?;

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), region);
        properties.insert("storage".to_owned(), "STANDARD_IA".into());
        properties.insert("access_key".to_owned(), access_key);
        properties.insert("secret_key".to_owned(), secret_key);
        let source = AmazonStore::new("amazonone", &properties)?;

        // store an object in a bucket that already exists and belongs to
        // another AWS account (surprise, mybucketname is already taken)
        let bucket = "mybucketname".to_owned();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let result = source.store_pack_sync(packfile, &bucket, &object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.downcast::<CollisionError>().is_ok());

        Ok(())
    }

    #[test]
    fn test_amazon_store_roundtrip() -> Result<(), Error> {
        //
        // N.B. the rusoto crate will pick up environment variables, such as
        // AWS_REGION, which can lead to false successes when running the tests
        //
        // set up the environment and remote connection
        dotenv().ok();
        let region_var = env::var("AWS_REGION");
        if region_var.is_err() {
            // bail out silently if amazon is not configured
            return Ok(());
        }
        let region = region_var?;
        let access_key = env::var("AWS_ACCESS_KEY")?;
        let secret_key = env::var("AWS_SECRET_KEY")?;

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), region);
        properties.insert("storage".to_owned(), "STANDARD_IA".into());
        properties.insert("access_key".to_owned(), access_key);
        properties.insert("secret_key".to_owned(), secret_key);
        let source = AmazonStore::new("amazonone", &properties)?;

        // store an object
        let bucket = xid::new().to_string();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "amazonone");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // store another object to ensure create bucket works repeatedly
        let object = "489492a49220c814f49487efb12adfbc372aa3f8".to_owned();
        let packfile = Path::new("../../test/fixtures/washington-journal.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "amazonone");
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
        assert_eq!(
            // this checksum is wrong and will need to be fixed
            md5sum,
            "4b9772cf2c623ad529900f0ffe4e8ded"
        );

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
}
