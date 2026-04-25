//
// Copyright (c) 2025 Nathan Fiedler
//

// seemingly impossible to use this on a single line
#![allow(clippy::await_holding_lock)]

use anyhow::{Error, anyhow};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
    ScalarAttributeType, TableStatus,
};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration, StorageClass};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use store_core::{CollisionError, Coordinates};

// Names of all existing S3 buckets. Populated and used only when too many
// buckets have been created.
static BUCKET_NAMES: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

// Name of the table in DynomaDB for tracking bucket renames.
const RENAMES_TABLE: &str = "zori_renames";

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
    s3: aws_sdk_s3::Client,
    ddb: aws_sdk_dynamodb::Client,
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

        let creds = Credentials::new(
            access_key.clone(),
            secret_key.clone(),
            None,
            None,
            "zorigami-static",
        );
        let shared = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .region(Region::new(region.clone()))
            .build();
        let s3 = aws_sdk_s3::Client::new(&shared);
        let ddb = aws_sdk_dynamodb::Client::new(&shared);

        Ok(Self {
            store_id: store_id.to_owned(),
            region: region.to_owned(),
            storage: storage.to_owned(),
            s3,
            ddb,
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

    // Try to create the named bucket.
    //
    // If creation fails because the bucket name is already taken by another
    // account, generate a new random bucket name and try again. If creation
    // fails because the account has too many buckets, select one of the
    // existing buckets at random and use that instead.
    //
    // Returns the name of the bucket that was created or selected.
    async fn try_create_bucket(&self, bucket: &str) -> Result<String, Error> {
        let mut bucket_name = bucket.to_owned();
        loop {
            match create_bucket(&self.s3, &bucket_name, &self.region).await {
                Ok(()) => return Ok(bucket_name),
                Err(err) => match err.downcast::<CollisionError>() {
                    Ok(_) => {
                        bucket_name = uuid::Uuid::new_v4().to_string();
                    }
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
                            use rand::RngExt;
                            let mut rng = rand::rng();
                            let idx = rng.random_range(0..names.len());
                            return Ok(names[idx].to_owned());
                        }
                        Err(err) => return Err(err),
                    },
                },
            }
        }
    }

    pub async fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        // a bucket must exist before receiving objects; note that the bucket
        // may be renamed if there are too many buckets already
        let bucket_name = self.try_create_bucket(bucket).await?;
        let body = ByteStream::from_path(packfile)
            .await
            .map_err(anyhow::Error::new)?;
        let result = self
            .s3
            .put_object()
            .bucket(&bucket_name)
            .key(object)
            .storage_class(StorageClass::from(self.storage.as_str()))
            .body(body)
            .send()
            .await?;
        if let Some(etag) = result.e_tag() {
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

    /// Record the new name of the bucket.
    async fn save_bucket_name(&self, original: &str, renamed: &str) -> Result<(), Error> {
        // ensure the renames table exists
        let attr_def = AttributeDefinition::builder()
            .attribute_name("original")
            .attribute_type(ScalarAttributeType::S)
            .build()?;
        let key_schema = KeySchemaElement::builder()
            .attribute_name("original")
            .key_type(KeyType::Hash)
            .build()?;
        let throughput = ProvisionedThroughput::builder()
            .read_capacity_units(5)
            .write_capacity_units(5)
            .build()?;
        let result = self
            .ddb
            .create_table()
            .table_name(RENAMES_TABLE)
            .attribute_definitions(attr_def)
            .key_schema(key_schema)
            .provisioned_throughput(throughput)
            .send()
            .await;
        let created_result: Result<bool, Error> = match result {
            Ok(_) => Ok(true),
            Err(err) => match err.into_service_error() {
                CreateTableError::ResourceInUseException(_) => Ok(false),
                other => Err(anyhow::Error::new(other)),
            },
        };

        // Wait for the new table to become ACTIVE, allowing for errors as the
        // describe table request may fail initially while the table is not yet
        // ready to be queried.
        if created_result? {
            let mut retries = 10;
            let delay = std::time::Duration::from_millis(1000);
            loop {
                match self
                    .ddb
                    .describe_table()
                    .table_name(RENAMES_TABLE)
                    .send()
                    .await
                {
                    Ok(output) => {
                        if let Some(table) = output.table()
                            && table.table_status() == Some(&TableStatus::Active)
                        {
                            break;
                        }
                        retries -= 1;
                        if retries == 0 {
                            return Err(anyhow!(
                                "table {} did not become ACTIVE in time",
                                RENAMES_TABLE
                            ));
                        }
                        tokio::time::sleep(delay).await;
                    }
                    Err(err) => {
                        retries -= 1;
                        if retries == 0 {
                            return Err(anyhow::Error::new(err));
                        }
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        // insert table entry that maps `original` to `renamed`
        self.ddb
            .put_item()
            .table_name(RENAMES_TABLE)
            .item("original", AttributeValue::S(original.to_owned()))
            .item("renamed", AttributeValue::S(renamed.to_owned()))
            .send()
            .await?;
        Ok(())
    }

    /// Retrieve the renamed value for the original bucket.
    async fn get_bucket_name(&self, original: &str) -> Result<Option<String>, Error> {
        match self
            .ddb
            .get_item()
            .table_name(RENAMES_TABLE)
            .key("original", AttributeValue::S(original.to_owned()))
            .send()
            .await
        {
            Ok(output) => {
                if let Some(items) = output.item
                    && let Some(AttributeValue::S(s)) = items.get("renamed")
                {
                    return Ok(Some(s.clone()));
                }
                Ok(None)
            }
            Err(err) => match err.into_service_error() {
                GetItemError::ResourceNotFoundException(_) => Ok(None),
                other => Err(anyhow::Error::new(other)),
            },
        }
    }

    // for testing purposes only
    #[allow(dead_code)]
    fn delete_bucket_name(&self, original: &str) -> Result<(), Error> {
        block_on(async {
            self.ddb
                .delete_item()
                .table_name(RENAMES_TABLE)
                .key("original", AttributeValue::S(original.to_owned()))
                .send()
                .await?;
            Ok(())
        })
        .and_then(std::convert::identity)
    }

    pub fn store_database_sync(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        block_on(self.store_database(packfile, bucket, object)).and_then(std::convert::identity)
    }

    pub async fn store_database(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        // If a previous call already remapped this bucket name, use the
        // remapped name; otherwise use the caller-provided name. store_pack()
        // handles bucket collisions internally by picking a new random name --
        // if the returned bucket differs from the one we attempted, persist the
        // mapping so subsequent calls (and retrievals) find it.
        let attempted = self
            .get_bucket_name(bucket)
            .await?
            .unwrap_or_else(|| bucket.to_owned());
        let coords = self.store_pack(packfile, &attempted, object).await?;
        if coords.bucket != attempted {
            self.save_bucket_name(bucket, &coords.bucket).await?;
        }
        Ok(coords)
    }

    pub fn retrieve_database_sync(
        &self,
        location: &Coordinates,
        outfile: &Path,
    ) -> Result<(), Error> {
        block_on(self.retrieve_database(location, outfile)).and_then(std::convert::identity)
    }

    pub async fn retrieve_database(
        &self,
        location: &Coordinates,
        outfile: &Path,
    ) -> Result<(), Error> {
        if let Some(renamed) = self.get_bucket_name(&location.bucket).await? {
            let mut adjusted = location.clone();
            adjusted.bucket = renamed;
            self.retrieve_pack(&adjusted, outfile).await
        } else {
            self.retrieve_pack(location, outfile).await
        }
    }

    pub fn list_databases_sync(&self, bucket: &str) -> Result<Vec<String>, Error> {
        block_on(self.list_databases(bucket)).and_then(std::convert::identity)
    }

    pub async fn list_databases(&self, bucket: &str) -> Result<Vec<String>, Error> {
        if let Some(renamed) = self.get_bucket_name(bucket).await? {
            self.list_objects(&renamed).await
        } else {
            self.list_objects(bucket).await
        }
    }
}

/// Ensure the named bucket exists.
async fn create_bucket(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    region: &str,
) -> Result<(), Error> {
    let mut req = client.create_bucket().bucket(bucket);
    // us-east-1 rejects an explicit LocationConstraint; all other regions
    // require it.
    if region != "us-east-1" {
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(region))
            .build();
        req = req.create_bucket_configuration(cfg);
    }
    match req.send().await {
        Ok(_) => Ok(()),
        Err(e) => match e.into_service_error() {
            CreateBucketError::BucketAlreadyExists(_) => Err(Error::from(CollisionError {})),
            CreateBucketError::BucketAlreadyOwnedByYou(_) => Ok(()),
            other => {
                // TooManyBuckets is not a modeled variant; pull the code from
                // the error metadata.
                if other.code() == Some("TooManyBuckets") {
                    Err(Error::from(TooManyBucketsError {}))
                } else {
                    Err(anyhow::Error::new(other))
                }
            }
        },
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
    use serial_test::serial;
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
    fn test_amazon_wrong_account() -> Result<(), Error> {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), "us-west-2".into());
        properties.insert("storage".to_owned(), "STANDARD_IA".into());
        properties.insert("access_key".to_owned(), "not_access_key".into());
        properties.insert("secret_key".to_owned(), "not_secret_key".into());
        let source = AmazonStore::new("amazon2", &properties)?;
        // act
        let result = source.list_buckets_sync();
        // assert
        assert!(result.is_err());
        // aws-sdk wraps the original error code in the cause chain rather than
        // the top-level Display, so use the Debug formatter to inspect it.
        let err_string = format!("{:?}", result.err().unwrap());
        assert!(err_string.contains("InvalidAccessKeyId"));
        Ok(())
    }

    #[test]
    #[serial]
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
        // another AWS account (surprise, mybucketname is already taken); the
        // store should recover by generating a new bucket name and retrying
        let bucket = "mybucketname".to_owned();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let result = source.store_pack_sync(packfile, &bucket, &object);
        assert!(result.is_ok());
        let coords = result.unwrap();
        assert_ne!(coords.bucket, bucket);

        // clean up the newly created object and bucket
        source.delete_object_sync(&coords.bucket, &coords.object)?;
        source.delete_bucket_sync(&coords.bucket)?;

        Ok(())
    }

    #[test]
    #[serial]
    fn test_amazon_store_roundtrip() -> Result<(), Error> {
        //
        // N.B. the AWS SDK will pick up environment variables, such as
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
    #[serial]
    fn test_amazon_database_bucket_collision() -> Result<(), Error> {
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
        let source = AmazonStore::new("amazon1", &properties)?;

        // store an object in a bucket that already exists and belongs to
        // another AWS account (surprise, mybucketname is already taken)
        let bucket = "mybucketname".to_owned();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let location = source.store_database_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "amazon1");
        assert_ne!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // retrieve the database file using the original coordinates
        let location = Coordinates::new("amazon1", &bucket, &object);
        let outdir = tempdir()?;
        let outfile = outdir.path().join("restored.txt");
        source.retrieve_database_sync(&location, &outfile)?;

        // list available databases
        let mut retries = 10;
        let delay = std::time::Duration::from_millis(1000);
        loop {
            // DynamoDB is eventually consistent, so try a few times
            let databases = source.list_databases_sync(&bucket)?;
            if databases.is_empty() {
                retries -= 1;
                if retries == 0 {
                    panic!("list_databases test failed after several tries");
                }
                std::thread::sleep(delay);
            } else {
                assert_eq!(databases.len(), 1);
                assert_eq!(&databases[0], &object);
                break;
            }
        }

        // remove database mapping for test reproduction
        source.delete_bucket_name(&bucket)?;
        Ok(())
    }
}
