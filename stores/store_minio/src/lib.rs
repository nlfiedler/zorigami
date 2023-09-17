//
// Copyright (c) 2023 Nathan Fiedler
//
use anyhow::{anyhow, Error};
use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    CreateBucketError, CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest,
    GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, S3,
};
use std::collections::HashMap;
use std::path::Path;
use store_core::{CollisionError, Coordinates};

///
/// A pack store implementation that uses the Amazon S3 protocol to connect to a
/// Minio storage server.
///
#[derive(Clone, Debug)]
pub struct MinioStore {
    store_id: String,
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
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
        Ok(Self {
            store_id: store_id.to_owned(),
            region: region.to_owned(),
            endpoint: endpoint.to_owned(),
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
        })
    }

    fn connect(&self) -> S3Client {
        //
        // Credentials are picked up in a variety of ways, see the rusoto docs:
        // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        //
        let region = Region::Custom {
            name: self.region.clone(),
            endpoint: self.endpoint.clone(),
        };
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

    pub async fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        let client = self.connect();
        // the bucket must exist before receiving objects
        create_bucket(&client, bucket).await?;
        //
        // An alternative to streaming the entire file is to use a multi-part
        // upload and upload the large file in chunks.
        //
        let meta = std::fs::metadata(packfile)?;
        let read_stream = tokio::fs::read(packfile.to_owned())
            .into_stream()
            .map_ok(Bytes::from);
        let req = PutObjectRequest {
            bucket: bucket.to_owned(),
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
        let loc = Coordinates::new(&self.store_id, bucket, object);
        Ok(loc)
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
async fn create_bucket(client: &S3Client, bucket: &str) -> Result<(), Error> {
    let request = CreateBucketRequest {
        bucket: bucket.to_owned(),
        ..Default::default()
    };
    // wait for the future(s) to complete
    let result = client.create_bucket(request).await;
    // certain error conditions are okay
    match result {
        Err(e) => match e {
            RusotoError::Service(se) => match se {
                CreateBucketError::BucketAlreadyExists(_) => Err(Error::from(CollisionError {})),
                CreateBucketError::BucketAlreadyOwnedByYou(_) => Ok(()),
            },
            _ => Err(anyhow!(format!("{}", e))),
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
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("InvalidAccessKeyId"));
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
