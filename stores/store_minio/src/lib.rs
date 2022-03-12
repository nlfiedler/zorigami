//
// Copyright (c) 2022 Nathan Fiedler
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
use store_core::Coordinates;

///
/// A pack store implementation that uses the Amazon S3 protocol to connect to a
/// Minio storage server.
///
#[derive(Debug)]
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
            .map_ok(|b| Bytes::from(b));
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
                CreateBucketError::BucketAlreadyExists(_) => Ok(()),
                CreateBucketError::BucketAlreadyOwnedByYou(_) => Ok(()),
            },
            _ => Err(anyhow!(format!("{}", e))),
        },
        Ok(_) => Ok(()),
    }
}

/// Run the given future either on the current runtime or on a newly created
/// single-threaded future executor.
fn block_on<F: core::future::Future>(future: F) -> Result<F::Output, Error> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        Ok(handle.block_on(future))
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
    fn test_minio_store_roundtrip() {
        // set up the environment and remote connection
        dotenv().ok();
        let endp_var = env::var("MINIO_ENDPOINT");
        if endp_var.is_err() {
            // bail out silently if minio is not available
            return;
        }
        let endpoint = endp_var.unwrap();
        let region = env::var("MINIO_REGION").unwrap();
        let access_key = env::var("MINIO_ACCESS_KEY").unwrap();
        let secret_key = env::var("MINIO_SECRET_KEY").unwrap();

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("region".to_owned(), region);
        properties.insert("endpoint".to_owned(), endpoint);
        properties.insert("access_key".to_owned(), access_key);
        properties.insert("secret_key".to_owned(), secret_key);
        let result = MinioStore::new("minioone", &properties);
        assert!(result.is_ok());
        let source = result.unwrap();

        // store an object
        let bucket = "747267d56e7057118a9aa40c24c1730f".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let result = source.store_pack_sync(packfile, &bucket, &object);
        assert!(result.is_ok());
        let location = result.unwrap();
        assert_eq!(location.store, "minioone");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // check for bucket(s) being present
        let result = source.list_buckets_sync();
        assert!(result.is_ok());
        let buckets = result.unwrap();
        assert!(!buckets.is_empty());
        assert!(buckets.contains(&bucket));

        // check for object(s) being present
        let result = source.list_objects_sync(&bucket);
        assert!(result.is_ok());
        let listing = result.unwrap();
        assert!(!listing.is_empty());
        assert!(listing.contains(&object));

        // retrieve the file and verify by checksum
        let outdir = tempdir().unwrap();
        let outfile = outdir.path().join("restored.txt");
        let result = source.retrieve_pack_sync(&location, &outfile);
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
            let result = source.list_objects_sync(&bucket);
            assert!(result.is_ok());
            let objects = result.unwrap();
            for obj in objects {
                source.delete_object_sync(&bucket, &obj).unwrap();
            }
            source.delete_bucket_sync(&bucket).unwrap();
        }
    }
}
