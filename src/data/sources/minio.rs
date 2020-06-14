//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::data::sources::PackDataSource;
use crate::domain::entities::{PackLocation, Store};
use failure::{err_msg, Error};
use futures::stream::Stream;
use futures::Future;
use futures_fs::FsPool;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    CreateBucketError, CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest,
    GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, S3,
};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

///
/// A `PackDataSource` implementation that uses the Amazon S3 protocol to
/// connect to a Minio storage server.
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
    pub fn new(store: &Store) -> Result<Self, Error> {
        let region = store
            .properties
            .get("region")
            .ok_or_else(|| err_msg("missing region property"))?;
        let endpoint = store
            .properties
            .get("endpoint")
            .ok_or_else(|| err_msg("missing endpoint property"))?;
        let access_key = store
            .properties
            .get("access_key")
            .ok_or_else(|| err_msg("missing access_key property"))?;
        let secret_key = store
            .properties
            .get("secret_key")
            .ok_or_else(|| err_msg("missing secret_key property"))?;
        Ok(Self {
            store_id: store.id.clone(),
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
}

impl PackDataSource for MinioStore {
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
        let client = self.connect();
        // the bucket must exist before receiving objects
        create_bucket(&client, bucket)?;
        //
        // An alternative to streaming the entire file is to use a multi-part
        // upload and upload the large file in chunks.
        //
        let meta = fs::metadata(packfile)?;
        let fs = FsPool::default();
        let read_stream = fs.read(packfile.to_owned(), Default::default());
        let req = PutObjectRequest {
            bucket: bucket.to_owned(),
            key: object.to_owned(),
            content_length: Some(meta.len() as i64),
            body: Some(StreamingBody::new(read_stream)),
            ..Default::default()
        };
        let result = client.put_object(req).sync()?;
        if let Some(ref etag) = result.e_tag {
            // compute MD5 of file and compare to returned e_tag
            let md5 = md5sum_file(packfile)?;
            // AWS S3 quotes the etag values for some reason
            let stripped_etag = etag.trim_matches('"');
            if !md5.eq(stripped_etag) {
                return Err(err_msg("returned e_tag does not match MD5 of pack file"));
            }
        }
        let loc = PackLocation::new(&self.store_id, bucket, object);
        Ok(loc)
    }

    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        let client = self.connect();
        let request = GetObjectRequest {
            bucket: location.bucket.clone(),
            key: location.object.clone(),
            ..Default::default()
        };
        let result = client.get_object(request).sync()?;
        let stream = result.body.ok_or_else(|| {
            err_msg(format!(
                "failed to retrieve object {} from bucket {}",
                location.object.clone(),
                location.bucket.clone()
            ))
        })?;
        let mut file = File::create(outfile)?;
        stream
            .for_each(move |chunk| file.write_all(&chunk).map_err(Into::into))
            .wait()?;
        Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let client = self.connect();
        let result = client.list_buckets().sync()?;
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

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let client = self.connect();
        // default AWS S3 max-keys is 1,000
        let mut request = ListObjectsV2Request {
            bucket: bucket.to_owned(),
            ..Default::default()
        };
        let mut results = Vec::new();
        loop {
            // we will be re-using the request, so clone it each time
            let result = client.list_objects_v2(request.clone()).sync()?;
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

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let client = self.connect();
        let request = DeleteObjectRequest {
            bucket: bucket.to_owned(),
            key: object.to_owned(),
            ..Default::default()
        };
        client.delete_object(request).sync()?;
        Ok(())
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let client = self.connect();
        let request = DeleteBucketRequest {
            bucket: bucket.to_owned(),
        };
        let result = client.delete_bucket(request).sync();
        // certain error conditions are okay
        match result {
            Err(e) => match e {
                RusotoError::Unknown(_) => Ok(()),
                _ => Err(Error::from_boxed_compat(Box::new(e))),
            },
            Ok(_) => Ok(()),
        }
    }
}

/// Ensure the named bucket exists.
fn create_bucket(client: &S3Client, bucket: &str) -> Result<(), Error> {
    let request = CreateBucketRequest {
        bucket: bucket.to_owned(),
        ..Default::default()
    };
    let result = client.create_bucket(request).sync();
    // certain error conditions are okay
    match result {
        Err(e) => match e {
            RusotoError::Service(se) => match se {
                CreateBucketError::BucketAlreadyExists(_) => Ok(()),
                CreateBucketError::BucketAlreadyOwnedByYou(_) => Ok(()),
            },
            _ => Err(Error::from_boxed_compat(Box::new(e))),
        },
        Ok(_) => Ok(()),
    }
}

/// Compute the MD5 digest of the given file.
fn md5sum_file(infile: &Path) -> Result<String, Error> {
    use md5::{Digest, Md5};
    let mut file = File::open(infile)?;
    let mut hasher = Md5::new();
    io::copy(&mut file, &mut hasher)?;
    let digest = hasher.result();
    let result = format!("{:x}", digest);
    Ok(result)
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
    fn test_new_minio_store_region() {
        let store = Store {
            id: "minio123".to_owned(),
            store_type: StoreType::MINIO,
            label: "s3clone".to_owned(),
            properties: HashMap::new(),
        };
        let result = MinioStore::new(&store);
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
        let store = Store {
            id: "minio123".to_owned(),
            store_type: StoreType::MINIO,
            label: "s3clone".to_owned(),
            properties,
        };
        let result = MinioStore::new(&store);
        assert!(result.is_ok());
        let source = result.unwrap();
        assert!(!source.is_local());
        assert!(!source.is_slow());
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
        let store = Store {
            id: "minioone".to_owned(),
            store_type: StoreType::MINIO,
            label: "s3clone".to_owned(),
            properties,
        };
        let result = MinioStore::new(&store);
        assert!(result.is_ok());
        let source = result.unwrap();

        // store an object
        let bucket = "747267d56e7057118a9aa40c24c1730f".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("./tests/fixtures/lorem-ipsum.txt");
        let result = source.store_pack(packfile, &bucket, &object);
        assert!(result.is_ok());
        let location = result.unwrap();
        assert_eq!(location.store, "minioone");
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
