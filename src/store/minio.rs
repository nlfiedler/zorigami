//
// Copyright (c) 2019 Nathan Fiedler
//
use crate::core::PackLocation;
use failure::{err_msg, Error};
use futures::stream::Stream;
use futures::Future;
use futures_fs::FsPool;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    CreateBucketError, CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest,
    GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, S3,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

///
/// Configuration for the MinioStore implementation.
///
#[derive(Serialize, Deserialize, Debug)]
struct MinioConfig {
    label: String,
    /// The AWS/Minio region to connect to (e.g. "us-east-1").
    region: String,
    /// The endpoint should be something like http://192.168.99.100:9000 such
    /// that it includes the scheme and port number, otherwise the client
    /// library will default to https and port 80(?).
    endpoint: String,
    /// The value for the AWS_ACCESS_KEY part of AWS credentials.
    access_key: String,
    /// The value for the AWS_SECRET_KEY part of AWS credentials.
    secret_key: String,
}

impl super::Config for MinioConfig {
    fn get_label(&self) -> String {
        self.label.clone()
    }

    fn from_json(&mut self, data: &str) -> Result<(), Error> {
        let conf: MinioConfig = serde_json::from_str(data)?;
        self.label = conf.label;
        self.region = conf.region;
        self.endpoint = conf.endpoint;
        self.access_key = conf.access_key;
        self.secret_key = conf.secret_key;
        Ok(())
    }

    fn to_json(&self) -> Result<String, Error> {
        let j = serde_json::to_string(&self)?;
        Ok(j)
    }
}

impl Default for MinioConfig {
    fn default() -> Self {
        Self {
            label: String::from("default minio"),
            region: String::from("us-west-1"),
            endpoint: String::from("http://localhost:9000"),
            access_key: String::from("AKIAIOSFODNN7EXAMPLE"),
            secret_key: String::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
        }
    }
}

///
/// A `Store` implementation that uses the Amazon S3 protocol to connect to a
/// Minio storage server.
///
pub struct MinioStore {
    unique_id: String,
    config: MinioConfig,
}

impl MinioStore {
    /// Construct a new instance of MinioStore with the given identifier.
    pub fn new(uuid: &str) -> Self {
        Self {
            unique_id: uuid.to_owned(),
            config: Default::default(),
        }
    }
}

impl MinioStore {
    ///
    /// Get an S3Client instance.
    ///
    fn connect(&self) -> S3Client {
        //
        // Credentials are picked up in a variety of ways, see the rusoto docs:
        // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        //
        let region = Region::Custom {
            name: self.config.region.clone(),
            endpoint: self.config.endpoint.clone(),
        };
        let client = rusoto_core::request::HttpClient::new().unwrap();
        let creds = rusoto_credential::StaticProvider::new(
            self.config.access_key.clone(),
            self.config.secret_key.clone(),
            None,
            None,
        );
        S3Client::new_with(client, creds, region)
    }
}

impl super::Store for MinioStore {
    fn get_id(&self) -> &str {
        &self.unique_id
    }

    fn get_type(&self) -> super::StoreType {
        super::StoreType::MINIO
    }

    fn get_speed(&self) -> super::StoreSpeed {
        super::StoreSpeed::FAST
    }

    fn get_config(&self) -> &dyn super::Config {
        &self.config
    }

    fn get_config_mut(&mut self) -> &mut dyn super::Config {
        &mut self.config
    }

    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error> {
        let client = self.connect();
        // Ensure the bucket exists
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
            let md5 = checksum_file(packfile)?;
            // AWS S3 quotes the etag values for some reason
            let stripped_etag = etag.trim_matches('"');
            if !md5.eq(stripped_etag) {
                return Err(err_msg("returned e_tag does not match MD5 of pack file"));
            }
        }
        let loc = PackLocation::new(&self.unique_id, bucket, object);
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

///
/// Ensure the named bucket exists.
///
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

///
/// Compute the MD5 digest of the given file.
///
fn checksum_file(infile: &Path) -> Result<String, Error> {
    use md5::{Digest, Md5};
    let mut file = File::open(infile)?;
    let mut hasher = Md5::new();
    io::copy(&mut file, &mut hasher)?;
    let digest = hasher.result();
    let result = format!("{:x}", digest);
    Ok(result)
}
