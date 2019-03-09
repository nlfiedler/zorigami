//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::{err_msg, Error};
use futures::Future;
use futures::stream::Stream;
use futures_fs::FsPool;
use rusoto_core::Region;
use rusoto_s3::{
    CreateBucketError,
    CreateBucketRequest,
    DeleteBucketError,
    DeleteBucketRequest,
    DeleteObjectRequest,
    GetObjectRequest,
    ListObjectsV2Request,
    PutObjectRequest,
    S3Client,
    S3,
    StreamingBody
};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

///
/// A `Store` implementation that uses the Amazon S3 protocol to connect to a
/// Minio storage server. Use the `new()` function to create an instance.
///
pub struct MinioStore {
    client: S3Client,
}

impl MinioStore {
    ///
    /// Create an instance of `MinioStore` to connect to the Minio server at the
    /// given region and endpoint.
    ///
    /// The endpoint should be something like http://192.168.99.100:9000
    /// such that it includes the scheme and port number, otherwise the
    /// client library will default to https and port 80(?)
    ///
    ///
    pub fn new(region: &str, endpoint: &str) -> Self {
        //
        // Credentials are picked up in a variety of ways, see the rusoto docs:
        // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
        //
        // Two different ways to get credentials via code (rather than the
        // client library doing it automatically):
        //
        // let credentials = DefaultCredentialsProvider::new()
        //     .unwrap()
        //     .credentials()
        //     .wait()
        //     .unwrap();
        //
        // let access_key = env::var("AWS_ACCESS_KEY").unwrap();
        // let secret_key = env::var("AWS_SECRET_KEY").unwrap();
        // let credentials = AwsCredentials::new(access_key, secret_key, None, None);
        //
        let region = Region::Custom {
            name: region.to_owned(),
            endpoint: endpoint.to_owned(),
        };
        let client = S3Client::new(region);
        Self {
            client
        }
    }

    ///
    /// Ensure the named bucket exists.
    ///
    fn create_bucket(&self, bucket: &str) -> Result<(), Error> {
        let request = CreateBucketRequest {
            bucket: bucket.to_owned(),
            ..Default::default()
        };
        let result = self.client.create_bucket(request).sync();
        // certain error conditions are okay
        match result {
            Err(e) => match e {
                CreateBucketError::BucketAlreadyExists(_) => Ok(()),
                CreateBucketError::BucketAlreadyOwnedByYou(_) => Ok(()),
                _ => Err(Error::from_boxed_compat(Box::new(e))),
            },
            Ok(_) => Ok(()),
        }
    }
}

impl super::Store for MinioStore {
    fn store_pack(&self, packfile: &Path, bucket: &str, object: &str) -> Result<(), Error> {
        // Ensure the bucket exists
        self.create_bucket(bucket)?;
        //
        // An alternative to streaming the entire file is to use a multi-part
        // upload and upload the large file in chunks.
        //
        let meta = fs::metadata(packfile).unwrap();
        let fs = FsPool::default();
        let read_stream = fs.read(packfile.to_owned(), Default::default());
        let req = PutObjectRequest {
            bucket: bucket.to_owned(),
            key: object.to_owned(),
            content_length: Some(meta.len() as i64),
            body: Some(StreamingBody::new(read_stream.map(|bytes| bytes.to_vec()))),
            ..Default::default()
        };
        let result = self.client.put_object(req).sync()?;
        if result.e_tag.is_some() {
            // compute MD5 of file and compare to returned e_tag
            let md5 = checksum_file(packfile)?;
            // AWS S3 quotes the etag values for some reason
            let quoted_etag = result.e_tag.as_ref().unwrap();
            let stripped_etag = &quoted_etag.trim_matches('"');
            if !md5.eq(stripped_etag) {
                return Err(err_msg("returned e_tag does not match MD5 of pack file"));
            }
        }
        Ok(())
    }

    fn retrieve_pack(&self, bucket: &str, object: &str, outfile: &Path) -> Result<(), Error> {
        let request = GetObjectRequest {
            bucket: bucket.to_owned(),
            key: object.to_owned(),
            ..Default::default()
        };
        let result = self.client.get_object(request).sync()?;
        let stream = result.body.unwrap();
        let mut file = File::create(outfile)?;
        stream.for_each(move |chunk| file.write_all(&chunk).map_err(Into::into)).wait()?;
        Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let result = self.client.list_buckets().sync()?;
        let mut results = Vec::new();
        for bucket in result.buckets.unwrap() {
            results.push(bucket.name.unwrap());
        }
        Ok(results)
    }

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        // default AWS S3 max-keys is 1,000
        let mut request = ListObjectsV2Request {
            bucket: bucket.to_owned(),
            ..Default::default()
        };
        let mut results = Vec::new();
        loop {
            // we will be re-using the request, so clone it each time
            let result = self.client.list_objects_v2(request.clone()).sync()?;
            if let Some(contents) = result.contents {
                for entry in contents {
                    if let Some(key) = entry.key {
                        results.push(key);
                    }
                }
            }
            // check if there are more results to be fetched
            if result.next_continuation_token.is_none() {
                break
            }
            request.continuation_token = result.next_continuation_token;
        }
        Ok(results)
    }

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let request = DeleteObjectRequest {
            bucket: bucket.to_owned(),
            key: object.to_owned(),
            ..Default::default()
        };
        self.client.delete_object(request).sync()?;
        Ok(())
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let request = DeleteBucketRequest {
            bucket: bucket.to_owned()
        };
        let result = self.client.delete_bucket(request).sync();
        // certain error conditions are okay
        match result {
            Err(e) => match e {
                DeleteBucketError::Unknown(_) => Ok(()),
                _ => Err(Error::from_boxed_compat(Box::new(e))),
            },
            Ok(_) => Ok(()),
        }
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

#[cfg(test)]
mod tests {
    use crate::core;
    use crate::store::Store;
    use dotenv::dotenv;
    use std::env;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn test_minio_roundtrip() {
        // set up the environment and remote connection
        dotenv().ok();
        let endp_var = env::var("MINIO_ENDPOINT");
        if endp_var.is_err() {
            return
        }
        let endpoint = endp_var.unwrap();
        let region = env::var("MINIO_REGION").unwrap();
        let store = super::MinioStore::new(&region, &endpoint);

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
