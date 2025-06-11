//
// Copyright (c) 2024 Nathan Fiedler
//
use anyhow::{anyhow, Error};
use azure_core::{RetryOptions, StatusCode};
use azure_storage::{CloudLocation, ErrorKind, StorageCredentials};
use azure_storage_blobs::prelude::{
    AccessTier, BlobBlockType, BlockId, BlockList, ClientBuilder, PublicAccess,
};
use futures::StreamExt;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use store_core::Coordinates;

///
/// A pack store implementation that uses Azure blob storage.
///
#[derive(Clone, Debug)]
pub struct AzureStore {
    store_id: String,
    account: String,
    access_key: String,
    access_tier: Option<AccessTier>,
    custom_uri: Option<String>,
    retry_options: Option<RetryOptions>,
}

impl AzureStore {
    /// Validate the given store and construct a azure pack source.
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let account = props
            .get("account")
            .ok_or_else(|| anyhow!("missing account property"))?;
        let access_key = props
            .get("access_key")
            .ok_or_else(|| anyhow!("missing access_key property"))?;
        let custom_uri = props.get("custom_uri");
        let access_tier = props.get("access_tier").and_then(|tier| {
            if tier.to_lowercase() == "hot" {
                Some(AccessTier::Hot)
            } else if tier.to_lowercase() == "cool" {
                Some(AccessTier::Cool)
            } else if tier.to_lowercase() == "archive" {
                Some(AccessTier::Archive)
            } else {
                None
            }
        });
        Ok(Self {
            store_id: store_id.to_owned(),
            account: account.to_owned(),
            access_key: access_key.to_owned(),
            custom_uri: custom_uri.cloned(),
            access_tier,
            retry_options: None,
        })
    }

    fn connect(&self) -> ClientBuilder {
        let account = self.account.clone();
        let access_key = self.access_key.clone();
        let credentials = StorageCredentials::access_key(account.clone(), access_key);
        let mut cb = if let Some(uri) = &self.custom_uri {
            let location = CloudLocation::Custom {
                account,
                uri: uri.to_owned(),
            };
            ClientBuilder::with_location(location, credentials)
        } else {
            ClientBuilder::new(account, credentials)
        };
        if let Some(ref retry) = self.retry_options {
            cb = cb.retry(retry.to_owned());
        }
        cb
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
        // the container must exist before receiving blobs
        let builder = self.connect();
        create_container(builder, bucket).await?;
        //
        // Process the pack file by uploading it in 8mb chunks as "blocks", then
        // assembling the "blob" from the list of blocks ("block list"). Blocks
        // larger than 5mb will benefit from the "high throughput" feature of
        // the Azure storage API.
        //
        let builder = self.connect();
        let blob_client = builder.blob_client(bucket, object);
        let mut file_handle = File::open(packfile)?;
        let block_size: usize = 8388608;
        let mut block_list: Vec<BlobBlockType> = Vec::new();
        loop {
            // blob API wants to take ownership of the data, so allocate a new
            // buffer for every put_block call (i.e. cannot use BufReader)
            let mut data = Vec::with_capacity(block_size);
            let mut take_handle = file_handle.take(block_size as u64);
            let read_bytes = take_handle.read_to_end(&mut data)?;
            if read_bytes == 0 {
                break;
            }
            file_handle = take_handle.into_inner();
            if data.is_empty() {
                break;
            }
            // block identifiers must all have the same length, and be less than
            // 64 bytes, otherwise only uniqueness matters; n.b. the client will
            // perform the base64 encoding
            let block_id = xid::new().to_string();
            let data_ref: &[u8] = data.as_ref();
            let md5 = md5sum_blob(data_ref)?;
            let hash = azure_storage_blobs::prelude::Hash::MD5(md5);
            let response = blob_client
                .put_block(block_id.clone(), data)
                .hash(hash)
                .await?;
            if let Some(content_md5) = response.content_md5 {
                if content_md5.as_slice() != &md5 {
                    return Err(anyhow!("returned MD5 does not match"));
                }
            }
            block_list.push(BlobBlockType::Uncommitted(BlockId::new(block_id)));
        }
        let mut builder = blob_client.put_block_list(BlockList { blocks: block_list });
        if let Some(tier) = &self.access_tier {
            builder = builder.access_tier(*tier);
        }
        builder.await?;
        let loc = Coordinates::new(&self.store_id, bucket, object);
        Ok(loc)
    }

    pub fn retrieve_pack_sync(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        block_on(self.retrieve_pack(location, outfile)).and_then(std::convert::identity)
    }

    pub async fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        let builder = self.connect();
        let client = builder.blob_client(&location.bucket, &location.object);
        let mut file_handle = File::create(outfile)?;
        // n.b. this uses the default chunk size of 1MB, which enables the
        // pipeline to handle intermittent connection failures with retry,
        // rather than restarting the whole blob on a failure.
        let mut stream = client.get().into_stream();
        while let Some(value) = stream.next().await {
            let data = value?.data.collect().await?;
            file_handle.write_all(&data)?;
        }
        Ok(())
    }

    pub fn list_buckets_sync(&self) -> Result<Vec<String>, Error> {
        block_on(self.list_buckets()).and_then(std::convert::identity)
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let mut results = Vec::new();
        let builder = self.connect();
        let blob_service = builder.blob_service_client();
        let mut pageable = blob_service.list_containers().into_stream();
        while let Some(result) = pageable.next().await {
            match result {
                Ok(response) => {
                    for container in response.containers {
                        results.push(container.name);
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
        Ok(results)
    }

    pub fn list_objects_sync(&self, bucket: &str) -> Result<Vec<String>, Error> {
        block_on(self.list_objects(bucket)).and_then(std::convert::identity)
    }

    pub async fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        use azure_storage_blobs::container::operations::BlobItem::Blob;
        let builder = self.connect();
        let client = builder.container_client(bucket);
        let mut results = Vec::new();
        let mut pageable = client.list_blobs().into_stream();
        while let Some(result) = pageable.next().await {
            match result {
                Ok(response) => {
                    for blob_item in response.blobs.items {
                        if let Blob(blob) = blob_item {
                            results.push(blob.name);
                        }
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
        Ok(results)
    }

    pub fn delete_object_sync(&self, bucket: &str, object: &str) -> Result<(), Error> {
        block_on(self.delete_object(bucket, object)).and_then(std::convert::identity)
    }

    pub async fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let builder = self.connect();
        let client = builder.blob_client(bucket, object);
        client.delete().await?;
        Ok(())
    }

    pub fn delete_bucket_sync(&self, bucket: &str) -> Result<(), Error> {
        block_on(self.delete_bucket(bucket)).and_then(std::convert::identity)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let builder = self.connect();
        let client = builder.container_client(bucket);
        client.delete().await?;
        Ok(())
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

/// Ensure the named container exists.
async fn create_container(builder: ClientBuilder, container: &str) -> Result<(), Error> {
    let client = builder.container_client(container);
    // certain error conditions are okay
    match client.create().public_access(PublicAccess::None).await {
        Err(e) => match e.kind() {
            #[allow(unused_variables)]
            ErrorKind::HttpResponse { status, error_code } => {
                if *status == StatusCode::Conflict {
                    Ok(())
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

fn md5sum_blob<T: AsRef<[u8]>>(data: T) -> Result<[u8; 16], Error> {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(data);
    let digest = hasher.finalize();
    Ok(digest.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy::dotenv;
    use std::env;
    use tempfile::tempdir;

    #[test]
    fn test_new_azure_store_params() {
        let props = HashMap::new();
        let result = AzureStore::new("azure123", &props);
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing account property"));
        // could check all of the others, I guess?
    }

    #[test]
    fn test_azure_wrong_account() -> Result<(), Error> {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("account".into(), "zoragamincorrect".into());
        properties.insert("access_key".into(), "kbMB+UABej9Y/dvLlKh05deDDSiS0TyraeUkDdCserQuLz9JgNEoK/EfOeaEiv8JHHhLAyflMRTb+ASt1Q7JCQ==".into());
        let mut source = AzureStore::new("azure2", &properties)?;
        source.retry_options = Some(RetryOptions::none());
        // act
        let result = source.list_buckets_sync();
        // assert
        assert!(result.is_err());
        let err_string = result.err().unwrap().to_string();
        assert!(err_string.contains("failed to execute `reqwest` request"));
        Ok(())
    }

    #[test]
    fn test_new_azure_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("account".to_owned(), "zorigami-test".to_owned());
        properties.insert("access_key".to_owned(), "azure-access-key".to_owned());
        let result = AzureStore::new("azure123", &properties);
        assert!(result.is_ok());
    }

    #[test]
    fn test_azure_store_roundtrip() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let acct_var = env::var("AZURE_STORAGE_ACCOUNT");
        if acct_var.is_err() {
            // bail out silently if azure is not available
            return Ok(());
        }
        let account = acct_var?;
        let access_key = env::var("AZURE_STORAGE_ACCESS_KEY")?;
        let custom_uri = env::var("AZURE_STORAGE_URI");
        let access_tier = env::var("AZURE_STORAGE_ACCESS_TIER");

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("account".to_owned(), account);
        properties.insert("access_key".to_owned(), access_key);
        if let Ok(uri) = custom_uri {
            properties.insert("custom_uri".to_owned(), uri);
        }
        if let Ok(tier) = access_tier {
            properties.insert("access_tier".to_owned(), tier);
        }
        let source = AzureStore::new("azure1", &properties)?;

        // store an object
        let bucket = xid::new().to_string();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "azure1");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // store another object to ensure create bucket works repeatedly
        let object = "489492a49220c814f49487efb12adfbc372aa3f8".to_owned();
        let packfile = Path::new("../../test/fixtures/washington-journal.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "azure1");
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
}
