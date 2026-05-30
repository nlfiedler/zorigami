//
// Copyright (c) 2024 Nathan Fiedler
//
use anyhow::{Error, anyhow};
use azure_core::credentials::{Secret, TokenCredential};
use azure_core::http::{RequestContent, Url};
use azure_identity::ClientSecretCredential;
use azure_storage_blob::models::{
    AccessTier, BlobClientDeleteOptions, BlobContainerClientCreateOptions,
    BlobContainerClientDeleteOptions, BlockBlobClientCommitBlockListOptions,
    BlockBlobClientStageBlockOptions, BlockLookupList,
};
use azure_storage_blob::{BlobContainerClient, BlobServiceClient, BlobServiceClientOptions};
use futures::StreamExt;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use store_core::Coordinates;
use tokio::time::timeout;

// Per-call deadline for short metadata operations (delete blob/container,
// create container, single page of a list operation).
const METADATA_TIMEOUT: Duration = Duration::from_secs(120);
// Per-await deadline for individual transfer operations (a single 8 MB
// put_block, a single 1 MB get chunk, a single list page during retrieval
// or pagination, or the final put_block_list commit). Bounds each
// individual network exchange so a stalled HTTP stream surfaces as an
// error and the outer retry loop in `data::repositories` can try again,
// rather than hanging the backup task forever.
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Wrap a future with a deadline, converting an elapsed timer into an
/// `anyhow::Error` so the surrounding `?` propagation continues to work.
async fn with_deadline<F, T>(label: &str, deadline: Duration, fut: F) -> Result<T, Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    match timeout(deadline, fut).await {
        Ok(inner) => inner,
        Err(_) => Err(anyhow!("{} timed out after {:?}", label, deadline)),
    }
}

///
/// A pack store implementation that uses Azure blob storage.
///
/// Authenticates via an Entra ID service principal. The new Microsoft Azure
/// SDK for Rust does not support storage-account shared-key authentication,
/// so the store requires `tenant_id`, `client_id`, and `client_secret`
/// properties identifying an Entra ID app registration that has been granted
/// a Storage Blob Data Contributor role on the target storage account.
///
#[derive(Clone, Debug)]
pub struct AzureStore {
    store_id: String,
    account: String,
    tenant_id: String,
    client_id: String,
    client_secret: String,
    access_tier: Option<AccessTier>,
    custom_uri: Option<String>,
}

impl AzureStore {
    /// Validate the given store and construct a azure pack source.
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let account = props
            .get("account")
            .ok_or_else(|| anyhow!("missing account property"))?;
        let tenant_id = props
            .get("tenant_id")
            .ok_or_else(|| anyhow!("missing tenant_id property"))?;
        let client_id = props
            .get("client_id")
            .ok_or_else(|| anyhow!("missing client_id property"))?;
        let client_secret = props
            .get("client_secret")
            .ok_or_else(|| anyhow!("missing client_secret property"))?;
        // an empty URI must be nullified to avoid errors from Azure
        let mut custom_uri = props.get("custom_uri");
        if let Some(uri) = custom_uri
            && uri.is_empty()
        {
            // tried using take_if() but that is either very confusing or
            // is not working as would be expected
            custom_uri = None;
        }
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
            tenant_id: tenant_id.to_owned(),
            client_id: client_id.to_owned(),
            client_secret: client_secret.to_owned(),
            custom_uri: custom_uri.cloned(),
            access_tier,
        })
    }

    fn credential(&self) -> Result<Arc<dyn TokenCredential>, Error> {
        let cred = ClientSecretCredential::new(
            &self.tenant_id,
            self.client_id.clone(),
            Secret::new(self.client_secret.clone()),
            None,
        )?;
        Ok(cred)
    }

    fn service_url(&self) -> Result<Url, Error> {
        let url = self
            .custom_uri
            .clone()
            .unwrap_or_else(|| format!("https://{}.blob.core.windows.net/", self.account));
        Ok(Url::parse(&url)?)
    }

    fn service_client(&self) -> Result<BlobServiceClient, Error> {
        let credential = self.credential()?;
        let url = self.service_url()?;
        let options = BlobServiceClientOptions::default();
        let client = BlobServiceClient::new(url, Some(credential), Some(options))?;
        Ok(client)
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
        let service = self.service_client()?;
        let container_client = service.blob_container_client(bucket);
        create_container(&container_client).await?;
        //
        // Process the pack file by uploading it in 8mb chunks as "blocks", then
        // assembling the "blob" from the list of blocks ("block list"). Blocks
        // larger than 5mb will benefit from the "high throughput" feature of
        // the Azure storage API.
        //
        let blob_client = container_client.blob_client(object);
        let block_blob_client = blob_client.block_blob_client();
        let mut file_handle = File::open(packfile)?;
        let block_size: usize = 8388608;
        let mut block_ids: Vec<Vec<u8>> = Vec::new();
        loop {
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
            // 64 bytes, otherwise only uniqueness matters; the SDK base64-
            // encodes the bytes before sending.
            let block_id = xid::new().to_string().into_bytes();
            let md5 = md5sum_blob(&data)?;
            let data_len = data.len() as u64;
            let options = BlockBlobClientStageBlockOptions {
                transactional_content_md5: Some(md5.to_vec()),
                ..Default::default()
            };
            with_deadline(
                &format!("put_block {}/{}", bucket, object),
                TRANSFER_TIMEOUT,
                async {
                    block_blob_client
                        .stage_block(
                            &block_id,
                            data_len,
                            RequestContent::from(data),
                            Some(options),
                        )
                        .await
                        .map(|_| ())
                        .map_err(Error::from)
                },
            )
            .await?;
            block_ids.push(block_id);
        }
        let block_list = BlockLookupList {
            committed: None,
            latest: Some(block_ids),
            uncommitted: None,
        };
        let commit_options = BlockBlobClientCommitBlockListOptions {
            tier: self.access_tier.clone(),
            ..Default::default()
        };
        with_deadline(
            &format!("put_block_list {}/{}", bucket, object),
            TRANSFER_TIMEOUT,
            async {
                block_blob_client
                    .commit_block_list(block_list.try_into()?, Some(commit_options))
                    .await
                    .map(|_| ())
                    .map_err(Error::from)
            },
        )
        .await?;
        let loc = Coordinates::new(&self.store_id, bucket, object);
        Ok(loc)
    }

    pub fn retrieve_pack_sync(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        block_on(self.retrieve_pack(location, outfile)).and_then(std::convert::identity)
    }

    pub async fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        let service = self.service_client()?;
        let client = service.blob_client(&location.bucket, &location.object);
        let mut file_handle = File::create(outfile)?;
        let label = format!("retrieve_pack {}/{}", location.bucket, location.object);
        let mut response = with_deadline(&label, TRANSFER_TIMEOUT, async {
            client.download(None).await.map_err(Error::from)
        })
        .await?;
        loop {
            let next = match timeout(TRANSFER_TIMEOUT, response.body.next()).await {
                Ok(n) => n,
                Err(_) => {
                    return Err(anyhow!(
                        "{} chunk fetch timed out after {:?}",
                        label,
                        TRANSFER_TIMEOUT
                    ));
                }
            };
            let chunk = match next {
                Some(Ok(c)) => c,
                Some(Err(err)) => return Err(err.into()),
                None => break,
            };
            file_handle.write_all(&chunk)?;
        }
        Ok(())
    }

    pub fn list_buckets_sync(&self) -> Result<Vec<String>, Error> {
        block_on(self.list_buckets()).and_then(std::convert::identity)
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let mut results = Vec::new();
        let service = self.service_client()?;
        let mut pager = service.list_containers(None)?;
        loop {
            let next = match timeout(TRANSFER_TIMEOUT, pager.next()).await {
                Ok(n) => n,
                Err(_) => {
                    return Err(anyhow!(
                        "list_buckets page fetch timed out after {:?}",
                        TRANSFER_TIMEOUT
                    ));
                }
            };
            match next {
                Some(Ok(container)) => {
                    if let Some(name) = container.name {
                        results.push(name);
                    }
                }
                Some(Err(err)) => return Err(err.into()),
                None => break,
            }
        }
        Ok(results)
    }

    pub fn list_objects_sync(&self, bucket: &str) -> Result<Vec<String>, Error> {
        block_on(self.list_objects(bucket)).and_then(std::convert::identity)
    }

    pub async fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let service = self.service_client()?;
        let container_client = service.blob_container_client(bucket);
        let mut results = Vec::new();
        let mut pager = container_client.list_blobs(None)?;
        loop {
            let next = match timeout(TRANSFER_TIMEOUT, pager.next()).await {
                Ok(n) => n,
                Err(_) => {
                    return Err(anyhow!(
                        "list_objects page fetch timed out after {:?}",
                        TRANSFER_TIMEOUT
                    ));
                }
            };
            match next {
                Some(Ok(blob)) => {
                    if let Some(name) = blob.name {
                        results.push(name);
                    }
                }
                Some(Err(err)) => return Err(err.into()),
                None => break,
            }
        }
        Ok(results)
    }

    pub fn delete_object_sync(&self, bucket: &str, object: &str) -> Result<(), Error> {
        block_on(self.delete_object(bucket, object)).and_then(std::convert::identity)
    }

    pub async fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let service = self.service_client()?;
        let client = service.blob_client(bucket, object);
        with_deadline("delete_object", METADATA_TIMEOUT, async {
            client
                .delete(None::<BlobClientDeleteOptions<'_>>)
                .await
                .map(|_| ())
                .map_err(Error::from)
        })
        .await
    }

    pub fn delete_bucket_sync(&self, bucket: &str) -> Result<(), Error> {
        block_on(self.delete_bucket(bucket)).and_then(std::convert::identity)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let service = self.service_client()?;
        let client = service.blob_container_client(bucket);
        with_deadline("delete_bucket", METADATA_TIMEOUT, async {
            client
                .delete(None::<BlobContainerClientDeleteOptions>)
                .await
                .map(|_| ())
                .map_err(Error::from)
        })
        .await
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

/// Ensure the named container exists.
///
/// Raw `timeout(...)` is used here (rather than `with_deadline`) because the
/// inner match needs to inspect the typed `azure_core::Error` to detect the
/// "already exists" 409 case below.
async fn create_container(client: &BlobContainerClient) -> Result<(), Error> {
    use azure_core::http::StatusCode;
    let result = match timeout(
        METADATA_TIMEOUT,
        client.create(None::<BlobContainerClientCreateOptions>),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => {
            return Err(anyhow!(
                "create_container timed out after {:?}",
                METADATA_TIMEOUT
            ));
        }
    };
    match result {
        Err(e) => {
            if e.http_status() == Some(StatusCode::Conflict) {
                Ok(())
            } else {
                Err(e.into())
            }
        }
        Ok(_) => Ok(()),
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
    }

    #[test]
    fn test_azure_wrong_account() -> Result<(), Error> {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("account".into(), "zoragamincorrect".into());
        properties.insert(
            "tenant_id".into(),
            "00000000-0000-0000-0000-000000000000".into(),
        );
        properties.insert(
            "client_id".into(),
            "00000000-0000-0000-0000-000000000000".into(),
        );
        properties.insert(
            "client_secret".into(),
            "definitely-not-a-real-secret".into(),
        );
        let source = AzureStore::new("azure2", &properties)?;
        // act
        let result = source.list_buckets_sync();
        // assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_new_azure_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("account".to_owned(), "zorigami-test".to_owned());
        properties.insert(
            "tenant_id".to_owned(),
            "00000000-0000-0000-0000-000000000000".to_owned(),
        );
        properties.insert(
            "client_id".to_owned(),
            "00000000-0000-0000-0000-000000000000".to_owned(),
        );
        properties.insert("client_secret".to_owned(), "secret-value".to_owned());
        let result = AzureStore::new("azure123", &properties);
        assert!(result.is_ok());
    }

    #[test]
    fn test_azure_store_roundtrip() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        // Bail out silently if Entra ID credentials are not configured; the
        // tenant ID is the gating variable now that we're on a TokenCredential.
        let tenant_id = match env::var("AZURE_TENANT_ID") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let account = env::var("AZURE_STORAGE_ACCOUNT")?;
        let client_id = env::var("AZURE_CLIENT_ID")?;
        let client_secret = env::var("AZURE_CLIENT_SECRET")?;
        let custom_uri = env::var("AZURE_STORAGE_URI");
        let access_tier = env::var("AZURE_STORAGE_ACCESS_TIER");

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("account".to_owned(), account);
        properties.insert("tenant_id".to_owned(), tenant_id);
        properties.insert("client_id".to_owned(), client_id);
        properties.insert("client_secret".to_owned(), client_secret);
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

    #[test]
    fn test_azure_store_custom_uri() -> Result<(), Error> {
        // test using the connection when custom_uri property is an empty string
        // which seems to cause Azure a lot of needless grief
        dotenv().ok();
        let tenant_id = match env::var("AZURE_TENANT_ID") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let account = env::var("AZURE_STORAGE_ACCOUNT")?;
        let client_id = env::var("AZURE_CLIENT_ID")?;
        let client_secret = env::var("AZURE_CLIENT_SECRET")?;
        let access_tier = env::var("AZURE_STORAGE_ACCESS_TIER");

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("account".to_owned(), account);
        properties.insert("tenant_id".to_owned(), tenant_id);
        properties.insert("client_id".to_owned(), client_id);
        properties.insert("client_secret".to_owned(), client_secret);
        properties.insert("custom_uri".to_owned(), String::new());
        if let Ok(tier) = access_tier {
            properties.insert("access_tier".to_owned(), tier);
        }
        let source = AzureStore::new("azure1", &properties)?;

        // act/assert
        let _ = source.list_buckets_sync()?;
        Ok(())
    }
}
