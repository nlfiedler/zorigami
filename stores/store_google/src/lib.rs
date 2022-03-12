//
// Copyright (c) 2022 Nathan Fiedler
//
//
// If storage_v1_types crate is not yet published to crates.io, then
// grab the generated code from example_crates/gcs_example in the
// https://github.com/dermesser/async-google-apis repository.
//
mod storage_v1_types;
use async_google_apis_common::{
    yup_oauth2::{self, authenticator::Authenticator},
    ApiError, DownloadResult,
};
use anyhow::{anyhow, Error};
use hyper::client::HttpConnector;
use hyper::Client;
use hyper_rustls::HttpsConnector;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use store_core::Coordinates;

type HttpClient = Client<HttpsConnector<HttpConnector>>;
type AuthConnector = Authenticator<HttpsConnector<HttpConnector>>;
type StorageHub = (HttpClient, Arc<AuthConnector>);

#[derive(Debug)]
pub struct GoogleStore {
    store_id: String,
    credentials: String,
    project: String,
    region: Option<String>,
    storage: Option<String>,
}

impl GoogleStore {
    /// Validate the given store and construct a google pack source.
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let credentials = props
            .get("credentials")
            .ok_or_else(|| anyhow!("missing credentials property"))?;
        let project = props
            .get("project")
            .ok_or_else(|| anyhow!("missing project property"))?;
        let region = props.get("region").map(|s| s.to_owned());
        let storage = props.get("storage").map(|s| s.to_owned());
        Ok(Self {
            store_id: store_id.to_owned(),
            credentials: credentials.to_owned(),
            project: project.to_owned(),
            region,
            storage,
        })
    }

    async fn connect(&self) -> Result<StorageHub, Error> {
        let conn = HttpsConnector::with_native_roots();
        let https_client = Client::builder().build(conn);
        let account_key = yup_oauth2::read_service_account_key(&self.credentials).await?;
        let authenticator = yup_oauth2::ServiceAccountAuthenticator::builder(account_key)
            .hyper_client(https_client.clone())
            .build()
            .await?;
        Ok((https_client, Arc::new(authenticator)))
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
        let hub = self.connect().await?;
        // the bucket must exist before receiving objects
        create_bucket(&hub, &self.project, bucket, &self.region, &self.storage).await?;
        let mut params = storage_v1_types::ObjectsInsertParams::default();
        params.bucket = bucket.into();
        params.name = Some(object.into());
        let f = tokio::fs::OpenOptions::new()
            .read(true)
            .open(packfile)
            .await?;
        let svc = storage_v1_types::ObjectsService::new(hub.0, hub.1);
        let obj = storage_v1_types::Object::default();
        let result = match svc.insert_resumable_upload(&params, &obj).await {
            Ok(mut upload) => match upload.set_max_chunksize(1024 * 1024 * 4) {
                Ok(_) => upload.upload_file(f).await,
                Err(err) => Err(err),
            },
            Err(err) => Err(err),
        };
        // storing the same object twice is not treated as an error
        match result {
            Err(error) => return Err(anyhow!(format!("{:?}", error))),
            Ok(object) => {
                if let Some(hash) = object.md5_hash.as_ref() {
                    let decoded = base64::decode(hash)?;
                    let md5 = md5sum_file(packfile)?;
                    if !md5.eq(&decoded) {
                        return Err(anyhow!("returned md5_hash does not match MD5 of pack file"));
                    }
                }
            }
        }
        Ok(Coordinates::new(&self.store_id, bucket, object))
    }

    pub fn retrieve_pack_sync(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        block_on(self.retrieve_pack(location, outfile)).and_then(std::convert::identity)
    }

    pub async fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        let hub = self.connect().await?;
        let svc = storage_v1_types::ObjectsService::new(hub.0, hub.1);
        let mut gparams = storage_v1_types::StorageParams::default();
        // the magic argument indicating to download the contents
        gparams.alt = Some(storage_v1_types::StorageParamsAlt::Media);
        let mut params = storage_v1_types::ObjectsGetParams::default();
        params.storage_params = Some(gparams);
        params.bucket = location.bucket.clone();
        params.object = location.object.clone();
        let mut f = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(outfile)
            .await?;
        match svc.get(&params).await {
            Ok(mut download) => match download.do_it(Some(&mut f)).await {
                Ok(result) => match result {
                    DownloadResult::Downloaded => Ok(()),
                    DownloadResult::Response(foo) => {
                        Err(anyhow!(format!("download response: {:?}", foo)))
                    }
                },
                Err(err) => Err(anyhow!(format!("{:?}", err))),
            },
            Err(err) => Err(anyhow!(format!("{:?}", err))),
        }
    }

    pub fn list_buckets_sync(&self) -> Result<Vec<String>, Error> {
        block_on(self.list_buckets()).and_then(std::convert::identity)
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let hub = self.connect().await?;
        let mut results: Vec<String> = Vec::new();
        let svc = storage_v1_types::BucketsService::new(hub.0, hub.1);
        let mut params = storage_v1_types::BucketsListParams::default();
        let mut page_token: Option<String> = None;
        params.project = self.project.clone();
        loop {
            if page_token.is_some() {
                params.page_token = page_token.take();
            }
            match svc.list(&params).await {
                Ok(buckets) => {
                    if let Some(bucks) = buckets.items.as_ref() {
                        // Only consider named buckets; this is no guarantee
                        // that they have one, despite the API requiring that
                        // names be provided when creating them.
                        for bucket in bucks.iter() {
                            if let Some(name) = bucket.name.as_ref() {
                                results.push(name.to_owned());
                            }
                        }
                    }
                    if buckets.next_page_token.is_none() {
                        break;
                    }
                    page_token = buckets.next_page_token;
                }
                Err(err) => return Err(anyhow!(format!("{:?}", err))),
            }
        }
        Ok(results)
    }

    pub fn list_objects_sync(&self, bucket: &str) -> Result<Vec<String>, Error> {
        block_on(self.list_objects(bucket)).and_then(std::convert::identity)
    }

    pub async fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let hub = self.connect().await?;
        let mut results: Vec<String> = Vec::new();
        let svc = storage_v1_types::ObjectsService::new(hub.0, hub.1);
        let mut params = storage_v1_types::ObjectsListParams::default();
        let mut page_token: Option<String> = None;
        params.bucket = bucket.into();
        loop {
            if page_token.is_some() {
                params.page_token = page_token.take();
            }
            match svc.list(&params).await {
                Ok(objects) => {
                    if let Some(objs) = objects.items.as_ref() {
                        // Only consider named objects; this is no guarantee
                        // that they have one, despite the API requiring that
                        // names be provided when uploading them.
                        for object in objs.iter() {
                            if let Some(name) = object.name.as_ref() {
                                results.push(name.to_owned());
                            }
                        }
                    }
                    if objects.next_page_token.is_none() {
                        break;
                    }
                    page_token = objects.next_page_token;
                }
                Err(err) => return Err(anyhow!(format!("{:?}", err))),
            }
        }
        Ok(results)
    }

    pub fn delete_object_sync(&self, bucket: &str, object: &str) -> Result<(), Error> {
        block_on(self.delete_object(bucket, object)).and_then(std::convert::identity)
    }

    pub async fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let hub = self.connect().await?;
        let svc = storage_v1_types::ObjectsService::new(hub.0, hub.1);
        let mut params = storage_v1_types::ObjectsDeleteParams::default();
        params.bucket = bucket.into();
        params.object = object.into();
        if let Err(err) = svc.delete(&params).await {
            return Err(anyhow!(format!("{:?}", err)));
        }
        Ok(())
    }

    pub fn delete_bucket_sync(&self, bucket: &str) -> Result<(), Error> {
        block_on(self.delete_bucket(bucket)).and_then(std::convert::identity)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let hub = self.connect().await?;
        let svc = storage_v1_types::BucketsService::new(hub.0, hub.1);
        let mut params = storage_v1_types::BucketsDeleteParams::default();
        params.bucket = bucket.into();
        if let Err(err) = svc.delete(&params).await {
            return Err(anyhow!(format!("{:?}", err)));
        }
        Ok(())
    }
}

/// Ensure the named bucket exists.
async fn create_bucket(
    hub: &StorageHub,
    project_id: &str,
    name: &str,
    region: &Option<String>,
    storage_class: &Option<String>,
) -> Result<(), Error> {
    let (https_client, authenticator) = hub.clone();
    let svc = storage_v1_types::BucketsService::new(https_client, authenticator);
    let mut params = storage_v1_types::BucketsInsertParams::default();
    params.project = project_id.to_owned();
    let mut bucket = storage_v1_types::Bucket::default();
    bucket.name = Some(name.to_owned());
    bucket.location = region.to_owned();
    bucket.storage_class = storage_class.to_owned();
    if let Err(error) = svc.insert(&params, &bucket).await {
        match error.downcast::<ApiError>() {
            Ok(err) => {
                match err {
                    ApiError::HTTPResponseError(code, _) => match code {
                        // bucket with the same name already exists
                        hyper::StatusCode::CONFLICT => return Ok(()),
                        _ => return Err(anyhow!(format!("unhandled response {:?}", err))),
                    },
                    _ => return Err(err.into()),
                }
            }
            Err(err) => return Err(anyhow!(format!("{:?}", err))),
        }
    }
    Ok(())
}

/// Compute the MD5 digest of the given file.
fn md5sum_file(infile: &Path) -> Result<Vec<u8>, Error> {
    use md5::{Digest, Md5};
    let mut file = std::fs::File::open(infile)?;
    let mut hasher = Md5::new();
    std::io::copy(&mut file, &mut hasher)?;
    let digest: Vec<u8> = hasher.finalize()[..].into();
    Ok(digest)
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
    fn test_new_google_store_region() {
        let props = HashMap::new();
        let result = GoogleStore::new("google123", &props);
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing credentials property"));
        // could check all of the others, I guess?
    }

    #[test]
    fn test_new_google_store_ok() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("credentials".to_owned(), "/path/to/file".to_owned());
        properties.insert("project".to_owned(), "shinkansen".to_owned());
        properties.insert("storage".to_owned(), "nearline".to_owned());
        let result = GoogleStore::new("google123", &properties);
        assert!(result.is_ok());
    }

    #[test]
    fn test_google_store_roundtrip() {
        // set up the environment and remote connection
        dotenv().ok();
        let creds_var = env::var("GOOGLE_CREDENTIALS");
        if creds_var.is_err() {
            // bail out silently if google is not configured
            return;
        }
        let credentials = creds_var.unwrap();
        let project_id = env::var("GOOGLE_PROJECT_ID").unwrap();
        let region = env::var("GOOGLE_REGION").unwrap();

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("credentials".to_owned(), credentials);
        properties.insert("project".to_owned(), project_id);
        // use standard storage class for testing since it is cheaper when
        // performing frequent downloads and deletions
        properties.insert("storage".to_owned(), "STANDARD".to_owned());
        properties.insert("region".to_owned(), region);
        let result = GoogleStore::new("google1", &properties);
        assert!(result.is_ok());
        let source = result.unwrap();

        // store an object
        let bucket = "747267d56e7057118a9aa40c24c1730f".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let result = source.store_pack_sync(packfile, &bucket, &object);
        assert!(result.is_ok());
        let location = result.unwrap();
        assert_eq!(location.store, "google1");
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
