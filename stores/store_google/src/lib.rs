//
// Copyright (c) 2023 Nathan Fiedler
//
extern crate google_firestore1 as firestore1;
extern crate google_storage1 as storage1;
use anyhow::{anyhow, Error};
use base64::{engine::general_purpose, Engine as _};
use std::collections::HashMap;
use std::default::Default;
use std::path::Path;
use storage1::hyper::client::HttpConnector;
use storage1::hyper_rustls::HttpsConnector;
use store_core::{CollisionError, Coordinates};

#[derive(Clone, Debug)]
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

    async fn connect(&self) -> Result<storage1::Storage<HttpsConnector<HttpConnector>>, Error> {
        let conn = storage1::hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let https_client = storage1::hyper::Client::builder().build(conn);
        let account_key = storage1::oauth2::read_service_account_key(&self.credentials).await?;
        let authenticator = storage1::oauth2::ServiceAccountAuthenticator::builder(account_key)
            .hyper_client(https_client.clone())
            .build()
            .await?;
        Ok(storage1::Storage::new(https_client, authenticator))
    }

    async fn connect_fire(
        &self,
    ) -> Result<firestore1::Firestore<HttpsConnector<HttpConnector>>, Error> {
        let conn = firestore1::hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let https_client = firestore1::hyper::Client::builder().build(conn);
        let account_key = firestore1::oauth2::read_service_account_key(&self.credentials).await?;
        let authenticator = firestore1::oauth2::ServiceAccountAuthenticator::builder(account_key)
            .hyper_client(https_client.clone())
            .build()
            .await?;
        Ok(firestore1::Firestore::new(https_client, authenticator))
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
        let req = storage1::api::Object::default();
        let infile = std::fs::File::open(packfile)?;
        let mimetype = "application/octet-stream"
            .parse()
            .map_err(|e| anyhow!(format!("{:?}", e)))?;
        // storing the same object twice is not treated as an error
        match hub
            .objects()
            .insert(req, bucket)
            .name(object)
            .upload_resumable(infile, mimetype)
            .await
        {
            Ok((_response, objdata)) => {
                // ensure uploaded file matches local contents
                if let Some(hash) = objdata.md5_hash.as_ref() {
                    let returned = general_purpose::STANDARD.decode(hash)?;
                    let expected = md5sum_file(packfile)?;
                    if !expected.eq(&returned) {
                        return Err(anyhow!("returned md5_hash does not match MD5 of pack file"));
                    }
                }
                Ok(Coordinates::new(&self.store_id, bucket, object))
            }
            Err(error) => match &error {
                // detect the case of a bucket that exists but belongs to
                // another project, in which case we are forbidden to write to
                // that bucket
                storage1::client::Error::BadRequest(value) => {
                    if let Some(object) = value.as_object() {
                        if let Some(errobj) = object.get("error") {
                            if let Some(code) = errobj.get("code") {
                                if let Some(num) = code.as_u64() {
                                    if num == 403 {
                                        return Err(Error::from(CollisionError {}));
                                    }
                                }
                            }
                        }
                    }
                    return Err(anyhow!(format!("{:?}", error)));
                }
                _ => return Err(anyhow!(format!("{:?}", error))),
            },
        }
    }

    pub fn retrieve_pack_sync(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        block_on(self.retrieve_pack(location, outfile)).and_then(std::convert::identity)
    }

    pub async fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        let hub = self.connect().await?;
        let (response, _object) = hub
            .objects()
            .get(&location.bucket, &location.object)
            .param("alt", "media")
            .doit()
            .await?;
        let buf = storage1::hyper::body::aggregate(response).await?;
        use storage1::hyper::body::Buf;
        let mut remote = buf.reader();
        let mut local = std::fs::File::create(outfile)?;
        std::io::copy(&mut remote, &mut local)?;
        Ok(())
    }

    pub fn list_buckets_sync(&self) -> Result<Vec<String>, Error> {
        block_on(self.list_buckets()).and_then(std::convert::identity)
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let hub = self.connect().await?;
        let mut results: Vec<String> = Vec::new();
        let mut page_token: Option<String> = None;
        let methods = hub.buckets();
        loop {
            let call = if let Some(token) = page_token.take() {
                methods.list(&self.project).page_token(&token)
            } else {
                methods.list(&self.project)
            };
            match call.doit().await {
                Ok((_response, buckets)) => {
                    if let Some(bucks) = buckets.items.as_ref() {
                        // Only consider named buckets; there is no guarantee
                        // that they have one, despite the API requiring that
                        // names be provided when creating them.
                        for bucket in bucks.iter() {
                            if let Some(name) = bucket.name.as_ref() {
                                // ignore the Firestore buckets, which we do not
                                // want to accidentally delete and thus lose the
                                // bucket collision database
                                if !name.ends_with(".appspot.com") {
                                    results.push(name.to_owned());
                                }
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
        let mut page_token: Option<String> = None;
        let methods = hub.objects();
        loop {
            let call = if let Some(token) = page_token.take() {
                methods.list(bucket).page_token(&token)
            } else {
                methods.list(bucket)
            };
            match call.doit().await {
                Ok((_response, objects)) => {
                    if let Some(objs) = objects.items.as_ref() {
                        // Only consider named objects; there is no guarantee
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
        hub.objects().delete(bucket, object).doit().await?;
        Ok(())
    }

    pub fn delete_bucket_sync(&self, bucket: &str) -> Result<(), Error> {
        block_on(self.delete_bucket(bucket)).and_then(std::convert::identity)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let hub = self.connect().await?;
        hub.buckets().delete(bucket).doit().await?;
        Ok(())
    }

    /// Record the new name of the bucket.
    async fn save_bucket_name(&self, original: &str, renamed: &str) -> Result<(), Error> {
        let hub = self.connect_fire().await?;
        let mut values: HashMap<String, firestore1::api::Value> = HashMap::new();
        let mut value: firestore1::api::Value = Default::default();
        value.string_value = Some(renamed.to_owned());
        values.insert("renamed".into(), value);
        let name = format!(
            "projects/{}/databases/{}/documents/renames/{}",
            &self.project, "(default)", original
        );
        let mut document: firestore1::api::Document = Default::default();
        document.fields = Some(values);
        // databases_documents_patch() will either insert or update
        let (_response, _document) = hub
            .projects()
            .databases_documents_patch(document, &name)
            .doit()
            .await?;
        Ok(())
    }

    /// Retrieve the renamed value for the original bucket.
    async fn get_bucket_name(&self, original: &str) -> Result<Option<String>, Error> {
        let hub = self.connect_fire().await?;
        let name = format!(
            "projects/{}/databases/{}/documents/renames/{}",
            &self.project, "(default)", original
        );
        // If the document is missing a 404 error is returned, and if the
        // document is returned and has missing values, still return none.
        if let Ok((_response, document)) =
            hub.projects().databases_documents_get(&name).doit().await
        {
            if let Some(fields) = document.fields {
                if let Some(renamed_field) = fields.get("renamed") {
                    return Ok(renamed_field.string_value.to_owned());
                }
            }
        }
        Ok(None)
    }

    // async fn get_renamed_buckets(&self) -> Result<Vec<String>, Error> {
    //     let hub = self.connect_fire().await?;
    //     let parent = format!(
    //         "projects/{}/databases/{}/documents",
    //         &self.project, "(default)"
    //     );
    //     // TODO: deal with paging of results
    //     let (_response, documents) = hub
    //         .projects()
    //         .databases_documents_list(&parent, "renames")
    //         .doit()
    //         .await?;
    //     println!("documents: {:?}", documents);
    //     Ok(vec![])
    // }

    // for testing purposes only
    #[allow(dead_code)]
    fn delete_bucket_name(&self, original: &str) -> Result<(), Error> {
        block_on(async {
            let hub = self.connect_fire().await?;
            let name = format!(
                "projects/{}/databases/{}/documents/renames/{}",
                &self.project, "(default)", original
            );
            hub.projects()
                .databases_documents_delete(&name)
                .doit()
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
        if let Some(renamed) = self.get_bucket_name(&bucket).await? {
            // If the renamed bucket fails for some reason, then report it
            // immediately, do not attempt to generate a new name again.
            self.store_pack(packfile, &renamed, object).await
        } else {
            // Store the database in the same manner as any pack file, using the
            // given bucket and object names. If there is a collision with an
            // existing bucket that belongs to a different project, generate a
            // new random bucket name and try that instead. Loop until it works
            // or fails in some other manner.
            let mut bucket_name = bucket.to_owned();
            loop {
                match self.store_pack(packfile, &bucket_name, object).await {
                    Ok(coords) => return Ok(coords),
                    Err(err) => {
                        match err.downcast::<CollisionError>() {
                            Ok(_) => {
                                // There was a collision, simply generate a new
                                // name and hope that it will work. Type 4 UUID
                                // works well since it conforms to Google's
                                // bucket naming conventions.
                                bucket_name = uuid::Uuid::new_v4().to_string();
                                self.save_bucket_name(bucket, &bucket_name).await?;
                            }
                            Err(err) => return Err(err),
                        }
                    }
                }
            }
        }
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
        if let Some(renamed) = self.get_bucket_name(&bucket).await? {
            self.list_objects(&renamed).await
        } else {
            self.list_objects(bucket).await
        }
    }
}

/// Ensure the named bucket exists.
async fn create_bucket(
    hub: &storage1::Storage<HttpsConnector<HttpConnector>>,
    project_id: &str,
    name: &str,
    region: &Option<String>,
    storage_class: &Option<String>,
) -> Result<(), Error> {
    let mut req = storage1::api::Bucket::default();
    req.location = region.to_owned();
    req.name = Some(name.to_owned());
    req.storage_class = storage_class.to_owned();
    // If bucket creation results in a 409, it means the bucket already exists,
    // but may possibly be owned by some other project.
    if let Err(error) = hub.buckets().insert(req, project_id).doit().await {
        match &error {
            storage1::client::Error::BadRequest(value) => {
                if let Some(object) = value.as_object() {
                    if let Some(errobj) = object.get("error") {
                        if let Some(code) = errobj.get("code") {
                            if let Some(num) = code.as_u64() {
                                if num == 409 {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                return Err(anyhow!(format!("{:?}", error)));
            }
            _ => return Err(anyhow!(format!("{:?}", error))),
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

/// Run the given future on a newly created single-threaded runtime if possible,
/// otherwise raise an error if this thread already has a runtime.
fn block_on<F: core::future::Future>(future: F) -> Result<F::Output, Error> {
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
    fn test_google_store_roundtrip() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let creds_var = env::var("GOOGLE_CREDENTIALS");
        if creds_var.is_err() {
            // bail out silently if google is not configured
            return Ok(());
        }
        let credentials = creds_var?;
        let project_id = env::var("GOOGLE_PROJECT_ID")?;
        let region = env::var("GOOGLE_REGION")?;

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("credentials".to_owned(), credentials);
        properties.insert("project".to_owned(), project_id);
        // use standard storage class for testing since it is cheaper when
        // performing frequent downloads and deletions
        properties.insert("storage".to_owned(), "STANDARD".to_owned());
        properties.insert("region".to_owned(), region);
        let source = GoogleStore::new("google1", &properties)?;

        // store an object
        let bucket = xid::new().to_string();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "google1");
        assert_eq!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // store another object to ensure create bucket works repeatedly
        let object = "489492a49220c814f49487efb12adfbc372aa3f8".to_owned();
        let packfile = Path::new("../../test/fixtures/washington-journal.txt");
        let location = source.store_pack_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "google1");
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

    #[test]
    fn test_google_collision_error() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let creds_var = env::var("GOOGLE_CREDENTIALS");
        if creds_var.is_err() {
            // bail out silently if google is not configured
            return Ok(());
        }
        let credentials = creds_var?;
        let project_id = env::var("GOOGLE_PROJECT_ID")?;
        let region = env::var("GOOGLE_REGION")?;

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("credentials".to_owned(), credentials);
        properties.insert("project".to_owned(), project_id);
        // use standard storage class for testing since it is cheaper when
        // performing frequent downloads and deletions
        properties.insert("storage".to_owned(), "STANDARD".to_owned());
        properties.insert("region".to_owned(), region);
        let source = GoogleStore::new("google1", &properties)?;

        // attempt to store an object in a bucket that exists but does not
        // belong to this project (yes, need to change this value whenever the
        // bucket suddenly becomes available again)
        let bucket = "df72e3b3-ce33-4c83-8f59-2e020296c8ab".to_owned();
        let object = "b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let result = source.store_pack_sync(packfile, &bucket, &object);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.downcast::<CollisionError>().is_ok());

        Ok(())
    }

    #[test]
    fn test_google_database_bucket_collision() -> Result<(), Error> {
        // set up the environment and remote connection
        dotenv().ok();
        let creds_var = env::var("GOOGLE_CREDENTIALS");
        if creds_var.is_err() {
            // bail out silently if google is not configured
            return Ok(());
        }
        let credentials = creds_var?;
        let project_id = env::var("GOOGLE_PROJECT_ID")?;
        let region = env::var("GOOGLE_REGION")?;

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("credentials".to_owned(), credentials);
        properties.insert("project".to_owned(), project_id);
        // use standard storage class for testing since it is cheaper when
        // performing frequent downloads and deletions
        properties.insert("storage".to_owned(), "STANDARD".to_owned());
        properties.insert("region".to_owned(), region);
        let source = GoogleStore::new("google1", &properties)?;

        // attempt to store an object in a bucket that exists but does not
        // belong to this project (yes, need to change this value whenever the
        // bucket suddenly becomes available again)
        let bucket = "df72e3b3-ce33-4c83-8f59-2e020296c8ab".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let location = source.store_database_sync(packfile, &bucket, &object)?;
        assert_eq!(location.store, "google1");
        assert_ne!(location.bucket, bucket);
        assert_eq!(location.object, object);

        // retrieve the database file
        let location = Coordinates::new("google1", &bucket, &object);
        let outdir = tempdir()?;
        let outfile = outdir.path().join("restored.txt");
        source.retrieve_database_sync(&location, &outfile)?;

        // list available databases
        let mut retries = 10;
        let delay = std::time::Duration::from_millis(1000);
        loop {
            // Firestore is eventually consistent, so try a few times
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
