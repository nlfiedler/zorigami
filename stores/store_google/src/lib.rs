//
// Copyright (c) 2024 Nathan Fiedler
//
extern crate google_firestore1 as firestore1;
extern crate google_storage1 as storage1;
use anyhow::{Error, anyhow};
use base64::{Engine as _, engine::general_purpose};
use firestore1::hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use firestore1::hyper_util::client::legacy::Client;
use firestore1::hyper_util::client::legacy::connect::HttpConnector;
use firestore1::{Firestore, hyper_util};
use std::collections::HashMap;
use std::default::Default;
use std::path::Path;
use std::time::Duration;
use storage1::{Storage, yup_oauth2};
use store_core::{CollisionError, Coordinates};
use tokio::time::timeout;

// TCP connect deadline; any longer almost certainly indicates a network issue
// rather than a slow handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
// TCP keepalive so half-open sockets surface as errors rather than hanging the
// upload/download future indefinitely.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
// Drop idle pooled connections fairly aggressively so the next request opens a
// fresh connection if the previous one went stale.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
// Per-call deadline for short metadata operations (list/get/delete/create).
const METADATA_TIMEOUT: Duration = Duration::from_secs(120);
// Per-attempt deadline for bulk transfers (pack uploads/downloads, database
// archive uploads/downloads). The retry loop in `data::repositories` will try
// again on timeout, so this only needs to be long enough for a healthy
// transfer of one pack/database archive.
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(15 * 60);
// Per-call deadline for OAuth token + Storage/Firestore hub construction.
const CONNECT_HUB_TIMEOUT: Duration = Duration::from_secs(60);

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

/// Build an HttpConnector configured with a connect timeout and TCP
/// keepalive so that stalled or half-open sockets surface as errors instead
/// of hanging the calling future indefinitely.
fn build_http_connector() -> HttpConnector {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_connect_timeout(Some(CONNECT_TIMEOUT));
    http.set_keepalive(Some(TCP_KEEPALIVE));
    http
}

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

    async fn connect(&self) -> Result<Storage<HttpsConnector<HttpConnector>>, Error> {
        let conn = HttpsConnectorBuilder::new()
            .with_native_roots()?
            .https_or_http()
            .enable_http1()
            .wrap_connector(build_http_connector());
        let client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(conn);
        let account_key = yup_oauth2::read_service_account_key(&self.credentials).await?;
        //
        // Would prefer to use with_client() instead of builder() in order to
        // re-use the client connection, but it is seemingly impossible to get
        // the correct types or gain access to CustomHyperClientBuilder in
        // yup_oauth2.
        //
        let authenticator = with_deadline(
            "ServiceAccountAuthenticator::build (storage)",
            CONNECT_HUB_TIMEOUT,
            async {
                yup_oauth2::ServiceAccountAuthenticator::builder(account_key)
                    .build()
                    .await
                    .map_err(Error::from)
            },
        )
        .await?;
        Ok(Storage::new(client, authenticator))
    }

    async fn connect_fire(&self) -> Result<Firestore<HttpsConnector<HttpConnector>>, Error> {
        let conn = HttpsConnectorBuilder::new()
            .with_native_roots()?
            .https_or_http()
            .enable_http1()
            .wrap_connector(build_http_connector());
        let client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(conn);
        let account_key = yup_oauth2::read_service_account_key(&self.credentials).await?;
        let authenticator = with_deadline(
            "ServiceAccountAuthenticator::build (firestore)",
            CONNECT_HUB_TIMEOUT,
            async {
                yup_oauth2::ServiceAccountAuthenticator::builder(account_key)
                    .build()
                    .await
                    .map_err(Error::from)
            },
        )
        .await?;
        Ok(Firestore::new(client, authenticator))
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
        let mut bucket_name = bucket.to_owned();
        loop {
            match self.try_store_pack(packfile, &bucket_name, object).await {
                Ok(coords) => return Ok(coords),
                Err(err) => match err.downcast::<CollisionError>() {
                    Ok(_) => {
                        bucket_name = uuid::Uuid::new_v4().to_string();
                    }
                    Err(err) => return Err(err),
                },
            }
        }
    }

    async fn try_store_pack(
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
        // Storing the same object twice is not treated as an error.
        //
        // Wrap the upload in a tokio timeout so a stalled HTTP stream (e.g.
        // following a transient 408 from GCS, or a half-open TCP socket) is
        // surfaced as an error and the outer retry loop in
        // `data::repositories` can try again, rather than hanging forever.
        // Raw `timeout(...)` is used here (rather than `with_deadline`)
        // because the inner match needs to inspect the typed `storage1::Error`
        // to detect a 403 bucket-collision case below.
        //
        // Caveat: cancelling `upload_resumable` mid-flight orphans the GCS
        // resumable session URL on the server. GCS auto-expires abandoned
        // sessions after ~7 days, so this is a quota-cleanup nuisance rather
        // than a correctness concern, and the next retry establishes a fresh
        // session.
        let upload_result = match timeout(
            TRANSFER_TIMEOUT,
            hub.objects()
                .insert(req, bucket)
                .name(object)
                .upload_resumable(infile, mimetype),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                return Err(anyhow!(
                    "upload_resumable timed out after {:?} for {}/{}",
                    TRANSFER_TIMEOUT,
                    bucket,
                    object
                ));
            }
        };
        match upload_result {
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
                storage1::Error::BadRequest(value) => {
                    if let Some(object) = value.as_object()
                        && let Some(errobj) = object.get("error")
                        && let Some(code) = errobj.get("code")
                        && let Some(num) = code.as_u64()
                        && num == 403
                    {
                        return Err(Error::from(CollisionError {}));
                    }
                    Err(anyhow!(format!("{:?}", error)))
                }
                _ => Err(anyhow!(format!("{:?}", error))),
            },
        }
    }

    pub fn retrieve_pack_sync(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        block_on(self.retrieve_pack(location, outfile)).and_then(std::convert::identity)
    }

    pub async fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        use http_body_util::BodyExt;
        use std::io::Write;
        let hub = self.connect().await?;
        // Bound the overall download (initial request + body streaming) so a
        // stalled HTTP stream surfaces as an error rather than hanging the
        // calling thread indefinitely.
        let download = async {
            let (mut response, _object) = hub
                .objects()
                .get(&location.bucket, &location.object)
                .param("alt", "media")
                .doit()
                .await?;
            let mut local = std::fs::File::create(outfile)?;
            while let Some(next) = response.frame().await {
                let frame = next?;
                if let Some(chunk) = frame.data_ref() {
                    local.write_all(chunk)?;
                }
            }
            Ok::<_, Error>(())
        };
        with_deadline("retrieve_pack", TRANSFER_TIMEOUT, download).await
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
            let (_response, buckets) = with_deadline("list_buckets", METADATA_TIMEOUT, async {
                call.doit().await.map_err(|e| anyhow!(format!("{:?}", e)))
            })
            .await?;
            if let Some(bucks) = buckets.items.as_ref() {
                // Only consider named buckets; there is no guarantee that they
                // have one, despite the API requiring that names be provided
                // when creating them.
                for bucket in bucks.iter() {
                    if let Some(name) = bucket.name.as_ref() {
                        // ignore the Firestore buckets, which we do not want
                        // to accidentally delete and thus lose the bucket
                        // collision database
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
            let (_response, objects) = with_deadline("list_objects", METADATA_TIMEOUT, async {
                call.doit().await.map_err(|e| anyhow!(format!("{:?}", e)))
            })
            .await?;
            if let Some(objs) = objects.items.as_ref() {
                // Only consider named objects; there is no guarantee that they
                // have one, despite the API requiring that names be provided
                // when uploading them.
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
        Ok(results)
    }

    pub fn delete_object_sync(&self, bucket: &str, object: &str) -> Result<(), Error> {
        block_on(self.delete_object(bucket, object)).and_then(std::convert::identity)
    }

    pub async fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let hub = self.connect().await?;
        with_deadline("delete_object", METADATA_TIMEOUT, async {
            hub.objects()
                .delete(bucket, object)
                .doit()
                .await
                .map_err(|e| anyhow!(format!("{:?}", e)))?;
            Ok(())
        })
        .await
    }

    pub fn delete_bucket_sync(&self, bucket: &str) -> Result<(), Error> {
        block_on(self.delete_bucket(bucket)).and_then(std::convert::identity)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let hub = self.connect().await?;
        with_deadline("delete_bucket", METADATA_TIMEOUT, async {
            hub.buckets()
                .delete(bucket)
                .doit()
                .await
                .map_err(|e| anyhow!(format!("{:?}", e)))?;
            Ok(())
        })
        .await
    }

    /// Record the new name of the bucket.
    async fn save_bucket_name(&self, original: &str, renamed: &str) -> Result<(), Error> {
        let hub = self.connect_fire().await?;
        let mut values: HashMap<String, firestore1::api::Value> = HashMap::new();
        let value = firestore1::api::Value {
            string_value: Some(renamed.to_owned()),
            ..Default::default()
        };
        values.insert("renamed".into(), value);
        let name = format!(
            "projects/{}/databases/{}/documents/renames/{}",
            &self.project, "(default)", original
        );
        let document = firestore1::api::Document {
            fields: Some(values),
            ..Default::default()
        };
        // databases_documents_patch() will either insert or update
        with_deadline("save_bucket_name", METADATA_TIMEOUT, async {
            hub.projects()
                .databases_documents_patch(document, &name)
                .doit()
                .await
                .map_err(|e| anyhow!(format!("{:?}", e)))?;
            Ok(())
        })
        .await
    }

    /// Retrieve the renamed value for the original bucket.
    ///
    /// A timeout here is propagated as an error rather than swallowed: if
    /// Firestore is slow but a rename mapping exists, treating the result as
    /// "no rename" would cause the caller to write to the wrong bucket (or
    /// create a duplicate mapping after a 403 collision). Other errors —
    /// notably the 404 returned for a missing document — are still treated
    /// as "no rename", preserving the prior contract.
    async fn get_bucket_name(&self, original: &str) -> Result<Option<String>, Error> {
        let hub = self.connect_fire().await?;
        let name = format!(
            "projects/{}/databases/{}/documents/renames/{}",
            &self.project, "(default)", original
        );
        let fetch = hub.projects().databases_documents_get(&name).doit();
        let result = match timeout(METADATA_TIMEOUT, fetch).await {
            Ok(r) => r,
            Err(_) => {
                return Err(anyhow!(
                    "get_bucket_name timed out after {:?} for {}",
                    METADATA_TIMEOUT,
                    original
                ));
            }
        };
        if let Ok((_response, document)) = result
            && let Some(fields) = document.fields
            && let Some(renamed_field) = fields.get("renamed")
        {
            return Ok(renamed_field.string_value.to_owned());
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
        if let Some(renamed) = self.get_bucket_name(bucket).await? {
            // If the renamed bucket fails for some reason, then report it
            // immediately, do not attempt to generate a new name again.
            self.try_store_pack(packfile, &renamed, object).await
        } else {
            // Store the database in the same manner as any pack file, using the
            // given bucket and object names. If there is a collision with an
            // existing bucket that belongs to a different project, generate a
            // new random bucket name and try that instead. Loop until it works
            // or fails in some other manner.
            let mut bucket_name = bucket.to_owned();
            loop {
                match self.try_store_pack(packfile, &bucket_name, object).await {
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
        if let Some(renamed) = self.get_bucket_name(bucket).await? {
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
    let req = storage1::api::Bucket {
        location: region.to_owned(),
        name: Some(name.to_owned()),
        storage_class: storage_class.to_owned(),
        ..Default::default()
    };
    // If bucket creation results in a 409, it means the bucket already exists,
    // but may possibly be owned by some other project. Raw `timeout(...)` is
    // used here (rather than `with_deadline`) because the inner match needs
    // to inspect the typed `storage1::Error` to detect that 409 case below.
    let insert_fut = hub.buckets().insert(req, project_id).doit();
    let insert_result = match timeout(METADATA_TIMEOUT, insert_fut).await {
        Ok(r) => r,
        Err(_) => {
            return Err(anyhow!(
                "create_bucket timed out after {:?} for {}",
                METADATA_TIMEOUT,
                name
            ));
        }
    };
    if let Err(error) = insert_result {
        match &error {
            storage1::Error::BadRequest(value) => {
                if let Some(object) = value.as_object()
                    && let Some(errobj) = object.get("error")
                    && let Some(code) = errobj.get("code")
                    && let Some(num) = code.as_u64()
                    && num == 409
                {
                    return Ok(());
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
    use std::sync::Once;
    use tempfile::tempdir;

    /// Install a rustls `CryptoProvider` exactly once per process.
    ///
    /// `cargo test -p A -p B` unifies workspace features, which means this
    /// crate's test binary can end up linked against rustls 0.23 with both
    /// the `ring` and `aws-lc-rs` providers active (pulled in by other
    /// workspace crates that depend on aws-config). Rustls then refuses to
    /// auto-select a default and panics on first use. The production binary
    /// installs a provider in `server/src/main.rs`; tests must do the same.
    fn ensure_crypto_provider() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

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
    #[serial]
    fn test_google_store_roundtrip() -> Result<(), Error> {
        ensure_crypto_provider();
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
    fn test_google_collision_error() -> Result<(), Error> {
        ensure_crypto_provider();
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
        // bucket suddenly becomes available again); the store should recover
        // by generating a new bucket name and retrying
        let bucket = "caefd289-4314-4ff3-bd0a-5be30c4fb8c2".to_owned();
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
    fn test_google_database_bucket_collision() -> Result<(), Error> {
        ensure_crypto_provider();
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
        let bucket = "caefd289-4314-4ff3-bd0a-5be30c4fb8c2".to_owned();
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
