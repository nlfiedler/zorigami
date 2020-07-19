//
// Copyright (c) 2020 Nathan Fiedler
//
use failure::{err_msg, Error};
use google_storage1::{Bucket, Object, Storage};
use hyper::Client;
use std::collections::HashMap;
use std::path::Path;
use store_core::Coordinates;
use yup_oauth2::ServiceAccountAccess;

type StorageHub = Storage<Client, ServiceAccountAccess<Client>>;

#[derive(Debug)]
pub struct GoogleStore {
    store_id: String,
    credentials: String,
    project: String,
    storage: String,
}

impl GoogleStore {
    /// Validate the given store and construct a google pack source.
    pub fn new(store_id: &str, props: &HashMap<String, String>) -> Result<Self, Error> {
        let credentials = props
            .get("credentials")
            .ok_or_else(|| err_msg("missing credentials property"))?;
        let project = props
            .get("project")
            .ok_or_else(|| err_msg("missing project property"))?;
        let storage = props
            .get("storage")
            .ok_or_else(|| err_msg("missing storage property"))?;
        Ok(Self {
            store_id: store_id.to_owned(),
            credentials: credentials.to_owned(),
            project: project.to_owned(),
            storage: storage.to_owned(),
        })
    }

    fn connect(&self) -> Result<StorageHub, Error> {
        let client_secret = yup_oauth2::service_account_key_from_file(&self.credentials)?;
        let access = ServiceAccountAccess::new(
            client_secret,
            Client::with_connector(hyper::net::HttpsConnector::new(
                hyper_rustls::TlsClient::new(),
            )),
        );
        let hub = Storage::new(
            Client::with_connector(hyper::net::HttpsConnector::new(
                hyper_rustls::TlsClient::new(),
            )),
            access,
        );
        Ok(hub)
    }

    pub fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<Coordinates, Error> {
        let hub = self.connect()?;
        // the bucket must exist before receiving objects
        create_bucket(&hub, &self.project, bucket, &self.storage)?;
        let mut req = Object::default();
        req.name = Some(object.to_owned());
        let result = hub.objects().insert(req, bucket).upload(
            std::fs::File::open(packfile).unwrap(),
            "application/octet-stream".parse().unwrap(),
        );
        // storing the same object twice is not treated as an error
        if let Err(error) = result {
            return Err(err_msg(format!("{:?}", error)));
        }
        let loc = Coordinates::new(&self.store_id, bucket, object);
        Ok(loc)
    }

    pub fn retrieve_pack(&self, location: &Coordinates, outfile: &Path) -> Result<(), Error> {
        let hub = self.connect()?;
        let result = hub
            .objects()
            .get(&location.bucket, &location.object)
            // the magic argument indicating to download the contents
            .param("alt", "media")
            .doit();
        match result {
            Err(error) => Err(err_msg(format!("{:?}", error))),
            Ok(mut response) => {
                let mut file = std::fs::File::create(outfile)?;
                // hyper v0.10 Response implements Read trait
                std::io::copy(&mut response.0, &mut file)?;
                Ok(())
            }
        }
    }

    pub fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let hub = self.connect()?;
        let mut results: Vec<String> = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut call = hub.buckets().list(&self.project);
            if let Some(token) = page_token {
                call = call.page_token(&token);
            }
            let result = call.doit();
            match result {
                Err(error) => return Err(err_msg(format!("{:?}", error))),
                Ok(response) => {
                    if let Some(buckets) = response.1.items {
                        // only consider named buckets (no guarantee they have one)
                        for entry in buckets {
                            if let Some(name) = entry.name {
                                results.push(name);
                            }
                        }
                    }
                    if response.1.next_page_token.is_none() {
                        break;
                    }
                    page_token = response.1.next_page_token;
                }
            }
        }
        Ok(results)
    }

    pub fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        let hub = self.connect()?;
        let mut results: Vec<String> = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut call = hub.objects().list(bucket);
            if let Some(token) = page_token {
                call = call.page_token(&token);
            }
            let result = call.doit();
            match result {
                Err(error) => return Err(err_msg(format!("{:?}", error))),
                Ok(response) => {
                    if let Some(objects) = response.1.items {
                        // only consider named objects (no guarantee they have one)
                        for entry in objects {
                            if let Some(name) = entry.name {
                                results.push(name);
                            }
                        }
                    }
                    if response.1.next_page_token.is_none() {
                        break;
                    }
                    page_token = response.1.next_page_token;
                }
            }
        }
        Ok(results)
    }

    pub fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        let hub = self.connect()?;
        let result = hub.objects().delete(bucket, object).doit();
        if let Err(error) = result {
            return Err(err_msg(format!("{:?}", error)));
        }
        Ok(())
    }

    pub fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        let hub = self.connect()?;
        let result = hub.buckets().delete(bucket).doit();
        if let Err(error) = result {
            return Err(err_msg(format!("{:?}", error)));
        }
        Ok(())
    }
}

/// Ensure the named bucket exists.
pub fn create_bucket(
    hub: &StorageHub,
    project_id: &str,
    name: &str,
    storage_class: &str,
) -> Result<(), Error> {
    let mut req = Bucket::default();
    req.name = Some(name.to_owned());
    req.storage_class = Some(storage_class.to_owned());
    let result = hub.buckets().insert(req, project_id).doit();
    if let Err(error) = result {
        match error {
            google_storage1::Error::BadRequest(response) => match response.error.code {
                // same bucket name already exists which is fine
                409 => return Ok(()),
                _ => return Err(err_msg(format!("unhandled response {:?}", response))),
            },
            _ => return Err(err_msg(format!("{:?}", error))),
        }
    }
    Ok(())
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

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("credentials".to_owned(), credentials);
        properties.insert("project".to_owned(), project_id);
        // use standard storage class for testing since it is cheaper when
        // performing frequent downloads and deletions
        properties.insert("storage".to_owned(), "STANDARD".to_owned());
        let result = GoogleStore::new("google1", &properties);
        assert!(result.is_ok());
        let source = result.unwrap();

        // store an object
        let bucket = "747267d56e7057118a9aa40c24c1730f".to_owned();
        let object = "39c6061a56b7711f92c6ccd2047d47fdcc1609c1".to_owned();
        let packfile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let result = source.store_pack(packfile, &bucket, &object);
        assert!(result.is_ok());
        let location = result.unwrap();
        assert_eq!(location.store, "google1");
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
