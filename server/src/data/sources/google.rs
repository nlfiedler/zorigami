//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::data::sources::PackDataSource;
use crate::domain::entities::{PackLocation, Store};
use failure::Error;
use std::path::Path;
use store_core::Coordinates;
use store_google::GoogleStore;

///
/// A `PackDataSource` implementation for Google (Cloud) Storage.
///
#[derive(Debug)]
pub struct GooglePackSource {
    store: GoogleStore,
}

impl GooglePackSource {
    /// Validate the given store and construct a google pack source.
    pub fn new(store: &Store) -> Result<Self, Error> {
        let store = GoogleStore::new(&store.id, &store.properties)?;
        Ok(Self { store })
    }
}

impl PackDataSource for GooglePackSource {
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
        let coords = self.store.store_pack(packfile, bucket, object)?;
        Ok(PackLocation::from(coords))
    }

    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        let coords: Coordinates = location.to_owned().into();
        self.store.retrieve_pack(&coords, outfile)
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        self.store.list_buckets()
    }

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        self.store.list_objects(bucket)
    }

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        self.store.delete_object(bucket, object)
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        self.store.delete_bucket(bucket)
    }
}