//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::data::sources::PackDataSource;
use crate::domain::entities::{PackLocation, Store};
use failure::Error;
use std::path::Path;
use store_core::Coordinates;
use store_sftp::SftpStore;

///
/// A `PackDataSource` implementation that operates over SSH2/SFTP to store pack
/// files on a remote system.
///
#[derive(Debug)]
pub struct SftpPackSource {
    store: SftpStore,
}

impl SftpPackSource {
    /// Validate the given store and construct a secure FTP pack source.
    pub fn new(store: &Store) -> Result<Self, Error> {
        let store = SftpStore::new(&store.id, &store.properties)?;
        Ok(Self { store })
    }
}

impl PackDataSource for SftpPackSource {
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
