//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::data::sources::PackDataSource;
use crate::domain::entities::{PackLocation, Store};
use anyhow::Error;
use std::path::Path;
use store_core::Coordinates;
use store_minio::MinioStore;

///
/// A `PackDataSource` implementation that uses the Amazon S3 protocol to
/// connect to a Minio storage server.
///
#[derive(Debug)]
pub struct MinioPackSource {
    store: MinioStore,
}

impl MinioPackSource {
    /// Validate the given store and construct a minio pack source.
    pub fn new(store: &Store) -> Result<Self, Error> {
        let store = MinioStore::new(&store.id, &store.properties)?;
        Ok(Self { store })
    }
}

impl PackDataSource for MinioPackSource {
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
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<Coordinates, Error>>();
        let store = self.store.clone();
        let pack = packfile.to_path_buf();
        let buck = bucket.to_owned();
        let obj = object.to_owned();
        std::thread::spawn(move || {
            tx.send(store.store_pack_sync(&pack, &buck, &obj)).unwrap();
        });
        Ok(PackLocation::from(rx.recv()??))
    }

    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        let coords: Coordinates = location.to_owned().into();
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
        let store = self.store.clone();
        let target = outfile.to_path_buf();
        std::thread::spawn(move || {
            tx.send(store.retrieve_pack_sync(&coords, &target)).unwrap();
        });
        rx.recv()?
    }

    fn list_buckets(&self) -> Result<Vec<String>, Error> {
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<Vec<String>, Error>>();
        let store = self.store.clone();
        std::thread::spawn(move || {
            tx.send(store.list_buckets_sync()).unwrap();
        });
        rx.recv()?
    }

    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<Vec<String>, Error>>();
        let store = self.store.clone();
        let buck = bucket.to_owned();
        std::thread::spawn(move || {
            tx.send(store.list_objects_sync(&buck)).unwrap();
        });
        rx.recv()?
    }

    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error> {
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
        let store = self.store.clone();
        let buck = bucket.to_owned();
        let obj = object.to_owned();
        std::thread::spawn(move || {
            tx.send(store.delete_object_sync(&buck, &obj)).unwrap();
        });
        rx.recv()?
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
        let store = self.store.clone();
        let buck = bucket.to_owned();
        std::thread::spawn(move || {
            tx.send(store.delete_bucket_sync(&buck)).unwrap();
        });
        rx.recv()?
    }

    fn store_database(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error> {
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<Coordinates, Error>>();
        let store = self.store.clone();
        let pack = packfile.to_path_buf();
        let buck = bucket.to_owned();
        let obj = object.to_owned();
        std::thread::spawn(move || {
            tx.send(store.store_database_sync(&pack, &buck, &obj))
                .unwrap();
        });
        Ok(PackLocation::from(rx.recv()??))
    }

    fn retrieve_database(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error> {
        let coords: Coordinates = location.to_owned().into();
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), Error>>();
        let store = self.store.clone();
        let target = outfile.to_path_buf();
        std::thread::spawn(move || {
            tx.send(store.retrieve_database_sync(&coords, &target))
                .unwrap();
        });
        rx.recv()?
    }

    fn list_databases(&self, bucket: &str) -> Result<Vec<String>, Error> {
        // work-around for async runtime not allowing block_on call
        let (tx, rx) = std::sync::mpsc::channel::<Result<Vec<String>, Error>>();
        let store = self.store.clone();
        let buck = bucket.to_owned();
        std::thread::spawn(move || {
            tx.send(store.list_databases_sync(&buck)).unwrap();
        });
        rx.recv()?
    }
}
