//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::Error;
use std::path::Path;

pub mod minio;
pub mod sftp;

pub trait Store {
    ///
    /// Store the pack file the local disk.
    ///
    fn store_pack(&self, packfile: &Path, bucket: &str, object: &str) -> Result<(), Error>;

    ///
    /// Retrieve a pack from the given bucket and object name.
    ///
    fn retrieve_pack(&self, bucket: &str, object: &str, outfile: &Path) -> Result<(), Error>;

    ///
    /// List the known buckets in the store.
    ///
    fn list_buckets(&self) -> Result<Vec<String>, Error>;

    ///
    /// List of all objects in the named bucket.
    ///
    fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error>;

    ///
    /// Delete the named object from the given bucket.
    ///
    fn delete_object(&self, bucket: &str, object: &str) -> Result<(), Error>;

    ///
    /// Delete the named bucket. It almost certainly needs to be empty first, so
    /// use `list_objects()` and `delete_object()` to remove the objects.
    ///
    fn delete_bucket(&self, bucket: &str) -> Result<(), Error>;
}
