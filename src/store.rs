//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `store` module defines functions for uploading and retrieving pack files
//! stored in local or remote sites.

use super::core::PackLocation;
use super::database::Database;
use failure::{err_msg, Error};
use std::path::Path;
use std::str::{self, FromStr};
use ulid::Ulid;

pub mod local;
pub mod minio;
pub mod sftp;

///
/// The type of store implementation to be constructed using the factory
/// function `build_store()`. Can be constructed from a string using the
/// `FromStr` trait's `from_str()` function.
///
#[derive(Debug, Eq, PartialEq, Hash)]
pub enum StoreType {
    LOCAL,
    MINIO,
    SFTP,
}

impl ToString for StoreType {
    fn to_string(&self) -> String {
        match self {
            StoreType::LOCAL => String::from("local"),
            StoreType::MINIO => String::from("minio"),
            StoreType::SFTP => String::from("sftp"),
        }
    }
}

impl FromStr for StoreType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local" => Ok(StoreType::LOCAL),
            "minio" => Ok(StoreType::MINIO),
            "sftp" => Ok(StoreType::SFTP),
            _ => Err(err_msg(format!("not a recognized store type: {}", s))),
        }
    }
}

///
/// Indicates if a pack store is considered to be "fast" or "slow" with regards
/// to pack retrieval. Some stores are rather slow. For example, Amazon Glacier
/// would be very slow, while SFTP or S3 would be fast.
///
#[derive(Debug, Eq, PartialEq, Hash)]
pub enum StoreSpeed {
    FAST,
    SLOW,
}

///
/// Construct an instance of a Store appropriate for the given type. The store
/// configuration will have default values that most likely need to be changed
/// in order to function properly (e.g. setting the host for the SFTP store, or
/// the base directory for the local store).
///
pub fn build_store(store_type: StoreType, id: Option<&str>) -> Box<Store> {
    let uuid = if id.is_some() {
        id.unwrap().to_owned()
    } else {
        Ulid::new().to_string().to_lowercase()
    };
    match store_type {
        StoreType::LOCAL => Box::new(local::LocalStore::new(&uuid)),
        StoreType::MINIO => Box::new(minio::MinioStore::new(&uuid)),
        StoreType::SFTP => Box::new(sftp::SftpStore::new(&uuid)),
    }
}

///
/// Retrieve those stores with saved configurations in the database. Returns a
/// list of database keys which can be used to build a store based on the saved
/// configuration by calling `load_store()`.
///
pub fn find_stores(dbase: &Database) -> Result<Vec<String>, Error> {
    let results = dbase.find_prefix("store/")?;
    Ok(results)
}

///
/// Find the store name by the given unique identifier (usually the last part of
/// the full name, such as "store/local/123ulidxyz"). With the full name the
/// store can be loaded from the database via `load_store()`.
///
pub fn find_store_by_id(dbase: &Database, id: &str) -> Result<Option<String>, Error> {
    let tail = format!("/{}", id);
    let candidates = find_stores(dbase)?;
    for fullname in candidates {
        if fullname.ends_with(&tail) {
            return Ok(Some(fullname));
        }
    }
    Ok(None)
}

///
/// Construct the unique name for the given store, which is used as the key to
/// saving the store configuration in the database, as well as referring to the
/// store in the `core::Dataset`.
///
pub fn store_name(store: &Store) -> String {
    let type_name = store.get_type().to_string();
    let unique_id = store.get_id();
    format!("store/{}/{}", type_name, unique_id)
}

///
/// Save the given store's configuration to the database.
///
pub fn save_store(dbase: &Database, store: &Store) -> Result<(), Error> {
    let key = store_name(store);
    let value = store.get_config().to_json()?;
    dbase.put_document(key.as_bytes(), value.as_bytes())?;
    Ok(())
}

///
/// Instantiate a store and read its saved configuration from the database, if
/// available. The `name` has the format `store/<type>/<name>`, where `<name>`
/// may contain additional slashes. The store names can be retrieved using
/// `find_stores()`. If the named store does not have a saved configuration, it
/// will have the default values for its type.
///
pub fn load_store(dbase: &Database, name: &str) -> Result<Box<Store>, Error> {
    let name_parts: Vec<&str> = name.split('/').collect();
    if name_parts.len() < 3 {
        return Err(err_msg(format!(
            "name {} requires three / separated parts",
            name
        )));
    }
    if name_parts[0] != "store" {
        return Err(err_msg(format!("name {} must start with 'store'", name)));
    }
    let store_type = StoreType::from_str(name_parts[1])?;
    let mut store_impl = build_store(store_type, Some(name_parts[2]));
    if let Some(dbv) = dbase.get_document(name.as_bytes())? {
        let value_str = str::from_utf8(&dbv)?;
        store_impl.get_config_mut().from_json(value_str)?;
    }
    Ok(store_impl)
}

///
/// Load all of the stores named in the list.
///
pub fn load_stores(dbase: &Database, names: &[String]) -> Result<Vec<Box<Store>>, Error> {
    let mut stores_boxed: Vec<Box<Store>> = Vec::new();
    for name in names {
        let boxed = load_store(dbase, name)?;
        stores_boxed.push(boxed);
    }
    Ok(stores_boxed)
}

///
/// Store the given pack to all of the provided store implementations. Returns
/// the list of all pack locations, which can be used to retrieve the pack
/// at a later time.
///
pub fn store_pack(
    packfile: &Path,
    bucket: &str,
    object: &str,
    stores: &[Box<Store>],
) -> Result<Vec<PackLocation>, Error> {
    let mut results: Vec<PackLocation> = Vec::new();
    for store in stores {
        let loc = store.store_pack(packfile, bucket, object)?;
        results.push(loc);
    }
    Ok(results)
}

///
/// Retrieve the pack from one of the stores provided. The most suitable store
/// will be utilized, preferring a local store over a remote one, and fast one
/// over a slow one.
///
pub fn retrieve_pack(
    stores: &[Box<Store>],
    locations: &[PackLocation],
    outfile: &Path,
) -> Result<(), Error> {
    // look for a local store over a remote one
    let mut store_index = 0;
    let mut found_local = false;
    for (idx, store) in stores.iter().enumerate() {
        if store.get_type() == StoreType::LOCAL {
            store_index = idx;
            found_local = true;
            break;
        }
    }
    // if no local store is available, look for a "fast" store
    if !found_local {
        for (idx, store) in stores.iter().enumerate() {
            if store.get_speed() == StoreSpeed::FAST {
                store_index = idx;
                break;
            }
        }
    }
    let store = &stores[store_index];
    // find the pack location whose store id matches the first choice
    let mut loc_index = 0;
    let mut found_location = false;
    for (idx, loc) in locations.iter().enumerate() {
        if loc.store == store.get_id() {
            loc_index = idx;
            found_location = true;
            break;
        }
    }
    if !found_location {
        return Err(err_msg(format!(
            "cannot find store for pack: {}",
            locations[loc_index].store
        )));
    }
    store.retrieve_pack(&locations[loc_index], outfile)
}

///
/// A `Store` configuration can serialize and deserialize using JSON. The
/// properties and behavior are specific to each store implementation.
///
pub trait Config {
    ///
    /// Return the user-defined label for this configuration, used to present
    /// the store in a friendly manner in the user interface.
    ///
    fn get_label(&self) -> String;

    ///
    /// Read the store configuration from the given JSON and update the
    /// properties of this configuration instance.
    ///
    fn from_json(&mut self, data: &str) -> Result<(), Error>;

    ///
    /// Write this store configuration to a JSON formatted string.
    ///
    fn to_json(&self) -> Result<String, Error>;
}

///
/// A pack store knows how to store, list, retrieve, and delete packs from a
/// storage system, such as local disk, SFTP, or cloud-based store.
///
pub trait Store {
    ///
    /// Return the unique identifier for this store.
    ///
    fn get_id(&self) -> &str;

    ///
    /// Return the type of this store.
    ///
    fn get_type(&self) -> StoreType;

    ///
    /// Return the speed that best represents pack retrieval for this store.
    ///
    fn get_speed(&self) -> StoreSpeed;

    ///
    /// Return a reference to the configuration for this store.
    ///
    fn get_config(&self) -> &Config;

    ///
    /// Return a mutable reference to the configuration for this store.
    ///
    fn get_config_mut(&mut self) -> &mut Config;

    ///
    /// Store the pack file under the named bucket and referenced by the object
    /// name. Returns the remote location of the pack, in case it was assigned
    /// new values by the backing store.
    ///
    fn store_pack(
        &self,
        packfile: &Path,
        bucket: &str,
        object: &str,
    ) -> Result<PackLocation, Error>;

    ///
    /// Retrieve a pack from the given location, writing the contents to the
    /// given path.
    ///
    fn retrieve_pack(&self, location: &PackLocation, outfile: &Path) -> Result<(), Error>;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storetype_fromstr() {
        let result = StoreType::from_str("local");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::LOCAL);
        assert_eq!(stype.to_string(), "local");
        let result = StoreType::from_str("minio");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::MINIO);
        assert_eq!(stype.to_string(), "minio");
        let result = StoreType::from_str("sftp");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::SFTP);
        assert_eq!(stype.to_string(), "sftp");
        let result = StoreType::from_str("foobar");
        assert!(result.is_err());
    }
}
