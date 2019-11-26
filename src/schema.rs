//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `schema` module defines the GraphQL schema and resolvers.

use super::core;
use super::database::Database;
use super::engine;
use super::store;
use chrono::prelude::*;
use cron::Schedule;
use juniper::{
    graphql_object, graphql_scalar, FieldError, FieldResult, GraphQLEnum, GraphQLInputObject,
    GraphQLObject, ParseScalarResult, ParseScalarValue, RootNode, Value,
};
use std::path::Path;
use std::str::FromStr;

// Our GraphQL version of the core::Checksum type. It is tedious to implement
// all of the juniper interfaces, and the macro requires having a `from_str`
// where our type already has that method. This just seemed easier...
pub struct Checksum(String);

// need `where Scalar = <S>` parameterization to use this with objects
// c.f. https://github.com/graphql-rust/juniper/issues/358 for details
graphql_scalar!(Checksum where Scalar = <S> {
    description: "A SHA1 or SHA256 checksum, with algorithm prefix."

    resolve(&self) -> Value {
        Value::scalar(self.0.clone())
    }

    from_input_value(v: &InputValue) -> Option<Checksum> {
        v.as_scalar_value::<String>().filter(|s| {
            // make sure the input value actually looks like a digest
            s.starts_with("sha1-") || s.starts_with("sha256-")
        }).map(|s| Checksum(s.to_owned()))
    }

    from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
});

impl From<core::Checksum> for Checksum {
    fn from(value: core::Checksum) -> Self {
        Checksum(format!("{}", &value))
    }
}

impl Into<core::Checksum> for Checksum {
    fn into(self) -> core::Checksum {
        // relying on input validation to make unwrap safe
        core::Checksum::from_str(&self.0).unwrap()
    }
}

// Define a larger integer type so we can represent those larger values, such as
// file sizes and epoch time in milliseconds. Some of the core types define
// properties that are unsigned 32-bit integers, so to be certain we can
// represent those values in GraphQL, we will use this type.
pub struct BigInt(i64);

impl BigInt {
    /// Construct a BigInt for the given value.
    pub fn new(value: i64) -> Self {
        BigInt(value)
    }
}

impl Into<u32> for BigInt {
    fn into(self) -> u32 {
        self.0 as u32
    }
}

impl From<u32> for BigInt {
    fn from(t: u32) -> Self {
        BigInt(i64::from(t))
    }
}

// need `where Scalar = <S>` parameterization to use this with objects
// c.f. https://github.com/graphql-rust/juniper/issues/358 for details
graphql_scalar!(BigInt where Scalar = <S> {
    description: "An integer type larger than the standard signed 32-bit."

    resolve(&self) -> Value {
        Value::scalar(format!("{}", self.0))
    }

    from_input_value(v: &InputValue) -> Option<BigInt> {
        v.as_scalar_value::<String>().filter(|s| {
            // make sure the input value parses as an integer
            i64::from_str_radix(s, 10).is_ok()
        }).map(|s| BigInt(i64::from_str_radix(s, 10).unwrap()))
    }

    from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
});

#[derive(GraphQLEnum)]
/// Type of the entry in the tree.
enum EntryType {
    /// Represents a file.
    File,
    /// Represents a directory.
    Directory,
    /// Represents a symbolic link.
    SymLink,
    /// An error occurred when processing this entry.
    Error,
}

impl From<core::EntryType> for EntryType {
    fn from(entype: core::EntryType) -> Self {
        match entype {
            core::EntryType::FILE => EntryType::File,
            core::EntryType::DIR => EntryType::Directory,
            core::EntryType::SYMLINK => EntryType::SymLink,
            core::EntryType::ERROR => EntryType::Error,
        }
    }
}

#[derive(GraphQLObject)]
/// A file, directory, or link within a tree.
struct TreeEntry {
    /// File system name of this entry.
    name: String,
    /// File system type of this entry.
    fstype: EntryType,
    /// Modification time of the entry, may not be available.
    mod_time: DateTime<Utc>,
    /// Coordinates for this entry in the database.
    reference: String,
}

impl From<core::TreeEntry> for TreeEntry {
    fn from(entry: core::TreeEntry) -> Self {
        Self {
            name: entry.name,
            fstype: EntryType::from(entry.fstype),
            mod_time: entry.mtime,
            reference: entry.reference.to_string(),
        }
    }
}

#[derive(GraphQLObject)]
/// A collection of files, directories, and links.
struct Tree {
    entries: Vec<TreeEntry>,
}

impl From<core::Tree> for Tree {
    fn from(tree: core::Tree) -> Self {
        let entries = tree.entries.into_iter().map(TreeEntry::from).collect();
        Self { entries }
    }
}

#[derive(GraphQLObject)]
/// A single backup.
struct Snapshot {
    /// Computed checksum of the snapshot.
    checksum: Checksum,
    /// The snapshot before this one, if any.
    parent: Option<Checksum>,
    /// Time when the snapshot was first created.
    start_time: DateTime<Utc>,
    /// Time when the snapshot completely finished.
    end_time: Option<DateTime<Utc>>,
    /// Total number of files contained in this snapshot.
    file_count: BigInt,
    /// Reference to the tree containing all of the files.
    tree: Checksum,
}

impl Into<core::Snapshot> for Snapshot {
    fn into(self) -> core::Snapshot {
        let parent = self.parent.map(Checksum::into);
        let tree = Checksum::into(self.tree);
        let mut snap = core::Snapshot::new(parent, tree);
        snap.start_time = self.start_time;
        snap.end_time = self.end_time;
        snap.file_count = self.file_count.into();
        snap
    }
}

impl From<core::Snapshot> for Snapshot {
    fn from(set: core::Snapshot) -> Self {
        Self {
            checksum: Checksum::from(set.checksum()),
            parent: set.parent.map(|v| v.into()),
            start_time: set.start_time,
            end_time: set.end_time,
            file_count: BigInt::from(set.file_count),
            tree: Checksum::from(set.tree),
        }
    }
}

#[derive(GraphQLObject)]
/// A single version of a saved file.
struct File {
    /// Reference to the file in the database.
    digest: Checksum,
    /// Byte length of this version of the file.
    length: BigInt,
}

#[derive(GraphQLObject)]
/// Application configuration record.
struct Configuration {
    /// Name of the computer on which this application is running.
    hostname: String,
    /// Name of the user running this application.
    username: String,
    /// Computer UUID for generating bucket names.
    computer_id: String,
}

impl From<core::Configuration> for Configuration {
    fn from(conf: core::Configuration) -> Self {
        Self {
            hostname: conf.hostname,
            username: conf.username,
            computer_id: conf.computer_id,
        }
    }
}

#[derive(GraphQLObject)]
/// The directory structure which will be saved.
struct Dataset {
    /// Opaque identifier for this dataset (w/o db prefix).
    key: String,
    /// Unique computer identifier.
    computer_id: String,
    /// Path that is being backed up.
    basepath: String,
    /// Cron-like expression for the backup schedule.
    schedule: Option<String>,
    /// Checksum for the latest snapshot, if any. However, this is not exposed
    /// to the client, only used to retrieve the full snapshot data as needed.
    #[graphql(skip)]
    latest_snapsum: Option<Checksum>,
    /// Reference to most recent snapshot.
    latest_snapshot: Option<Snapshot>,
    /// Path to temporary workspace for backup process.
    // workspace: String,
    /// Specified byte length of pack files.
    pack_size: BigInt,
    /// Identifiers of stores used for saving packs.
    stores: Vec<String>,
}

impl Dataset {
    /// Update the fields of this dataset with the values from the input.
    fn copy_input(mut self, set: InputDataset) -> Self {
        self.basepath = set.basepath;
        self.schedule = set.schedule;
        // self.workspace = set.workspace;
        self.pack_size = set.pack_size;
        self.stores = set.stores;
        self
    }

    /// If the snapshot can be found in the database, convert to the GraphQL
    /// representation, otherwise leave the value as `None`.
    fn fill_snapshot(mut self, dbase: &Database) -> Self {
        if let Some(value) = self.latest_snapsum.as_ref() {
            if let Ok(digest) = core::Checksum::from_str(&value.0) {
                if let Ok(Some(snapshot)) = dbase.get_snapshot(&digest) {
                    self.latest_snapshot = Some(Snapshot::from(snapshot));
                }
            }
        }
        self
    }
}

impl Into<core::Dataset> for Dataset {
    fn into(self) -> core::Dataset {
        let store = self.stores[0].clone();
        let mut set = core::Dataset::new(&self.computer_id, Path::new(&self.basepath), &store);
        set.key = self.key;
        set.schedule = self.schedule;
        set.latest_snapshot = self.latest_snapsum.map(|s| s.into());
        // set.workspace = PathBuf::from(&self.workspace);
        let new_pack_size = self.pack_size.0 as u64;
        if new_pack_size > 0 {
            set.pack_size = new_pack_size;
        }
        for stor in self.stores.iter().skip(1) {
            set = set.add_store(&stor);
        }
        set
    }
}

impl From<core::Dataset> for Dataset {
    fn from(set: core::Dataset) -> Self {
        let basepath = set
            .basepath
            .to_str()
            .map(|v| v.to_owned())
            .unwrap_or_else(|| set.basepath.to_string_lossy().into_owned());
        let snapshot = set.latest_snapshot.map(Checksum::from);
        Self {
            key: set.key,
            computer_id: set.computer_id,
            basepath,
            schedule: set.schedule,
            latest_snapsum: snapshot,
            latest_snapshot: None,
            // workspace: set.workspace.to_str().unwrap().to_owned(),
            pack_size: BigInt(set.pack_size as i64),
            stores: set.stores,
        }
    }
}

#[derive(GraphQLInputObject)]
pub struct InputDataset {
    /// Identifier of dataset to update, None if creating.
    pub key: Option<String>,
    /// Path that is being backed up.
    pub basepath: String,
    /// Cron-like expression for the backup schedule.
    pub schedule: Option<String>,
    /// Path to temporary workspace for backup process.
    // pub workspace: String,
    /// Desired byte length of pack files.
    pub pack_size: BigInt,
    /// Identifiers of stores used for saving packs.
    pub stores: Vec<String>,
}

impl InputDataset {
    /// Perform basic validation on the input dataset.
    fn validate(&self, database: &Database) -> FieldResult<()> {
        if self.stores.is_empty() {
            return Err(FieldError::new(
                "Require at least one store in dataset",
                Value::null(),
            ));
        }
        // verify the stores exist in the database
        for store in self.stores.iter() {
            // cannot use store::load_store() since it always succeeds
            let opt = database.get_document(store.as_bytes())?;
            if opt.is_none() {
                return Err(FieldError::new(
                    format!("Named store does not exist: {}", &store),
                    Value::null(),
                ));
            }
        }
        // ensure the basepath actually exists
        let bpath = Path::new(&self.basepath);
        if !bpath.exists() {
            return Err(FieldError::new(
                format!("Base path does not exist: {}", &self.basepath),
                Value::null(),
            ));
        }
        // ensure the schedule, if any, can be parsed successfully
        if let Some(schedule) = self.schedule.as_ref() {
            let result = Schedule::from_str(schedule);
            if result.is_err() {
                return Err(FieldError::new(
                    format!("schedule expression could not be parsed: {}", schedule),
                    Value::null(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(GraphQLObject)]
/// Local or remote store for pack files.
struct Store {
    /// Opaque identifier of this store.
    key: String,
    /// User-defined label for this store configuration.
    label: String,
    /// The type, or kind, of the store (e.g. "local", "minio", "glacier").
    kind: String,
    /// Base64 encoded JSON of store options.
    options: String,
}

impl From<Box<dyn store::Store>> for Store {
    fn from(store: Box<dyn store::Store>) -> Self {
        let type_name = store.get_type().to_string();
        let json: String = store.get_config().to_json().unwrap();
        let label: String = store.get_config().get_label();
        let encoded = base64::encode(&json);
        let key = store::store_name(store.as_ref());
        Self {
            key,
            label,
            kind: type_name,
            options: encoded,
        }
    }
}

pub struct QueryRoot;

graphql_object!(QueryRoot: Database |&self| {
    #[doc = "Retrieve the configuration record."]
    field configuration(&executor) -> FieldResult<Configuration> {
        let database = executor.context();
        let conf = engine::get_configuration(&database)?;
        Ok(Configuration::from(conf))
    }

    #[doc = "Find all dataset configurations."]
    field datasets(&executor) -> FieldResult<Vec<Dataset>> {
        let database = executor.context();
        let datasets = database.get_all_datasets()?;
        let mut results: Vec<Dataset> = Vec::new();
        for set in datasets {
            let mut ds = Dataset::from(set);
            ds = ds.fill_snapshot(database);
            results.push(ds);
        }
        Ok(results)
    }

    #[doc = "Retrieve a specific dataset configuration."]
    field dataset(&executor, key: String) -> FieldResult<Option<Dataset>> {
        let database = executor.context();
        let opt = database.get_dataset(&key)?;
        if let Some(set) = opt {
            let mut ds = Dataset::from(set);
            ds = ds.fill_snapshot(database);
            Ok(Some(ds))
        } else {
            Ok(None)
        }
    }

    #[doc = "Find all named store configurations."]
    field stores(&executor) -> FieldResult<Vec<Store>> {
        let database = executor.context();
        let store_names = store::find_stores(database)?;
        let stores = store::load_stores(database, store_names.as_slice())?;
        let mut results: Vec<Store> = Vec::new();
        for stor in stores {
            results.push(Store::from(stor))
        }
        Ok(results)
    }

    #[doc = "Retrieve the named store configuration."]
    field store(&executor, key: String) -> FieldResult<Store> {
        let database = executor.context();
        let stor = store::load_store(database, &key)?;
        Ok(Store::from(stor))
    }

    #[doc = "Retrieve a specific snapshot."]
    field snapshot(&executor, digest: Checksum) -> FieldResult<Option<Snapshot>> {
        let database = executor.context();
        let snapsum = digest.into();
        let opt = database.get_snapshot(&snapsum)?;
        if let Some(snap) = opt {
            Ok(Some(Snapshot::from(snap)))
        } else {
            Ok(None)
        }
    }

    #[doc = "Retrieve a specific tree."]
    field tree(&executor, digest: Checksum) -> FieldResult<Option<Tree>> {
        let database = executor.context();
        let treesum = digest.into();
        let opt = database.get_tree(&treesum)?;
        if let Some(tree) = opt {
            Ok(Some(Tree::from(tree)))
        } else {
            Ok(None)
        }
    }
});

pub struct MutationRoot;

graphql_object!(MutationRoot: Database | &self | {
    #[doc = "Define a new store with the given configuration."]
    field defineStore(&executor, type_name: String, options: String) -> FieldResult<Store> {
        let database = executor.context();
        let decoded = base64::decode(&options)?;
        let json = std::str::from_utf8(&decoded)?;
        let store_type = store::StoreType::from_str(&type_name)?;
        let mut stor = store::build_store(store_type, None);
        stor.get_config_mut().from_json(&json)?;
        store::save_store(&database, stor.as_ref())?;
        let key = store::store_name(stor.as_ref());
        Ok(Store::from(stor))
    }

    #[doc = "Update the saved store configuration."]
    field updateStore(&executor, key: String, options: String) -> FieldResult<Store> {
        let database = executor.context();
        let decoded = base64::decode(&options)?;
        let json = std::str::from_utf8(&decoded)?;
        let mut stor = store::load_store(database, &key)?;
        stor.get_config_mut().from_json(&json)?;
        store::save_store(&database, stor.as_ref())?;
        Ok(Store::from(stor))
    }

    #[doc = "Delete the named store, returning its current configuration."]
    field deleteStore(&executor, key: String) -> FieldResult<Store> {
        let database = executor.context();
        let stor = store::load_store(database, &key)?;
        store::delete_store(&database, &key)?;
        Ok(Store::from(stor))
    }

    #[doc = "Define a new dataset with the given configuration."]
    field defineDataset(&executor, dataset: InputDataset) -> FieldResult<Dataset> {
        let database = executor.context();
        dataset.validate(&database)?;
        let config = engine::get_configuration(&database)?;
        let computer_id = config.computer_id;
        let set = core::Dataset::new(&computer_id, Path::new(&dataset.basepath), &dataset.stores[0]);
        let mut ds = Dataset::from(set);
        ds = ds.copy_input(dataset);
        let set: core::Dataset = ds.into();
        database.put_dataset(&set)?;
        Ok(Dataset::from(set))
    }

    #[doc = "Update an existing dataset with the given configuration."]
    field updateDataset(&executor, dataset: InputDataset) -> FieldResult<Dataset> {
        match dataset.key {
            None => Err(FieldError::new(
                "Dataset must specify a key",
                Value::null()
            )),
            Some(ref set_key) => {
                let database = executor.context();
                dataset.validate(&database)?;
                match database.get_dataset(set_key)? {
                    None => Err(FieldError::new(
                        format!("Dataset does not exist: {}", set_key),
                        Value::null()
                    )),
                    Some(dset) => {
                        let mut ds = Dataset::from(dset);
                        ds = ds.copy_input(dataset);
                        let set: core::Dataset = ds.into();
                        database.put_dataset(&set)?;
                        Ok(Dataset::from(set))
                    }
                }
            }
        }
    }

    #[doc = "Delete the named dataset, returning its current configuration."]
    field deleteDataset(&executor, key: String) -> FieldResult<Dataset> {
        let database = executor.context();
        let opt = database.get_dataset(&key)?;
        if let Some(set) = opt {
            database.delete_dataset(&key)?;
            Ok(Dataset::from(set))
        } else {
            Err(FieldError::new(
                format!("Dataset does not exist: {}", &key),
                Value::null()
            ))
        }
    }
});

pub type Schema = RootNode<'static, QueryRoot, MutationRoot>;

/// Create the GraphQL schema.
pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {})
}
