//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `schema` module defines the GraphQL schema and resolvers.

use super::core::{Checksum, Configuration, Dataset, Snapshot, Tree, TreeReference};
use super::database::Database;
use super::engine;
use super::store;
use chrono::prelude::*;
use cron::Schedule;
use juniper::{
    graphql_scalar, FieldError, FieldResult, GraphQLInputObject, GraphQLObject, ParseScalarResult,
    ParseScalarValue, RootNode, Value,
};
use std::path::{Path, PathBuf};
use std::str::FromStr;

// Define a larger integer type so we can represent those larger values, such as
// file sizes. Some of the core types define fields that are larger than i32, so
// this type is used to represent those values in GraphQL.
#[derive(Copy, Clone)]
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

impl Into<u64> for BigInt {
    fn into(self) -> u64 {
        self.0 as u64
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

// Using the graphql_scalar macro here because it is tedious to implement all of
// the juniper interfaces. However, the macro requires having a `from_str` where
// our type already has that method, so using `from_str` is just a little more
// complicated than it would be normally.
//
// need `where Scalar = <S>` parameterization to use this with objects c.f.
// https://github.com/graphql-rust/juniper/issues/358 for details
graphql_scalar!(Checksum where Scalar = <S> {
    description: "A SHA1 or SHA256 checksum, with algorithm prefix."

    resolve(&self) -> Value {
        let value = format!("{}", self);
        Value::scalar(value)
    }

    from_input_value(v: &InputValue) -> Option<Checksum> {
        v.as_scalar_value::<String>().filter(|s| {
            // make sure the input value actually looks like a digest
            s.starts_with("sha1-") || s.starts_with("sha256-")
        }).map(|s| FromStr::from_str(s).unwrap())
    }

    from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
});

// Using the graphql_scalar macro here because it is tedious to implement all of
// the juniper interfaces. However, the macro requires having a `from_str` where
// our type already has that method, so using `from_str` is just a little more
// complicated than it would be normally.
//
// need `where Scalar = <S>` parameterization to use this with objects c.f.
// https://github.com/graphql-rust/juniper/issues/358 for details
graphql_scalar!(TreeReference where Scalar = <S> {
    description: "Reference for a tree entry, such as a file or tree."

    resolve(&self) -> Value {
        let value = format!("{}", self);
        Value::scalar(value)
    }

    from_input_value(v: &InputValue) -> Option<TreeReference> {
        v.as_scalar_value::<String>().filter(|s| {
            // make sure the input value actually looks like a digest
            s.starts_with("sha1-") || s.starts_with("sha256-")
        }).map(|s| FromStr::from_str(s).unwrap())
    }

    from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
});

#[juniper::object(description = "A single backup, either in progress or completed.")]
impl Snapshot {
    /// Computed checksum of the snapshot.
    fn checksum(&self) -> Checksum {
        self.checksum()
    }

    /// The snapshot before this one, if any.
    fn parent(&self) -> Option<Checksum> {
        self.parent.clone()
    }

    /// Time when the snapshot was first created.
    fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// Time when the snapshot completely finished.
    fn end_time(&self) -> Option<DateTime<Utc>> {
        self.end_time
    }

    /// Total number of files contained in this snapshot.
    fn file_count(&self) -> BigInt {
        BigInt(self.file_count as i64)
    }

    /// Reference to the tree containing all of the files.
    fn tree(&self) -> Checksum {
        self.tree.clone()
    }
}

#[juniper::object(
    Context = Database,
    description = "Location, schedule, and pack store for a backup data set.")
]
impl Dataset {
    /// Identifier for this dataset.
    fn key(&self) -> String {
        self.key.clone()
    }

    /// Unique computer identifier.
    fn computer_id(&self) -> String {
        self.computer_id.clone()
    }

    /// Path that is being backed up.
    fn basepath(&self) -> String {
        self.basepath
            .to_str()
            .map(|v| v.to_owned())
            .unwrap_or_else(|| self.basepath.to_string_lossy().into_owned())
    }

    /// Cron-like expression for the backup schedule.
    fn schedule(&self) -> Option<String> {
        self.schedule.clone()
    }

    /// Most recent snapshot for this dataset, if any.
    fn latest_snapshot(&self, executor: &Executor) -> Option<Snapshot> {
        if let Some(digest) = self.latest_snapshot.as_ref() {
            let dbase = executor.context();
            if let Ok(result) = dbase.get_snapshot(&digest) {
                return result;
            }
        }
        None
    }

    /// Preferred byte length of pack files.
    fn pack_size(&self) -> BigInt {
        BigInt(self.pack_size as i64)
    }

    /// Identifiers of stores used for saving packs.
    fn stores(&self) -> Vec<String> {
        self.stores.clone()
    }
}

#[derive(GraphQLInputObject)]
pub struct InputDataset {
    /// Identifier of dataset to update, null if creating.
    pub key: Option<String>,
    /// Path that is being backed up.
    pub basepath: String,
    /// Cron-like expression for the backup schedule.
    pub schedule: Option<String>,
    // Path to temporary workspace for backup process.
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

    /// Update the fields of the dataset with the values from this input.
    fn copy_input(&self, dataset: &mut Dataset) {
        dataset.basepath = PathBuf::from(self.basepath.clone());
        dataset.schedule = self.schedule.clone();
        // dataset.workspace = self.workspace;
        dataset.pack_size = self.pack_size.clone().into();
        dataset.stores = self.stores.clone();
    }
}

#[derive(GraphQLObject)]
/// Local or remote store for pack files.
struct Store {
    /// Identifier of this store.
    key: String,
    /// User-defined label for this store configuration.
    label: String,
    /// The type, or kind, of the store (e.g. "local", "minio", "sftp").
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

#[juniper::object(Context = Database)]
impl QueryRoot {
    /// Retrieve the configuration record.
    fn configuration(executor: &Executor) -> FieldResult<Configuration> {
        let database = executor.context();
        Ok(engine::get_configuration(&database)?)
    }

    /// Find all dataset configurations.
    fn datasets(executor: &Executor) -> FieldResult<Vec<Dataset>> {
        let database = executor.context();
        Ok(database.get_all_datasets()?)
    }

    /// Retrieve a specific dataset configuration.
    fn dataset(executor: &Executor, key: String) -> FieldResult<Option<Dataset>> {
        let database = executor.context();
        Ok(database.get_dataset(&key)?)
    }

    /// Find all named store configurations.
    fn stores(executor: &Executor) -> FieldResult<Vec<Store>> {
        let database = executor.context();
        let store_names = store::find_stores(database)?;
        let stores = store::load_stores(database, store_names.as_slice())?;
        let mut results: Vec<Store> = Vec::new();
        for stor in stores {
            results.push(Store::from(stor))
        }
        Ok(results)
    }

    /// Retrieve the named store configuration.
    fn store(executor: &Executor, key: String) -> FieldResult<Store> {
        let database = executor.context();
        let stor = store::load_store(database, &key)?;
        Ok(Store::from(stor))
    }

    /// Retrieve a specific snapshot.
    fn snapshot(executor: &Executor, digest: Checksum) -> FieldResult<Option<Snapshot>> {
        let database = executor.context();
        Ok(database.get_snapshot(&digest)?)
    }

    /// Retrieve a specific tree.
    fn tree(executor: &Executor, digest: Checksum) -> FieldResult<Option<Tree>> {
        let database = executor.context();
        Ok(database.get_tree(&digest)?)
    }
}

pub struct MutationRoot;

#[juniper::object(Context = Database)]
impl MutationRoot {
    /// Define a new store with the given configuration.
    fn defineStore(executor: &Executor, type_name: String, options: String) -> FieldResult<Store> {
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

    /// Update the saved store configuration.
    fn updateStore(executor: &Executor, key: String, options: String) -> FieldResult<Store> {
        let database = executor.context();
        let decoded = base64::decode(&options)?;
        let json = std::str::from_utf8(&decoded)?;
        let mut stor = store::load_store(database, &key)?;
        stor.get_config_mut().from_json(&json)?;
        store::save_store(&database, stor.as_ref())?;
        Ok(Store::from(stor))
    }

    /// Delete the named store, returning its current configuration.
    fn deleteStore(executor: &Executor, key: String) -> FieldResult<Store> {
        let database = executor.context();
        let stor = store::load_store(database, &key)?;
        store::delete_store(&database, &key)?;
        Ok(Store::from(stor))
    }

    /// Define a new dataset with the given configuration.
    fn defineDataset(executor: &Executor, dataset: InputDataset) -> FieldResult<Dataset> {
        let database = executor.context();
        dataset.validate(&database)?;
        let config = engine::get_configuration(&database)?;
        let computer_id = config.computer_id;
        let mut updated = Dataset::new(
            &computer_id,
            Path::new(&dataset.basepath),
            &dataset.stores[0],
        );
        dataset.copy_input(&mut updated);
        database.put_dataset(&updated)?;
        Ok(updated)
    }

    /// Update an existing dataset with the given configuration.
    fn updateDataset(executor: &Executor, dataset: InputDataset) -> FieldResult<Dataset> {
        match dataset.key {
            None => Err(FieldError::new("Dataset must specify a key", Value::null())),
            Some(ref set_key) => {
                let database = executor.context();
                dataset.validate(&database)?;
                match database.get_dataset(set_key)? {
                    None => Err(FieldError::new(
                        format!("Dataset does not exist: {}", set_key),
                        Value::null(),
                    )),
                    Some(mut updated) => {
                        dataset.copy_input(&mut updated);
                        database.put_dataset(&updated)?;
                        Ok(updated)
                    }
                }
            }
        }
    }

    /// Delete the named dataset, returning its current configuration.
    fn deleteDataset(executor: &Executor, key: String) -> FieldResult<Dataset> {
        let database = executor.context();
        let opt = database.get_dataset(&key)?;
        if let Some(set) = opt {
            database.delete_dataset(&key)?;
            Ok(set)
        } else {
            Err(FieldError::new(
                format!("Dataset does not exist: {}", &key),
                Value::null(),
            ))
        }
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot>;

/// Create the GraphQL schema.
pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {})
}
