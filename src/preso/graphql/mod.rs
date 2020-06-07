//
// Copyright (c) 2020 Nathan Fiedler
//

//! The `schema` module defines the GraphQL schema and resolvers.

use crate::data::repositories::RecordRepositoryImpl;
use crate::data::sources::EntityDataSource;
use crate::domain::entities;
use juniper::{
    graphql_scalar, FieldResult, GraphQLInputObject, GraphQLObject, ParseScalarResult,
    ParseScalarValue, RootNode, Value,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

// Context for the GraphQL schema.
pub struct GraphContext {
    datasource: Arc<dyn EntityDataSource>,
}

impl GraphContext {
    pub fn new(datasource: Arc<dyn EntityDataSource>) -> Self {
        Self { datasource }
    }
}

// Mark the data source as a valid context type for Juniper.
impl juniper::Context for GraphContext {}

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
graphql_scalar!(entities::Checksum where Scalar = <S> {
    description: "A SHA1 or SHA256 checksum, with algorithm prefix."

    resolve(&self) -> Value {
        let value = format!("{}", self);
        Value::scalar(value)
    }

    from_input_value(v: &InputValue) -> Option<entities::Checksum> {
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
// graphql_scalar!(TreeReference where Scalar = <S> {
//     description: "Reference for a tree entry, such as a file or tree."

//     resolve(&self) -> Value {
//         let value = format!("{}", self);
//         Value::scalar(value)
//     }

//     from_input_value(v: &InputValue) -> Option<TreeReference> {
//         v.as_scalar_value::<String>().filter(|s| {
//             // make sure the input value actually looks like a digest
//             s.starts_with("sha1-") || s.starts_with("sha256-")
//         }).map(|s| FromStr::from_str(s).unwrap())
//     }

//     from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
//         <String as ParseScalarValue<S>>::from_str(value)
//     }
// });

// #[juniper::object(description = "A single backup, either in progress or completed.")]
// impl Snapshot {
//     /// Original computed checksum of the snapshot.
//     fn checksum(&self) -> Checksum {
//         self.digest.clone()
//     }

//     /// The snapshot before this one, if any.
//     fn parent(&self) -> Option<Checksum> {
//         self.parent.clone()
//     }

//     /// Time when the snapshot was first created.
//     fn start_time(&self) -> DateTime<Utc> {
//         self.start_time
//     }

//     /// Time when the snapshot completely finished.
//     fn end_time(&self) -> Option<DateTime<Utc>> {
//         self.end_time
//     }

//     /// Total number of files contained in this snapshot.
//     fn file_count(&self) -> BigInt {
//         BigInt(self.file_count as i64)
//     }

//     /// Reference to the tree containing all of the files.
//     fn tree(&self) -> Checksum {
//         self.tree.clone()
//     }
// }

// #[juniper::object(
//     Context = Database,
//     description = "Location, schedule, and pack store for a backup data set.")
// ]
// impl Dataset {
//     /// Identifier for this dataset.
//     fn key(&self) -> String {
//         self.key.clone()
//     }

//     /// Unique computer identifier.
//     fn computer_id(&self) -> String {
//         self.computer_id.clone()
//     }

//     /// Path that is being backed up.
//     fn basepath(&self) -> String {
//         self.basepath
//             .to_str()
//             .map(|v| v.to_owned())
//             .unwrap_or_else(|| self.basepath.to_string_lossy().into_owned())
//     }

//     /// Set of schedules that apply to this dataset.
//     fn schedules(&self) -> Vec<schedule::Schedule> {
//         self.schedules.clone()
//     }

//     /// Most recent snapshot for this dataset, if any.
//     fn latest_snapshot(&self, executor: &Executor) -> Option<Snapshot> {
//         if let Some(digest) = self.latest_snapshot.as_ref() {
//             let dbase = executor.context();
//             if let Ok(result) = dbase.get_snapshot(&digest) {
//                 return result;
//             }
//         }
//         None
//     }

//     /// Preferred byte length of pack files.
//     fn pack_size(&self) -> BigInt {
//         BigInt(self.pack_size as i64)
//     }

//     /// Identifiers of stores used for saving packs.
//     fn stores(&self) -> Vec<String> {
//         self.stores.clone()
//     }
// }

// #[derive(GraphQLInputObject)]
// pub struct InputDataset {
//     /// Identifier of dataset to update, null if creating.
//     pub key: Option<String>,
//     /// Path that is being backed up.
//     pub basepath: String,
//     /// List of schedules to apply to this dataset.
//     pub schedules: Vec<InputSchedule>,
//     // Path to temporary workspace for backup process.
//     // pub workspace: String,
//     /// Desired byte length of pack files.
//     pub pack_size: BigInt,
//     /// Identifiers of stores used for saving packs.
//     pub stores: Vec<String>,
// }

// impl InputDataset {
//     /// Perform basic validation on the input dataset.
//     fn validate(&self, database: &Database) -> FieldResult<()> {
//         if self.stores.is_empty() {
//             return Err(FieldError::new(
//                 "Require at least one store in dataset",
//                 Value::null(),
//             ));
//         }
//         // verify the stores exist in the database
//         for store in self.stores.iter() {
//             // cannot use store::load_store() since it always succeeds
//             let opt = database.get_document(store.as_bytes())?;
//             if opt.is_none() {
//                 return Err(FieldError::new(
//                     format!("Named store does not exist: {}", &store),
//                     Value::null(),
//                 ));
//             }
//         }
//         // ensure the basepath actually exists
//         let bpath = Path::new(&self.basepath);
//         if !bpath.exists() {
//             return Err(FieldError::new(
//                 format!("Base path does not exist: {}", &self.basepath),
//                 Value::null(),
//             ));
//         }
//         // ensure the schedules, if any, make sense
//         for schedule in self.schedules.iter() {
//             schedule.validate()?;
//         }
//         Ok(())
//     }

//     /// Update the fields of the dataset with the values from this input.
//     fn copy_input(self, dataset: &mut Dataset) {
//         dataset.basepath = PathBuf::from(self.basepath.clone());
//         dataset.schedules = self.schedules.into_iter().map(|e| e.into()).collect();
//         // dataset.workspace = self.workspace;
//         dataset.pack_size = self.pack_size.clone().into();
//         dataset.stores = self.stores.clone();
//     }
// }

// #[juniper::object(description = "Range of time in which to run backup.")]
// impl TimeRange {
//     /// Seconds from midnight at which to start.
//     fn start_time(&self) -> i32 {
//         self.start as i32
//     }
//     /// Seconds from midnight at which to stop.
//     fn stop_time(&self) -> i32 {
//         self.stop as i32
//     }
// }

// #[derive(GraphQLInputObject)]
// pub struct InputTimeRange {
//     /// Seconds from midnight at which to start.
//     pub start_time: i32,
//     /// Seconds from midnight at which to stop.
//     pub stop_time: i32,
// }

// impl InputTimeRange {
//     /// Perform basic validation on the input time range.
//     fn validate(&self) -> FieldResult<()> {
//         if self.start_time < 0 || self.start_time > 86_400 {
//             return Err(FieldError::new(
//                 "Start time must be between 0 and 86,400",
//                 Value::null(),
//             ));
//         }
//         if self.stop_time < 0 || self.stop_time > 86_400 {
//             return Err(FieldError::new(
//                 "Stop time must be between 0 and 86,400",
//                 Value::null(),
//             ));
//         }
//         Ok(())
//     }
// }

// impl Into<TimeRange> for InputTimeRange {
//     fn into(self) -> TimeRange {
//         TimeRange::new_secs(self.start_time as u32, self.stop_time as u32)
//     }
// }

// /// In combination with DayOfWeek, selects the particular week.
// #[derive(Copy, Clone, GraphQLEnum)]
// pub enum WeekOfMonth {
//     /// First such weekday of the month.
//     First,
//     /// Second such weekday of the month.
//     Second,
//     /// Third such weekday of the month.
//     Third,
//     /// Fourth such weekday of the month.
//     Fourth,
//     /// Fifth such weekday of the month.
//     Fifth,
// }

// impl WeekOfMonth {
//     fn into_dow(self, dow: DayOfWeek) -> schedule::DayOfMonth {
//         match self {
//             WeekOfMonth::First => schedule::DayOfMonth::First(dow),
//             WeekOfMonth::Second => schedule::DayOfMonth::Second(dow),
//             WeekOfMonth::Third => schedule::DayOfMonth::Third(dow),
//             WeekOfMonth::Fourth => schedule::DayOfMonth::Fourth(dow),
//             WeekOfMonth::Fifth => schedule::DayOfMonth::Fifth(dow),
//         }
//     }
// }

// /// How often should the backup run for the dataset.
// #[derive(Copy, Clone, GraphQLEnum)]
// pub enum Frequency {
//     /// Run every hour.
//     Hourly,
//     /// Run every day, with optional time range.
//     Daily,
//     /// Run every week, with optional day-of-week and time range.
//     Weekly,
//     /// Run every month, with optional day-of-month and time range.
//     Monthly,
// }

// #[juniper::object(description = "A schedule for when to run the backup.")]
// impl schedule::Schedule {
//     /// How often the backup will be run. Combines with other elements to
//     /// control exactly when the backup is performed.
//     fn frequency(&self) -> Frequency {
//         match self {
//             schedule::Schedule::Hourly => Frequency::Hourly,
//             schedule::Schedule::Daily(_) => Frequency::Daily,
//             schedule::Schedule::Weekly(_) => Frequency::Weekly,
//             schedule::Schedule::Monthly(_) => Frequency::Monthly,
//         }
//     }

//     /// Time within the day when the backup will be run. The start time will
//     /// come before the stop time if the range spans midnight.
//     fn time_range(&self) -> Option<TimeRange> {
//         match self {
//             schedule::Schedule::Hourly => None,
//             schedule::Schedule::Daily(None) => None,
//             schedule::Schedule::Daily(Some(v)) => Some(v.clone()),
//             schedule::Schedule::Weekly(None) => None,
//             schedule::Schedule::Weekly(Some((_, None))) => None,
//             schedule::Schedule::Weekly(Some((_, Some(v)))) => Some(v.clone()),
//             schedule::Schedule::Monthly(None) => None,
//             schedule::Schedule::Monthly(Some((_, None))) => None,
//             schedule::Schedule::Monthly(Some((_, Some(v)))) => Some(v.clone()),
//         }
//     }

//     /// Which week, in combination with the day of the week, to run the backup.
//     fn week_of_month(&self) -> Option<WeekOfMonth> {
//         match self {
//             schedule::Schedule::Hourly => None,
//             schedule::Schedule::Daily(_) => None,
//             schedule::Schedule::Weekly(_) => None,
//             schedule::Schedule::Monthly(None) => None,
//             schedule::Schedule::Monthly(Some((v, _))) => match v {
//                 schedule::DayOfMonth::First(_) => Some(WeekOfMonth::First),
//                 schedule::DayOfMonth::Second(_) => Some(WeekOfMonth::Second),
//                 schedule::DayOfMonth::Third(_) => Some(WeekOfMonth::Third),
//                 schedule::DayOfMonth::Fourth(_) => Some(WeekOfMonth::Fourth),
//                 schedule::DayOfMonth::Fifth(_) => Some(WeekOfMonth::Fifth),
//                 schedule::DayOfMonth::Day(_) => None,
//             },
//         }
//     }

//     /// Day of the week on which to run the backup, for schedules with a weekly
//     /// or monthly frequency.
//     fn day_of_week(&self) -> Option<DayOfWeek> {
//         match self {
//             schedule::Schedule::Hourly => None,
//             schedule::Schedule::Daily(_) => None,
//             schedule::Schedule::Weekly(None) => None,
//             schedule::Schedule::Weekly(Some((v, _))) => Some(*v),
//             schedule::Schedule::Monthly(None) => None,
//             schedule::Schedule::Monthly(Some((v, _))) => match v {
//                 schedule::DayOfMonth::First(v) => Some(*v),
//                 schedule::DayOfMonth::Second(v) => Some(*v),
//                 schedule::DayOfMonth::Third(v) => Some(*v),
//                 schedule::DayOfMonth::Fourth(v) => Some(*v),
//                 schedule::DayOfMonth::Fifth(v) => Some(*v),
//                 schedule::DayOfMonth::Day(_) => None,
//             },
//         }
//     }

//     /// Day of the month, instead of a week and weekday, to run the backup, for
//     /// schedules with a monthly frequency.
//     fn day_of_month(&self) -> Option<i32> {
//         match self {
//             schedule::Schedule::Hourly => None,
//             schedule::Schedule::Daily(_) => None,
//             schedule::Schedule::Weekly(_) => None,
//             schedule::Schedule::Monthly(None) => None,
//             schedule::Schedule::Monthly(Some((v, _))) => match v {
//                 schedule::DayOfMonth::First(_) => None,
//                 schedule::DayOfMonth::Second(_) => None,
//                 schedule::DayOfMonth::Third(_) => None,
//                 schedule::DayOfMonth::Fourth(_) => None,
//                 schedule::DayOfMonth::Fifth(_) => None,
//                 schedule::DayOfMonth::Day(v) => Some(*v as i32),
//             },
//         }
//     }
// }

// /// New schedule for the dataset. Combine elements to get backups to run on a
// /// certain day of the week, month, and/or within a given time range.
// #[derive(GraphQLInputObject)]
// pub struct InputSchedule {
//     /// How often to run the backup.
//     pub frequency: Frequency,
//     /// Range of time during the day in which to run backup.
//     pub time_range: Option<InputTimeRange>,
//     /// Which week within the month to run the backup.
//     pub week_of_month: Option<WeekOfMonth>,
//     /// Which day of the week to run the backup.
//     pub day_of_week: Option<DayOfWeek>,
//     /// The day of the month to run the backup.
//     pub day_of_month: Option<i32>,
// }

// impl InputSchedule {
//     /// Construct a "hourly" schedule, for testing purposes.
//     pub fn hourly() -> Self {
//         Self {
//             frequency: Frequency::Hourly,
//             time_range: None,
//             week_of_month: None,
//             day_of_week: None,
//             day_of_month: None,
//         }
//     }

//     /// Construct a "daily" schedule, for testing purposes.
//     pub fn daily() -> Self {
//         Self {
//             frequency: Frequency::Daily,
//             time_range: None,
//             week_of_month: None,
//             day_of_week: None,
//             day_of_month: None,
//         }
//     }

//     fn validate(&self) -> FieldResult<()> {
//         match &self.frequency {
//             Frequency::Hourly => {
//                 if self.week_of_month.is_some()
//                     || self.day_of_week.is_some()
//                     || self.day_of_month.is_some()
//                     || self.time_range.is_some()
//                 {
//                     return Err(FieldError::new(
//                         "Hourly cannot take any range or days",
//                         Value::null(),
//                     ));
//                 }
//             }
//             Frequency::Daily => {
//                 if self.week_of_month.is_some()
//                     || self.day_of_week.is_some()
//                     || self.day_of_month.is_some()
//                 {
//                     return Err(FieldError::new(
//                         "Daily can only take a time_range",
//                         Value::null(),
//                     ));
//                 }
//                 if let Some(ref range) = self.time_range {
//                     range.validate()?
//                 }
//             }
//             Frequency::Weekly => {
//                 if self.week_of_month.is_some() || self.day_of_month.is_some() {
//                     return Err(FieldError::new(
//                         "Weekly can only take a time_range and day_of_week",
//                         Value::null(),
//                     ));
//                 }
//                 if let Some(ref range) = self.time_range {
//                     range.validate()?
//                 }
//             }
//             Frequency::Monthly => {
//                 if self.day_of_month.is_some() && self.day_of_week.is_some() {
//                     return Err(FieldError::new(
//                         "Monthly can only take day_of_month *or* day_of_week and week_of_month",
//                         Value::null(),
//                     ));
//                 }
//                 if self.day_of_week.is_some() && self.week_of_month.is_none() {
//                     return Err(FieldError::new(
//                         "Monthly requires week_of_month when using day_of_week",
//                         Value::null(),
//                     ));
//                 }
//                 if let Some(ref range) = self.time_range {
//                     range.validate()?
//                 }
//             }
//         }
//         Ok(())
//     }
// }

// impl Into<schedule::Schedule> for InputSchedule {
//     fn into(self) -> schedule::Schedule {
//         match &self.frequency {
//             Frequency::Hourly => schedule::Schedule::Hourly,
//             Frequency::Daily => schedule::Schedule::Daily(self.time_range.map(|s| s.into())),
//             Frequency::Weekly => {
//                 let dow = if let Some(dow) = self.day_of_week {
//                     Some((dow, self.time_range.map(|s| s.into())))
//                 } else {
//                     None
//                 };
//                 schedule::Schedule::Weekly(dow)
//             }
//             Frequency::Monthly => {
//                 let dom: Option<(schedule::DayOfMonth, Option<TimeRange>)> =
//                     if let Some(day) = self.day_of_month {
//                         Some((
//                             schedule::DayOfMonth::from(day as u32),
//                             self.time_range.map(|s| s.into()),
//                         ))
//                     } else if let Some(wn) = self.week_of_month {
//                         let dow = self.day_of_week.unwrap();
//                         let dom = wn.into_dow(dow);
//                         Some((dom, self.time_range.map(|s| s.into())))
//                     } else {
//                         None
//                     };
//                 schedule::Schedule::Monthly(dom)
//             }
//         }
//     }
// }

/// Property defines a name/value pair.
#[derive(GraphQLObject)]
struct Property {
    name: String,
    value: String,
}

/// Store defines a location where packs will be saved.
#[derive(GraphQLObject)]
struct Store {
    /// Unique identifier for this store.
    id: String,
    /// Name of the type of this store (e.g. "local").
    store_type: String,
    /// User-defined label for this store.
    label: String,
    /// Name/value pairs that make up this store configuration.
    properties: Vec<Property>,
}

impl From<entities::Store> for Store {
    fn from(store: entities::Store) -> Self {
        let mut properties: Vec<Property> = Vec::new();
        for (key, val) in store.properties.iter() {
            properties.push(Property {
                name: key.to_owned(),
                value: val.to_owned(),
            });
        }
        Self {
            id: store.id,
            store_type: store.store_type.to_string(),
            label: store.label,
            properties,
        }
    }
}

pub struct QueryRoot;

#[juniper::object(Context = GraphContext)]
impl QueryRoot {
    // /// Retrieve the configuration record.
    // fn configuration(executor: &Executor) -> FieldResult<Configuration> {
    //     let database = executor.context();
    //     Ok(engine::get_configuration(&database)?)
    // }

    // /// Find all dataset configurations.
    // fn datasets(executor: &Executor) -> FieldResult<Vec<Dataset>> {
    //     let database = executor.context();
    //     Ok(database.get_all_datasets()?)
    // }

    // /// Retrieve a specific dataset configuration.
    // fn dataset(executor: &Executor, key: String) -> FieldResult<Option<Dataset>> {
    //     let database = executor.context();
    //     Ok(database.get_dataset(&key)?)
    // }

    /// Find all named store configurations.
    fn stores(executor: &Executor) -> FieldResult<Vec<Store>> {
        use crate::domain::usecases::get_stores::GetStores;
        use crate::domain::usecases::{NoParams, UseCase};
        let ctx = executor.context().clone();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetStores::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let result: Vec<crate::domain::entities::Store> = usecase.call(params)?;
        let stores: Vec<Store> = result.into_iter().map(|s| s.into()).collect();
        Ok(stores)
    }

    // /// Retrieve the named store configuration.
    // fn store(executor: &Executor, key: String) -> FieldResult<Store> {
    //     let database = executor.context();
    //     let stor = store::load_store(database, &key)?;
    //     Ok(Store::from(stor))
    // }

    // /// Retrieve a specific snapshot.
    // fn snapshot(executor: &Executor, digest: Checksum) -> FieldResult<Option<Snapshot>> {
    //     let database = executor.context();
    //     Ok(database.get_snapshot(&digest)?)
    // }

    // /// Retrieve a specific tree.
    // fn tree(executor: &Executor, digest: Checksum) -> FieldResult<Option<Tree>> {
    //     let database = executor.context();
    //     Ok(database.get_tree(&digest)?)
    // }
}

/// Property defines a name/value pair.
#[derive(GraphQLInputObject)]
struct PropertyInput {
    name: String,
    value: String,
}

/// Store defines a location where packs will be saved.
#[derive(GraphQLInputObject)]
struct StoreInput {
    /// Name of the type of this store (e.g. "local").
    store_type: String,
    /// User-defined label for this store.
    label: String,
    /// Name/value pairs that make up this store configuration.
    properties: Vec<PropertyInput>,
}

impl Into<crate::domain::usecases::new_store::Params> for StoreInput {
    fn into(self) -> crate::domain::usecases::new_store::Params {
        let mut properties: HashMap<String, String> = HashMap::new();
        for prop in self.properties.iter() {
            properties.insert(prop.name.to_owned(), prop.value.to_owned());
        }
        crate::domain::usecases::new_store::Params::new(self.store_type, self.label, properties)
    }
}

pub struct MutationRoot;

#[juniper::object(Context = GraphContext)]
impl MutationRoot {
    /// Define a new store with the given configuration.
    fn defineStore(executor: &Executor, store: StoreInput) -> FieldResult<Store> {
        use crate::domain::usecases::new_store::{NewStore, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context().clone();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = NewStore::new(Box::new(repo));
        let params: Params = store.into();
        let result: crate::domain::entities::Store = usecase.call(params)?;
        Ok(result.into())
    }

    // /// Update the saved store configuration.
    // fn updateStore(executor: &Executor, key: String, options: String) -> FieldResult<Store> {
    //     let database = executor.context();
    //     let decoded = base64::decode(&options)?;
    //     let json = std::str::from_utf8(&decoded)?;
    //     let mut stor = store::load_store(database, &key)?;
    //     stor.get_config_mut().from_json(&json)?;
    //     store::save_store(&database, stor.as_ref())?;
    //     Ok(Store::from(stor))
    // }

    // /// Delete the named store, returning its current configuration.
    // fn deleteStore(executor: &Executor, key: String) -> FieldResult<Store> {
    //     let database = executor.context();
    //     let stor = store::load_store(database, &key)?;
    //     store::delete_store(&database, &key)?;
    //     Ok(Store::from(stor))
    // }

    // /// Define a new dataset with the given configuration.
    // fn defineDataset(executor: &Executor, dataset: InputDataset) -> FieldResult<Dataset> {
    //     let database = executor.context();
    //     dataset.validate(&database)?;
    //     let config = engine::get_configuration(&database)?;
    //     let computer_id = config.computer_id;
    //     let mut updated = Dataset::new(
    //         &computer_id,
    //         Path::new(&dataset.basepath),
    //         &dataset.stores[0],
    //     );
    //     dataset.copy_input(&mut updated);
    //     database.put_dataset(&updated)?;
    //     Ok(updated)
    // }

    // /// Update an existing dataset with the given configuration.
    // fn updateDataset(executor: &Executor, dataset: InputDataset) -> FieldResult<Dataset> {
    //     match dataset.key {
    //         None => Err(FieldError::new("Dataset must specify a key", Value::null())),
    //         Some(ref set_key) => {
    //             let database = executor.context();
    //             dataset.validate(&database)?;
    //             match database.get_dataset(set_key)? {
    //                 None => Err(FieldError::new(
    //                     format!("Dataset does not exist: {}", set_key),
    //                     Value::null(),
    //                 )),
    //                 Some(mut updated) => {
    //                     dataset.copy_input(&mut updated);
    //                     database.put_dataset(&updated)?;
    //                     Ok(updated)
    //                 }
    //             }
    //         }
    //     }
    // }

    // /// Delete the named dataset, returning its current configuration.
    // fn deleteDataset(executor: &Executor, key: String) -> FieldResult<Dataset> {
    //     let database = executor.context();
    //     let opt = database.get_dataset(&key)?;
    //     if let Some(set) = opt {
    //         database.delete_dataset(&key)?;
    //         Ok(set)
    //     } else {
    //         Err(FieldError::new(
    //             format!("Dataset does not exist: {}", &key),
    //             Value::null(),
    //         ))
    //     }
    // }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot>;

/// Create the GraphQL schema.
pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::sources::MockEntityDataSource;
    use failure::err_msg;
    use juniper::{ToInputValue, Variables};
    use mockall::predicate::*;

    #[test]
    fn test_query_stores_ok() {
        // arrange
        let properties: HashMap<String, String> = HashMap::new();
        let stores = vec![crate::domain::entities::Store {
            id: "cafebabe".to_owned(),
            store_type: crate::domain::entities::StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        }];
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_stores()
            .returning(move || Ok(stores.clone()));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let ctx = Arc::new(GraphContext::new(datasource));
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute(
            r#"query {
                stores { storeType label }
            }"#,
            None,
            &schema,
            &Variables::new(),
            &ctx,
        )
        .unwrap();
        // assert
        assert_eq!(errors.len(), 0);
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("stores").unwrap();
        let list = res.as_list_value().unwrap();
        assert_eq!(list.len(), 1);
        let object = list[0].as_object_value().unwrap();
        let field = object.get_field_value("storeType").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "local");
        let field = object.get_field_value("label").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "mylocalstore");
    }

    #[test]
    fn test_query_stores_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_stores()
            .returning(move || Err(err_msg("oh no")));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let ctx = Arc::new(GraphContext::new(datasource));
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute(
            r#"query {
                stores { storeType label }
            }"#,
            None,
            &schema,
            &Variables::new(),
            &ctx,
        )
        .unwrap();
        // assert
        assert!(res.is_null());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].error().message().contains("oh no"));
    }

    #[test]
    fn test_mutation_define_store_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_store().with(always()).returning(|_| Ok(()));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let ctx = Arc::new(GraphContext::new(datasource));
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let properties = vec![PropertyInput {
            name: "basepath".to_owned(),
            value: "/home/planet".to_owned(),
        }];
        let input = StoreInput {
            store_type: "local".to_owned(),
            label: "my local".to_owned(),
            properties,
        };
        vars.insert("store".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute(
            r#"mutation Define($store: StoreInput!) {
                defineStore(store: $store) {
                    id storeType label properties { name value }
                }
            }"#,
            None,
            &schema,
            &vars,
            &ctx,
        )
        .unwrap();
        // assert
        assert_eq!(errors.len(), 0);
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("defineStore").unwrap();
        let object = res.as_object_value().unwrap();
        let field = object.get_field_value("storeType").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "local");
        let field = object.get_field_value("label").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "my local");
        let field = object.get_field_value("properties").unwrap();
        let value = field.as_list_value().unwrap();
        let names = ["basepath"];
        for (idx, entry) in value.iter().enumerate() {
            let prop = entry.as_object_value().unwrap();
            let field = prop.get_field_value("name").unwrap();
            let name = field.as_scalar_value::<String>().unwrap();
            assert_eq!(name, names[idx]);
        }
    }

    #[test]
    fn test_mutation_define_store_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_store()
            .with(always())
            .returning(|_| Err(err_msg("oh no")));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let ctx = Arc::new(GraphContext::new(datasource));
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let properties = vec![PropertyInput {
            name: "basepath".to_owned(),
            value: "/home/planet".to_owned(),
        }];
        let input = StoreInput {
            store_type: "local".to_owned(),
            label: "my local".to_owned(),
            properties,
        };
        vars.insert("store".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute(
            r#"mutation Define($store: StoreInput!) {
                defineStore(store: $store) {
                    id storeType label properties { name value }
                }
            }"#,
            None,
            &schema,
            &vars,
            &ctx,
        )
        .unwrap();
        // assert
        assert!(res.is_null());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].error().message().contains("oh no"));
    }
}
