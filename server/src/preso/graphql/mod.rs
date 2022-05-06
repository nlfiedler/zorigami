//
// Copyright (c) 2022 Nathan Fiedler
//

//! The `schema` module defines the GraphQL schema and resolvers.

use crate::data::repositories::RecordRepositoryImpl;
use crate::data::sources::EntityDataSource;
use crate::domain::entities::{self, Checksum, TreeReference};
use crate::domain::helpers;
use crate::domain::managers::process::Processor;
use crate::domain::managers::restore::{self, Restorer};
use crate::domain::managers::state::StateStore;
use crate::domain::repositories::RecordRepository;
use chrono::prelude::*;
use juniper::{
    graphql_scalar, EmptySubscription, FieldError, FieldResult, GraphQLEnum, GraphQLInputObject,
    GraphQLObject, ParseScalarResult, ParseScalarValue, RootNode, Value,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

// Context for the GraphQL schema.
pub struct GraphContext {
    datasource: Arc<dyn EntityDataSource>,
    appstate: Arc<dyn StateStore>,
    processor: Arc<dyn Processor>,
    restorer: Arc<dyn Restorer>,
}

impl GraphContext {
    pub fn new(
        datasource: Arc<dyn EntityDataSource>,
        appstate: Arc<dyn StateStore>,
        processor: Arc<dyn Processor>,
        restorer: Arc<dyn Restorer>,
    ) -> Self {
        Self {
            datasource,
            appstate,
            processor,
            restorer,
        }
    }
}

// Mark the data source as a valid context type for Juniper.
impl juniper::Context for GraphContext {}

// Define a larger integer type so we can represent those larger values, such as
// file sizes. Some of the core types define fields that are larger than i32, so
// this type is used to represent those values in GraphQL.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[graphql_scalar(description = "An integer type larger than the standard signed 32-bit.")]
impl<S> GraphQLScalar for BigInt
where
    S: ScalarValue,
{
    fn resolve(&self) -> Value {
        Value::scalar(format!("{}", self.0))
    }

    fn from_input_value(v: &InputValue) -> Option<BigInt> {
        v.as_scalar_value()
            .and_then(|v| v.as_str())
            .and_then(|s| i64::from_str_radix(s, 10).ok())
            .map(|i| BigInt(i))
    }

    fn from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
}

#[graphql_scalar(description = "A SHA1 or SHA256 checksum, with algorithm prefix.")]
impl<S> GraphQLScalar for Checksum
where
    S: ScalarValue,
{
    fn resolve(&self) -> Value {
        Value::scalar(format!("{}", self))
    }

    fn from_input_value(v: &InputValue) -> Option<Checksum> {
        v.as_scalar_value()
            .and_then(|v| v.as_str())
            .filter(|s| {
                // make sure the input value actually looks like a digest
                s.starts_with("sha1-") || s.starts_with("sha256-")
            })
            .map(|s| FromStr::from_str(s).unwrap())
    }

    fn from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
}

#[graphql_scalar(description = "Reference for a tree entry, such as a file or tree.")]
impl<S> GraphQLScalar for TreeReference
where
    S: ScalarValue,
{
    fn resolve(&self) -> Value {
        let value = format!("{}", self);
        Value::scalar(value)
    }

    fn from_input_value(v: &InputValue) -> Option<TreeReference> {
        v.as_scalar_value()
            .and_then(|v| v.as_str())
            .filter(|s| {
                // make sure the input value actually looks like a digest
                s.starts_with("file-") || s.starts_with("link-") || s.starts_with("tree-")
            })
            .map(|s| FromStr::from_str(s).unwrap())
    }

    fn from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
}

#[juniper::graphql_object(description = "A file, directory, or symbolic link within a tree.")]
impl entities::TreeEntry {
    /// Name of the file, directory, or symbolic link.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Modification time of the entry.
    fn mod_time(&self) -> DateTime<Utc> {
        self.mtime.clone()
    }

    /// Reference to the entry itself.
    fn reference(&self) -> TreeReference {
        self.reference.clone()
    }
}

#[juniper::graphql_object(description = "A set of file system entries in a directory.")]
impl entities::Tree {
    /// Set of entries making up this tree.
    fn entries(&self) -> Vec<entities::TreeEntry> {
        self.entries.clone()
    }
}

#[juniper::graphql_object(description = "Number of files and directories in a snapshot.")]
impl entities::FileCounts {
    fn directories(&self) -> BigInt {
        BigInt(self.directories as i64)
    }

    fn symlinks(&self) -> BigInt {
        BigInt(self.symlinks as i64)
    }

    fn files_below_80(&self) -> BigInt {
        BigInt(self.files_below_80 as i64)
    }

    fn files_below_1k(&self) -> BigInt {
        BigInt(self.files_below_1k as i64)
    }

    fn files_below_10k(&self) -> BigInt {
        BigInt(self.files_below_10k as i64)
    }

    fn files_below_100k(&self) -> BigInt {
        BigInt(self.files_below_100k as i64)
    }

    fn files_below_1m(&self) -> BigInt {
        BigInt(self.files_below_1m as i64)
    }

    fn files_below_10m(&self) -> BigInt {
        BigInt(self.files_below_10m as i64)
    }

    fn files_below_100m(&self) -> BigInt {
        BigInt(self.files_below_100m as i64)
    }

    fn very_large_files(&self) -> BigInt {
        BigInt(self.very_large_files as i64)
    }
}

#[juniper::graphql_object(description = "A single backup, either in progress or completed.")]
impl entities::Snapshot {
    /// Original computed checksum of the snapshot.
    fn checksum(&self) -> Checksum {
        self.digest.clone()
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
        BigInt(self.file_counts.total_files() as i64)
    }

    /// Number of files and directories contained in this snapshot.
    fn file_counts(&self) -> entities::FileCounts {
        self.file_counts.clone()
    }

    /// Reference to the tree containing all of the files.
    fn tree(&self) -> Checksum {
        self.tree.clone()
    }
}

/// Status of the most recent snapshot for a dataset.
#[derive(Copy, Clone, GraphQLEnum)]
pub enum Status {
    /// Backup has not run yet.
    NONE,
    /// Backup is still running.
    RUNNING,
    /// Backup has finished.
    FINISHED,
    /// Backup paused due to schedule.
    PAUSED,
    /// Backup failed, see `errorMessage` property.
    FAILED,
}

#[juniper::graphql_object(
    Context = GraphContext,
    description = "Location, schedule, and pack store for a backup data set.")
]
impl entities::Dataset {
    /// Identifier for this dataset.
    fn id(&self) -> String {
        self.id.clone()
    }

    /// Unique computer identifier.
    fn computer_id(&self, executor: &Executor) -> Option<String> {
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        if let Ok(value) = repo.get_computer_id(&self.id) {
            value
        } else {
            None
        }
    }

    /// Path that is being backed up.
    fn basepath(&self) -> String {
        self.basepath
            .to_str()
            .map(|v| v.to_owned())
            .unwrap_or_else(|| self.basepath.to_string_lossy().into_owned())
    }

    /// Path for temporary pack building.
    fn workspace(&self) -> String {
        self.workspace
            .to_str()
            .map(|v| v.to_owned())
            .unwrap_or_else(|| self.workspace.to_string_lossy().into_owned())
    }

    /// Set of schedules that apply to this dataset.
    fn schedules(&self) -> Vec<entities::schedule::Schedule> {
        self.schedules.clone()
    }

    /// Status of the most recent snapshot for this dataset.
    fn status(&self, executor: &Executor) -> Status {
        let ctx = executor.context();
        let redux = ctx.appstate.get_state();
        if let Some(backup) = redux.backups(&self.id) {
            if backup.is_paused() {
                Status::PAUSED
            } else if backup.had_error() {
                Status::FAILED
            } else if backup.end_time().is_none() {
                Status::RUNNING
            } else {
                Status::FINISHED
            }
        } else {
            Status::NONE
        }
    }

    /// Error message for the most recent snapshot, if any.
    fn error_message(&self, executor: &Executor) -> Option<String> {
        let ctx = executor.context();
        let redux = ctx.appstate.get_state();
        redux.backups(&self.id).map(|e| e.error_message()).flatten()
    }

    /// Most recent snapshot for this dataset, if any.
    fn latest_snapshot(&self, executor: &Executor) -> Option<entities::Snapshot> {
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        if let Ok(Some(digest)) = repo.get_latest_snapshot(&self.id) {
            if let Ok(result) = repo.get_snapshot(&digest) {
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

    /// List of paths to be excluded from backups.
    fn excludes(&self) -> Vec<String> {
        self.excludes.clone()
    }
}

#[juniper::graphql_object(description = "Range of time in which to run backup.")]
impl entities::schedule::TimeRange {
    /// Seconds from midnight at which to start.
    fn start_time(&self) -> i32 {
        self.start as i32
    }
    /// Seconds from midnight at which to stop.
    fn stop_time(&self) -> i32 {
        self.stop as i32
    }
}

#[derive(GraphQLInputObject)]
pub struct InputTimeRange {
    /// Seconds from midnight at which to start.
    pub start_time: i32,
    /// Seconds from midnight at which to stop.
    pub stop_time: i32,
}

impl InputTimeRange {
    /// Perform basic validation on the input time range.
    fn validate(&self) -> FieldResult<()> {
        if self.start_time < 0 || self.start_time > 86_400 {
            return Err(FieldError::new(
                "Start time must be between 0 and 86,400",
                Value::null(),
            ));
        }
        if self.stop_time < 0 || self.stop_time > 86_400 {
            return Err(FieldError::new(
                "Stop time must be between 0 and 86,400",
                Value::null(),
            ));
        }
        Ok(())
    }
}

impl Into<entities::schedule::TimeRange> for InputTimeRange {
    fn into(self) -> entities::schedule::TimeRange {
        entities::schedule::TimeRange::new_secs(self.start_time as u32, self.stop_time as u32)
    }
}

#[derive(Copy, Clone, GraphQLEnum)]
pub enum DayOfWeek {
    Sun,
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
}

impl From<entities::schedule::DayOfWeek> for DayOfWeek {
    fn from(dow: entities::schedule::DayOfWeek) -> Self {
        match dow {
            entities::schedule::DayOfWeek::Sun => DayOfWeek::Sun,
            entities::schedule::DayOfWeek::Mon => DayOfWeek::Mon,
            entities::schedule::DayOfWeek::Tue => DayOfWeek::Tue,
            entities::schedule::DayOfWeek::Wed => DayOfWeek::Wed,
            entities::schedule::DayOfWeek::Thu => DayOfWeek::Thu,
            entities::schedule::DayOfWeek::Fri => DayOfWeek::Fri,
            entities::schedule::DayOfWeek::Sat => DayOfWeek::Sat,
        }
    }
}

impl Into<entities::schedule::DayOfWeek> for DayOfWeek {
    fn into(self) -> entities::schedule::DayOfWeek {
        match self {
            DayOfWeek::Sun => entities::schedule::DayOfWeek::Sun,
            DayOfWeek::Mon => entities::schedule::DayOfWeek::Mon,
            DayOfWeek::Tue => entities::schedule::DayOfWeek::Tue,
            DayOfWeek::Wed => entities::schedule::DayOfWeek::Wed,
            DayOfWeek::Thu => entities::schedule::DayOfWeek::Thu,
            DayOfWeek::Fri => entities::schedule::DayOfWeek::Fri,
            DayOfWeek::Sat => entities::schedule::DayOfWeek::Sat,
        }
    }
}

/// In combination with DayOfWeek, selects the particular week.
#[derive(Copy, Clone, GraphQLEnum)]
pub enum WeekOfMonth {
    /// First such weekday of the month.
    First,
    /// Second such weekday of the month.
    Second,
    /// Third such weekday of the month.
    Third,
    /// Fourth such weekday of the month.
    Fourth,
    /// Fifth such weekday of the month.
    Fifth,
}

impl WeekOfMonth {
    fn into_dom(self, dow: DayOfWeek) -> entities::schedule::DayOfMonth {
        match self {
            WeekOfMonth::First => entities::schedule::DayOfMonth::First(dow.into()),
            WeekOfMonth::Second => entities::schedule::DayOfMonth::Second(dow.into()),
            WeekOfMonth::Third => entities::schedule::DayOfMonth::Third(dow.into()),
            WeekOfMonth::Fourth => entities::schedule::DayOfMonth::Fourth(dow.into()),
            WeekOfMonth::Fifth => entities::schedule::DayOfMonth::Fifth(dow.into()),
        }
    }
}

/// How often should the backup run for the dataset.
#[derive(Copy, Clone, GraphQLEnum)]
pub enum Frequency {
    /// Run every hour.
    Hourly,
    /// Run every day, with optional time range.
    Daily,
    /// Run every week, with optional day-of-week and time range.
    Weekly,
    /// Run every month, with optional day-of-month and time range.
    Monthly,
}

#[juniper::graphql_object(description = "A schedule for when to run the backup.")]
impl entities::schedule::Schedule {
    /// How often the backup will be run. Combines with other elements to
    /// control exactly when the backup is performed.
    fn frequency(&self) -> Frequency {
        match self {
            entities::schedule::Schedule::Hourly => Frequency::Hourly,
            entities::schedule::Schedule::Daily(_) => Frequency::Daily,
            entities::schedule::Schedule::Weekly(_) => Frequency::Weekly,
            entities::schedule::Schedule::Monthly(_) => Frequency::Monthly,
        }
    }

    /// Time within the day when the backup will be run. The start time will
    /// come before the stop time if the range spans midnight.
    fn time_range(&self) -> Option<entities::schedule::TimeRange> {
        match self {
            entities::schedule::Schedule::Hourly => None,
            entities::schedule::Schedule::Daily(None) => None,
            entities::schedule::Schedule::Daily(Some(v)) => Some(v.clone()),
            entities::schedule::Schedule::Weekly(None) => None,
            entities::schedule::Schedule::Weekly(Some((_, None))) => None,
            entities::schedule::Schedule::Weekly(Some((_, Some(v)))) => Some(v.clone()),
            entities::schedule::Schedule::Monthly(None) => None,
            entities::schedule::Schedule::Monthly(Some((_, None))) => None,
            entities::schedule::Schedule::Monthly(Some((_, Some(v)))) => Some(v.clone()),
        }
    }

    /// Which week, in combination with the day of the week, to run the backup.
    fn week_of_month(&self) -> Option<WeekOfMonth> {
        match self {
            entities::schedule::Schedule::Hourly => None,
            entities::schedule::Schedule::Daily(_) => None,
            entities::schedule::Schedule::Weekly(_) => None,
            entities::schedule::Schedule::Monthly(None) => None,
            entities::schedule::Schedule::Monthly(Some((v, _))) => match v {
                entities::schedule::DayOfMonth::First(_) => Some(WeekOfMonth::First),
                entities::schedule::DayOfMonth::Second(_) => Some(WeekOfMonth::Second),
                entities::schedule::DayOfMonth::Third(_) => Some(WeekOfMonth::Third),
                entities::schedule::DayOfMonth::Fourth(_) => Some(WeekOfMonth::Fourth),
                entities::schedule::DayOfMonth::Fifth(_) => Some(WeekOfMonth::Fifth),
                entities::schedule::DayOfMonth::Day(_) => None,
            },
        }
    }

    /// Day of the week on which to run the backup, for schedules with a weekly
    /// or monthly frequency.
    fn day_of_week(&self) -> Option<DayOfWeek> {
        match self {
            entities::schedule::Schedule::Hourly => None,
            entities::schedule::Schedule::Daily(_) => None,
            entities::schedule::Schedule::Weekly(None) => None,
            entities::schedule::Schedule::Weekly(Some((v, _))) => Some(DayOfWeek::from(*v)),
            entities::schedule::Schedule::Monthly(None) => None,
            entities::schedule::Schedule::Monthly(Some((v, _))) => match v {
                entities::schedule::DayOfMonth::First(v) => Some(DayOfWeek::from(*v)),
                entities::schedule::DayOfMonth::Second(v) => Some(DayOfWeek::from(*v)),
                entities::schedule::DayOfMonth::Third(v) => Some(DayOfWeek::from(*v)),
                entities::schedule::DayOfMonth::Fourth(v) => Some(DayOfWeek::from(*v)),
                entities::schedule::DayOfMonth::Fifth(v) => Some(DayOfWeek::from(*v)),
                entities::schedule::DayOfMonth::Day(_) => None,
            },
        }
    }

    /// Day of the month, instead of a week and weekday, to run the backup, for
    /// schedules with a monthly frequency.
    fn day_of_month(&self) -> Option<i32> {
        match self {
            entities::schedule::Schedule::Hourly => None,
            entities::schedule::Schedule::Daily(_) => None,
            entities::schedule::Schedule::Weekly(_) => None,
            entities::schedule::Schedule::Monthly(None) => None,
            entities::schedule::Schedule::Monthly(Some((v, _))) => match v {
                entities::schedule::DayOfMonth::First(_) => None,
                entities::schedule::DayOfMonth::Second(_) => None,
                entities::schedule::DayOfMonth::Third(_) => None,
                entities::schedule::DayOfMonth::Fourth(_) => None,
                entities::schedule::DayOfMonth::Fifth(_) => None,
                entities::schedule::DayOfMonth::Day(v) => Some(*v as i32),
            },
        }
    }
}

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

#[juniper::graphql_object(description = "Configuration of the application.")]
impl entities::Configuration {
    /// Name of the computer on which this application is running.
    fn hostname(&self) -> String {
        self.hostname.clone()
    }
    /// Name of the user running this application.
    fn username(&self) -> String {
        self.username.clone()
    }
    /// Computer UUID for generating bucket names.
    fn computer_id(&self) -> String {
        self.computer_id.clone()
    }
}

#[juniper::graphql_object(description = "Entry within a saved pack file.")]
impl entities::PackEntry {
    /// File name of the entry in the pack file.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Length of the content of the entry.
    fn size(&self) -> BigInt {
        BigInt(self.size as i64)
    }
}

#[juniper::graphql_object(description = "Details about a pack file.")]
impl entities::PackFile {
    /// Number of entries in the pack file.
    fn count(&self) -> i32 {
        self.entries.len() as i32
    }

    /// All entries in the pack file.
    fn entries(&self) -> Vec<entities::PackEntry> {
        self.entries.clone()
    }

    /// Length of the pack file in bytes.
    fn length(&self) -> BigInt {
        BigInt(self.length as i64)
    }

    /// Total size of all pack entries.
    fn content_length(&self) -> BigInt {
        BigInt(self.content_length as i64)
    }

    /// Size of the smallest entry in the pack file.
    fn smallest(&self) -> BigInt {
        BigInt(self.smallest as i64)
    }

    /// Size of the largest entry in the pack file.
    fn largest(&self) -> BigInt {
        BigInt(self.largest as i64)
    }

    /// Average size of all entries in the pack file.
    fn average(&self) -> BigInt {
        BigInt(self.average as i64)
    }
}

#[juniper::graphql_object(description = "Location within a store of a saved pack.")]
impl entities::PackLocation {
    /// ULID of the pack store.
    fn store(&self) -> String {
        self.store.clone()
    }

    /// Remote bucket name.
    fn bucket(&self) -> String {
        self.bucket.clone()
    }

    /// Remote object name.
    fn object(&self) -> String {
        self.object.clone()
    }
}

#[juniper::graphql_object(description = "An archive containing saved files.")]
impl entities::Pack {
    /// Unique checksum of the pack contents.
    fn checksum(&self) -> Checksum {
        self.digest.clone()
    }

    /// List of store-specific coordinates where the pack is saved.
    fn locations(&self) -> Vec<entities::PackLocation> {
        self.locations.clone()
    }

    /// Date and time of successful upload.
    fn upload_time(&self) -> DateTime<Utc> {
        self.upload_time
    }
}

#[juniper::graphql_object(description = "A request to restore a file or directory.")]
impl restore::Request {
    /// Digest of the tree containing the entry to restore.
    fn tree(&self) -> Checksum {
        self.tree.clone()
    }

    /// Name of the entry within the tree to be restored.
    fn entry(&self) -> String {
        self.entry.clone()
    }

    /// Relative path where file/tree will be restored.
    fn filepath(&self) -> String {
        self.filepath.to_string_lossy().into()
    }

    /// Identifier of the dataset containing the data.
    fn dataset(&self) -> String {
        self.dataset.clone()
    }

    /// The datetime when the request was completed.
    fn finished(&self) -> Option<DateTime<Utc>> {
        self.finished.clone()
    }

    /// Number of files restored so far during the restoration.
    fn files_restored(&self) -> i32 {
        self.files_restored as i32
    }

    /// Error message if request processing failed.
    fn error_message(&self) -> Option<String> {
        self.error_msg.clone()
    }
}

#[juniper::graphql_object(description = "Number of database records for each entity type.")]
impl entities::RecordCounts {
    /// Number of chunks stored in the repository.
    fn chunks(&self) -> i32 {
        self.chunk as i32
    }

    /// Number of datasets stored in the repository.
    fn datasets(&self) -> i32 {
        self.dataset as i32
    }

    /// Number of files stored in the repository.
    fn files(&self) -> i32 {
        self.file as i32
    }

    /// Number of packs stored in the repository.
    fn packs(&self) -> i32 {
        self.pack as i32
    }

    /// Number of snapshots stored in the repository.
    fn snapshots(&self) -> i32 {
        self.snapshot as i32
    }

    /// Number of stores stored in the repository.
    fn stores(&self) -> i32 {
        self.store as i32
    }

    /// Number of trees stored in the repository.
    fn trees(&self) -> i32 {
        self.tree as i32
    }

    /// Number of extended attributes stored in the repository.
    fn xattrs(&self) -> i32 {
        self.xattr as i32
    }
}

pub struct QueryRoot;

#[juniper::graphql_object(Context = GraphContext)]
impl QueryRoot {
    /// Retrieve the configuration record.
    fn configuration(executor: &Executor) -> FieldResult<entities::Configuration> {
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        Ok(repo.get_configuration()?)
    }

    /// Find all dataset configurations.
    fn datasets(executor: &Executor) -> FieldResult<Vec<entities::Dataset>> {
        use crate::domain::usecases::get_datasets::GetDatasets;
        use crate::domain::usecases::{NoParams, UseCase};
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetDatasets::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let datasets = usecase.call(params)?;
        Ok(datasets)
    }

    /// Find any packs that are missing from the given store.
    fn missing_packs(executor: &Executor, store_id: String) -> FieldResult<Vec<entities::Pack>> {
        use crate::domain::usecases::find_missing::{FindMissingPacks, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = FindMissingPacks::new(Box::new(repo));
        let params: Params = Params::new(store_id);
        let result: Vec<entities::Pack> = usecase.call(params)?;
        Ok(result)
    }

    /// Retrieve entry listing a specific pack.
    fn pack(
        executor: &Executor,
        dataset: String,
        digest: Checksum,
    ) -> FieldResult<entities::PackFile> {
        use crate::domain::usecases::get_pack::{GetPack, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetPack::new(Box::new(repo));
        let passphrase = helpers::crypto::get_passphrase();
        let params: Params = Params::new(dataset, digest, passphrase);
        let result: entities::PackFile = usecase.call(params)?;
        Ok(result)
    }

    /// Return the number of each type of database record.
    fn record_counts(executor: &Executor) -> FieldResult<entities::RecordCounts> {
        use crate::domain::usecases::get_counts::GetCounts;
        use crate::domain::usecases::{NoParams, UseCase};
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetCounts::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let counts = usecase.call(params)?;
        Ok(counts)
    }

    /// Query for any pending and recently completed file restore operations.
    fn restores(executor: &Executor) -> FieldResult<Vec<restore::Request>> {
        use crate::domain::usecases::query_restores::QueryRestores;
        use crate::domain::usecases::{NoParams, UseCase};
        let ctx = executor.context();
        let usecase = QueryRestores::new(ctx.restorer.clone());
        let params: NoParams = NoParams {};
        let requests: Vec<restore::Request> = usecase.call(params)?;
        Ok(requests)
    }

    /// Retrieve a specific snapshot.
    fn snapshot(executor: &Executor, digest: Checksum) -> FieldResult<Option<entities::Snapshot>> {
        use crate::domain::usecases::get_snapshot::{GetSnapshot, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetSnapshot::new(Box::new(repo));
        let params: Params = Params::new(digest);
        let result: Option<entities::Snapshot> = usecase.call(params)?;
        Ok(result)
    }

    /// Find all named store configurations.
    fn stores(executor: &Executor) -> FieldResult<Vec<Store>> {
        use crate::domain::usecases::get_stores::GetStores;
        use crate::domain::usecases::{NoParams, UseCase};
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetStores::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let result: Vec<crate::domain::entities::Store> = usecase.call(params)?;
        let stores: Vec<Store> = result.into_iter().map(|s| s.into()).collect();
        Ok(stores)
    }

    /// Retrieve a specific tree.
    fn tree(executor: &Executor, digest: Checksum) -> FieldResult<Option<entities::Tree>> {
        use crate::domain::usecases::get_tree::{GetTree, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetTree::new(Box::new(repo));
        let params: Params = Params::new(digest);
        let result: Option<entities::Tree> = usecase.call(params)?;
        Ok(result)
    }
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
    /// Store identifier, only used when updating a store.
    id: Option<String>,
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

impl Into<crate::domain::usecases::test_store::Params> for StoreInput {
    fn into(self) -> crate::domain::usecases::test_store::Params {
        let mut properties: HashMap<String, String> = HashMap::new();
        for prop in self.properties.iter() {
            properties.insert(prop.name.to_owned(), prop.value.to_owned());
        }
        crate::domain::usecases::test_store::Params::new(
            self.id.unwrap_or(String::from("default")),
            self.store_type,
            self.label,
            properties,
        )
    }
}

impl Into<crate::domain::usecases::update_store::Params> for StoreInput {
    fn into(self) -> crate::domain::usecases::update_store::Params {
        let mut properties: HashMap<String, String> = HashMap::new();
        for prop in self.properties.iter() {
            properties.insert(prop.name.to_owned(), prop.value.to_owned());
        }
        crate::domain::usecases::update_store::Params::new(
            self.id.unwrap_or(String::from("default")),
            self.store_type,
            self.label,
            properties,
        )
    }
}

#[derive(GraphQLInputObject)]
pub struct DatasetInput {
    /// Identifier of dataset to update, null if creating.
    pub id: Option<String>,
    /// Path that is being backed up.
    pub basepath: String,
    /// List of schedules to apply to this dataset.
    pub schedules: Vec<ScheduleInput>,
    /// Path to temporary workspace for backup process.
    pub workspace: Option<String>,
    /// Desired byte length of pack files.
    pub pack_size: BigInt,
    /// Identifiers of stores used for saving packs.
    pub stores: Vec<String>,
    /// List of paths to be excluded from backups. Can include * and ** wildcards.
    pub excludes: Vec<String>,
}

impl Into<crate::domain::usecases::new_dataset::Params> for DatasetInput {
    fn into(self) -> crate::domain::usecases::new_dataset::Params {
        crate::domain::usecases::new_dataset::Params::new(
            PathBuf::from(self.basepath),
            self.schedules.into_iter().map(|s| s.into()).collect(),
            self.pack_size.into(),
            self.stores,
            self.excludes,
        )
    }
}

impl Into<crate::domain::usecases::update_dataset::Params> for DatasetInput {
    fn into(self) -> crate::domain::usecases::update_dataset::Params {
        crate::domain::usecases::update_dataset::Params::new(
            self.id.unwrap_or(String::from("default")),
            PathBuf::from(self.basepath),
            self.schedules.into_iter().map(|s| s.into()).collect(),
            self.workspace.map(|s| PathBuf::from(s)),
            self.pack_size.into(),
            self.stores,
            self.excludes,
        )
    }
}

impl DatasetInput {
    /// Perform basic validation on the input dataset.
    fn validate(&self, datasource: Arc<dyn EntityDataSource>) -> FieldResult<()> {
        // not convinced this is necessary
        // if self.stores.is_empty() {
        //     return Err(FieldError::new(
        //         "Require at least one store in dataset",
        //         Value::null(),
        //     ));
        // }
        // verify the stores exist in the database
        for store in self.stores.iter() {
            let opt = datasource.get_store(store)?;
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
        // ensure the schedules, if any, make sense
        for schedule in self.schedules.iter() {
            schedule.validate()?;
        }
        Ok(())
    }
}

/// New schedule for the dataset. Combine elements to get backups to run on a
/// certain day of the week, month, and/or within a given time range.
#[derive(GraphQLInputObject)]
pub struct ScheduleInput {
    /// How often to run the backup.
    pub frequency: Frequency,
    /// Range of time during the day in which to run backup.
    pub time_range: Option<InputTimeRange>,
    /// Which week within the month to run the backup.
    pub week_of_month: Option<WeekOfMonth>,
    /// Which day of the week to run the backup.
    pub day_of_week: Option<DayOfWeek>,
    /// The day of the month to run the backup.
    pub day_of_month: Option<i32>,
}

impl ScheduleInput {
    /// Construct a "hourly" schedule, for testing purposes.
    pub fn hourly() -> Self {
        Self {
            frequency: Frequency::Hourly,
            time_range: None,
            week_of_month: None,
            day_of_week: None,
            day_of_month: None,
        }
    }

    /// Construct a "daily" schedule, for testing purposes.
    pub fn daily() -> Self {
        Self {
            frequency: Frequency::Daily,
            time_range: None,
            week_of_month: None,
            day_of_week: None,
            day_of_month: None,
        }
    }

    fn validate(&self) -> FieldResult<()> {
        match &self.frequency {
            Frequency::Hourly => {
                if self.week_of_month.is_some()
                    || self.day_of_week.is_some()
                    || self.day_of_month.is_some()
                    || self.time_range.is_some()
                {
                    return Err(FieldError::new(
                        "Hourly cannot take any range or days",
                        Value::null(),
                    ));
                }
            }
            Frequency::Daily => {
                if self.week_of_month.is_some()
                    || self.day_of_week.is_some()
                    || self.day_of_month.is_some()
                {
                    return Err(FieldError::new(
                        "Daily can only take a time_range",
                        Value::null(),
                    ));
                }
                if let Some(ref range) = self.time_range {
                    range.validate()?
                }
            }
            Frequency::Weekly => {
                if self.week_of_month.is_some() || self.day_of_month.is_some() {
                    return Err(FieldError::new(
                        "Weekly can only take a time_range and day_of_week",
                        Value::null(),
                    ));
                }
                if let Some(ref range) = self.time_range {
                    range.validate()?
                }
            }
            Frequency::Monthly => {
                if self.day_of_month.is_some() && self.day_of_week.is_some() {
                    return Err(FieldError::new(
                        "Monthly can only take day_of_month *or* day_of_week and week_of_month",
                        Value::null(),
                    ));
                }
                if self.day_of_week.is_some() && self.week_of_month.is_none() {
                    return Err(FieldError::new(
                        "Monthly requires week_of_month when using day_of_week",
                        Value::null(),
                    ));
                }
                if let Some(ref range) = self.time_range {
                    range.validate()?
                }
            }
        }
        Ok(())
    }
}

impl Into<entities::schedule::Schedule> for ScheduleInput {
    fn into(self) -> entities::schedule::Schedule {
        match &self.frequency {
            Frequency::Hourly => entities::schedule::Schedule::Hourly,
            Frequency::Daily => {
                entities::schedule::Schedule::Daily(self.time_range.map(|s| s.into()))
            }
            Frequency::Weekly => {
                let dow = if let Some(dow) = self.day_of_week {
                    Some((dow.into(), self.time_range.map(|s| s.into())))
                } else {
                    None
                };
                entities::schedule::Schedule::Weekly(dow)
            }
            Frequency::Monthly => {
                let dom: Option<(
                    entities::schedule::DayOfMonth,
                    Option<entities::schedule::TimeRange>,
                )> = if let Some(day) = self.day_of_month {
                    Some((
                        entities::schedule::DayOfMonth::from(day as u32),
                        self.time_range.map(|s| s.into()),
                    ))
                } else if let Some(wn) = self.week_of_month {
                    let dow = self.day_of_week.unwrap();
                    let dom = wn.into_dom(dow);
                    Some((dom, self.time_range.map(|s| s.into())))
                } else {
                    None
                };
                entities::schedule::Schedule::Monthly(dom)
            }
        }
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(Context = GraphContext)]
impl MutationRoot {
    /// Define a new store with the given configuration.
    fn defineStore(executor: &Executor, input: StoreInput) -> FieldResult<Store> {
        use crate::domain::usecases::new_store::{NewStore, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = NewStore::new(Box::new(repo));
        let params: Params = input.into();
        let result: crate::domain::entities::Store = usecase.call(params)?;
        Ok(result.into())
    }

    /// Update the saved store configuration.
    fn updateStore(executor: &Executor, input: StoreInput) -> FieldResult<Store> {
        if input.id.is_none() {
            return Err(FieldError::new(
                "Cannot update store without id field",
                Value::null(),
            ));
        }
        use crate::domain::usecases::update_store::{Params, UpdateStore};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = UpdateStore::new(Box::new(repo));
        let params: Params = input.into();
        let result: crate::domain::entities::Store = usecase.call(params)?;
        Ok(result.into())
    }

    /// Test the given pack store definition for basic connectivity.
    ///
    /// Returns an error message, or 'ok' if there were no errors.
    fn testStore(executor: &Executor, input: StoreInput) -> FieldResult<String> {
        use crate::domain::usecases::test_store::{Params, TestStore};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = TestStore::new(Box::new(repo));
        let params: Params = input.into();
        let result = usecase.call(params);
        if result.is_err() {
            Ok(format!("{:?}", result.unwrap_err()))
        } else {
            Ok(String::from("ok"))
        }
    }

    /// Delete the named store, returning the identifier.
    fn deleteStore(executor: &Executor, id: String) -> FieldResult<String> {
        use crate::domain::usecases::delete_store::{DeleteStore, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = DeleteStore::new(Box::new(repo));
        let params: Params = Params::new(id.clone());
        usecase.call(params)?;
        Ok(id)
    }

    /// Define a new dataset with the given configuration.
    fn defineDataset(executor: &Executor, input: DatasetInput) -> FieldResult<entities::Dataset> {
        use crate::domain::usecases::new_dataset::{NewDataset, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let datasource = ctx.datasource.clone();
        input.validate(datasource.clone())?;
        let repo = RecordRepositoryImpl::new(datasource);
        let usecase = NewDataset::new(Box::new(repo));
        let params: Params = input.into();
        let dataset = usecase.call(params)?;
        Ok(dataset)
    }

    /// Update an existing dataset with the given configuration.
    fn updateDataset(executor: &Executor, input: DatasetInput) -> FieldResult<entities::Dataset> {
        if input.id.is_none() {
            return Err(FieldError::new(
                "Cannot update dataset without id field",
                Value::null(),
            ));
        }
        use crate::domain::usecases::update_dataset::{Params, UpdateDataset};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let datasource = ctx.datasource.clone();
        input.validate(datasource.clone())?;
        let repo = RecordRepositoryImpl::new(datasource);
        let usecase = UpdateDataset::new(Box::new(repo));
        let params: Params = input.into();
        let result = usecase.call(params)?;
        Ok(result)
    }

    /// Delete the dataset with the given identifier, returning the identifier.
    fn deleteDataset(executor: &Executor, id: String) -> FieldResult<String> {
        use crate::domain::usecases::delete_dataset::{DeleteDataset, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = DeleteDataset::new(Box::new(repo));
        let params: Params = Params::new(id.clone());
        usecase.call(params)?;
        Ok(id)
    }

    /// Begin the backup procedure for the dataset with the given identifier.
    fn startBackup(executor: &Executor, id: String) -> FieldResult<bool> {
        use crate::domain::usecases::start_backup::{Params, StartBackup};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = StartBackup::new(Box::new(repo), ctx.processor.clone());
        let params: Params = Params::new(id);
        usecase.call(params)?;
        Ok(true)
    }

    /// Signal the running backup for the given dataset to stop prematurely.
    fn stopBackup(executor: &Executor, id: String) -> FieldResult<bool> {
        use crate::domain::usecases::stop_backup::{Params, StopBackup};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = StopBackup::new(Box::new(repo), ctx.appstate.clone());
        let params: Params = Params::new(id);
        usecase.call(params)?;
        Ok(true)
    }

    /// Restore the database from the most recent snapshot.
    fn restoreDatabase(executor: &Executor, store_id: String) -> FieldResult<String> {
        use crate::domain::usecases::restore_database::{Params, RestoreDatabase};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = RestoreDatabase::new(Box::new(repo));
        let params: Params = Params::new(store_id, ctx.appstate.clone());
        let result = usecase.call(params)?;
        Ok(result)
    }

    /// Enqueue a request to restore the given file or directory tree.
    fn restore_files(
        executor: &Executor,
        tree: Checksum,
        entry: String,
        filepath: String,
        dataset: String,
    ) -> FieldResult<bool> {
        use crate::domain::usecases::restore_files::{Params, RestoreFiles};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let usecase = RestoreFiles::new(ctx.restorer.clone());
        let fpath = PathBuf::from(filepath);
        let params: Params = Params::new(tree.clone(), entry.clone(), fpath, dataset);
        usecase.call(params)?;
        Ok(true)
    }

    /// Cancel the pending restore request that matches the given values.
    fn cancel_restore(
        executor: &Executor,
        tree: Checksum,
        entry: String,
        filepath: String,
        dataset: String,
    ) -> FieldResult<bool> {
        use crate::domain::usecases::cancel_restore::{CancelRestore, Params};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let usecase = CancelRestore::new(ctx.restorer.clone());
        let fpath = PathBuf::from(filepath);
        let params: Params = Params::new(tree.clone(), entry.clone(), fpath, dataset);
        let result = usecase.call(params)?;
        Ok(result)
    }

    /// Restore any missing packs, copying from the other pack store.
    fn restore_packs(
        executor: &Executor,
        source_id: String,
        target_id: String,
    ) -> FieldResult<Vec<entities::Pack>> {
        use crate::domain::usecases::restore_missing::{Params, RestoreMissingPacks};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = RestoreMissingPacks::new(Box::new(repo));
        let params: Params = Params::new(source_id, target_id);
        let result: Vec<entities::Pack> = usecase.call(params)?;
        Ok(result)
    }

    /// Remove extraneous packs from the given pack store.
    fn prune_extra(executor: &Executor, store_id: String) -> FieldResult<i32> {
        use crate::domain::usecases::prune_extra::{Params, PruneExtraPacks};
        use crate::domain::usecases::UseCase;
        let ctx = executor.context();
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = PruneExtraPacks::new(Box::new(repo));
        let params: Params = Params::new(store_id);
        let result: u32 = usecase.call(params)?;
        Ok(result as i32)
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<GraphContext>>;

/// Create the GraphQL schema.
pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::sources::MockEntityDataSource;
    use crate::domain::managers::process::MockProcessor;
    use crate::domain::managers::restore::MockRestorer;
    use crate::domain::managers::state::MockStateStore;
    use anyhow::anyhow;
    use juniper::{FromInputValue, InputValue, ToInputValue, Variables};
    use mockall::predicate::*;

    fn make_context(mock: MockEntityDataSource) -> Arc<GraphContext> {
        // build the most basic GraphContext
        // if this turns out to be too limited, create a new builder
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let appstate = Arc::new(MockStateStore::new());
        let processor = Arc::new(MockProcessor::new());
        let restorer = Arc::new(MockRestorer::new());
        Arc::new(GraphContext::new(datasource, appstate, processor, restorer))
    }

    #[test]
    fn test_bigint_scalar() {
        let iv: InputValue<juniper::DefaultScalarValue> =
            juniper::InputValue::Scalar(juniper::DefaultScalarValue::String("1048576".to_owned()));
        let option: Option<BigInt> = BigInt::from_input_value(&iv);
        assert!(option.is_some());
        let actual = option.unwrap();
        assert_eq!(actual, BigInt(1048576));

        // not a number
        let iv: InputValue<juniper::DefaultScalarValue> =
            juniper::InputValue::Scalar(juniper::DefaultScalarValue::String("madokami".to_owned()));
        let option: Option<BigInt> = BigInt::from_input_value(&iv);
        assert!(option.is_none());
    }

    #[test]
    fn test_checksum_scalar() {
        let iv: InputValue<juniper::DefaultScalarValue> = juniper::InputValue::Scalar(
            juniper::DefaultScalarValue::String("sha1-cafebabe".to_owned()),
        );
        let option: Option<Checksum> = Checksum::from_input_value(&iv);
        assert!(option.is_some());
        let actual = option.unwrap();
        assert!(actual.is_sha1());

        // missing algorithm prefix
        let iv: InputValue<juniper::DefaultScalarValue> =
            juniper::InputValue::Scalar(juniper::DefaultScalarValue::String("cafebabe".to_owned()));
        let option: Option<Checksum> = Checksum::from_input_value(&iv);
        assert!(option.is_none());
    }

    #[test]
    fn test_treereference_scalar() {
        let iv: InputValue<juniper::DefaultScalarValue> = juniper::InputValue::Scalar(
            juniper::DefaultScalarValue::String("tree-sha1-cafebabe".to_owned()),
        );
        let option: Option<TreeReference> = TreeReference::from_input_value(&iv);
        assert!(option.is_some());
        let actual = option.unwrap();
        assert!(actual.is_tree());

        // missing entry type prefix
        let iv: InputValue<juniper::DefaultScalarValue> = juniper::InputValue::Scalar(
            juniper::DefaultScalarValue::String("sha1-cafebabe".to_owned()),
        );
        let option: Option<TreeReference> = TreeReference::from_input_value(&iv);
        assert!(option.is_none());
    }

    #[test]
    fn test_query_configuration() {
        // arrange
        let config: entities::Configuration = Default::default();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_configuration()
            .returning(move || Ok(Some(config.clone())));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
            r#"query { configuration { computerId } }"#,
            None,
            &schema,
            &Variables::new(),
            &ctx,
        )
        .unwrap();
        // assert
        assert_eq!(errors.len(), 0);
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("configuration").unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("computerId").unwrap();
        let actual = res.as_scalar_value::<String>().unwrap();
        let username = whoami::username();
        let hostname = whoami::hostname();
        let expected = entities::Configuration::generate_unique_id(&username, &hostname);
        assert_eq!(actual, &expected);
    }

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
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
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
    fn test_query_stores_none() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_stores().returning(move || Ok(Vec::new()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
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
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_query_stores_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_stores()
            .returning(move || Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
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
    fn test_query_datasets_ok() {
        use crate::domain::managers::state;
        // arrange
        let datasets = vec![entities::Dataset::new(Path::new("/home/planet"))];
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let mut stater = MockStateStore::new();
        stater
            .expect_get_state()
            .returning(|| state::State::default());
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        let processor = Arc::new(MockProcessor::new());
        let restorer = Arc::new(MockRestorer::new());
        let ctx = Arc::new(GraphContext::new(datasource, appstate, processor, restorer));
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
            r#"query {
                datasets { basepath status }
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
        let res = res.get_field_value("datasets").unwrap();
        let list = res.as_list_value().unwrap();
        assert_eq!(list.len(), 1);
        let object = list[0].as_object_value().unwrap();
        let field = object.get_field_value("basepath").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "/home/planet");
        let field = object.get_field_value("status").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "NONE");
    }

    #[test]
    fn test_query_dataset_status_running() {
        use crate::domain::managers::state;
        // arrange
        let stater = state::StateStoreImpl::new();
        let datasets = vec![entities::Dataset::new(Path::new("/home/planet"))];
        stater.backup_event(state::BackupAction::Start(datasets[0].id.clone()));
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        let processor = Arc::new(MockProcessor::new());
        let restorer = Arc::new(MockRestorer::new());
        let ctx = Arc::new(GraphContext::new(datasource, appstate, processor, restorer));
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
            r#"query {
                datasets { basepath status }
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
        let res = res.get_field_value("datasets").unwrap();
        let list = res.as_list_value().unwrap();
        assert_eq!(list.len(), 1);
        let object = list[0].as_object_value().unwrap();
        let field = object.get_field_value("basepath").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "/home/planet");
        let field = object.get_field_value("status").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "RUNNING");
    }

    #[test]
    fn test_query_dataset_status_error() {
        use crate::domain::managers::state;
        // arrange
        let stater = state::StateStoreImpl::new();
        let datasets = vec![entities::Dataset::new(Path::new("/home/planet"))];
        stater.backup_event(state::BackupAction::Start(datasets[0].id.clone()));
        let err_msg = String::from("oh no");
        stater.backup_event(state::BackupAction::Error(datasets[0].id.clone(), err_msg));
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        let processor = Arc::new(MockProcessor::new());
        let restorer = Arc::new(MockRestorer::new());
        let ctx = Arc::new(GraphContext::new(datasource, appstate, processor, restorer));
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
            r#"query {
                datasets { basepath status errorMessage }
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
        let res = res.get_field_value("datasets").unwrap();
        let list = res.as_list_value().unwrap();
        assert_eq!(list.len(), 1);
        let object = list[0].as_object_value().unwrap();
        let field = object.get_field_value("basepath").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "/home/planet");
        let field = object.get_field_value("status").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "FAILED");
        let field = object.get_field_value("errorMessage").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "oh no");
    }

    #[test]
    fn test_query_datasets_none() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_datasets().returning(move || Ok(Vec::new()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
            r#"query {
                datasets { basepath }
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
        let res = res.get_field_value("datasets").unwrap();
        let list = res.as_list_value().unwrap();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_query_datasets_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_datasets()
            .returning(move || Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
            r#"query {
                datasets { computerId }
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
    fn test_query_record_counts_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_entity_counts().returning(move || {
            Ok(entities::RecordCounts {
                chunk: 5,
                dataset: 1,
                file: 25,
                pack: 1,
                snapshot: 1,
                store: 1,
                tree: 3,
                xattr: 4,
            })
        });
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
            r#"query {
                recordCounts { chunks datasets files packs snapshots trees }
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
        let res = res.get_field_value("recordCounts").unwrap();
        let object = res.as_object_value().unwrap();
        let field = object.get_field_value("chunks").unwrap();
        let value = field.as_scalar_value::<i32>().unwrap();
        assert_eq!(*value, 5);
    }

    #[test]
    fn test_query_record_counts_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_entity_counts()
            .returning(move || Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let (res, errors) = juniper::execute_sync(
            r#"query {
                recordCounts { chunks files trees }
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
    fn test_query_snapshot_some() {
        // arrange
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let mut file_counts: entities::FileCounts = Default::default();
        file_counts.files_below_10k = 110;
        let snapshot = entities::Snapshot::new(None, tree_sha, file_counts);
        let snapshot_sha1 = snapshot.digest.clone();
        let snapshot_sha2 = snapshot.digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(move |_| Ok(Some(snapshot.clone())));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("digest".to_owned(), snapshot_sha2.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"query Snapshot($digest: Checksum!) {
                snapshot(digest: $digest) { fileCount }
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
        let res = res.get_field_value("snapshot").unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("fileCount").unwrap();
        // fileCounts are bigints that comes over the wire as strings
        let value = res.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "110");
    }

    #[test]
    fn test_query_snapshot_none() {
        // arrange
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = entities::Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        let snapshot_sha2 = snapshot.digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(move |_| Ok(None));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("digest".to_owned(), snapshot_sha2.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"query Snapshot($digest: Checksum!) {
                snapshot(digest: $digest) { fileCount }
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
        let res = res.get_field_value("snapshot").unwrap();
        assert!(res.is_null());
    }

    #[test]
    fn test_query_snapshot_err() {
        // arrange
        let snapshot_sha1 = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot_sha2 = snapshot_sha1.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(move |_| Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("digest".to_owned(), snapshot_sha2.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"query Snapshot($digest: Checksum!) {
                snapshot(digest: $digest) { fileCount }
            }"#,
            None,
            &schema,
            &vars,
            &ctx,
        )
        .unwrap();
        // assert
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("snapshot").unwrap();
        assert!(res.is_null());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].error().message().contains("oh no"));
    }

    #[test]
    fn test_query_tree_some() {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let reference = TreeReference::FILE(file_digest);
        let filepath = Path::new("../test/fixtures/lorem-ipsum.txt");
        let entry = entities::TreeEntry::new(filepath, reference);
        let tree = entities::Tree::new(vec![entry], 1);
        let tree_sha1 = tree.digest.clone();
        let tree_sha2 = tree.digest.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_tree()
            .withf(move |d| d == &tree_sha1)
            .returning(move |_| Ok(Some(tree.clone())));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("digest".to_owned(), tree_sha2.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"query Tree($digest: Checksum!) {
                tree(digest: $digest) { entries { name } }
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
        let res = res.get_field_value("tree").unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("entries").unwrap();
        let list = res.as_list_value().unwrap();
        assert_eq!(list.len(), 1);
        let object = list[0].as_object_value().unwrap();
        let field = object.get_field_value("name").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "lorem-ipsum.txt");
    }

    #[test]
    fn test_query_tree_none() {
        // arrange
        let tree_sha1 = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let tree_sha2 = tree_sha1.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_tree()
            .withf(move |d| d == &tree_sha1)
            .returning(move |_| Ok(None));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("digest".to_owned(), tree_sha2.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"query Tree($digest: Checksum!) {
                tree(digest: $digest) { entries { name } }
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
        let res = res.get_field_value("tree").unwrap();
        assert!(res.is_null());
    }

    #[test]
    fn test_query_tree_err() {
        // arrange
        let tree_sha1 = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let tree_sha2 = tree_sha1.clone();
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_tree()
            .withf(move |d| d == &tree_sha1)
            .returning(move |_| Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("digest".to_owned(), tree_sha2.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"query Tree($digest: Checksum!) {
                tree(digest: $digest) { entries { name } }
            }"#,
            None,
            &schema,
            &vars,
            &ctx,
        )
        .unwrap();
        // assert
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("tree").unwrap();
        assert!(res.is_null());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].error().message().contains("oh no"));
    }

    #[test]
    fn test_mutation_define_store_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_store().returning(|_| Ok(()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let properties = vec![PropertyInput {
            name: "basepath".to_owned(),
            value: "/home/planet".to_owned(),
        }];
        let input = StoreInput {
            id: None,
            store_type: "local".to_owned(),
            label: "my local".to_owned(),
            properties,
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Define($input: StoreInput!) {
                defineStore(input: $input) {
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
        mock.expect_put_store().returning(|_| Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let properties = vec![PropertyInput {
            name: "basepath".to_owned(),
            value: "/home/planet".to_owned(),
        }];
        let input = StoreInput {
            id: None,
            store_type: "local".to_owned(),
            label: "my local".to_owned(),
            properties,
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Define($input: StoreInput!) {
                defineStore(input: $input) {
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

    #[test]
    fn test_mutation_update_store_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_store().returning(|_| Ok(()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let properties = vec![PropertyInput {
            name: "basepath".to_owned(),
            value: "/home/planet".to_owned(),
        }];
        let input = StoreInput {
            id: Some("cafebabe".to_owned()),
            store_type: "local".to_owned(),
            label: "my local".to_owned(),
            properties,
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Update($input: StoreInput!) {
                updateStore(input: $input) {
                    id storeType label
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
        let res = res.get_field_value("updateStore").unwrap();
        let object = res.as_object_value().unwrap();
        let field = object.get_field_value("storeType").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "local");
        let field = object.get_field_value("label").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "my local");
    }

    #[test]
    fn test_mutation_update_store_id() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_store().returning(|_| Ok(()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let properties = vec![];
        let input = StoreInput {
            id: None,
            store_type: "local".to_owned(),
            label: "my local".to_owned(),
            properties,
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Update($input: StoreInput!) {
                updateStore(input: $input) { id }
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
        assert!(errors[0]
            .error()
            .message()
            .contains("store without id field"));
    }

    #[test]
    fn test_mutation_update_store_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_store().returning(|_| Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let properties = vec![PropertyInput {
            name: "basepath".to_owned(),
            value: "/home/planet".to_owned(),
        }];
        let input = StoreInput {
            id: Some("cafebabe".to_owned()),
            store_type: "local".to_owned(),
            label: "my local".to_owned(),
            properties,
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Define($input: StoreInput!) {
                defineStore(input: $input) { id }
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

    #[test]
    fn test_mutation_delete_store_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_store().returning(|_| Ok(()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("input".to_owned(), InputValue::scalar("abc123"));
        let (res, errors) = juniper::execute_sync(
            r#"mutation Delete($input: String!) {
                deleteStore(id: $input)
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
        let field = res.get_field_value("deleteStore").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "abc123");
    }

    #[test]
    fn test_mutation_delete_store_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_store()
            .returning(|_| Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("input".to_owned(), InputValue::scalar("abc123"));
        let (res, errors) = juniper::execute_sync(
            r#"mutation Delete($input: String!) {
                deleteStore(id: $input)
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

    #[test]
    fn test_mutation_define_dataset_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        let config: entities::Configuration = Default::default();
        mock.expect_get_configuration()
            .returning(move || Ok(Some(config.clone())));
        mock.expect_put_dataset().returning(|_| Ok(()));
        mock.expect_put_computer_id()
            .with(always(), always())
            .returning(|_, _| Ok(()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let cwd = std::env::current_dir().unwrap();
        let mut ws = cwd.clone();
        ws.push(".tmp");
        let input = DatasetInput {
            id: None,
            basepath: cwd.to_str().unwrap().to_owned(),
            schedules: vec![],
            workspace: None,
            pack_size: BigInt(1048576),
            stores: vec![],
            excludes: vec![],
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Define($input: DatasetInput!) {
                defineDataset(input: $input) {
                    basepath workspace packSize
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
        let res = res.get_field_value("defineDataset").unwrap();
        let object = res.as_object_value().unwrap();
        let field = object.get_field_value("basepath").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, cwd.to_str().unwrap());
        let field = object.get_field_value("workspace").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, ws.to_str().unwrap());
        let field = object.get_field_value("packSize").unwrap();
        // packSize is a bigint that comes over the wire as a string
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "1048576");
    }

    #[test]
    fn test_mutation_define_dataset_store() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        let config: entities::Configuration = Default::default();
        mock.expect_get_configuration()
            .returning(move || Ok(Some(config.clone())));
        mock.expect_get_store().returning(|_| Ok(None));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let cwd = std::env::current_dir().unwrap();
        let input = DatasetInput {
            id: None,
            basepath: cwd.to_str().unwrap().to_owned(),
            schedules: vec![],
            workspace: None,
            pack_size: BigInt(1048576),
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Define($input: DatasetInput!) {
                defineDataset(input: $input) {
                    basepath packSize
                }
            }"#,
            None,
            &schema,
            &vars,
            &ctx,
        )
        .unwrap();
        // assert
        println!("errors: {:?}", errors);
        assert!(res.is_null());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].error().message().contains("store does not exist"));
    }

    #[test]
    fn test_mutation_define_dataset_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        let config: entities::Configuration = Default::default();
        mock.expect_get_configuration()
            .returning(move || Ok(Some(config.clone())));
        mock.expect_put_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let cwd = std::env::current_dir().unwrap();
        let input = DatasetInput {
            id: None,
            basepath: cwd.to_str().unwrap().to_owned(),
            schedules: vec![],
            workspace: None,
            pack_size: BigInt(1048576),
            stores: vec![],
            excludes: vec![],
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Define($input: DatasetInput!) {
                defineDataset(input: $input) { id }
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

    #[test]
    fn test_mutation_update_dataset_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_dataset().returning(|_| Ok(()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let cwd = std::env::current_dir().unwrap();
        let mut ws = cwd.clone();
        ws.push(".tmp");
        let input = DatasetInput {
            id: Some("cafebabe".to_owned()),
            basepath: cwd.to_str().unwrap().to_owned(),
            schedules: vec![],
            workspace: None,
            pack_size: BigInt(1048576),
            stores: vec![],
            excludes: vec![],
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Update($input: DatasetInput!) {
                updateDataset(input: $input) {
                    basepath workspace packSize
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
        let res = res.get_field_value("updateDataset").unwrap();
        let object = res.as_object_value().unwrap();
        let field = object.get_field_value("basepath").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, cwd.to_str().unwrap());
        let field = object.get_field_value("workspace").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, ws.to_str().unwrap());
        let field = object.get_field_value("packSize").unwrap();
        // packSize is a bigint that comes over the wire as a string
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "1048576");
    }

    #[test]
    fn test_mutation_update_dataset_store() {
        // arrange
        let mock = MockEntityDataSource::new();
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let cwd = std::env::current_dir().unwrap();
        let input = DatasetInput {
            id: None,
            basepath: cwd.to_str().unwrap().to_owned(),
            schedules: vec![],
            workspace: None,
            pack_size: BigInt(1048576),
            stores: vec![],
            excludes: vec![],
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Update($input: DatasetInput!) {
                updateDataset(input: $input) {
                    basepath packSize
                }
            }"#,
            None,
            &schema,
            &vars,
            &ctx,
        )
        .unwrap();
        // assert
        println!("errors: {:?}", errors);
        assert!(res.is_null());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].error().message().contains("dataset without id"));
    }

    #[test]
    fn test_mutation_update_dataset_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_put_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        let cwd = std::env::current_dir().unwrap();
        let input = DatasetInput {
            id: Some("cafebabe".to_owned()),
            basepath: cwd.to_str().unwrap().to_owned(),
            schedules: vec![],
            workspace: None,
            pack_size: BigInt(1048576),
            stores: vec![],
            excludes: vec![],
        };
        vars.insert("input".to_owned(), input.to_input_value());
        let (res, errors) = juniper::execute_sync(
            r#"mutation Update($input: DatasetInput!) {
                updateDataset(input: $input) { id }
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

    #[test]
    fn test_mutation_delete_dataset_ok() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_dataset().returning(|_| Ok(()));
        mock.expect_delete_computer_id().returning(|_| Ok(()));
        mock.expect_delete_latest_snapshot().returning(|_| Ok(()));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("input".to_owned(), InputValue::scalar("abc123"));
        let (res, errors) = juniper::execute_sync(
            r#"mutation Delete($input: String!) {
                deleteDataset(id: $input)
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
        let field = res.get_field_value("deleteDataset").unwrap();
        let value = field.as_scalar_value::<String>().unwrap();
        assert_eq!(value, "abc123");
    }

    #[test]
    fn test_mutation_delete_dataset_err() {
        // arrange
        let mut mock = MockEntityDataSource::new();
        mock.expect_delete_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        let ctx = make_context(mock);
        // act
        let schema = create_schema();
        let mut vars = Variables::new();
        vars.insert("input".to_owned(), InputValue::scalar("abc123"));
        let (res, errors) = juniper::execute_sync(
            r#"mutation Delete($input: String!) {
                deleteDataset(id: $input)
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
