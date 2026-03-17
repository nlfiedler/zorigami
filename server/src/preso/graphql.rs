//
// Copyright (c) 2020 Nathan Fiedler
//

//! The `schema` module defines the GraphQL schema and resolvers.

use crate::data::repositories::RecordRepositoryImpl;
use crate::domain::entities::{self, Checksum, TreeReference};
use crate::domain::repositories::RecordRepository;
use crate::domain::sources::EntityDataSource;
use crate::tasks::backup::Scheduler;
use crate::tasks::helpers;
use crate::tasks::restore::{self, Restorer};
use crate::tasks::state::{self, StateStore};
use chrono::prelude::*;
use juniper::{
    EmptySubscription, FieldError, FieldResult, GraphQLEnum, GraphQLInputObject, GraphQLObject,
    GraphQLScalar, ParseScalarResult, ParseScalarValue, RootNode, ScalarToken, ScalarValue, Value,
};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

// Context for the GraphQL schema.
pub struct GraphContext {
    datasource: Arc<dyn EntityDataSource>,
    appstate: Arc<dyn StateStore>,
    processor: Arc<dyn Scheduler>,
    restorer: Arc<dyn Restorer>,
}

impl GraphContext {
    pub fn new(
        datasource: Arc<dyn EntityDataSource>,
        appstate: Arc<dyn StateStore>,
        processor: Arc<dyn Scheduler>,
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

/// An integer type larger than the standard signed 32-bit.
#[derive(Copy, Clone, Debug, Eq, GraphQLScalar, PartialEq)]
#[graphql(with = Self)]
struct BigInt(i64);

impl BigInt {
    #[allow(clippy::wrong_self_convention)]
    fn to_output(&self) -> impl std::fmt::Display + use<> {
        format!("{}", self.0)
    }

    fn from_input(s: &str) -> Result<Self, String> {
        s.parse::<i64>()
            .ok()
            .map(BigInt)
            .ok_or_else(|| format!("Expected `BigInt`, found: {s}"))
    }

    fn parse_token<S: ScalarValue>(value: ScalarToken<'_>) -> ParseScalarResult<S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
}

impl From<BigInt> for u32 {
    fn from(val: BigInt) -> Self {
        val.0 as u32
    }
}

impl From<BigInt> for u64 {
    fn from(val: BigInt) -> Self {
        val.0 as u64
    }
}

impl From<u32> for BigInt {
    fn from(t: u32) -> Self {
        BigInt(i64::from(t))
    }
}

/// A SHA1 or BLAKE3 checksum, with algorithm prefix.
#[derive(GraphQLScalar)]
#[graphql(with = Self, name = "Checksum")]
struct ChecksumGQL(Checksum);

impl ChecksumGQL {
    fn to_output(&self) -> impl std::fmt::Display + use<> {
        format!("{}", self.0)
    }

    fn from_input(s: &str) -> Result<Self, String> {
        Checksum::from_str(s)
            .ok()
            .map(ChecksumGQL)
            .ok_or_else(|| format!("Expected `Checksum`, found: {s}"))
    }

    fn parse_token<S: ScalarValue>(value: ScalarToken<'_>) -> ParseScalarResult<S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
}

/// Reference for a tree entry, such as a file or tree.
#[derive(GraphQLScalar)]
#[graphql(with = Self, name = "TreeReference")]
struct TreeReferenceGQL(TreeReference);

impl TreeReferenceGQL {
    fn to_output(&self) -> impl std::fmt::Display + use<> {
        format!("{}", self.0)
    }

    fn from_input(s: &str) -> Result<Self, String> {
        TreeReference::from_str(s)
            .ok()
            .map(TreeReferenceGQL)
            .ok_or_else(|| format!("Expected `TreeReference`, found: {s}"))
    }

    fn parse_token<S: ScalarValue>(value: ScalarToken<'_>) -> ParseScalarResult<S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
}

#[juniper::graphql_object(description = "A file, directory, or symbolic link within a tree.")]
impl entities::TreeEntry {
    /// Name of the file, directory, or symbolic link.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Modification time of the entry in UTC.
    fn mod_time(&self) -> DateTime<Utc> {
        self.mtime
    }

    /// Reference to the entry itself.
    fn reference(&self) -> TreeReferenceGQL {
        TreeReferenceGQL(self.reference.clone())
    }
}

#[juniper::graphql_object(description = "A set of file system entries in a directory.")]
impl entities::Tree {
    /// Set of entries making up this tree.
    fn entries(&self) -> Vec<entities::TreeEntry> {
        self.entries.clone()
    }
}

#[derive(GraphQLObject)]
/// Number of files whose size is close to the given power of 2.
struct FileSize {
    /// File size category as a power of 2 (such as "1024", "1048576").
    power: String,
    /// Number of files in this size category.
    count: BigInt,
}

#[juniper::graphql_object(description = "Number of files and directories in a snapshot.")]
impl entities::FileCounts {
    fn directories(&self) -> BigInt {
        BigInt(self.directories as i64)
    }

    fn symlinks(&self) -> BigInt {
        BigInt(self.symlinks as i64)
    }

    fn very_small_files(&self) -> BigInt {
        BigInt(self.very_small_files as i64)
    }

    fn file_sizes(&self) -> Vec<FileSize> {
        let mut result: Vec<FileSize> = Vec::new();
        for (bits, count) in self.file_sizes.iter() {
            result.push(FileSize {
                power: format!("{}", 2u64.pow(*bits as u32)),
                count: BigInt(*count as i64),
            });
        }
        result
    }

    fn very_large_files(&self) -> BigInt {
        BigInt(self.very_large_files as i64)
    }
}

#[juniper::graphql_object(description = "A single backup, either in progress or completed.")]
impl entities::Snapshot {
    /// Original computed checksum of the snapshot.
    fn checksum(&self) -> ChecksumGQL {
        ChecksumGQL(self.digest.clone())
    }

    /// The snapshot before this one, if any.
    fn parent(&self) -> Option<ChecksumGQL> {
        self.parent.clone().map(ChecksumGQL)
    }

    /// Time when the snapshot was first created in UTC.
    fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// Time when the snapshot completely finished in UTC.
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
    fn tree(&self) -> ChecksumGQL {
        ChecksumGQL(self.tree.clone())
    }
}

/// Status of the most recent snapshot for a dataset.
#[derive(Copy, Clone, GraphQLEnum)]
enum Status {
    /// Backup has not run yet.
    None,
    /// Backup is still running.
    Running,
    /// Backup has finished.
    Finished,
    /// Backup paused due to schedule.
    Paused,
    /// Backup failed, see `errorMessage` property.
    Failed,
}

#[juniper::graphql_object(description = "Detailed information of the state of the backup.")]
impl state::BackupState {
    /// True if the running backup has been paused.
    fn paused(&self) -> bool {
        self.is_paused()
    }

    /// True if the running backup received a request to stop prematurely.
    fn stop_requested(&self) -> bool {
        self.should_stop()
    }

    /// Number of files that changed in this snapshot.
    #[graphql(name = "changedFiles")]
    fn files_changed(&self) -> BigInt {
        BigInt(self.changed_files() as i64)
    }

    /// Number of pack files uploaded so far.
    #[graphql(name = "packsUploaded")]
    fn uploaded_packs(&self) -> BigInt {
        BigInt(self.packs_uploaded() as i64)
    }

    /// Number of files uploaded so far.
    #[graphql(name = "filesUploaded")]
    fn uploaded_files(&self) -> BigInt {
        BigInt(self.files_uploaded() as i64)
    }

    /// Number of bytes uploaded so far, which may change more often than the
    #[graphql(name = "bytesUploaded")]
    fn uploaded_bytes(&self) -> BigInt {
        BigInt(self.bytes_uploaded() as i64)
    }
}

/// Specifies the policy for retaining snapshots.
#[derive(Copy, Clone, GraphQLEnum)]
enum SnapshotRetentionPolicy {
    /// Retain all snapshots.
    All,
    /// Keep only the N most recent snapshots.
    Count,
    /// Keep only snapshots completed in the last N days.
    Days,
}

#[derive(GraphQLObject)]
struct SnapshotRetention {
    /// Policy for retaining snapshots.
    policy: SnapshotRetentionPolicy,
    /// Value associated with the policy (the N value for "days" policy).
    value: i32,
}

impl From<entities::SnapshotRetention> for SnapshotRetention {
    fn from(retention: entities::SnapshotRetention) -> Self {
        match retention {
            entities::SnapshotRetention::ALL => SnapshotRetention {
                policy: SnapshotRetentionPolicy::All,
                value: 0,
            },
            entities::SnapshotRetention::COUNT(n) => SnapshotRetention {
                policy: SnapshotRetentionPolicy::Count,
                value: n as i32,
            },
            entities::SnapshotRetention::DAYS(n) => SnapshotRetention {
                policy: SnapshotRetentionPolicy::Days,
                value: n as i32,
            },
        }
    }
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
    fn status(&self, #[graphql(ctx)] ctx: &GraphContext) -> Status {
        let redux = ctx.appstate.get_state();
        if let Some(backup) = redux.backups(&self.id) {
            if backup.is_paused() {
                Status::Paused
            } else if backup.had_error() {
                Status::Failed
            } else if backup.end_time().is_none() {
                Status::Running
            } else {
                Status::Finished
            }
        } else {
            Status::None
        }
    }

    /// Detailed state of the backup for this dataset.
    fn backup_state(&self, #[graphql(ctx)] ctx: &GraphContext) -> Option<state::BackupState> {
        let redux = ctx.appstate.get_state();
        redux.backups(&self.id).cloned()
    }

    /// Error message for the most recent snapshot, if any.
    fn error_message(&self, #[graphql(ctx)] ctx: &GraphContext) -> Option<String> {
        let redux = ctx.appstate.get_state();
        redux.backups(&self.id).and_then(|e| e.error_message())
    }

    /// Most recent snapshot for this dataset, if any.
    fn latest_snapshot(&self, #[graphql(ctx)] ctx: &GraphContext) -> Option<entities::Snapshot> {
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        #[allow(clippy::collapsible_if)]
        if let Some(ref digest) = self.snapshot {
            if let Ok(result) = repo.get_snapshot(digest) {
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

    /// Retention policy for snapshots in this data set.
    fn retention(&self) -> SnapshotRetention {
        self.retention.clone().into()
    }
}

#[juniper::graphql_object(
    name = "TimeRange",
    desc = "Range of time in which to run backup. If stopTime is less than startTime, the times span the midnight hour."
)]
impl entities::schedule::TimeRange {
    /// Seconds from midnight at which to start in UTC.
    fn start_time(&self) -> i32 {
        self.start as i32
    }
    /// Seconds from midnight at which to stop in UTC.
    fn stop_time(&self) -> i32 {
        self.stop as i32
    }
}

#[derive(Copy, Clone, GraphQLEnum)]
enum DayOfWeek {
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

impl From<DayOfWeek> for entities::schedule::DayOfWeek {
    fn from(val: DayOfWeek) -> Self {
        match val {
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
enum WeekOfMonth {
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
enum Frequency {
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

/// Specifies how many pack files the pack store will retain.
#[derive(Copy, Clone, GraphQLEnum)]
enum PackRetentionPolicy {
    /// Retain all pack files.
    All,
    /// Keep only pack files uploaded in the last N days.
    Days,
}

#[derive(GraphQLObject)]
struct PackRetention {
    /// Policy for retaining pack files.
    policy: PackRetentionPolicy,
    /// Value associated with the policy (the N value for "days" policy).
    value: i32,
}

impl From<entities::PackRetention> for PackRetention {
    fn from(retention: entities::PackRetention) -> Self {
        match retention {
            entities::PackRetention::ALL => PackRetention {
                policy: PackRetentionPolicy::All,
                value: 0,
            },
            entities::PackRetention::DAYS(n) => PackRetention {
                policy: PackRetentionPolicy::Days,
                value: n as i32,
            },
        }
    }
}

impl From<PackRetention> for entities::PackRetention {
    fn from(retention: PackRetention) -> entities::PackRetention {
        match retention.policy {
            PackRetentionPolicy::All => entities::PackRetention::ALL,
            PackRetentionPolicy::Days => entities::PackRetention::DAYS(retention.value as u16),
        }
    }
}

/// Store defines a location where packs will be saved.
#[derive(GraphQLObject)]
struct Store {
    /// Unique identifier for this store.
    id: String,
    /// The kind of the pack store (such as "local" or "sftp").
    store_type: String,
    /// User-defined label for this store.
    label: String,
    /// Name/value pairs that make up this store configuration.
    properties: Vec<Property>,
    /// Pack retention policy.
    retention: PackRetention,
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
        let retention: PackRetention = store.retention.into();
        Self {
            id: store.id,
            store_type: store.store_type.to_string(),
            label: store.label,
            properties,
            retention,
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

    /// Name of the bucket used for storing the database snapshots.
    fn computer_bucket(&self) -> String {
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
    fn checksum(&self) -> ChecksumGQL {
        ChecksumGQL(self.digest.clone())
    }

    /// List of store-specific coordinates where the pack is saved.
    fn locations(&self) -> Vec<entities::PackLocation> {
        self.locations.clone()
    }
}

#[juniper::graphql_object(description = "A request to restore a file or directory.")]
impl restore::Request {
    /// Digest of the tree containing the entry to restore.
    fn tree(&self) -> ChecksumGQL {
        ChecksumGQL(self.tree.clone())
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

    /// The datetime when the request was completed in UTC.
    fn finished(&self) -> Option<DateTime<Utc>> {
        self.finished
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

pub struct Query;

#[juniper::graphql_object(Context = GraphContext)]
impl Query {
    /// Retrieve the configuration record.
    fn configuration(#[graphql(ctx)] ctx: &GraphContext) -> FieldResult<entities::Configuration> {
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        Ok(repo.get_configuration()?)
    }

    /// Retrieve all dataset definitions.
    fn datasets(#[graphql(ctx)] ctx: &GraphContext) -> FieldResult<Vec<entities::Dataset>> {
        use crate::domain::usecases::get_datasets::GetDatasets;
        use crate::domain::usecases::{NoParams, UseCase};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetDatasets::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let datasets = usecase.call(params)?;
        Ok(datasets)
    }

    /// Retrieve a specific dataset definition.
    fn dataset(
        #[graphql(ctx)] ctx: &GraphContext,
        id: String,
    ) -> FieldResult<Option<entities::Dataset>> {
        use crate::domain::usecases::get_datasets::GetDatasets;
        use crate::domain::usecases::{NoParams, UseCase};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetDatasets::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let result: Vec<entities::Dataset> = usecase.call(params)?;
        Ok(result
            .into_iter()
            .find_map(|s| if s.id == id { Some(s) } else { None }))
    }

    /// Find any packs that are missing from the given store.
    fn missing_packs(
        #[graphql(ctx)] ctx: &GraphContext,
        store_id: String,
    ) -> FieldResult<Vec<entities::Pack>> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::find_missing::{FindMissingPacks, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = FindMissingPacks::new(Box::new(repo));
        let params: Params = Params::new(store_id);
        let result: Vec<entities::Pack> = usecase.call(params)?;
        Ok(result)
    }

    /// Retrieve entry listing a specific pack.
    fn pack(
        #[graphql(ctx)] ctx: &GraphContext,
        dataset_id: String,
        digest: ChecksumGQL,
    ) -> FieldResult<entities::PackFile> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::get_pack::{GetPack, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetPack::new(Box::new(repo));
        let passphrase = helpers::crypto::get_passphrase();
        let params: Params = Params::new(dataset_id, digest.0, passphrase);
        let result: entities::PackFile = usecase.call(params)?;
        Ok(result)
    }

    /// Exhaustively search all pack file entries to the given chunk.
    ///
    /// This is an expensive operation as it scans many records in the database
    /// and downloads every single pack file to find the missing chunk.
    fn scan_packs(
        #[graphql(ctx)] ctx: &GraphContext,
        dataset_id: String,
        digest: ChecksumGQL,
    ) -> FieldResult<Option<ChecksumGQL>> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::scan_packs::{Params, ScanPacks};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = ScanPacks::new(Box::new(repo));
        let passphrase = helpers::crypto::get_passphrase();
        let params: Params = Params::new(dataset_id, digest.0, passphrase);
        let result: Option<Checksum> = usecase.call(params)?;
        Ok(result.map(ChecksumGQL))
    }

    /// Return the number of each type of database record.
    fn record_counts(#[graphql(ctx)] ctx: &GraphContext) -> FieldResult<entities::RecordCounts> {
        use crate::domain::usecases::get_counts::GetCounts;
        use crate::domain::usecases::{NoParams, UseCase};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetCounts::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let counts = usecase.call(params)?;
        Ok(counts)
    }

    /// Query for any pending and recently completed file restore operations.
    fn restores(#[graphql(ctx)] ctx: &GraphContext) -> FieldResult<Vec<restore::Request>> {
        use crate::domain::usecases::query_restores::QueryRestores;
        use crate::domain::usecases::{NoParams, UseCase};
        let usecase = QueryRestores::new(ctx.restorer.clone());
        let params: NoParams = NoParams {};
        let requests: Vec<restore::Request> = usecase.call(params)?;
        Ok(requests)
    }

    /// Retrieve all snapshots for a given data set in chronological order.
    fn snapshots(
        #[graphql(ctx)] ctx: &GraphContext,
        id: String,
    ) -> FieldResult<Vec<entities::Snapshot>> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::get_snapshots::{GetSnapshots, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetSnapshots::new(Box::new(repo));
        let params: Params = Params::new(id);
        let result: Vec<entities::Snapshot> = usecase.call(params)?;
        Ok(result)
    }

    /// Retrieve a specific snapshot.
    fn snapshot(
        #[graphql(ctx)] ctx: &GraphContext,
        digest: ChecksumGQL,
    ) -> FieldResult<Option<entities::Snapshot>> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::get_snapshot::{GetSnapshot, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetSnapshot::new(Box::new(repo));
        let params: Params = Params::new(digest.0);
        let result: Option<entities::Snapshot> = usecase.call(params)?;
        Ok(result)
    }

    /// Retrieve all pack store definitions.
    fn stores(#[graphql(ctx)] ctx: &GraphContext) -> FieldResult<Vec<Store>> {
        use crate::domain::usecases::get_stores::GetStores;
        use crate::domain::usecases::{NoParams, UseCase};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetStores::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let result: Vec<entities::Store> = usecase.call(params)?;
        let stores: Vec<Store> = result.into_iter().map(|s| s.into()).collect();
        Ok(stores)
    }

    /// Retrieve a specific pack store definition.
    fn store(#[graphql(ctx)] ctx: &GraphContext, id: String) -> FieldResult<Option<Store>> {
        use crate::domain::usecases::get_stores::GetStores;
        use crate::domain::usecases::{NoParams, UseCase};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetStores::new(Box::new(repo));
        let params: NoParams = NoParams {};
        let result: Vec<entities::Store> = usecase.call(params)?;
        Ok(result
            .into_iter()
            .find_map(|s| if s.id == id { Some(s.into()) } else { None }))
    }

    /// Retrieve a specific tree.
    fn tree(
        #[graphql(ctx)] ctx: &GraphContext,
        digest: ChecksumGQL,
    ) -> FieldResult<Option<entities::Tree>> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::get_tree::{GetTree, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = GetTree::new(Box::new(repo));
        let params: Params = Params::new(digest.0);
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

#[derive(GraphQLInputObject)]
struct PackRetentionInput {
    /// Policy for retaining pack files.
    policy: PackRetentionPolicy,
    /// Value associated with the policy (the N value for "days" policy).
    value: i32,
}

impl From<PackRetentionInput> for entities::PackRetention {
    fn from(retention: PackRetentionInput) -> entities::PackRetention {
        match retention.policy {
            PackRetentionPolicy::All => entities::PackRetention::ALL,
            PackRetentionPolicy::Days => entities::PackRetention::DAYS(retention.value as u16),
        }
    }
}

/// Store defines a location where packs will be saved.
#[derive(GraphQLInputObject)]
struct StoreInput {
    /// Unique identifier for this store.
    id: String,
    /// The kind of the pack store (such as "local" or "sftp").
    store_type: String,
    /// User-defined label for this store.
    label: String,
    /// Name/value pairs that make up this store configuration.
    properties: Vec<PropertyInput>,
    /// Pack retention policy.
    retention: PackRetentionInput,
}

impl From<StoreInput> for entities::Store {
    fn from(store: StoreInput) -> entities::Store {
        let mut properties: HashMap<String, String> = HashMap::new();
        for prop in store.properties.into_iter() {
            properties.insert(prop.name, prop.value);
        }
        let retention: entities::PackRetention = store.retention.into();
        Self {
            id: store.id,
            store_type: entities::StoreType::from_str(&store.store_type)
                .expect("unknown store type"),
            label: store.label,
            properties,
            retention,
        }
    }
}

#[derive(GraphQLInputObject)]
struct TimeRangeInput {
    /// Seconds from midnight at which to start in UTC.
    start_time: i32,
    /// Seconds from midnight at which to stop in UTC.
    stop_time: i32,
}

impl From<TimeRangeInput> for entities::schedule::TimeRange {
    fn from(val: TimeRangeInput) -> Self {
        // only need to convert negative values to non-negative, the new_secs()
        // constructor will handle out-of-bound values
        let start = if val.start_time < 0 {
            0
        } else {
            val.start_time
        };
        let stop = if val.stop_time < 0 { 0 } else { val.stop_time };
        entities::schedule::TimeRange::new_secs(start as u32, stop as u32)
    }
}

/// New schedule for the dataset. Combine elements to get backups to run on a
/// certain day of the week, month, and/or within a given time range.
#[derive(GraphQLInputObject)]
struct ScheduleInput {
    /// How often to run the backup.
    frequency: Frequency,
    /// Range of time during the day in which to run backup.
    time_range: Option<TimeRangeInput>,
    /// Which week within the month to run the backup.
    week_of_month: Option<WeekOfMonth>,
    /// Which day of the week to run the backup.
    day_of_week: Option<DayOfWeek>,
    /// The day of the month to run the backup.
    day_of_month: Option<i32>,
}

impl From<ScheduleInput> for entities::schedule::Schedule {
    fn from(val: ScheduleInput) -> Self {
        match &val.frequency {
            Frequency::Hourly => entities::schedule::Schedule::Hourly,
            Frequency::Daily => {
                entities::schedule::Schedule::Daily(val.time_range.map(|s| s.into()))
            }
            Frequency::Weekly => {
                let dow = if let Some(dow) = val.day_of_week {
                    Some((dow.into(), val.time_range.map(|s| s.into())))
                } else {
                    None
                };
                entities::schedule::Schedule::Weekly(dow)
            }
            Frequency::Monthly => {
                let dom: Option<(
                    entities::schedule::DayOfMonth,
                    Option<entities::schedule::TimeRange>,
                )> = if let Some(day) = val.day_of_month {
                    Some((
                        entities::schedule::DayOfMonth::from(day as u32),
                        val.time_range.map(|s| s.into()),
                    ))
                } else if let Some(wn) = val.week_of_month {
                    let dow = val.day_of_week.unwrap();
                    let dom = wn.into_dom(dow);
                    Some((dom, val.time_range.map(|s| s.into())))
                } else {
                    None
                };
                entities::schedule::Schedule::Monthly(dom)
            }
        }
    }
}

#[derive(GraphQLInputObject)]
struct SnapshotRetentionInput {
    /// Policy for retaining snapshots.
    policy: SnapshotRetentionPolicy,
    /// Value associated with the policy (the N value for "days" policy).
    value: i32,
}

impl From<SnapshotRetentionInput> for entities::SnapshotRetention {
    fn from(retention: SnapshotRetentionInput) -> entities::SnapshotRetention {
        let value = if retention.value < 0 {
            0
        } else {
            retention.value
        };
        match retention.policy {
            SnapshotRetentionPolicy::All => entities::SnapshotRetention::ALL,
            SnapshotRetentionPolicy::Count => entities::SnapshotRetention::COUNT(value as u16),
            SnapshotRetentionPolicy::Days => entities::SnapshotRetention::DAYS(value as u16),
        }
    }
}

#[derive(GraphQLInputObject)]
struct DatasetInput {
    /// Identifier of dataset to update, null if creating.
    id: Option<String>,
    /// Path that is being backed up.
    basepath: String,
    /// List of schedules to apply to this dataset.
    schedules: Vec<ScheduleInput>,
    /// Path to temporary workspace for backup process.
    workspace: Option<String>,
    /// Desired byte length of pack files.
    pack_size: BigInt,
    /// Identifiers of stores used for saving packs.
    stores: Vec<String>,
    /// List of paths to be excluded from backups. Can include * and ** wildcards.
    excludes: Vec<String>,
    /// Number of snapshots to retain, or retain all if `null`.
    retention: SnapshotRetentionInput,
}

impl From<DatasetInput> for entities::Dataset {
    fn from(val: DatasetInput) -> Self {
        let basepath = std::path::Path::new(&val.basepath);
        let mut ds = entities::Dataset::with_pack_size(basepath, val.pack_size.into());
        ds.id = val.id.unwrap_or(String::from("default"));
        for sched in val.schedules.into_iter() {
            ds.add_schedule(sched.into());
        }
        ds.stores = val.stores;
        ds.excludes = val.excludes;
        if let Some(ws) = val.workspace {
            ds.workspace = std::path::PathBuf::from(ws);
        }
        ds.retention = val.retention.into();
        ds
    }
}

pub struct Mutation;

#[juniper::graphql_object(Context = GraphContext)]
impl Mutation {
    /// Create a new pack store of the given kind (e.g. "local").
    fn new_store(
        #[graphql(ctx)] ctx: &GraphContext,
        kind: String,
        label: String,
        properties: Vec<PropertyInput>,
    ) -> FieldResult<Store> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::new_store::{NewStore, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = NewStore::new(Box::new(repo));
        let mut props: HashMap<String, String> = HashMap::new();
        for prop in properties.into_iter() {
            props.insert(prop.name, prop.value);
        }
        let params: Params = Params::new(kind, label, props);
        let result: entities::Store = usecase.call(params)?;
        Ok(result.into())
    }

    /// Update the pack store label and properties (its kind cannot be changed).
    fn update_store(#[graphql(ctx)] ctx: &GraphContext, store: StoreInput) -> FieldResult<Store> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::update_store::{Params, UpdateStore};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = UpdateStore::new(Box::new(repo));
        let estore: entities::Store = store.into();
        let params: Params = estore.into();
        let result: entities::Store = usecase.call(params)?;
        Ok(result.into())
    }

    /// Perform a basic test of the given store definition.
    ///
    /// Returns "OK" if no error, otherwise the error message.
    fn test_store(#[graphql(ctx)] ctx: &GraphContext, store: StoreInput) -> FieldResult<String> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::test_store::{Params, TestStore};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = TestStore::new(Box::new(repo));
        let estore: entities::Store = store.into();
        let params: Params = estore.into();
        match usecase.call(params) {
            Ok(()) => Ok(String::from("OK")),
            Err(err) => Ok(err.to_string()),
        }
    }

    /// Delete the pack store with the given unique identifier.
    fn delete_store(#[graphql(ctx)] ctx: &GraphContext, id: String) -> FieldResult<bool> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::delete_store::{DeleteStore, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = DeleteStore::new(Box::new(repo));
        let params: Params = Params::new(id);
        usecase.call(params)?;
        Ok(true)
    }

    /// Create a new data set with all default properties.
    fn new_dataset(#[graphql(ctx)] ctx: &GraphContext) -> FieldResult<entities::Dataset> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::new_dataset::{NewDataset, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = NewDataset::new(Box::new(repo));
        let basepath = std::path::PathBuf::from(".");
        let schedules: Vec<entities::schedule::Schedule> = vec![];
        let pack_size: u64 = 64 * 1048756;
        let stores: Vec<String> = vec![];
        let excludes: Vec<String> = vec![];
        let params: Params = Params::new(basepath, schedules, pack_size, stores, excludes);
        let result: entities::Dataset = usecase.call(params)?;
        Ok(result)
    }

    /// Update an existing dataset with the given configuration.
    fn update_dataset(
        #[graphql(ctx)] ctx: &GraphContext,
        dataset: DatasetInput,
    ) -> FieldResult<entities::Dataset> {
        if dataset.id.is_none() {
            return Err(FieldError::new(
                "Cannot update dataset without id field",
                Value::null(),
            ));
        }
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::update_dataset::{Params, UpdateDataset};
        let datasource = ctx.datasource.clone();
        let edataset: entities::Dataset = dataset.into();
        let repo = RecordRepositoryImpl::new(datasource);
        let usecase = UpdateDataset::new(Box::new(repo));
        let params: Params = edataset.into();
        let result = usecase.call(params)?;
        Ok(result)
    }

    /// Delete the dataset with the given identifier, returning the identifier.
    fn delete_dataset(#[graphql(ctx)] ctx: &GraphContext, id: String) -> FieldResult<String> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::delete_dataset::{DeleteDataset, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = DeleteDataset::new(Box::new(repo));
        let params: Params = Params::new(id.clone());
        usecase.call(params)?;
        Ok(id)
    }

    /// Begin the backup procedure for the dataset with the given identifier.
    fn start_backup(#[graphql(ctx)] ctx: &GraphContext, id: String) -> FieldResult<bool> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::start_backup::{Params, StartBackup};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = StartBackup::new(Arc::new(repo), ctx.appstate.clone(), ctx.processor.clone());
        let params: Params = Params::new(id);
        usecase.call(params)?;
        Ok(true)
    }

    /// Signal the running backup for the given dataset to stop prematurely.
    fn stop_backup(#[graphql(ctx)] ctx: &GraphContext, id: String) -> FieldResult<bool> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::stop_backup::{Params, StopBackup};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = StopBackup::new(Box::new(repo), ctx.appstate.clone());
        let params: Params = Params::new(id);
        usecase.call(params)?;
        Ok(true)
    }

    /// Restore the database from the most recent snapshot.
    fn restore_database(
        #[graphql(ctx)] ctx: &GraphContext,
        store_id: String,
    ) -> FieldResult<String> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::restore_database::{Params, RestoreDatabase};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let passphrase = helpers::crypto::get_passphrase();
        let usecase = RestoreDatabase::new(Box::new(repo));
        let params: Params = Params::new(store_id, ctx.appstate.clone(), passphrase);
        let result = usecase.call(params)?;
        Ok(result)
    }

    /// Enqueue a request to restore the given file or directory tree.
    fn restore_files(
        #[graphql(ctx)] ctx: &GraphContext,
        tree: ChecksumGQL,
        entry: String,
        filepath: String,
        dataset: String,
    ) -> FieldResult<bool> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::restore_files::{Params, RestoreFiles};
        let usecase = RestoreFiles::new(ctx.restorer.clone());
        let fpath = PathBuf::from(filepath);
        let params: Params = Params::new(tree.0.clone(), entry.clone(), fpath, dataset);
        usecase.call(params)?;
        Ok(true)
    }

    /// Cancel the pending restore request that matches the given values.
    fn cancel_restore(
        #[graphql(ctx)] ctx: &GraphContext,
        tree: ChecksumGQL,
        entry: String,
        filepath: String,
        dataset: String,
    ) -> FieldResult<bool> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::cancel_restore::{CancelRestore, Params};
        let usecase = CancelRestore::new(ctx.restorer.clone());
        let fpath = PathBuf::from(filepath);
        let params: Params = Params::new(tree.0.clone(), entry.clone(), fpath, dataset);
        let result = usecase.call(params)?;
        Ok(result)
    }

    /// Change the store from old to new for all matching pack records.
    ///
    /// This is a dangerous action and should be used very carefully.
    fn reassign_packs(
        #[graphql(ctx)] ctx: &GraphContext,
        source_id: String,
        target_id: String,
    ) -> FieldResult<i32> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::reassign_packs::{Params, ReassignPacks};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = ReassignPacks::new(Box::new(repo));
        let params: Params = Params::new(source_id, target_id);
        let result: u64 = usecase.call(params)?;
        // let's hope we never update more than 2 billion pack records
        #[allow(clippy::useless_conversion)]
        let result_i32: i32 = result
            .try_into()
            .map_or(2_147_483_647_i32, |v: u64| v as i32);
        Ok(result_i32)
    }

    /// Restore any missing packs, copying from the other pack store.
    fn restore_packs(
        #[graphql(ctx)] ctx: &GraphContext,
        source_id: String,
        target_id: String,
    ) -> FieldResult<Vec<entities::Pack>> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::restore_missing::{Params, RestoreMissingPacks};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = RestoreMissingPacks::new(Box::new(repo));
        let params: Params = Params::new(source_id, target_id);
        let result: Vec<entities::Pack> = usecase.call(params)?;
        Ok(result)
    }

    /// Remove extraneous packs from the given pack store.
    fn prune_extra(#[graphql(ctx)] ctx: &GraphContext, store_id: String) -> FieldResult<i32> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::prune_extra::{Params, PruneExtraPacks};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = PruneExtraPacks::new(Box::new(repo));
        let params: Params = Params::new(store_id);
        let result: u32 = usecase.call(params)?;
        Ok(result as i32)
    }

    /// Apply the retention policy to the named dataset, pruning snapshots and
    /// everything that is no longer reachable (except for packs).
    ///
    /// Returns the number of snapshots removed from the dataset.
    fn prune_snapshots(#[graphql(ctx)] ctx: &GraphContext, id: String) -> FieldResult<i32> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::prune_snapshots::{Params, PruneSnapshots};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let usecase = PruneSnapshots::new(Box::new(repo));
        let params: Params = Params::new(id.clone());
        let count = usecase.call(params)?;
        Ok(count as i32)
    }

    /// Create a missing file record from the given information.
    ///
    /// This will fetch the given pack file to verify the chunk is contained
    /// therein, as well as to get the file size. This only works for files
    /// containing a single chunk.
    fn insert_file(
        #[graphql(ctx)] ctx: &GraphContext,
        dataset: String,
        chunk_digest: ChecksumGQL,
        pack_digest: ChecksumGQL,
    ) -> FieldResult<bool> {
        use crate::domain::usecases::UseCase;
        use crate::domain::usecases::insert_file::{InsertFile, Params};
        let repo = RecordRepositoryImpl::new(ctx.datasource.clone());
        let passphrase = helpers::crypto::get_passphrase();
        let usecase = InsertFile::new(Box::new(repo));
        let params: Params = Params::new(dataset, chunk_digest.0, pack_digest.0, passphrase);
        usecase.call(params)?;
        Ok(true)
    }
}

pub type Schema = RootNode<Query, Mutation, EmptySubscription<GraphContext>>;

/// Create the GraphQL schema.
pub fn create_schema() -> Schema {
    let schema = Schema::new(Query {}, Mutation {}, EmptySubscription::new());
    if let Ok(path) = std::env::var("GENERATE_SDL") {
        let mut file = std::fs::File::create(&path).expect("create file");
        file.write_all(b"#\n# GENERATED FILE, DO NOT EDIT\n#\n\n")
            .expect("write_all header");
        file.write_all(schema.as_sdl().as_bytes())
            .expect("write_all schema");
        println!("GraphQL schema written to {path}");
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::PackRetention;
    use crate::domain::sources::MockEntityDataSource;
    use crate::tasks::backup::scheduler::MockScheduler;
    use crate::tasks::restore::MockRestorer;
    use crate::tasks::state::MockStateStore;
    use anyhow::anyhow;
    use juniper::{FieldError, FromInputValue, InputValue, ToInputValue, Variables};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    fn make_context(mock: MockEntityDataSource) -> Arc<GraphContext> {
        // build the most basic GraphContext
        // if this turns out to be too limited, create a new builder
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let appstate = Arc::new(MockStateStore::new());
        let processor = Arc::new(MockScheduler::new());
        let restorer = Arc::new(MockRestorer::new());
        Arc::new(GraphContext::new(datasource, appstate, processor, restorer))
    }

    #[test]
    fn test_bigint_scalar() {
        let iv: InputValue<juniper::DefaultScalarValue> =
            juniper::InputValue::Scalar(juniper::DefaultScalarValue::String("1048576".to_owned()));
        let option: Result<BigInt, FieldError> = BigInt::from_input_value(&iv);
        assert!(option.is_ok());
        let actual = option.unwrap();
        assert_eq!(actual, BigInt(1048576));

        // not a number
        let iv: InputValue<juniper::DefaultScalarValue> =
            juniper::InputValue::Scalar(juniper::DefaultScalarValue::String("madokami".to_owned()));
        let option: Result<BigInt, FieldError> = BigInt::from_input_value(&iv);
        assert!(option.is_err());
    }

    #[test]
    fn test_checksum_scalar() {
        let iv: InputValue<juniper::DefaultScalarValue> = juniper::InputValue::Scalar(
            juniper::DefaultScalarValue::String("sha1-cafebabe".to_owned()),
        );
        let option: Result<ChecksumGQL, FieldError> = ChecksumGQL::from_input_value(&iv);
        assert!(option.is_ok());
        let actual = option.unwrap();
        assert!(actual.0.is_sha1());

        // missing algorithm prefix
        let iv: InputValue<juniper::DefaultScalarValue> =
            juniper::InputValue::Scalar(juniper::DefaultScalarValue::String("cafebabe".to_owned()));
        let option: Result<ChecksumGQL, FieldError> = ChecksumGQL::from_input_value(&iv);
        assert!(option.is_err());
    }

    #[test]
    fn test_treereference_scalar() {
        let iv: InputValue<juniper::DefaultScalarValue> = juniper::InputValue::Scalar(
            juniper::DefaultScalarValue::String("tree-sha1-cafebabe".to_owned()),
        );
        let option: Result<TreeReferenceGQL, FieldError> = TreeReferenceGQL::from_input_value(&iv);
        assert!(option.is_ok());
        let actual = option.unwrap();
        assert!(actual.0.is_tree());

        // missing entry type prefix
        let iv: InputValue<juniper::DefaultScalarValue> = juniper::InputValue::Scalar(
            juniper::DefaultScalarValue::String("sha1-cafebabe".to_owned()),
        );
        let option: Result<TreeReferenceGQL, FieldError> = TreeReferenceGQL::from_input_value(&iv);
        assert!(option.is_err());
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
        let actual = res.as_scalar().unwrap().try_as_str().unwrap();
        let username = whoami::username().unwrap_or("charlie".into());
        let hostname = whoami::hostname().unwrap_or("localhost".into());
        let expected = entities::Configuration::generate_unique_id(&username, &hostname);
        assert_eq!(actual, expected);
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
            retention: PackRetention::ALL,
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
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
        assert_eq!(value, "local");
        let field = object.get_field_value("label").unwrap();
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
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
        use crate::tasks::state;
        // arrange
        let datasets = vec![entities::Dataset::new(Path::new("/home/planet"))];
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let mut stater = MockStateStore::new();
        stater.expect_get_state().returning(state::State::default);
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        let processor = Arc::new(MockScheduler::new());
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
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
        assert_eq!(value, "/home/planet");
        let field = object.get_field_value("status").unwrap();
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
        assert_eq!(value, "NONE");
    }

    #[test]
    fn test_query_dataset_status_running() {
        use crate::tasks::state;
        // arrange
        let stater = state::StateStoreImpl::new();
        let datasets = vec![entities::Dataset::new(Path::new("/home/planet"))];
        stater.backup_event(state::BackupAction::Start(datasets[0].id.clone()));
        let mut mock = MockEntityDataSource::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let datasource: Arc<dyn EntityDataSource> = Arc::new(mock);
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        let processor = Arc::new(MockScheduler::new());
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
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
        assert_eq!(value, "/home/planet");
        let field = object.get_field_value("status").unwrap();
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
        assert_eq!(value, "RUNNING");
    }

    #[test]
    fn test_query_dataset_status_error() {
        use crate::tasks::state;
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
        let processor = Arc::new(MockScheduler::new());
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
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
        assert_eq!(value, "/home/planet");
        let field = object.get_field_value("status").unwrap();
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
        assert_eq!(value, "FAILED");
        let field = object.get_field_value("errorMessage").unwrap();
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
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
                datasets { id }
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
        let value = field.as_scalar().unwrap().try_to_int().unwrap();
        assert_eq!(value, 5);
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
        let file_counts = entities::FileCounts {
            directories: 4,
            symlinks: 6,
            very_small_files: 100,
            very_large_files: 10,
            ..Default::default()
        };
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
        vars.insert(
            "digest".to_owned(),
            ChecksumGQL(snapshot_sha2).to_input_value(),
        );
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
        let value = res.as_scalar().unwrap().try_as_str().unwrap();
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
        vars.insert(
            "digest".to_owned(),
            ChecksumGQL(snapshot_sha2).to_input_value(),
        );
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
        vars.insert(
            "digest".to_owned(),
            ChecksumGQL(snapshot_sha2).to_input_value(),
        );
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
        let b3sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::BLAKE3(String::from(b3sum));
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
        vars.insert("digest".to_owned(), ChecksumGQL(tree_sha2).to_input_value());
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
        let value = field.as_scalar().unwrap().try_as_str().unwrap();
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
        vars.insert("digest".to_owned(), ChecksumGQL(tree_sha2).to_input_value());
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
        vars.insert("digest".to_owned(), ChecksumGQL(tree_sha2).to_input_value());
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
}
