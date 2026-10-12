//
// Copyright (c) 2020 Nathan Fiedler
//

//! The hand-written (de)serializers here serve the purpose of creating very
//! concise binary representations of the entities when storing to a key/value
//! store. This is in contrast to serde for wire protocols like HTTP, in which
//! JSON or similar is appropriate and needs no further refinement. For those
//! use cases, having the entity key as part of the serialized output is both
//! appropriate and often necessary. Likewise, storing records in a relational
//! database will be significantly different, marshalling entities as typed
//! values stored in tables, and typically using their keys as the primary key
//! for the table.
//!
//! The use of custom (de)serializers for the entities allows for more easily
//! dealing with changes to fields, such as adding, removing, or changing the
//! types of fields. With binary object representation libraries like speedy or
//! borsh, that becomes more difficult. By using a format that is similar to a
//! collection of type-length-value rows, changes to entities can easily be
//! accommodated as necessary. While some other crates have macros for skipping
//! a field, or defaulting a missing field, there is little else available for
//! dealing with more complex changes.
//!
//! By using the ciborium crate, representing various types of fields is easy.
//! In particular, there are nulls, arrays, maps, bools, bytes, and so on.
//!
//! The exception is the tree, which accounts for the bulk of the records by
//! size and so is written in a more compact format, described in the `Model`
//! implementation for `Tree`. Tree records in the original CBOR format are
//! still readable.
//!
//! Rust crates that support concise binary object representation:
//!
//! * https://github.com/bincode-org/bincode
//! * https://github.com/near/borsh-rs
//! * https://github.com/koute/speedy

use crate::domain::entities::schedule::{DayOfMonth, DayOfWeek, Schedule, TimeRange};
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, FileCounts, Pack, PackLocation, PackRetention,
    Snapshot, SnapshotRetention, Store, StoreType, Tree, TreeEntry, TreeReference,
};
use crate::domain::services::buckets::BucketNamingPolicy;
use anyhow::{Error, anyhow};
use chrono::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

///
/// Trait for entities that can be serialized to and from bytes.
///
/// Specifically to be used when storing entities in a key/value store in which
/// the entity key (its unique identifier) will be used to form the key when
/// storing the record. Hence, [Model::to_bytes()] should _not_ emit the key as
/// part of the value. The key will be provided to [Model::from_bytes()] upon
/// deserialization.
///
pub trait Model: Sized {
    ///
    /// Deserializes a sequence of bytes to return a value of this type.
    ///
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error>;
    ///
    /// Serializes this value into a sequence of bytes.
    ///
    fn to_bytes(&self) -> Result<Vec<u8>, Error>;
}

impl Model for Configuration {
    fn from_bytes(_key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use ciborium::Value;

        let mut config: Configuration = Default::default();
        let mut policy_kind: Option<String> = None;
        let mut policy_days: Option<usize> = None;
        let mut policy_limit: Option<usize> = None;

        let raw_value: Value =
            ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
        let as_tuple_vec: Vec<(Value, Value)> = raw_value
            .into_map()
            .map_err(|_| anyhow!("value: cbor into_map() error"))?;

        for (key, value) in as_tuple_vec.into_iter() {
            if let Some(name) = key.as_text() {
                if name == "hostname" {
                    // hostname
                    let hostname: String = value
                        .into_text()
                        .map_err(|_| anyhow!("hostname: cbor into_text() error"))?;
                    config.hostname = hostname;
                } else if name == "username" {
                    // username
                    let username: String = value
                        .into_text()
                        .map_err(|_| anyhow!("username: cbor into_text() error"))?;
                    config.username = username;
                } else if name == "computer_id" {
                    // computer_id
                    let computer_id: String = value
                        .into_text()
                        .map_err(|_| anyhow!("computer_id: cbor into_text() error"))?;
                    config.computer_id = computer_id;
                } else if name == "policy" {
                    let kind: String = value
                        .into_text()
                        .map_err(|_| anyhow!("policy: cbor into_text() error"))?;
                    policy_kind = Some(kind);
                } else if name == "days" {
                    let iv = value
                        .into_integer()
                        .map_err(|_| anyhow!("days: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    policy_days = Some(ii as usize);
                } else if name == "limit" {
                    let iv = value
                        .into_integer()
                        .map_err(|_| anyhow!("limit: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    policy_limit = Some(ii as usize);
                } else if name == "timezone" {
                    let tz: String = value
                        .into_text()
                        .map_err(|_| anyhow!("timezone: cbor into_text() error"))?;
                    config.timezone = Some(tz);
                }
            }
        }

        config.bucket_naming = match policy_kind.as_deref() {
            Some("randompool") => policy_limit.map(BucketNamingPolicy::RandomPool),
            Some("scheduled") => policy_days.map(BucketNamingPolicy::Scheduled),
            Some("scheduledpool") => match (policy_days, policy_limit) {
                (Some(days), Some(limit)) => {
                    Some(BucketNamingPolicy::ScheduledRandomPool { days, limit })
                }
                _ => None,
            },
            _ => None,
        };

        Ok(config)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value;
        let mut fields: Vec<(Value, Value)> = vec![
            (
                Value::Text("hostname".into()),
                Value::Text(self.hostname.clone()),
            ),
            (
                Value::Text("username".into()),
                Value::Text(self.username.clone()),
            ),
            (
                Value::Text("computer_id".into()),
                Value::Text(self.computer_id.clone()),
            ),
        ];

        if let Some(tz) = &self.timezone {
            fields.push((Value::Text("timezone".into()), Value::Text(tz.clone())));
        }

        if let Some(policy) = &self.bucket_naming {
            match policy {
                BucketNamingPolicy::RandomPool(limit) => {
                    fields.push((
                        Value::Text("policy".into()),
                        Value::Text("randompool".into()),
                    ));
                    fields.push((
                        Value::Text("limit".into()),
                        Value::Integer((*limit as u64).into()),
                    ));
                }
                BucketNamingPolicy::Scheduled(days) => {
                    fields.push((
                        Value::Text("policy".into()),
                        Value::Text("scheduled".into()),
                    ));
                    fields.push((
                        Value::Text("days".into()),
                        Value::Integer((*days as u64).into()),
                    ));
                }
                BucketNamingPolicy::ScheduledRandomPool { days, limit } => {
                    fields.push((
                        Value::Text("policy".into()),
                        Value::Text("scheduledpool".into()),
                    ));
                    fields.push((
                        Value::Text("days".into()),
                        Value::Integer((*days as u64).into()),
                    ));
                    fields.push((
                        Value::Text("limit".into()),
                        Value::Integer((*limit as u64).into()),
                    ));
                }
            }
        }

        let doc = Value::Map(fields);
        let mut encoded: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&doc, &mut encoded)
            .map_err(|err| anyhow!("cbor write error: {}", err))?;
        Ok(encoded)
    }
}

impl Model for Schedule {
    fn from_bytes(_key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use ciborium::Value;

        let raw_value: Value =
            ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
        let mut as_tuple_vec: Vec<(Value, Value)> = raw_value
            .into_map()
            .map_err(|_| anyhow!("value: cbor into_map() error"))?;
        #[allow(clippy::collapsible_if)]
        if let Some((code, params)) = as_tuple_vec.pop() {
            if let Some(code_str) = code.as_text() {
                match code_str {
                    "h" => {
                        return Ok(Schedule::Hourly);
                    }
                    "d" => match params {
                        Value::Array(arr) => {
                            let iv = arr[0]
                                .clone()
                                .into_integer()
                                .map_err(|_| anyhow!("ch: cbor into_integer() error"))?;
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let start_secs = ii as u32;
                            let iv = arr[1]
                                .clone()
                                .into_integer()
                                .map_err(|_| anyhow!("ch: cbor into_integer() error"))?;
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let stop_secs = ii as u32;
                            return Ok(Schedule::Daily(Some(TimeRange::new_secs(
                                start_secs, stop_secs,
                            ))));
                        }
                        _ => {
                            return Ok(Schedule::Daily(None));
                        }
                    },
                    "w" => match params {
                        Value::Array(arr) => {
                            let iv = arr[0]
                                .clone()
                                .into_integer()
                                .map_err(|_| anyhow!("ch: cbor into_integer() error"))?;
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let day_of_week = DayOfWeek::from(ii as u32);
                            let iv = arr[1]
                                .clone()
                                .into_integer()
                                .map_err(|_| anyhow!("ch: cbor into_integer() error"))?;
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let start_secs = ii as u32;
                            let iv = arr[2]
                                .clone()
                                .into_integer()
                                .map_err(|_| anyhow!("ch: cbor into_integer() error"))?;
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let stop_secs = ii as u32;
                            return Ok(Schedule::Weekly(Some((
                                day_of_week,
                                Some(TimeRange::new_secs(start_secs, stop_secs)),
                            ))));
                        }
                        Value::Integer(iv) => {
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let day_of_week = DayOfWeek::from(ii as u32);
                            return Ok(Schedule::Weekly(Some((day_of_week, None))));
                        }
                        _ => {
                            return Ok(Schedule::Weekly(None));
                        }
                    },
                    "m" => match params {
                        Value::Array(arr) => {
                            let iv = arr[0]
                                .clone()
                                .into_integer()
                                .map_err(|_| anyhow!("ch: cbor into_integer() error"))?;
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let day_of_month = DayOfMonth::decode_from_u16(ii as u16);
                            let iv = arr[1]
                                .clone()
                                .into_integer()
                                .map_err(|_| anyhow!("ch: cbor into_integer() error"))?;
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let start_secs = ii as u32;
                            let iv = arr[2]
                                .clone()
                                .into_integer()
                                .map_err(|_| anyhow!("ch: cbor into_integer() error"))?;
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let stop_secs = ii as u32;
                            return Ok(Schedule::Monthly(Some((
                                day_of_month,
                                Some(TimeRange::new_secs(start_secs, stop_secs)),
                            ))));
                        }
                        Value::Integer(iv) => {
                            let ii: i128 = ciborium::value::Integer::into(iv);
                            let day_of_month = DayOfMonth::decode_from_u16(ii as u16);
                            return Ok(Schedule::Monthly(Some((day_of_month, None))));
                        }
                        _ => {
                            return Ok(Schedule::Monthly(None));
                        }
                    },
                    _ => {
                        return Ok(Schedule::Hourly);
                    }
                };
            }
        }

        // if all else fails, return something sensible
        Ok(Schedule::Hourly)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value;
        let mut fields: Vec<(Value, Value)> = vec![];

        match self {
            Schedule::Hourly => {
                fields.push((Value::Text("h".into()), Value::Null));
            }
            Schedule::Daily(otr) => {
                if let Some(time_range) = otr {
                    fields.push((
                        Value::Text("d".into()),
                        Value::Array(vec![
                            Value::Integer(time_range.start.into()),
                            Value::Integer(time_range.stop.into()),
                        ]),
                    ));
                } else {
                    fields.push((Value::Text("d".into()), Value::Null));
                }
            }
            Schedule::Weekly(odowotr) => {
                if let Some((dow, otr)) = odowotr {
                    let dow_u32 = dow.number_from_sunday();
                    if let Some(time_range) = otr {
                        fields.push((
                            Value::Text("w".into()),
                            Value::Array(vec![
                                Value::Integer(dow_u32.into()),
                                Value::Integer(time_range.start.into()),
                                Value::Integer(time_range.stop.into()),
                            ]),
                        ));
                    } else {
                        fields.push((Value::Text("w".into()), Value::Integer(dow_u32.into())));
                    }
                } else {
                    fields.push((Value::Text("w".into()), Value::Null));
                }
            }
            Schedule::Monthly(odomotr) => {
                if let Some((dom, otr)) = odomotr {
                    let dom_u16 = dom.encode_into_u16();
                    if let Some(time_range) = otr {
                        fields.push((
                            Value::Text("m".into()),
                            Value::Array(vec![
                                Value::Integer(dom_u16.into()),
                                Value::Integer(time_range.start.into()),
                                Value::Integer(time_range.stop.into()),
                            ]),
                        ));
                    } else {
                        fields.push((Value::Text("m".into()), Value::Integer(dom_u16.into())));
                    }
                } else {
                    fields.push((Value::Text("m".into()), Value::Null));
                }
            }
        }

        let doc = Value::Map(fields);
        let mut encoded: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&doc, &mut encoded)
            .map_err(|err| anyhow!("cbor write error: {}", err))?;
        Ok(encoded)
    }
}

impl Model for Store {
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use ciborium::Value;
        use std::str::FromStr;

        let store_id = str::from_utf8(key)?.to_owned();
        let mut store_type = StoreType::LOCAL;
        let mut store_label = String::from("default");
        let mut store_props: HashMap<String, String> = HashMap::new();
        let mut retention = PackRetention::ALL;

        let raw_value: Value =
            ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
        let as_tuple_vec: Vec<(Value, Value)> = raw_value
            .into_map()
            .map_err(|_| anyhow!("value: cbor into_map() error"))?;

        for (key, value) in as_tuple_vec.into_iter() {
            if let Some(name) = key.as_text() {
                if name == "store_type" {
                    // store_type
                    let store_type_str: String = value
                        .into_text()
                        .map_err(|_| anyhow!("store_type: cbor into_text() error"))?;
                    store_type = StoreType::from_str(&store_type_str)?;
                } else if name == "label" {
                    // label
                    store_label = value
                        .into_text()
                        .map_err(|_| anyhow!("label: cbor into_text() error"))?;
                } else if name == "properties" {
                    // properties
                    let properties: Vec<(Value, Value)> = value
                        .into_map()
                        .map_err(|_| anyhow!("properties: cbor into_map() error"))?;
                    for (name, value) in properties.into_iter() {
                        let name_str: String = name
                            .into_text()
                            .map_err(|_| anyhow!("prop-name: cbor into_text() error"))?;
                        let value_str: String = value
                            .into_text()
                            .map_err(|_| anyhow!("prop-value: cbor into_text() error"))?;
                        store_props.insert(name_str, value_str);
                    }
                } else if name == "retain_all" {
                    // retention (all)
                    retention = PackRetention::ALL;
                } else if name == "retain_days" {
                    // retention (days)
                    let iv: ciborium::value::Integer = value
                        .into_integer()
                        .map_err(|_| anyhow!("retain_days: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    retention = PackRetention::DAYS(ii as u16);
                }
            }
        }

        Ok(Store {
            id: store_id,
            store_type,
            label: store_label,
            properties: store_props,
            retention,
        })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value;
        // do not emit the key since that is part of the data store
        let mut fields: Vec<(Value, Value)> = vec![];

        // store_type
        fields.push((
            Value::Text("store_type".into()),
            Value::Text(self.store_type.to_string()),
        ));

        // label
        fields.push((Value::Text("label".into()), Value::Text(self.label.clone())));

        // properties
        let mut properties: Vec<(Value, Value)> = vec![];
        for (name, value) in self.properties.iter() {
            properties.push((Value::Text(name.to_owned()), Value::Text(value.to_owned())));
        }
        fields.push((Value::Text("properties".into()), Value::Map(properties)));

        // retention
        match self.retention {
            PackRetention::ALL => {
                fields.push((Value::Text("retain_all".into()), Value::Null));
            }
            PackRetention::DAYS(days) => {
                fields.push((
                    Value::Text("retain_days".into()),
                    Value::Integer(days.into()),
                ));
            }
        }

        let doc = Value::Map(fields);
        let mut encoded: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&doc, &mut encoded)
            .map_err(|err| anyhow!("cbor write error: {}", err))?;
        Ok(encoded)
    }
}

impl Model for Dataset {
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use ciborium::Value;

        let mut dataset: Dataset = Dataset {
            id: str::from_utf8(key)?.to_owned(),
            ..Default::default()
        };

        let raw_value: Value =
            ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
        let as_tuple_vec: Vec<(Value, Value)> = raw_value
            .into_map()
            .map_err(|_| anyhow!("value: cbor into_map() error"))?;

        for (key, value) in as_tuple_vec.into_iter() {
            if let Some(name) = key.as_text() {
                if name == "basepath" {
                    // basepath
                    let basepath: String = value
                        .into_text()
                        .map_err(|_| anyhow!("basepath: cbor into_text() error"))?;
                    dataset.basepath = PathBuf::from(basepath);
                } else if name == "schedules" {
                    // schedules
                    let schedules: Vec<Value> = value
                        .into_array()
                        .map_err(|_| anyhow!("schedules: cbor into_array() error"))?;
                    let unused_key: Vec<u8> = vec![];
                    for sched in schedules.into_iter() {
                        let as_hex: Vec<u8> = sched
                            .into_bytes()
                            .map_err(|_| anyhow!("schedule entry: cbor into_bytes() error"))?;
                        let schedule = Schedule::from_bytes(&unused_key, &as_hex)?;
                        dataset.schedules.push(schedule);
                    }
                } else if name == "snapshot" {
                    // snapshot
                    if !value.is_null() {
                        let as_bytes = value
                            .into_bytes()
                            .map_err(|_| anyhow!("snapshot: cbor into_bytes() error"))?;
                        dataset.snapshot = Some(Checksum::from_hex(&as_bytes)?);
                    }
                } else if name == "workspace" {
                    // workspace
                    let workspace: String = value
                        .into_text()
                        .map_err(|_| anyhow!("workspace: cbor into_text() error"))?;
                    dataset.workspace = PathBuf::from(workspace);
                } else if name == "chunksize" {
                    // chunk_size
                    let iv: ciborium::value::Integer = value
                        .into_integer()
                        .map_err(|_| anyhow!("chunksize: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    dataset.chunk_size = ii as usize;
                } else if name == "packsize" {
                    // pack_size
                    let iv: ciborium::value::Integer = value
                        .into_integer()
                        .map_err(|_| anyhow!("packsize: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    dataset.pack_size = ii as u64;
                } else if name == "datashards" {
                    // data_shards
                    let iv: ciborium::value::Integer = value
                        .into_integer()
                        .map_err(|_| anyhow!("datashards: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    dataset.data_shards = ii as u8;
                } else if name == "parityshards" {
                    // parity_shards
                    let iv: ciborium::value::Integer = value
                        .into_integer()
                        .map_err(|_| anyhow!("parityshards: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    dataset.parity_shards = ii as u8;
                } else if name == "stores" {
                    // stores
                    let stores: Vec<Value> = value
                        .into_array()
                        .map_err(|_| anyhow!("stores: cbor into_array() error"))?;
                    for st_val in stores.into_iter() {
                        let store: String = st_val
                            .into_text()
                            .map_err(|_| anyhow!("store entry: cbor into_text() error"))?;
                        dataset.stores.push(store);
                    }
                } else if name == "excludes" {
                    // excludes
                    let excludes: Vec<Value> = value
                        .into_array()
                        .map_err(|_| anyhow!("excludes: cbor into_array() error"))?;
                    for ex_val in excludes.into_iter() {
                        let exclude: String = ex_val
                            .into_text()
                            .map_err(|_| anyhow!("exclude entry: cbor into_text() error"))?;
                        dataset.excludes.push(exclude);
                    }
                } else if name == "retain_all" {
                    // retention (all)
                    dataset.retention = SnapshotRetention::ALL;
                } else if name == "retain_auto" {
                    // retention (auto)
                    dataset.retention = SnapshotRetention::AUTO;
                } else if name == "retain_count" {
                    // retention (count)
                    let iv: ciborium::value::Integer = value
                        .into_integer()
                        .map_err(|_| anyhow!("retain_count: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    dataset.retention = SnapshotRetention::COUNT(ii as u16);
                } else if name == "retain_days" {
                    // retention (days)
                    let iv: ciborium::value::Integer = value
                        .into_integer()
                        .map_err(|_| anyhow!("retain_days: cbor into_integer() error"))?;
                    let ii: i128 = ciborium::value::Integer::into(iv);
                    dataset.retention = SnapshotRetention::DAYS(ii as u16);
                }
            }
        }

        Ok(dataset)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value;
        // do not emit the key
        let mut fields: Vec<(Value, Value)> = vec![];

        // basepath
        let basepath_string = self.basepath.to_string_lossy();
        fields.push((
            Value::Text("basepath".into()),
            Value::Text(basepath_string.to_string()),
        ));

        // schedules
        let mut schedules: Vec<Value> = vec![];
        for schedule in self.schedules.iter() {
            let as_bytes = schedule.to_bytes()?;
            schedules.push(Value::Bytes(as_bytes));
        }
        fields.push((Value::Text("schedules".into()), Value::Array(schedules)));

        // snapshot
        if let Some(ref latest) = self.snapshot {
            let as_hex = latest.to_hex()?;
            fields.push((Value::Text("snapshot".into()), Value::Bytes(as_hex)));
        } else {
            fields.push((Value::Text("snapshot".into()), Value::Null));
        }

        // workspace
        let workspace_string = self.workspace.to_string_lossy();
        fields.push((
            Value::Text("workspace".into()),
            Value::Text(workspace_string.to_string()),
        ));

        // chunk_size
        fields.push((
            Value::Text("chunksize".into()),
            Value::Integer(self.chunk_size.into()),
        ));

        // pack_size
        fields.push((
            Value::Text("packsize".into()),
            Value::Integer(self.pack_size.into()),
        ));

        // data_shards
        fields.push((
            Value::Text("datashards".into()),
            Value::Integer(self.data_shards.into()),
        ));

        // parity_shards
        fields.push((
            Value::Text("parityshards".into()),
            Value::Integer(self.parity_shards.into()),
        ));

        // stores
        let mut stores: Vec<Value> = vec![];
        for store in self.stores.iter() {
            stores.push(Value::Text(store.to_owned()));
        }
        fields.push((Value::Text("stores".into()), Value::Array(stores)));

        // excludes
        let mut excludes: Vec<Value> = vec![];
        for store in self.excludes.iter() {
            excludes.push(Value::Text(store.to_owned()));
        }
        fields.push((Value::Text("excludes".into()), Value::Array(excludes)));

        // retention
        match self.retention {
            SnapshotRetention::ALL => {
                fields.push((Value::Text("retain_all".into()), Value::Null));
            }
            SnapshotRetention::AUTO => {
                fields.push((Value::Text("retain_auto".into()), Value::Null));
            }
            SnapshotRetention::COUNT(count) => {
                fields.push((
                    Value::Text("retain_count".into()),
                    Value::Integer(count.into()),
                ));
            }
            SnapshotRetention::DAYS(days) => {
                fields.push((
                    Value::Text("retain_days".into()),
                    Value::Integer(days.into()),
                ));
            }
        }

        let doc = Value::Map(fields);
        let mut encoded: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&doc, &mut encoded)
            .map_err(|err| anyhow!("cbor write error: {}", err))?;
        Ok(encoded)
    }
}

// Decode a tree record written in the original CBOR format, in which each entry
// is a map keyed by two-letter field names.
fn tree_from_cbor(digest: Checksum, value: &[u8]) -> Result<Tree, Error> {
    use ciborium::Value;

    let mut entries: Vec<TreeEntry> = vec![];

    let raw_value: Value =
        ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
    let as_tuple_vec: Vec<(Value, Value)> = raw_value
        .into_map()
        .map_err(|_| anyhow!("value: cbor into_map() error"))?;

    for (key, value) in as_tuple_vec.into_iter() {
        let key_str = key
            .into_text()
            .map_err(|_| anyhow!("key: cbor into_text() error"))?;
        if key_str == "e" {
            // entries
            let entry_values: Vec<Value> = value
                .into_array()
                .map_err(|_| anyhow!("e: cbor into_array() error"))?;
            for entry in entry_values.into_iter() {
                let enfields: Vec<(Value, Value)> = entry
                    .into_map()
                    .map_err(|_| anyhow!("e.entry: cbor into_map() error"))?;
                // using a map makes it easier to collect the required
                // pieces in order to build the tree entry
                let mut field_map: HashMap<String, Value> = HashMap::new();
                for (key, value) in enfields.into_iter() {
                    field_map.insert(
                        key.into_text()
                            .map_err(|_| anyhow!("key: cbor into_text() error"))?,
                        value,
                    );
                }

                // name
                let name: String = field_map
                    .remove("nm")
                    .ok_or_else(|| anyhow!("missing 'nm' field"))?
                    .into_text()
                    .map_err(|_| anyhow!("nm: cbor into_text() error"))?;

                // mode
                let mode = if let Some(mode_val) = field_map.remove("mo") {
                    if mode_val.is_null() {
                        None
                    } else {
                        let mode_vec = mode_val
                            .into_bytes()
                            .map_err(|_| anyhow!("mo: cbor into_bytes() error"))?;
                        let mode_arr: [u8; 4] = mode_vec.as_slice().try_into()?;
                        Some(u32::from_be_bytes(mode_arr))
                    }
                } else {
                    None
                };

                // uid
                let uid = if let Some(uid_val) = field_map.remove("ui") {
                    if uid_val.is_null() {
                        None
                    } else {
                        let uid_vec = uid_val
                            .into_bytes()
                            .map_err(|_| anyhow!("ui: cbor into_bytes() error"))?;
                        let uid_arr: [u8; 4] = uid_vec.as_slice().try_into()?;
                        Some(u32::from_be_bytes(uid_arr))
                    }
                } else {
                    None
                };

                // user
                let user = if let Some(user_val) = field_map.remove("un") {
                    if user_val.is_null() {
                        None
                    } else {
                        Some(
                            user_val
                                .into_text()
                                .map_err(|_| anyhow!("un: cbor into_text() error"))?,
                        )
                    }
                } else {
                    None
                };

                // gid
                let gid = if let Some(gid_val) = field_map.remove("gi") {
                    if gid_val.is_null() {
                        None
                    } else {
                        let gid_vec = gid_val
                            .into_bytes()
                            .map_err(|_| anyhow!("gi: cbor into_bytes() error"))?;
                        let gid_arr: [u8; 4] = gid_vec.as_slice().try_into()?;
                        Some(u32::from_be_bytes(gid_arr))
                    }
                } else {
                    None
                };

                // group
                let group = if let Some(group_val) = field_map.remove("gn") {
                    if group_val.is_null() {
                        None
                    } else {
                        Some(
                            group_val
                                .into_text()
                                .map_err(|_| anyhow!("gn: cbor into_text() error"))?,
                        )
                    }
                } else {
                    None
                };

                // ctime
                let ctime_vec = field_map
                    .remove("ct")
                    .ok_or_else(|| anyhow!("missing 'ct' field"))?
                    .into_bytes()
                    .map_err(|_| anyhow!("ct: cbor into_bytes() error"))?;
                let ctime_arr: [u8; 8] = ctime_vec.as_slice().try_into()?;
                let secs = i64::from_be_bytes(ctime_arr);
                let ctime = DateTime::from_timestamp(secs, 0)
                    .ok_or_else(|| anyhow!("'ct' from_timestamp() failed"))?;

                // mtime
                let mtime_vec = field_map
                    .remove("mt")
                    .ok_or_else(|| anyhow!("missing 'mt' field"))?
                    .into_bytes()
                    .map_err(|_| anyhow!("mt: cbor into_bytes() error"))?;
                let mtime_arr: [u8; 8] = mtime_vec.as_slice().try_into()?;
                let secs = i64::from_be_bytes(mtime_arr);
                let mtime = DateTime::from_timestamp(secs, 0)
                    .ok_or_else(|| anyhow!("'mt' from_timestamp() failed"))?;

                // reference
                let reference_vec: Vec<u8> = field_map
                    .remove("tr")
                    .ok_or_else(|| anyhow!("missing 'tr' field"))?
                    .into_bytes()
                    .map_err(|_| anyhow!("tr: cbor into_bytes() error"))?;
                let (kind, content) = reference_vec
                    .split_first()
                    .ok_or_else(|| anyhow!("tr: empty reference"))?;
                let reference = match kind {
                    b'l' => TreeReference::LINK(content.to_owned()),
                    b't' => {
                        let digest = Checksum::from_hex(content)?;
                        TreeReference::TREE(digest)
                    }
                    b'f' => {
                        let digest = Checksum::from_hex(content)?;
                        TreeReference::FILE(digest)
                    }
                    b's' => TreeReference::SMALL(content.to_owned()),
                    _ => TreeReference::SMALL("malformed entry".as_bytes().to_owned()),
                };

                // xattrs
                let mut xattrs: HashMap<String, Checksum> = HashMap::new();
                let xa_map_val = field_map
                    .remove("xa")
                    .ok_or_else(|| anyhow!("missing 'xa' field"))?
                    .into_map()
                    .map_err(|_| anyhow!("xa: cbor into_map() error"))?;
                for (key, value) in xa_map_val.into_iter() {
                    let value_bytes = value
                        .into_bytes()
                        .map_err(|_| anyhow!("xa.val: cbor into_bytes() error"))?;
                    let digest = Checksum::from_hex(&value_bytes)?;
                    xattrs.insert(
                        key.into_text()
                            .map_err(|_| anyhow!("xa.key: cbor into_text() error"))?,
                        digest,
                    );
                }

                entries.push(TreeEntry {
                    name,
                    mode,
                    uid,
                    user,
                    gid,
                    group,
                    ctime,
                    mtime,
                    reference,
                    xattrs,
                });
            }
        }
    }

    Ok(Tree {
        digest,
        entries,
        file_count: 0,
    })
}

// Encode a tree in the original CBOR format, retained so the tests can verify
// that such records are still readable.
#[cfg(test)]
fn tree_to_cbor(tree: &Tree) -> Result<Vec<u8>, Error> {
    use ciborium::Value;
    let mut fields: Vec<(Value, Value)> = vec![];

    // entries
    let mut entries: Vec<Value> = vec![];
    for entry in tree.entries.iter() {
        let mut enfields: Vec<(Value, Value)> = vec![];

        // name
        enfields.push((Value::Text("nm".into()), Value::Text(entry.name.clone())));

        // mode
        if let Some(mode) = entry.mode {
            let mode_bytes = u32::to_be_bytes(mode);
            let mut mode_vec: Vec<u8> = vec![];
            mode_vec.extend_from_slice(&mode_bytes);
            enfields.push((Value::Text("mo".into()), Value::Bytes(mode_vec)));
        } else {
            enfields.push((Value::Text("mo".into()), Value::Null));
        }

        // uid
        if let Some(uid) = entry.uid {
            let uid_bytes = u32::to_be_bytes(uid);
            let mut uid_vec: Vec<u8> = vec![];
            uid_vec.extend_from_slice(&uid_bytes);
            enfields.push((Value::Text("ui".into()), Value::Bytes(uid_vec)));
        } else {
            enfields.push((Value::Text("ui".into()), Value::Null));
        }

        // user
        if let Some(ref user) = entry.user {
            enfields.push((Value::Text("un".into()), Value::Text(user.to_owned())));
        } else {
            enfields.push((Value::Text("un".into()), Value::Null));
        }

        // gid
        if let Some(gid) = entry.gid {
            let gid_bytes = u32::to_be_bytes(gid);
            let mut gid_vec: Vec<u8> = vec![];
            gid_vec.extend_from_slice(&gid_bytes);
            enfields.push((Value::Text("gi".into()), Value::Bytes(gid_vec)));
        } else {
            enfields.push((Value::Text("gi".into()), Value::Null));
        }

        // group
        if let Some(ref group) = entry.group {
            enfields.push((Value::Text("gn".into()), Value::Text(group.to_owned())));
        } else {
            enfields.push((Value::Text("gn".into()), Value::Null));
        }

        // ctime
        let ctime_s = entry.ctime.timestamp();
        let ctime_bytes = i64::to_be_bytes(ctime_s);
        let mut ctime_vec: Vec<u8> = vec![];
        ctime_vec.extend_from_slice(&ctime_bytes);
        enfields.push((Value::Text("ct".into()), Value::Bytes(ctime_vec)));

        // mtime
        let mtime_s = entry.mtime.timestamp();
        let mtime_bytes = i64::to_be_bytes(mtime_s);
        let mut mtime_vec: Vec<u8> = vec![];
        mtime_vec.extend_from_slice(&mtime_bytes);
        enfields.push((Value::Text("mt".into()), Value::Bytes(mtime_vec)));

        // reference
        let mut ref_vec: Vec<u8> = vec![];
        match &entry.reference {
            TreeReference::LINK(bytes) => {
                ref_vec.push(b'l');
                ref_vec.extend_from_slice(bytes);
            }
            TreeReference::TREE(digest) => {
                ref_vec.push(b't');
                let bytes = digest.to_hex()?;
                ref_vec.extend_from_slice(&bytes);
            }
            TreeReference::FILE(digest) => {
                ref_vec.push(b'f');
                let bytes = digest.to_hex()?;
                ref_vec.extend_from_slice(&bytes);
            }
            TreeReference::SMALL(bytes) => {
                ref_vec.push(b's');
                ref_vec.extend_from_slice(bytes);
            }
        }
        enfields.push((Value::Text("tr".into()), Value::Bytes(ref_vec)));

        // xattrs
        let mut xattrs: Vec<(Value, Value)> = vec![];
        for (name, digest) in entry.xattrs.iter() {
            let di_bytes = digest.to_hex()?;
            xattrs.push((Value::Text(name.to_owned()), Value::Bytes(di_bytes)));
        }
        enfields.push((Value::Text("xa".into()), Value::Map(xattrs)));

        entries.push(Value::Map(enfields));
    }
    fields.push((Value::Text("e".into()), Value::Array(entries)));

    let doc = Value::Map(fields);
    let mut encoded: Vec<u8> = Vec::new();
    ciborium::ser::into_writer(&doc, &mut encoded)
        .map_err(|err| anyhow!("cbor write error: {}", err))?;
    Ok(encoded)
}

// Leading byte of a tree record in the compact binary format.
const TREE_FORMAT_V1: u8 = 1;

// Leading byte of a tree record in the original CBOR format, that being the
// header of a map with a single pair.
const TREE_FORMAT_CBOR: u8 = 0xa1;

// Bits of the flags byte that leads each tree entry.
const REF_KIND_MASK: u8 = 0b011;
const REF_LINK: u8 = 0;
const REF_TREE: u8 = 1;
const REF_FILE: u8 = 2;
const REF_SMALL: u8 = 3;
const REF_BLAKE3: u8 = 0b100;

// Bits of the flags byte that leads each row of the attributes table,
// indicating which of the optional fields are present.
const ATTR_MODE: u8 = 0b00001;
const ATTR_UID: u8 = 0b00010;
const ATTR_USER: u8 = 0b00100;
const ATTR_GID: u8 = 0b01000;
const ATTR_GROUP: u8 = 0b10000;
const ATTR_MASK: u8 = 0b11111;

// Number of bytes in the raw form of each type of digest.
const SHA1_LEN: usize = 20;
const BLAKE3_LEN: usize = 32;

// The mode and ownership of a tree entry, which tends to be the same for most
// of the entries in any given tree. The names are borrowed from the entries
// when encoding and owned when decoding.
#[derive(Clone, Eq, Hash, PartialEq)]
struct TreeAttrs<S> {
    mode: Option<u32>,
    uid: Option<u32>,
    user: Option<S>,
    gid: Option<u32>,
    group: Option<S>,
}

// Append the unsigned value as a LEB128 variable-length integer.
fn put_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

// Append the signed value as a zigzag-encoded variable-length integer.
fn put_signed(buf: &mut Vec<u8>, value: i64) {
    put_varint(buf, ((value << 1) ^ (value >> 63)) as u64);
}

// Append the bytes, preceded by their length.
fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

// Cursor over the bytes of a record in the compact binary format, returning an
// error rather than panicking if the record turns out to be malformed.
struct ByteReader<'a> {
    data: &'a [u8],
}

impl<'a> ByteReader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], Error> {
        if count > self.data.len() {
            return Err(anyhow!("record truncated"));
        }
        let (head, tail) = self.data.split_at(count);
        self.data = tail;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    fn varint(&mut self) -> Result<u64, Error> {
        let mut value: u64 = 0;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            // the tenth byte has room for only the last bit of the value
            if shift == 63 && byte > 1 {
                break;
            }
            value |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(anyhow!("variable-length integer too long"))
    }

    fn signed(&mut self) -> Result<i64, Error> {
        let value = self.varint()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    fn uint32(&mut self) -> Result<u32, Error> {
        Ok(u32::try_from(self.varint()?)?)
    }

    fn length(&mut self) -> Result<usize, Error> {
        Ok(usize::try_from(self.varint()?)?)
    }

    fn bytes(&mut self) -> Result<&'a [u8], Error> {
        let count = self.length()?;
        self.take(count)
    }

    fn text(&mut self) -> Result<String, Error> {
        Ok(str::from_utf8(self.bytes()?)?.to_owned())
    }
}

// Decode a tree record written in the compact binary format, with `value`
// starting after the leading format byte. See `Tree::to_bytes()` for the
// layout.
fn tree_from_binary(digest: Checksum, value: &[u8]) -> Result<Tree, Error> {
    let mut reader = ByteReader { data: value };

    // attributes table
    let mut attrs: Vec<TreeAttrs<String>> = vec![];
    for _ in 0..reader.length()? {
        let flags = reader.byte()?;
        if flags & !ATTR_MASK != 0 {
            return Err(anyhow!("unrecognized tree attributes flags: {:#x}", flags));
        }
        let mode = if flags & ATTR_MODE != 0 {
            Some(reader.uint32()?)
        } else {
            None
        };
        let uid = if flags & ATTR_UID != 0 {
            Some(reader.uint32()?)
        } else {
            None
        };
        let user = if flags & ATTR_USER != 0 {
            Some(reader.text()?)
        } else {
            None
        };
        let gid = if flags & ATTR_GID != 0 {
            Some(reader.uint32()?)
        } else {
            None
        };
        let group = if flags & ATTR_GROUP != 0 {
            Some(reader.text()?)
        } else {
            None
        };
        attrs.push(TreeAttrs {
            mode,
            uid,
            user,
            gid,
            group,
        });
    }

    // extended attributes table
    let mut xattrs: Vec<(String, Checksum)> = vec![];
    for _ in 0..reader.length()? {
        let name = reader.text()?;
        let digest = Checksum::from_hex(reader.bytes()?)?;
        xattrs.push((name, digest));
    }

    // entries
    let mut entries: Vec<TreeEntry> = vec![];
    for _ in 0..reader.length()? {
        let flags = reader.byte()?;
        if flags & !(REF_KIND_MASK | REF_BLAKE3) != 0 {
            return Err(anyhow!("unrecognized tree entry flags: {:#x}", flags));
        }
        let TreeAttrs {
            mode,
            uid,
            user,
            gid,
            group,
        } = attrs
            .get(reader.length()?)
            .cloned()
            .ok_or_else(|| anyhow!("tree entry attributes index out of range"))?;
        let name = reader.text()?;
        let ctime_s = reader.signed()?;
        let mtime_s = ctime_s.wrapping_add(reader.signed()?);
        let ctime = DateTime::from_timestamp(ctime_s, 0)
            .ok_or_else(|| anyhow!("ctime from_timestamp() failed"))?;
        let mtime = DateTime::from_timestamp(mtime_s, 0)
            .ok_or_else(|| anyhow!("mtime from_timestamp() failed"))?;
        let digest_len = if flags & REF_BLAKE3 != 0 {
            BLAKE3_LEN
        } else {
            SHA1_LEN
        };
        let reference = match flags & REF_KIND_MASK {
            REF_LINK => TreeReference::LINK(reader.bytes()?.to_owned()),
            REF_TREE => TreeReference::TREE(Checksum::from_hex(reader.take(digest_len)?)?),
            REF_FILE => TreeReference::FILE(Checksum::from_hex(reader.take(digest_len)?)?),
            _ => TreeReference::SMALL(reader.bytes()?.to_owned()),
        };
        let mut entry_xattrs: HashMap<String, Checksum> = HashMap::new();
        for _ in 0..reader.length()? {
            let (name, digest) = xattrs
                .get(reader.length()?)
                .cloned()
                .ok_or_else(|| anyhow!("tree entry xattr index out of range"))?;
            entry_xattrs.insert(name, digest);
        }
        entries.push(TreeEntry {
            name,
            mode,
            uid,
            user,
            gid,
            group,
            ctime,
            mtime,
            reference,
            xattrs: entry_xattrs,
        });
    }

    if !reader.data.is_empty() {
        return Err(anyhow!(
            "tree record has {} unexpected trailing bytes",
            reader.data.len()
        ));
    }

    Ok(Tree {
        digest,
        entries,
        file_count: 0,
    })
}

impl Model for Tree {
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use std::str::FromStr;

        let digest_str = str::from_utf8(key)?.to_owned();
        let digest = Checksum::from_str(&digest_str)?;
        match value.split_first() {
            Some((&TREE_FORMAT_V1, rest)) => tree_from_binary(digest, rest),
            Some((&TREE_FORMAT_CBOR, _)) => tree_from_cbor(digest, value),
            Some((format, _)) => Err(anyhow!("unsupported tree record format: {}", format)),
            None => Err(anyhow!("tree record is empty")),
        }
    }

    /// Trees account for the bulk of the database, so rather than CBOR they are
    /// written in a compact binary format in which all integers are LEB128
    /// variable-length values (zigzag-encoded if signed), and strings and byte
    /// sequences are preceded by their length.
    ///
    /// ```text
    /// tree    := format(u8 = 1) attrs-table xattrs-table entries
    /// attrs-table  := count { flags(u8) [mode] [uid] [user] [gid] [group] }
    /// xattrs-table := count { name digest-length digest }
    /// entries := count { entry }
    /// entry   := flags(u8) attrs-index name ctime mtime-ctime reference
    ///            xattr-count { xattrs-index }
    /// ```
    ///
    /// The mode and ownership are typically identical for most of the entries
    /// in a tree, as are the extended attributes, so each distinct value is
    /// written once in a table and the entries refer to them by position. The
    /// entry flags hold the type of reference in the low two bits, and for tree
    /// and file references the third bit is set if the digest is BLAKE3 rather
    /// than SHA1. Such a reference is the raw 20 or 32 byte digest, while a
    /// symbolic link or small file is the length-prefixed content. Times are in
    /// seconds, with the modification time relative to the creation time.
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use std::collections::BTreeSet;

        // collect the distinct sets of attributes in order of appearance
        // and remember which one belongs to each entry
        let mut attrs_table: Vec<TreeAttrs<&str>> = vec![];
        let mut attrs_index: HashMap<TreeAttrs<&str>, usize> = HashMap::new();
        let mut entry_attrs: Vec<usize> = Vec::with_capacity(self.entries.len());
        // sort the distinct extended attributes for a deterministic encoding
        let mut xattrs_table: BTreeSet<(&str, &Checksum)> = BTreeSet::new();
        for entry in self.entries.iter() {
            let attrs = TreeAttrs {
                mode: entry.mode,
                uid: entry.uid,
                user: entry.user.as_deref(),
                gid: entry.gid,
                group: entry.group.as_deref(),
            };
            let index = *attrs_index.entry(attrs.clone()).or_insert_with(|| {
                attrs_table.push(attrs);
                attrs_table.len() - 1
            });
            entry_attrs.push(index);
            for (name, digest) in entry.xattrs.iter() {
                xattrs_table.insert((name.as_str(), digest));
            }
        }
        let xattrs_index: HashMap<(&str, &Checksum), usize> = xattrs_table
            .iter()
            .enumerate()
            .map(|(index, xattr)| (*xattr, index))
            .collect();

        let mut encoded: Vec<u8> = vec![TREE_FORMAT_V1];

        // attributes table
        put_varint(&mut encoded, attrs_table.len() as u64);
        for attrs in attrs_table.iter() {
            let mut flags: u8 = 0;
            if attrs.mode.is_some() {
                flags |= ATTR_MODE;
            }
            if attrs.uid.is_some() {
                flags |= ATTR_UID;
            }
            if attrs.user.is_some() {
                flags |= ATTR_USER;
            }
            if attrs.gid.is_some() {
                flags |= ATTR_GID;
            }
            if attrs.group.is_some() {
                flags |= ATTR_GROUP;
            }
            encoded.push(flags);
            if let Some(mode) = attrs.mode {
                put_varint(&mut encoded, mode as u64);
            }
            if let Some(uid) = attrs.uid {
                put_varint(&mut encoded, uid as u64);
            }
            if let Some(user) = attrs.user {
                put_bytes(&mut encoded, user.as_bytes());
            }
            if let Some(gid) = attrs.gid {
                put_varint(&mut encoded, gid as u64);
            }
            if let Some(group) = attrs.group {
                put_bytes(&mut encoded, group.as_bytes());
            }
        }

        // extended attributes table
        put_varint(&mut encoded, xattrs_table.len() as u64);
        for (name, digest) in xattrs_table.iter() {
            put_bytes(&mut encoded, name.as_bytes());
            put_bytes(&mut encoded, &digest.to_hex()?);
        }

        // entries
        put_varint(&mut encoded, self.entries.len() as u64);
        for (entry, attrs) in self.entries.iter().zip(entry_attrs) {
            let (kind, digest) = match &entry.reference {
                TreeReference::LINK(_) => (REF_LINK, None),
                TreeReference::TREE(digest) => (REF_TREE, Some(digest)),
                TreeReference::FILE(digest) => (REF_FILE, Some(digest)),
                TreeReference::SMALL(_) => (REF_SMALL, None),
            };
            let mut flags = kind;
            if digest.is_some_and(|d| d.is_blake3()) {
                flags |= REF_BLAKE3;
            }
            encoded.push(flags);
            put_varint(&mut encoded, attrs as u64);
            put_bytes(&mut encoded, entry.name.as_bytes());
            let ctime_s = entry.ctime.timestamp();
            let mtime_s = entry.mtime.timestamp();
            put_signed(&mut encoded, ctime_s);
            put_signed(&mut encoded, mtime_s.wrapping_sub(ctime_s));
            match &entry.reference {
                TreeReference::LINK(bytes) | TreeReference::SMALL(bytes) => {
                    put_bytes(&mut encoded, bytes);
                }
                TreeReference::TREE(digest) | TreeReference::FILE(digest) => {
                    let raw = digest.to_hex()?;
                    let expected = if digest.is_blake3() {
                        BLAKE3_LEN
                    } else {
                        SHA1_LEN
                    };
                    if raw.len() != expected {
                        return Err(anyhow!("digest {} has unexpected length", digest));
                    }
                    encoded.extend_from_slice(&raw);
                }
            }
            let mut indices: Vec<usize> = entry
                .xattrs
                .iter()
                .map(|(name, digest)| xattrs_index[&(name.as_str(), digest)])
                .collect();
            indices.sort_unstable();
            put_varint(&mut encoded, indices.len() as u64);
            for index in indices {
                put_varint(&mut encoded, index as u64);
            }
        }
        Ok(encoded)
    }
}

impl Model for File {
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use ciborium::Value;
        use std::str::FromStr;

        let digest_str = str::from_utf8(key)?.to_owned();
        let digest = Checksum::from_str(&digest_str)?;

        let raw_value: Value =
            ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
        let as_tuple_vec: Vec<(Value, Value)> = raw_value
            .into_map()
            .map_err(|_| anyhow!("value: cbor into_map() error"))?;

        // using a map makes it easier to collect the required
        // pieces in order to build the tree entry
        let mut field_map: HashMap<String, Value> = HashMap::new();
        for (key, value) in as_tuple_vec.into_iter() {
            field_map.insert(
                key.into_text()
                    .map_err(|_| anyhow!("key: cbor into_text() error"))?,
                value,
            );
        }

        // length
        let iv = field_map
            .remove("l")
            .ok_or_else(|| anyhow!("missing 'l' field"))?
            .into_integer()
            .map_err(|_| anyhow!("l: cbor into_integer() error"))?;
        let ii: i128 = ciborium::value::Integer::into(iv);
        let length = ii as u64;

        // chunks
        let chunks_val = field_map
            .remove("c")
            .ok_or_else(|| anyhow!("missing 'c' field"))?
            .into_array()
            .map_err(|_| anyhow!("c: cbor into_array() error"))?;
        let mut chunks: Vec<(u64, Checksum)> = vec![];
        for chunk_val in chunks_val.into_iter() {
            let chunk_bytes = chunk_val
                .into_bytes()
                .map_err(|_| anyhow!("c.val: cbor into_bytes() error"))?;
            let off_arr: [u8; 8] = chunk_bytes[0..8].try_into()?;
            let offset = u64::from_be_bytes(off_arr);
            let digest = Checksum::from_hex(&chunk_bytes[8..])?;
            chunks.push((offset, digest));
        }

        Ok(File::new(digest, length, chunks))
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value;
        // do not emit the key and use very short names
        let mut fields: Vec<(Value, Value)> = vec![];

        // length
        fields.push((Value::Text("l".into()), Value::Integer(self.length.into())));

        // chunks
        let mut chunks: Vec<Value> = vec![];
        for (offset, digest) in self.chunks.iter() {
            let off_bytes = u64::to_be_bytes(*offset);
            let mut chunk_vec: Vec<u8> = Vec::with_capacity(72);
            chunk_vec.extend_from_slice(&off_bytes);
            let mut di_bytes = digest.to_hex()?;
            chunk_vec.append(&mut di_bytes);
            chunks.push(Value::Bytes(chunk_vec));
        }
        fields.push((Value::Text("c".into()), Value::Array(chunks)));

        let doc = Value::Map(fields);
        let mut encoded: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&doc, &mut encoded)
            .map_err(|err| anyhow!("cbor write error: {}", err))?;
        Ok(encoded)
    }
}

impl Model for Snapshot {
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use ciborium::Value;
        use std::str::FromStr;

        let digest_str = str::from_utf8(key)?.to_owned();
        let digest = Checksum::from_str(&digest_str)?;

        let raw_value: Value =
            ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
        let as_tuple_vec: Vec<(Value, Value)> = raw_value
            .into_map()
            .map_err(|_| anyhow!("value: cbor into_map() error"))?;

        // using a map makes it easier to collect the required
        // pieces in order to build the tree entry
        let mut field_map: HashMap<String, Value> = HashMap::new();
        for (key, value) in as_tuple_vec.into_iter() {
            field_map.insert(
                key.into_text()
                    .map_err(|_| anyhow!("key: cbor into_text() error"))?,
                value,
            );
        }

        // parent
        let parent = if let Some(pa_val) = field_map.remove("pa") {
            if pa_val.is_null() {
                None
            } else {
                let pa_vec = pa_val
                    .into_bytes()
                    .map_err(|_| anyhow!("pa: cbor into_bytes() error"))?;
                Some(Checksum::from_hex(&pa_vec)?)
            }
        } else {
            None
        };

        // tree
        let tree_vec = field_map
            .remove("tr")
            .ok_or_else(|| anyhow!("missing 'tr' field"))?
            .into_bytes()
            .map_err(|_| anyhow!("pa: cbor into_bytes() error"))?;
        let tree = Checksum::from_hex(&tree_vec)?;

        // file_counts
        let mut file_counts: FileCounts = Default::default();
        let iv = field_map
            .remove("fcd")
            .ok_or_else(|| anyhow!("missing 'fcd' field"))?
            .into_integer()
            .map_err(|_| anyhow!("fcd: cbor into_integer() error"))?;
        let ii: i128 = ciborium::value::Integer::into(iv);
        file_counts.directories = ii as u32;

        let iv = field_map
            .remove("fcl")
            .ok_or_else(|| anyhow!("missing 'fcl' field"))?
            .into_integer()
            .map_err(|_| anyhow!("fcl: cbor into_integer() error"))?;
        let ii: i128 = ciborium::value::Integer::into(iv);
        file_counts.symlinks = ii as u32;

        let iv = field_map
            .remove("fct")
            .ok_or_else(|| anyhow!("missing 'fct' field"))?
            .into_integer()
            .map_err(|_| anyhow!("fct: cbor into_integer() error"))?;
        let ii: i128 = ciborium::value::Integer::into(iv);
        file_counts.very_small_files = ii as u32;

        let iv = field_map
            .remove("fch")
            .ok_or_else(|| anyhow!("missing 'fch' field"))?
            .into_integer()
            .map_err(|_| anyhow!("fch: cbor into_integer() error"))?;
        let ii: i128 = ciborium::value::Integer::into(iv);
        file_counts.very_large_files = ii as u32;

        let file_sizes_vec: Vec<(Value, Value)> = field_map
            .remove("fcs")
            .ok_or_else(|| anyhow!("missing 'fcs' field"))?
            .into_map()
            .map_err(|_| anyhow!("fcs: cbor into_map() error"))?;
        for (key_val, val_val) in file_sizes_vec.into_iter() {
            let iv = key_val
                .into_integer()
                .map_err(|_| anyhow!("fcs.key: cbor into_integer() error"))?;
            let key: i128 = ciborium::value::Integer::into(iv);
            let iv = val_val
                .into_integer()
                .map_err(|_| anyhow!("fcs.val: cbor into_integer() error"))?;
            let value: i128 = ciborium::value::Integer::into(iv);
            file_counts.file_sizes.insert(key as u8, value as u32);
        }

        // build snapshot with minimum members
        let mut snapshot = Snapshot::new(parent, tree, file_counts);
        snapshot.digest = digest;

        // start_time
        let start_vec = field_map
            .remove("st")
            .ok_or_else(|| anyhow!("missing 'st' field"))?
            .into_bytes()
            .map_err(|_| anyhow!("st: cbor into_bytes() error"))?;
        let start_arr: [u8; 8] = start_vec[0..8].try_into()?;
        let secs = i64::from_be_bytes(start_arr);
        snapshot.start_time = DateTime::from_timestamp(secs, 0)
            .ok_or_else(|| anyhow!("'st' from_timestamp() failed"))?;

        // end_time
        #[allow(clippy::collapsible_if)]
        if let Some(end_val) = field_map.remove("et") {
            if let Ok(end_vec) = end_val.into_bytes() {
                let end_arr: [u8; 8] = end_vec[0..8].try_into()?;
                let secs = i64::from_be_bytes(end_arr);
                snapshot.end_time = Some(
                    DateTime::from_timestamp(secs, 0)
                        .ok_or_else(|| anyhow!("'et' from_timestamp() failed"))?,
                );
            }
        }

        Ok(snapshot)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value;
        // do not emit the key and use very short names
        let mut fields: Vec<(Value, Value)> = vec![];

        // parent
        if let Some(ref parent) = self.parent {
            let as_hex = parent.to_hex()?;
            fields.push((Value::Text("pa".into()), Value::Bytes(as_hex)));
        } else {
            fields.push((Value::Text("pa".into()), Value::Null));
        }

        // start_time
        let start_s = self.start_time.timestamp();
        let start_bytes = i64::to_be_bytes(start_s);
        let mut start_vec: Vec<u8> = vec![];
        start_vec.extend_from_slice(&start_bytes);
        fields.push((Value::Text("st".into()), Value::Bytes(start_vec)));

        // end_time
        if let Some(ref end_time) = self.end_time {
            let end_s = end_time.timestamp();
            let end_bytes = i64::to_be_bytes(end_s);
            let mut end_vec: Vec<u8> = vec![];
            end_vec.extend_from_slice(&end_bytes);
            fields.push((Value::Text("et".into()), Value::Bytes(end_vec)));
        } else {
            fields.push((Value::Text("et".into()), Value::Null));
        }

        // file_counts
        fields.push((
            Value::Text("fcd".into()),
            Value::Integer(self.file_counts.directories.into()),
        ));
        fields.push((
            Value::Text("fcl".into()),
            Value::Integer(self.file_counts.symlinks.into()),
        ));
        fields.push((
            Value::Text("fct".into()),
            Value::Integer(self.file_counts.very_small_files.into()),
        ));
        fields.push((
            Value::Text("fch".into()),
            Value::Integer(self.file_counts.very_large_files.into()),
        ));
        let mut file_sizes: Vec<(Value, Value)> = vec![];
        for (key, value) in self.file_counts.file_sizes.iter() {
            file_sizes.push((
                Value::Integer((*key as u32).into()),
                Value::Integer((*value).into()),
            ));
        }
        fields.push((Value::Text("fcs".into()), Value::Map(file_sizes)));

        // tree
        let as_hex = self.tree.to_hex()?;
        fields.push((Value::Text("tr".into()), Value::Bytes(as_hex)));

        let doc = Value::Map(fields);
        let mut encoded: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&doc, &mut encoded)
            .map_err(|err| anyhow!("cbor write error: {}", err))?;
        Ok(encoded)
    }
}

impl Model for Chunk {
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use ciborium::Value;
        use std::str::FromStr;

        let digest_str = str::from_utf8(key)?.to_owned();
        let digest = Checksum::from_str(&digest_str)?;
        let mut chunk: Chunk = Chunk::new(digest, 0, 0);

        let raw_value: Value =
            ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
        let as_tuple_vec: Vec<(Value, Value)> = raw_value
            .into_map()
            .map_err(|_| anyhow!("value: cbor into_map() error"))?;

        for (key, value) in as_tuple_vec.into_iter() {
            #[allow(clippy::collapsible_if)]
            if let Some(name) = key.as_text() {
                if name == "pf" {
                    // packfile
                    if value.is_null() {
                        chunk.packfile = None;
                    } else {
                        let as_hex: Vec<u8> = value
                            .into_bytes()
                            .map_err(|_| anyhow!("pf: cbor into_bytes() error"))?;
                        let digest = Checksum::from_hex(&as_hex)?;
                        chunk.packfile = Some(digest);
                    }
                }
            }
        }

        Ok(chunk)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value;
        // do not emit the key and use very short names
        let mut fields: Vec<(Value, Value)> = vec![];

        // The offset field is _not_ saved to the database since an identical
        // chunk may appear in different file records at different offsets.
        //
        // The length field is not used once the pack files are created.
        //
        // The filepath field is not used once the pack files are created.

        // packfile
        if let Some(ref packfile) = self.packfile {
            let as_hex = packfile.to_hex()?;
            fields.push((Value::Text("pf".into()), Value::Bytes(as_hex)));
        } else {
            fields.push((Value::Text("pf".into()), Value::Null));
        }

        let doc = Value::Map(fields);
        let mut encoded: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&doc, &mut encoded)
            .map_err(|err| anyhow!("cbor write error: {}", err))?;
        Ok(encoded)
    }
}

impl Model for Pack {
    fn from_bytes(key: &[u8], value: &[u8]) -> Result<Self, Error> {
        use ciborium::Value;
        use std::str::FromStr;

        let digest_str = str::from_utf8(key)?;
        let digest: Checksum = Checksum::from_str(digest_str)?;
        let mut pack = Pack::new(digest, vec![]);

        let raw_value: Value =
            ciborium::de::from_reader(value).map_err(|err| anyhow!("cbor read error: {}", err))?;
        let as_tuple_vec: Vec<(Value, Value)> = raw_value
            .into_map()
            .map_err(|_| anyhow!("value: cbor into_map() error"))?;

        for (key, value) in as_tuple_vec.into_iter() {
            if let Some(name) = key.as_text() {
                if name == "l" {
                    // locations
                    let locations: Vec<Value> = value
                        .into_array()
                        .map_err(|_| anyhow!("l: cbor into_array() error"))?;
                    for entry in locations.into_iter() {
                        let mut pl = PackLocation::new("", "", "");
                        let coords: Vec<(Value, Value)> = entry
                            .into_map()
                            .map_err(|_| anyhow!("coords: cbor into_bytes() error"))?;
                        for (key, value) in coords.into_iter() {
                            if let Some(name) = key.as_text() {
                                if name == "s" {
                                    pl.store = value
                                        .into_text()
                                        .map_err(|_| anyhow!("store: cbor into_text() error"))?;
                                } else if name == "b" {
                                    pl.bucket = value
                                        .into_text()
                                        .map_err(|_| anyhow!("bucket: cbor into_text() error"))?;
                                } else if name == "o" {
                                    pl.object = value
                                        .into_text()
                                        .map_err(|_| anyhow!("object: cbor into_text() error"))?;
                                }
                            }
                        }
                        pack.locations.push(pl);
                    }
                } else if name == "u" {
                    // upload_time
                    let upload_vec = value
                        .into_bytes()
                        .map_err(|_| anyhow!("u: cbor into_bytes() error"))?;
                    let upload_arr: [u8; 8] = upload_vec[0..8].try_into()?;
                    let secs = i64::from_be_bytes(upload_arr);
                    pack.upload_time = DateTime::from_timestamp(secs, 0)
                        .ok_or_else(|| anyhow!("'u' from_timestamp() failed"))?;
                }
            }
        }

        Ok(pack)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        use ciborium::Value;
        // do not emit the key and use very short names
        let mut fields: Vec<(Value, Value)> = vec![];

        // locations
        let mut locations: Vec<Value> = vec![];
        for loc in self.locations.iter() {
            let fields: Vec<(Value, Value)> = vec![
                (Value::Text("s".into()), Value::Text(loc.store.clone())),
                (Value::Text("b".into()), Value::Text(loc.bucket.clone())),
                (Value::Text("o".into()), Value::Text(loc.object.clone())),
            ];
            locations.push(Value::Map(fields));
        }
        fields.push((Value::Text("l".into()), Value::Array(locations)));

        // upload_time
        let upload_s = self.upload_time.timestamp();
        let upload_bytes = i64::to_be_bytes(upload_s);
        let mut upload_vec: Vec<u8> = vec![];
        upload_vec.extend_from_slice(&upload_bytes);
        fields.push((Value::Text("u".into()), Value::Bytes(upload_vec)));

        let doc = Value::Map(fields);
        let mut encoded: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&doc, &mut encoded)
            .map_err(|err| anyhow!("cbor write error: {}", err))?;
        Ok(encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::schedule::TimeRange;
    use crate::domain::entities::{PackRetention, Tree, TreeEntry, TreeReference};
    use anyhow::Error;
    use std::path::Path;

    #[test]
    fn test_chunk_serde() -> Result<(), Error> {
        let digest = Checksum::BLAKE3(
            "5ff9cee979889639c6190b30f1e7ecf413c16d3ce953d6dfec24f64a9b9108fb".into(),
        );
        let mut original = Chunk::new(digest, 1024, 2048);
        let packfile = Checksum::BLAKE3(
            "315c3ac8418daddab4f5736b944107fc54bb1cd494121b76a8f9a63da18d7ec8".into(),
        );
        original = original.packfile(packfile.clone());

        let key =
            "blake3-5ff9cee979889639c6190b30f1e7ecf413c16d3ce953d6dfec24f64a9b9108fb".as_bytes();
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 38);
        let actual = Chunk::from_bytes(key, &as_bytes)?;

        // compare field-by-field since not all fields are serialized
        assert_eq!(actual.digest, original.digest);
        assert_eq!(actual.offset, 0);
        assert_eq!(actual.length, 0);
        assert_eq!(actual.packfile, original.packfile);
        Ok(())
    }

    #[test]
    fn test_store_serde() -> Result<(), Error> {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        properties.insert("format".to_owned(), "ZFS".to_owned());
        let store = Store {
            id: "retainall".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
            retention: PackRetention::ALL,
        };
        // act
        let encoded = store.to_bytes()?;
        assert_eq!(encoded.len(), 94);
        let key = "retainall";
        let actual = Store::from_bytes(key.as_bytes(), &encoded)?;
        // assert
        assert_eq!(actual.id, "retainall");
        assert_eq!(actual.store_type, StoreType::LOCAL);
        assert_eq!(actual.label, "mylocalstore");
        assert_eq!(actual.properties.len(), 2);
        assert_eq!(actual.properties["basepath"], "/home/planet");
        assert_eq!(actual.properties["format"], "ZFS");
        assert_eq!(actual.retention, PackRetention::ALL);

        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "retaindays".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
            retention: PackRetention::DAYS(90),
        };
        // act
        let encoded = store.to_bytes()?;
        assert_eq!(encoded.len(), 85);
        let key = "retaindays";
        let actual = Store::from_bytes(key.as_bytes(), &encoded)?;
        // assert
        assert_eq!(actual.id, "retaindays");
        assert_eq!(actual.store_type, StoreType::LOCAL);
        assert_eq!(actual.label, "mylocalstore");
        assert_eq!(actual.properties.len(), 1);
        assert_eq!(actual.properties["basepath"], "/home/planet");
        assert_eq!(actual.retention, PackRetention::DAYS(90));
        Ok(())
    }

    #[test]
    fn test_schedule_serde() -> Result<(), Error> {
        // schedule does not need the key for serde
        let unused: Vec<u8> = vec![];

        let original = Schedule::Hourly;
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 4);
        let actual = Schedule::from_bytes(&unused, &as_bytes)?;
        assert_eq!(original, actual);

        let original = Schedule::Daily(None);
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 4);
        let actual = Schedule::from_bytes(&unused, &as_bytes)?;
        assert_eq!(original, actual);

        let original = Schedule::Daily(Some(TimeRange::new(12, 0, 18, 0)));
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 10);
        let actual = Schedule::from_bytes(&unused, &as_bytes)?;
        assert_eq!(original, actual);

        let original = Schedule::Weekly(Some((DayOfWeek::Sun, None)));
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 4);
        let actual = Schedule::from_bytes(&unused, &as_bytes)?;
        assert_eq!(original, actual);

        let original = Schedule::Weekly(Some((DayOfWeek::Sun, Some(TimeRange::new(12, 0, 18, 0)))));
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 11);
        let actual = Schedule::from_bytes(&unused, &as_bytes)?;
        assert_eq!(original, actual);

        let original = Schedule::Monthly(Some((DayOfMonth::Day(15), None)));
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 4);
        let actual = Schedule::from_bytes(&unused, &as_bytes)?;
        assert_eq!(original, actual);

        let original = Schedule::Monthly(Some((
            DayOfMonth::Day(15),
            Some(TimeRange::new(12, 0, 18, 0)),
        )));
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 11);
        let actual = Schedule::from_bytes(&unused, &as_bytes)?;
        assert_eq!(original, actual);

        Ok(())
    }

    #[test]
    fn test_dataset_serde() -> Result<(), Error> {
        // bare minimum
        let original = Dataset::new(Path::new("/home/planet"));
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 157);
        let key = original.id.as_bytes();
        let actual = Dataset::from_bytes(key, &as_bytes)?;
        assert_eq!(original, actual);
        assert_eq!(actual.schedules.len(), 0);
        assert_eq!(actual.stores.len(), 0);
        assert_eq!(actual.excludes.len(), 0);

        // very full
        let mut original = Dataset::new(Path::new("/home/planet"));
        let range = TimeRange::new(12, 0, 18, 0);
        let schedule_1 = Schedule::Daily(Some(range));
        original.schedules.push(schedule_1.clone());
        let schedule_2 = Schedule::Hourly;
        original.schedules.push(schedule_2.clone());
        original.snapshot = Some(Checksum::SHA1(String::from(
            "811ea7199968a119eeba4b65ace06cc7f835c497",
        )));
        original.stores.push("abcstore".into());
        original.stores.push("7-Eleven".into());
        original.excludes.push(".DS_Store".into());
        original.excludes.push("target".into());
        original.retention = SnapshotRetention::COUNT(10);
        original.data_shards = 10;
        original.parity_shards = 2;
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 230);
        let key = original.id.as_bytes();
        let actual = Dataset::from_bytes(key, &as_bytes)?;
        assert_eq!(original, actual);
        // must compare the collections properties (Eq does not handle these)
        assert_eq!(actual.schedules.len(), 2);
        assert_eq!(actual.schedules[0], schedule_1);
        assert_eq!(actual.schedules[1], schedule_2);
        assert_eq!(
            actual.snapshot.map(|v| v.to_string()),
            Some("sha1-811ea7199968a119eeba4b65ace06cc7f835c497".to_owned())
        );
        assert_eq!(actual.stores.len(), 2);
        assert_eq!(actual.stores[0], "abcstore");
        assert_eq!(actual.stores[1], "7-Eleven");
        assert_eq!(actual.excludes.len(), 2);
        assert_eq!(actual.excludes[0], ".DS_Store");
        assert_eq!(actual.excludes[1], "target");

        Ok(())
    }

    #[test]
    fn test_snapshot_serde_min() -> Result<(), Error> {
        // arrange
        let tree = Checksum::SHA1(String::from("811ea7199968a119eeba4b65ace06cc7f835c497"));
        let file_counts: FileCounts = Default::default();
        let snapshot = Snapshot::new(None, tree, file_counts);
        // act
        let encoded = snapshot.to_bytes()?;
        assert_eq!(encoded.len(), 70);
        let key = snapshot.digest.to_string();
        let actual = Snapshot::from_bytes(key.as_bytes(), &encoded)?;
        // assert
        assert_eq!(actual.digest.to_string(), key);
        assert!(actual.parent.is_none());
        assert_eq!(
            actual.start_time.timestamp(),
            snapshot.start_time.timestamp()
        );
        assert!(actual.end_time.is_none());
        assert_eq!(actual.tree, snapshot.tree);
        assert_eq!(actual.file_counts.directories, 0);
        assert_eq!(actual.file_counts.symlinks, 0);
        assert_eq!(actual.file_counts.very_small_files, 0);
        assert_eq!(actual.file_counts.very_large_files, 0);
        assert_eq!(actual.file_counts.file_sizes.len(), 0);
        Ok(())
    }

    #[test]
    fn test_snapshot_serde_full() -> Result<(), Error> {
        // arrange
        let parent = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let tree = Checksum::SHA1(String::from("811ea7199968a119eeba4b65ace06cc7f835c497"));
        let mut file_counts: FileCounts = FileCounts {
            directories: 5,
            symlinks: 2,
            ..Default::default()
        };
        file_counts.register_file(64);
        file_counts.register_file(128);
        file_counts.register_file(1024);
        file_counts.register_file(16384);
        file_counts.register_file(16777216);
        file_counts.register_file(1048576);
        file_counts.register_file(4_294_967_296);
        let mut snapshot = Snapshot::new(Some(parent), tree, file_counts);
        snapshot.set_end_time(Utc::now());
        // act
        let encoded = snapshot.to_bytes()?;
        assert_eq!(encoded.len(), 109);
        let key = snapshot.digest.to_string();
        let actual = Snapshot::from_bytes(key.as_bytes(), &encoded)?;
        // assert
        assert_eq!(actual.digest.to_string(), key);
        assert_eq!(actual.parent, snapshot.parent);
        assert_eq!(
            actual.start_time.timestamp(),
            snapshot.start_time.timestamp()
        );
        assert_eq!(
            actual.end_time.map(|v| v.timestamp()),
            snapshot.end_time.map(|v| v.timestamp())
        );
        assert_eq!(actual.file_counts, snapshot.file_counts);
        assert_eq!(actual.tree, snapshot.tree);
        assert_eq!(actual.file_counts.directories, 5);
        assert_eq!(actual.file_counts.symlinks, 2);
        assert_eq!(actual.file_counts.very_small_files, 1);
        assert_eq!(actual.file_counts.very_large_files, 1);
        assert_eq!(
            actual.file_counts.file_sizes.len(),
            snapshot.file_counts.file_sizes.len()
        );
        Ok(())
    }

    #[test]
    fn test_pack_serde() -> Result<(), Error> {
        // arrange
        let digest = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let coords = vec![
            PackLocation::new("store1", "bucket1", "object1"),
            PackLocation::new("store1", "bucket2", "object2"),
        ];
        let original = Pack::new(digest, coords);
        // act
        let as_bytes = original.to_bytes()?;
        assert_eq!(as_bytes.len(), 75);
        let key = "sha1-65ace06cc7f835c497811ea7199968a119eeba4b";
        let actual = Pack::from_bytes(key.as_bytes(), &as_bytes)?;
        // assert
        assert_eq!(actual.locations.len(), 2);
        assert_eq!(actual.locations[0].store, "store1");
        assert_eq!(actual.locations[0].bucket, "bucket1");
        assert_eq!(actual.locations[0].object, "object1");
        assert_eq!(actual.locations[1].store, "store1");
        assert_eq!(actual.locations[1].bucket, "bucket2");
        assert_eq!(actual.locations[1].object, "object2");
        assert_eq!(
            actual.upload_time.timestamp(),
            original.upload_time.timestamp()
        );
        Ok(())
    }

    #[test]
    fn test_configuration_serde() -> Result<(), Error> {
        // arrange
        let original: Configuration = Default::default();
        // act
        let as_bytes = original.to_bytes()?;
        let unused_key: Vec<u8> = vec![];
        let actual = Configuration::from_bytes(&unused_key, &as_bytes)?;
        // assert
        assert_eq!(actual.hostname, original.hostname);
        assert_eq!(actual.username, original.username);
        assert_eq!(actual.computer_id, original.computer_id);
        assert_eq!(actual.bucket_naming, None);
        Ok(())
    }

    #[test]
    fn test_configuration_serde_random_pool() -> Result<(), Error> {
        let original = Configuration {
            bucket_naming: Some(BucketNamingPolicy::RandomPool(42)),
            ..Default::default()
        };
        let as_bytes = original.to_bytes()?;
        let unused_key: Vec<u8> = vec![];
        let actual = Configuration::from_bytes(&unused_key, &as_bytes)?;
        assert_eq!(actual.hostname, original.hostname);
        assert_eq!(actual.username, original.username);
        assert_eq!(actual.computer_id, original.computer_id);
        assert_eq!(
            actual.bucket_naming,
            Some(BucketNamingPolicy::RandomPool(42))
        );
        Ok(())
    }

    #[test]
    fn test_configuration_serde_scheduled() -> Result<(), Error> {
        let original = Configuration {
            bucket_naming: Some(BucketNamingPolicy::Scheduled(7)),
            ..Default::default()
        };
        let as_bytes = original.to_bytes()?;
        let unused_key: Vec<u8> = vec![];
        let actual = Configuration::from_bytes(&unused_key, &as_bytes)?;
        assert_eq!(actual.hostname, original.hostname);
        assert_eq!(actual.username, original.username);
        assert_eq!(actual.computer_id, original.computer_id);
        assert_eq!(actual.bucket_naming, Some(BucketNamingPolicy::Scheduled(7)));
        Ok(())
    }

    #[test]
    fn test_configuration_serde_scheduled_pool() -> Result<(), Error> {
        let original = Configuration {
            bucket_naming: Some(BucketNamingPolicy::ScheduledRandomPool {
                days: 30,
                limit: 12,
            }),
            ..Default::default()
        };
        let as_bytes = original.to_bytes()?;
        let unused_key: Vec<u8> = vec![];
        let actual = Configuration::from_bytes(&unused_key, &as_bytes)?;
        assert_eq!(actual.hostname, original.hostname);
        assert_eq!(actual.username, original.username);
        assert_eq!(actual.computer_id, original.computer_id);
        assert_eq!(
            actual.bucket_naming,
            Some(BucketNamingPolicy::ScheduledRandomPool {
                days: 30,
                limit: 12,
            })
        );
        Ok(())
    }

    #[test]
    fn test_configuration_serde_timezone() -> Result<(), Error> {
        // default has no timezone
        let original: Configuration = Default::default();
        let as_bytes = original.to_bytes()?;
        let unused_key: Vec<u8> = vec![];
        let actual = Configuration::from_bytes(&unused_key, &as_bytes)?;
        assert_eq!(actual.timezone, None);

        // round-trip a populated timezone
        let original = Configuration {
            timezone: Some("America/Los_Angeles".to_string()),
            ..Default::default()
        };
        let as_bytes = original.to_bytes()?;
        let actual = Configuration::from_bytes(&unused_key, &as_bytes)?;
        assert_eq!(actual.timezone, Some("America/Los_Angeles".to_string()));
        Ok(())
    }

    #[test]
    fn test_tree_serde() -> Result<(), Error> {
        // arrange
        let mut tree = Tree::new(
            vec![
                TreeEntry::new(
                    Path::new("../test/fixtures/lorem-ipsum.txt"),
                    TreeReference::FILE(Checksum::BLAKE3(
                        "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128".into(),
                    )),
                ),
                TreeEntry::new(
                    Path::new("../test/fixtures"),
                    TreeReference::TREE(Checksum::SHA1(
                        "5d0e3cbd8f5ba75213f71889d5020bdcda03e1c6".into(),
                    )),
                ),
                TreeEntry::new(
                    Path::new("../test/fixtures/baby-birth.jpg"),
                    TreeReference::LINK(vec![
                        0x65, 0x6d, 0x56, 0x79, 0x62, 0x79, 0x31, 0x73, 0x5a, 0x57, 0x35,
                    ]),
                ),
                TreeEntry::new(
                    Path::new("../test/fixtures/zero-length.txt"),
                    TreeReference::SMALL(vec![
                        0x6e, 0x64, 0x47, 0x67, 0x75, 0x64, 0x48, 0x68, 0x30,
                    ]),
                ),
            ],
            1,
        );
        tree.entries[1].xattrs.insert(
            "ExtAttr".into(),
            Checksum::SHA1("10846411651c5442be373f4d402c476ebcb3f644".into()),
        );
        // act
        let encoded = tree.to_bytes()?;
        // length varies with the times of the fixture files
        println!("tree size: binary {}", encoded.len());
        let key = tree.digest.to_string();
        let actual = Tree::from_bytes(key.as_bytes(), &encoded)?;
        // assert
        assert_tree_serde(&actual);
        // records in the original CBOR format are still readable
        let legacy = tree_to_cbor(&tree)?;
        assert_eq!(legacy.len(), 390);
        assert!(encoded.len() < legacy.len());
        let actual = Tree::from_bytes(key.as_bytes(), &legacy)?;
        assert_tree_serde(&actual);
        Ok(())
    }

    fn assert_tree_serde(actual: &Tree) {
        assert_eq!(actual.entries.len(), 4);
        // entries will be sorted by name on the way into the tree
        assert_eq!(actual.entries[0].name, "baby-birth.jpg");
        assert!(actual.entries[0].reference.is_link());
        assert_eq!(
            actual.entries[0].reference.symlink().unwrap(),
            vec![
                0x65, 0x6d, 0x56, 0x79, 0x62, 0x79, 0x31, 0x73, 0x5a, 0x57, 0x35,
            ]
        );
        assert_eq!(actual.entries[1].name, "fixtures");
        assert!(actual.entries[1].reference.is_tree());
        assert_eq!(
            actual.entries[1].reference.checksum().unwrap(),
            Checksum::SHA1("5d0e3cbd8f5ba75213f71889d5020bdcda03e1c6".into(),)
        );
        assert_eq!(actual.entries[1].xattrs.len(), 1);
        let xattr = actual.entries[1].xattrs.get("ExtAttr").unwrap();
        let expected = Checksum::SHA1("10846411651c5442be373f4d402c476ebcb3f644".into());
        assert_eq!(xattr, &expected);
        assert_eq!(actual.entries[2].name, "lorem-ipsum.txt");
        assert!(actual.entries[2].reference.is_file());
        assert_eq!(
            actual.entries[2].reference.checksum().unwrap(),
            Checksum::BLAKE3(
                "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128".into(),
            )
        );
        assert_eq!(actual.entries[3].name, "zero-length.txt");
        assert!(actual.entries[3].reference.is_small());
        assert_eq!(
            actual.entries[3].reference.content().unwrap(),
            vec![0x6e, 0x64, 0x47, 0x67, 0x75, 0x64, 0x48, 0x68, 0x30,]
        );
    }

    #[test]
    fn test_tree_serde_all_fields() -> Result<(), Error> {
        // arrange
        let xattr1 = Checksum::SHA1("10846411651c5442be373f4d402c476ebcb3f644".into());
        let xattr2 = Checksum::SHA1("5d0e3cbd8f5ba75213f71889d5020bdcda03e1c6".into());
        let mut entries: Vec<TreeEntry> = vec![];
        for index in 0..100 {
            let digest = Checksum::blake3_from_bytes(format!("file{}", index).as_bytes());
            let mut entry = TreeEntry {
                name: format!("document-{:03}.txt", index),
                mode: Some(0o100_644),
                uid: Some(501),
                user: Some("nfiedler".into()),
                gid: Some(20),
                group: Some("staff".into()),
                ctime: DateTime::from_timestamp(1_700_000_000 + index, 0).unwrap(),
                mtime: DateTime::from_timestamp(1_750_000_000 - index, 0).unwrap(),
                reference: TreeReference::FILE(digest),
                xattrs: HashMap::new(),
            };
            entry
                .xattrs
                .insert("com.apple.provenance".into(), xattr1.clone());
            if index % 10 == 0 {
                entry
                    .xattrs
                    .insert("com.apple.quarantine".into(), xattr2.clone());
            }
            entries.push(entry);
        }
        // entries with no attributes, a time before the epoch, and a file
        // reference that happens to be a SHA1
        entries.push(TreeEntry {
            name: "ancient".into(),
            mode: None,
            uid: None,
            user: None,
            gid: None,
            group: None,
            ctime: DateTime::from_timestamp(-86_400, 0).unwrap(),
            mtime: DateTime::from_timestamp(0, 0).unwrap(),
            reference: TreeReference::FILE(xattr1.clone()),
            xattrs: HashMap::new(),
        });
        entries.push(TreeEntry {
            name: "subdir".into(),
            mode: Some(0o040_755),
            uid: Some(0),
            user: Some("root".into()),
            gid: Some(0),
            group: None,
            ctime: DateTime::from_timestamp(0, 0).unwrap(),
            mtime: DateTime::from_timestamp(1_750_000_000, 0).unwrap(),
            reference: TreeReference::TREE(xattr2.clone()),
            xattrs: HashMap::new(),
        });
        let tree = Tree::new(entries, 101);
        // act
        let encoded = tree.to_bytes()?;
        let key = tree.digest.to_string();
        let actual = Tree::from_bytes(key.as_bytes(), &encoded)?;
        // assert
        assert_eq!(actual.digest, tree.digest);
        assert_eq!(actual.entries.len(), tree.entries.len());
        for (left, right) in actual.entries.iter().zip(tree.entries.iter()) {
            assert_eq!(left.name, right.name);
            assert_eq!(left.mode, right.mode);
            assert_eq!(left.uid, right.uid);
            assert_eq!(left.user, right.user);
            assert_eq!(left.gid, right.gid);
            assert_eq!(left.group, right.group);
            assert_eq!(left.ctime, right.ctime);
            assert_eq!(left.mtime, right.mtime);
            assert_eq!(left.reference, right.reference);
            assert_eq!(left.xattrs, right.xattrs);
        }
        // encoding is deterministic and much smaller than the original format
        assert_eq!(encoded, tree.to_bytes()?);
        let legacy = tree_to_cbor(&tree)?;
        println!(
            "tree sizes: binary {}, cbor {}",
            encoded.len(),
            legacy.len()
        );
        assert!(encoded.len() * 2 < legacy.len());
        Ok(())
    }

    #[test]
    fn test_tree_serde_layout() -> Result<(), Error> {
        // arrange
        let epoch = DateTime::from_timestamp(0, 0).unwrap();
        let bare = |name: &str, mtime: i64, reference: TreeReference| TreeEntry {
            name: name.into(),
            mode: None,
            uid: None,
            user: None,
            gid: None,
            group: None,
            ctime: epoch,
            mtime: DateTime::from_timestamp(mtime, 0).unwrap(),
            reference,
            xattrs: HashMap::new(),
        };
        let mut file = TreeEntry {
            name: "a".into(),
            mode: Some(0o100_644),
            uid: Some(501),
            user: Some("me".into()),
            gid: Some(20),
            group: Some("staff".into()),
            ctime: DateTime::from_timestamp(1000, 0).unwrap(),
            mtime: DateTime::from_timestamp(1010, 0).unwrap(),
            reference: TreeReference::FILE(Checksum::BLAKE3("11".repeat(32))),
            xattrs: HashMap::new(),
        };
        file.xattrs
            .insert("x".into(), Checksum::SHA1("22".repeat(20)));
        let tree = Tree::new(
            vec![
                file,
                bare("b", -1, TreeReference::LINK(b"t".to_vec())),
                bare("c", 0, TreeReference::TREE(Checksum::SHA1("33".repeat(20)))),
                bare("d", 0, TreeReference::SMALL(b"hi".to_vec())),
            ],
            1,
        );
        // act
        let encoded = tree.to_bytes()?;
        // assert: these bytes are the format of the records on disk and must
        // not change without introducing a new format byte
        let mut expected: Vec<u8> = vec![
            0x01, // format
            0x02, // attributes table with two rows
            0x1f, 0xa4, 0x83, 0x02, 0xf5, 0x03, 0x02, b'm', b'e', 0x14, 0x05, b's', b't', b'a',
            b'f', b'f', // all fields: mode, uid, user, gid, group
            0x00, // no fields
            0x01, // extended attributes table with one row
            0x01, b'x', 0x14,
        ];
        expected.extend_from_slice(&[0x22; 20]);
        expected.push(0x04); // four entries
        // file with blake3 digest, first attributes, ctime 1000, mtime +10
        expected.extend_from_slice(&[0x06, 0x00, 0x01, b'a', 0xd0, 0x0f, 0x14]);
        expected.extend_from_slice(&[0x11; 32]);
        expected.extend_from_slice(&[0x01, 0x00]); // one xattr, the first
        // link, second attributes, ctime 0, mtime -1, no xattrs
        expected.extend_from_slice(&[0x00, 0x01, 0x01, b'b', 0x00, 0x01, 0x01, b't', 0x00]);
        // tree with sha1 digest
        expected.extend_from_slice(&[0x01, 0x01, 0x01, b'c', 0x00, 0x00]);
        expected.extend_from_slice(&[0x33; 20]);
        expected.push(0x00);
        // small file
        expected.extend_from_slice(&[0x03, 0x01, 0x01, b'd', 0x00, 0x00, 0x02, b'h', b'i', 0x00]);
        assert_eq!(encoded, expected);
        let key = tree.digest.to_string();
        let actual = Tree::from_bytes(key.as_bytes(), &expected)?;
        assert_eq!(actual.entries.len(), 4);
        assert_eq!(actual.entries[0].mode, Some(0o100_644));
        assert_eq!(actual.entries[0].uid, Some(501));
        assert_eq!(actual.entries[0].user.as_deref(), Some("me"));
        assert_eq!(actual.entries[0].gid, Some(20));
        assert_eq!(actual.entries[0].group.as_deref(), Some("staff"));
        assert_eq!(actual.entries[0].ctime.timestamp(), 1000);
        assert_eq!(actual.entries[0].mtime.timestamp(), 1010);
        assert_eq!(actual.entries[1].mtime.timestamp(), -1);
        for (left, right) in actual.entries.iter().zip(tree.entries.iter()) {
            assert_eq!(left.name, right.name);
            assert_eq!(left.reference, right.reference);
            assert_eq!(left.xattrs, right.xattrs);
        }
        Ok(())
    }

    #[test]
    fn test_tree_serde_empty() -> Result<(), Error> {
        let tree = Tree::new(vec![], 0);
        let key = tree.digest.to_string();
        let encoded = tree.to_bytes()?;
        assert_eq!(encoded, vec![1, 0, 0, 0]);
        let actual = Tree::from_bytes(key.as_bytes(), &encoded)?;
        assert!(actual.entries.is_empty());
        let actual = Tree::from_bytes(key.as_bytes(), &tree_to_cbor(&tree)?)?;
        assert!(actual.entries.is_empty());
        Ok(())
    }

    #[test]
    fn test_tree_serde_malformed() -> Result<(), Error> {
        let tree = Tree::new(
            vec![TreeEntry::new(
                Path::new("../test/fixtures/lorem-ipsum.txt"),
                TreeReference::FILE(Checksum::BLAKE3(
                    "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128".into(),
                )),
            )],
            1,
        );
        let key = tree.digest.to_string();
        let encoded = tree.to_bytes()?;
        // every truncation of the record yields an error instead of a panic
        for length in 0..encoded.len() {
            assert!(Tree::from_bytes(key.as_bytes(), &encoded[..length]).is_err());
        }
        // attributes index that refers beyond the end of the table
        let bogus: Vec<u8> = vec![1, 0, 0, 1, 0, 5];
        assert!(Tree::from_bytes(key.as_bytes(), &bogus).is_err());
        // bytes remaining after the last entry
        let mut trailing = encoded.clone();
        trailing.push(0);
        let err = Tree::from_bytes(key.as_bytes(), &trailing).unwrap_err();
        assert!(err.to_string().contains("trailing"));
        // flags with bits that have no meaning
        assert!(Tree::from_bytes(key.as_bytes(), &[1, 1, 0x20, 0, 0]).is_err());
        assert!(Tree::from_bytes(key.as_bytes(), &[1, 0, 0, 1, 0x08]).is_err());
        // variable-length integer that overflows 64 bits
        let mut overflow: Vec<u8> = vec![1];
        overflow.extend_from_slice(&[0xff; 9]);
        overflow.push(0x7f);
        let err = Tree::from_bytes(key.as_bytes(), &overflow).unwrap_err();
        assert!(err.to_string().contains("too long"));
        // neither of the known formats
        let err = Tree::from_bytes(key.as_bytes(), &[2, 0, 0, 0]).unwrap_err();
        assert!(err.to_string().contains("unsupported tree record format"));
        Ok(())
    }

    #[test]
    fn test_tree_serde_malformed_cbor() -> Result<(), Error> {
        use ciborium::Value;
        let tree = Tree::new(
            vec![
                TreeEntry::new(
                    Path::new("../test/fixtures/lorem-ipsum.txt"),
                    TreeReference::FILE(Checksum::BLAKE3(
                        "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128".into(),
                    )),
                )
                .mode(Path::new("../test/fixtures/lorem-ipsum.txt"))
                .owners(Path::new("../test/fixtures/lorem-ipsum.txt")),
            ],
            1,
        );
        let key = tree.digest.to_string();
        let legacy = tree_to_cbor(&tree)?;
        assert!(Tree::from_bytes(key.as_bytes(), &legacy).is_ok());
        for length in 0..legacy.len() {
            assert!(Tree::from_bytes(key.as_bytes(), &legacy[..length]).is_err());
        }
        // shorten each of the byte string fields of the entry in turn, which
        // must yield an error instead of a panic
        for field in ["mo", "ui", "gi", "ct", "mt", "tr"] {
            let mut doc: Value = ciborium::de::from_reader(legacy.as_slice())?;
            let entry = match &mut doc {
                Value::Map(fields) => match &mut fields[0].1 {
                    Value::Array(entries) => &mut entries[0],
                    _ => panic!("expected array of entries"),
                },
                _ => panic!("expected map"),
            };
            let mut found = false;
            if let Value::Map(enfields) = entry {
                for (name, value) in enfields.iter_mut() {
                    if name.as_text() == Some(field) && value.is_bytes() {
                        *value = Value::Bytes(vec![]);
                        found = true;
                    }
                }
            }
            if !found {
                // mode and owners are not available on all platforms
                continue;
            }
            let mut damaged: Vec<u8> = Vec::new();
            ciborium::ser::into_writer(&doc, &mut damaged)?;
            assert!(
                Tree::from_bytes(key.as_bytes(), &damaged).is_err(),
                "field {}",
                field
            );
        }
        Ok(())
    }

    #[test]
    fn test_file_serde_single_chunk() -> Result<(), Error> {
        // arrange
        let file_digest = Checksum::SHA1("c648ebac0ed42e3ce4bd5e042b6cbd33a924baa8".into());
        let c1_digest = Checksum::SHA1("ad85838aff89a46a4d747056179457f7a031f4ff".into());
        let chunks = vec![(0, c1_digest.clone())];
        let file = File::new(file_digest.clone(), 3129, chunks);
        // act
        let encoded = file.to_bytes()?;
        assert_eq!(encoded.len(), 39);
        let key = "sha1-c648ebac0ed42e3ce4bd5e042b6cbd33a924baa8";
        let actual = File::from_bytes(key.as_bytes(), &encoded)?;
        // assert
        assert_eq!(actual.digest, file_digest);
        assert_eq!(actual.length, file.length);
        assert_eq!(actual.chunks.len(), 1);
        assert_eq!(actual.chunks[0].0, 0);
        assert_eq!(actual.chunks[0].1, c1_digest);
        Ok(())
    }

    #[test]
    fn test_file_serde_multi_chunk() -> Result<(), Error> {
        // arrange
        let file_digest = Checksum::SHA1("c648ebac0ed42e3ce4bd5e042b6cbd33a924baa8".into());
        let c1_digest = Checksum::SHA1("ad85838aff89a46a4d747056179457f7a031f4ff".into());
        let c2_digest = Checksum::SHA1("b1495ee99c1c27b360b063fdd225d57fb24fea13".into());
        let c3_digest = Checksum::SHA1("b58aedbe9f95138ed449b411903fdac3669f49ad".into());
        let chunks = vec![
            (0, c1_digest.clone()),
            (1024, c2_digest.clone()),
            (2048, c3_digest.clone()),
        ];
        let file = File::new(file_digest.clone(), 3129, chunks);
        // act
        let encoded = file.to_bytes()?;
        assert_eq!(encoded.len(), 99);
        let key = "sha1-c648ebac0ed42e3ce4bd5e042b6cbd33a924baa8";
        let actual = File::from_bytes(key.as_bytes(), &encoded)?;
        // assert
        assert_eq!(actual.digest, file_digest);
        assert_eq!(actual.length, file.length);
        assert_eq!(actual.chunks.len(), 3);
        assert_eq!(actual.chunks[0].0, 0);
        assert_eq!(actual.chunks[0].1, c1_digest);
        assert_eq!(actual.chunks[1].0, 1024);
        assert_eq!(actual.chunks[1].1, c2_digest);
        assert_eq!(actual.chunks[2].0, 2048);
        assert_eq!(actual.chunks[2].1, c3_digest);
        Ok(())
    }
}
