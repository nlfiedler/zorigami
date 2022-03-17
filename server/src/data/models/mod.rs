//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, Pack, PackLocation, Snapshot, Store, StoreType,
};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::pwhash::Salt;
use std::collections::HashMap;
use std::path::PathBuf;

//
// serde_derive has trouble with the combination of remote derivations and
// structs with optional or nested (e.g. vec) properties that are themselves
// remote and using derived serialization. As a result, put the generated code
// in separate files because there is a lot of code, and writing it by hand
// would be very difficult.
//
// What the struct _would_ have looked like using the derive macro is shown
// below each section.
//
mod checksum;
// #[derive(Serialize, Deserialize)]
// pub enum Checksum {
//     SHA1(String),
//     SHA256(String),
// }

mod pack_location;
// #[derive(Serialize, Deserialize)]
// pub struct PackLocation {
//     #[serde(rename = "s")]
//     pub store: String,
//     #[serde(rename = "b")]
//     pub bucket: String,
//     #[serde(rename = "o")]
//     pub object: String,
// }

mod schedule;
// #[derive(Serialize, Deserialize)]
// pub enum DayOfWeek {
//     Sun,
//     Mon,
//     Tue,
//     Wed,
//     Thu,
//     Fri,
//     Sat,
// }
// #[derive(Serialize, Deserialize)]
// pub struct TimeRange {
//     pub start: u32,
//     pub stop: u32,
// }
// #[derive(Serialize, Deserialize)]
// pub enum DayOfMonth {
//     First(DayOfWeek),
//     Second(DayOfWeek),
//     Third(DayOfWeek),
//     Fourth(DayOfWeek),
//     Fifth(DayOfWeek),
//     Day(u8),
// }
// #[derive(Serialize, Deserialize)]
// pub enum Schedule {
//     Hourly,
//     Daily(Option<TimeRange>),
//     Weekly(Option<(DayOfWeek, Option<TimeRange>)>),
//     Monthly(Option<(DayOfMonth, Option<TimeRange>)>),
// }

mod tree;
// #[derive(Serialize, Deserialize)]
// pub enum EntryType {
//     FILE,
//     DIR,
//     LINK,
//     ERROR,
// }
// #[derive(Serialize, Deserialize)]
// pub enum TreeReference {
//     LINK(String),
//     TREE(Checksum),
//     FILE(Checksum),
// }
// #[derive(Serialize, Deserialize)]
// pub struct TreeEntry {
//     #[serde(rename = "nm")]
//     pub name: String,
//     #[serde(rename = "ty")]
//     pub fstype: EntryType,
//     #[serde(rename = "mo")]
//     pub mode: Option<u32>,
//     #[serde(rename = "ui")]
//     pub uid: Option<u32>,
//     #[serde(rename = "us")]
//     pub user: Option<String>,
//     #[serde(rename = "gi")]
//     pub gid: Option<u32>,
//     #[serde(rename = "gr")]
//     pub group: Option<String>,
//     #[serde(rename = "ct")]
//     pub ctime: DateTime<Utc>,
//     #[serde(rename = "mt")]
//     pub mtime: DateTime<Utc>,
//     #[serde(rename = "tr")]
//     pub reference: TreeReference,
//     #[serde(rename = "xa")]
//     pub xattrs: HashMap<String, Checksum>,
// }
// #[derive(Serialize, Deserialize)]
// pub struct Tree {
//     #[serde(skip)]
//     pub digest: Checksum,
//     #[serde(rename = "en")]
//     pub entries: Vec<TreeEntry>,
//     #[serde(skip)]
//     pub file_count: u32,
// }

#[derive(Serialize, Deserialize)]
#[serde(remote = "Chunk")]
pub struct ChunkDef {
    #[serde(skip)]
    pub digest: Checksum,
    // This is _not_ saved to the database since an identical chunk may appear
    // in different files at different offsets.
    #[serde(skip)]
    pub offset: usize,
    #[serde(skip)]
    pub length: usize,
    #[serde(skip)]
    pub filepath: Option<PathBuf>,
    #[serde(rename = "pf")]
    pub packfile: Option<Checksum>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "StoreType")]
pub enum StoreTypeDef {
    GOOGLE,
    LOCAL,
    MINIO,
    SFTP,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Store")]
pub struct StoreDef {
    #[serde(skip)]
    pub id: String,
    #[serde(rename = "st", with = "StoreTypeDef")]
    pub store_type: StoreType,
    #[serde(rename = "la")]
    pub label: String,
    #[serde(rename = "pp")]
    pub properties: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
#[serde(remote = "Dataset")]
pub struct DatasetDef {
    #[serde(skip)]
    pub id: String,
    #[serde(rename = "bp")]
    pub basepath: PathBuf,
    #[serde(rename = "sc")]
    pub schedules: Vec<Schedule>,
    #[serde(rename = "ws")]
    pub workspace: PathBuf,
    #[serde(rename = "ps")]
    pub pack_size: u64,
    #[serde(rename = "st")]
    pub stores: Vec<String>,
    #[serde(rename = "ex")]
    pub excludes: Vec<String>,
}

impl Default for DatasetDef {
    fn default() -> Self {
        Self {
            id: String::new(),
            basepath: PathBuf::new(),
            schedules: vec![],
            workspace: PathBuf::new(),
            pack_size: 0,
            stores: vec![],
            excludes: vec![],
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "File")]
pub struct FileDef {
    #[serde(skip)]
    pub digest: Checksum,
    #[serde(rename = "l")]
    pub length: u64,
    #[serde(rename = "c")]
    pub chunks: Vec<(u64, Checksum)>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Snapshot")]
pub struct SnapshotDef {
    #[serde(skip)]
    pub digest: Checksum,
    #[serde(rename = "pa")]
    pub parent: Option<Checksum>,
    #[serde(rename = "st")]
    pub start_time: DateTime<Utc>,
    #[serde(rename = "et")]
    pub end_time: Option<DateTime<Utc>>,
    #[serde(rename = "fc")]
    pub file_count: u32,
    #[serde(rename = "tr")]
    pub tree: Checksum,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Pack")]
pub struct PackDef {
    #[serde(skip)]
    pub digest: Checksum,
    #[serde(rename = "l")]
    pub locations: Vec<PackLocation>,
    #[serde(rename = "t")]
    pub upload_time: DateTime<Utc>,
    #[serde(rename = "s")]
    pub crypto_salt: Option<Salt>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Configuration")]
pub struct ConfigurationDef {
    #[serde(rename = "hn")]
    pub hostname: String,
    #[serde(rename = "un")]
    pub username: String,
    #[serde(rename = "id")]
    pub computer_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::schedule::TimeRange;
    use crate::domain::entities::{Tree, TreeEntry, TreeReference};
    use anyhow::Error;
    use sodiumoxide::crypto::pwhash;
    use std::path::Path;

    #[test]
    fn test_checksum_serde() -> Result<(), Error> {
        // arrange
        let digest = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        Checksum::serialize(&digest, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = Checksum::deserialize(&mut de)?;
        // assert
        assert_eq!(actual, digest);
        Ok(())
    }

    #[test]
    fn test_chunk_serde() -> Result<(), Error> {
        // arrange
        let digest = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let mut chunk = Chunk::new(digest, 0, 1024);
        let packfile = Checksum::SHA1(String::from("835c497811ea71999665ace06cc7f8a119eeba4b"));
        chunk = chunk.packfile(packfile.clone());
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        ChunkDef::serialize(&chunk, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = ChunkDef::deserialize(&mut de)?;
        // assert
        let null_digest = Checksum::SHA1(String::from("0000000000000000000000000000000000000000"));
        assert_eq!(actual.digest, null_digest);
        assert_eq!(actual.length, 0);
        assert_eq!(actual.packfile, chunk.packfile);
        Ok(())
    }

    #[test]
    fn test_store_serde() -> Result<(), Error> {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        };
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        StoreDef::serialize(&store, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = StoreDef::deserialize(&mut de)?;
        // assert
        // id is not serialized in the record itself
        assert_eq!(actual.id, "");
        assert_eq!(actual.store_type, store.store_type);
        assert_eq!(actual.label, store.label);
        assert_eq!(actual.properties, store.properties);
        Ok(())
    }

    #[test]
    fn test_dataset_serde() -> Result<(), Error> {
        // arrange
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        let range = TimeRange::new(12, 0, 18, 0);
        let schedule = Schedule::Daily(Some(range));
        dataset.schedules.push(schedule.clone());
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        DatasetDef::serialize(&dataset, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = DatasetDef::deserialize(&mut de)?;
        // assert
        assert_eq!(actual.basepath, dataset.basepath);
        assert_eq!(actual.pack_size, dataset.pack_size);
        assert_eq!(actual.schedules.len(), 1);
        assert_eq!(actual.schedules[0], schedule);
        Ok(())
    }

    #[test]
    fn test_snapshot_serde() -> Result<(), Error> {
        // arrange
        let parent = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let tree = Checksum::SHA1(String::from("811ea7199968a119eeba4b65ace06cc7f835c497"));
        let mut snapshot = Snapshot::new(Some(parent), tree, 1024);
        snapshot = snapshot.end_time(Utc::now());
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        SnapshotDef::serialize(&snapshot, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = SnapshotDef::deserialize(&mut de)?;
        // assert
        let null_digest = Checksum::SHA1(String::from("0000000000000000000000000000000000000000"));
        assert_eq!(actual.digest, null_digest);
        assert_eq!(actual.parent, snapshot.parent);
        assert_eq!(actual.start_time, snapshot.start_time);
        assert_eq!(actual.end_time, snapshot.end_time);
        assert_eq!(actual.file_count, snapshot.file_count);
        assert_eq!(actual.tree, snapshot.tree);
        Ok(())
    }

    #[test]
    fn test_pack_serde() -> Result<(), Error> {
        // arrange
        let digest = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let coords = vec![PackLocation::new("store1", "bucket1", "object1")];
        let mut pack = Pack::new(digest, coords);
        // (normally should init sodiumoxide but for the tests it is okay)
        pack.crypto_salt = Some(pwhash::gen_salt());
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        PackDef::serialize(&pack, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = PackDef::deserialize(&mut de)?;
        // assert
        assert_eq!(actual.locations.len(), pack.locations.len());
        assert_eq!(actual.locations.len(), 1);
        assert_eq!(actual.locations[0], pack.locations[0]);
        assert_eq!(actual.upload_time, pack.upload_time);
        assert_eq!(actual.crypto_salt, pack.crypto_salt);
        Ok(())
    }

    #[test]
    fn test_configuration_serde() -> Result<(), Error> {
        // arrange
        let config: Configuration = Default::default();
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        ConfigurationDef::serialize(&config, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = ConfigurationDef::deserialize(&mut de)?;
        // assert
        assert_eq!(actual.hostname, config.hostname);
        assert_eq!(actual.username, config.username);
        assert_eq!(actual.computer_id, config.computer_id);
        Ok(())
    }

    #[test]
    fn test_tree_serde() -> Result<(), Error> {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let reference = TreeReference::FILE(file_digest);
        let filepath = Path::new("../test/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(filepath, reference);
        let tree = Tree::new(vec![entry], 1);
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        Tree::serialize(&tree, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = Tree::deserialize(&mut de)?;
        // assert
        assert_eq!(actual.entries.len(), 1);
        assert_eq!(actual.entries[0].name, "lorem-ipsum.txt");
        Ok(())
    }

    #[test]
    fn test_file_serde() -> Result<(), Error> {
        // arrange
        let sha256sum = "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f";
        let file_digest = Checksum::SHA256(String::from(sha256sum));
        let chunks = vec![(0, file_digest.clone())];
        let file = File::new(file_digest.clone(), 3129, chunks);
        // act
        let mut buffer: Vec<u8> = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buffer);
        FileDef::serialize(&file, &mut ser)?;
        let as_text = String::from_utf8(buffer)?;
        let mut de = serde_json::Deserializer::from_str(&as_text);
        let actual = FileDef::deserialize(&mut de)?;
        // assert
        let null_digest = Checksum::SHA1(String::from("0000000000000000000000000000000000000000"));
        assert_eq!(actual.digest, null_digest);
        assert_eq!(actual.length, file.length);
        assert_eq!(actual.chunks.len(), 1);
        assert_eq!(actual.chunks[0].0, 0);
        assert_eq!(actual.chunks[0].1, file_digest);
        Ok(())
    }
}
