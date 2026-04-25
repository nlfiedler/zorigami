//
// Copyright (c) 2026 Nathan Fiedler
//

//! SQLite-backed implementation of the `EntityDataSource` trait. Unlike the
//! RocksDB backend (which serializes entire entities to CBOR via the
//! [`crate::data::models::Model`] trait), this backend stores entity fields in
//! normalized columns, with child tables for collections (pack locations, file
//! chunks, tree entries, dataset stores/excludes/schedules, store properties).
//! The only field stored as JSON is `Schedule`, whose nested-enum shape is
//! awkward to fully normalize without query benefit.
//!
//! See `server/src/data/models.rs` for the rationale behind the CBOR approach
//! used by the RocksDB backend; this file is the relational alternative
//! foreshadowed in those comments.

use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::{
    Checksum, Chunk, Configuration, Dataset, File, FileCounts, Pack, PackLocation, PackRetention,
    RecordCounts, Snapshot, SnapshotRetention, Store, StoreType, Tree, TreeEntry, TreeReference,
};
use crate::domain::services::buckets::BucketNamingPolicy;
use crate::domain::sources::EntityDataSource;
use anyhow::{Error, anyhow};
use chrono::TimeZone;
use chrono::prelude::*;
use hashed_array_tree::HashedArrayTree;
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;

/// Filename of the SQLite database stored under the configured directory.
/// Mirrors the pattern used by RocksDB (which stores its files in a directory
/// at the configured path).
const DB_FILENAME: &str = "zorigami.sqlite";

const SCHEMA_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS configuration (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    hostname TEXT NOT NULL,
    username TEXT NOT NULL,
    computer_id TEXT NOT NULL,
    timezone TEXT,
    bucket_policy_kind TEXT,
    bucket_policy_param1 INTEGER,
    bucket_policy_param2 INTEGER
);

CREATE TABLE IF NOT EXISTS chunks (
    digest TEXT PRIMARY KEY,
    packfile TEXT
);

CREATE TABLE IF NOT EXISTS packs (
    digest TEXT PRIMARY KEY,
    upload_time INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pack_locations (
    pack_digest TEXT NOT NULL,
    store_id TEXT NOT NULL,
    bucket TEXT NOT NULL,
    object TEXT NOT NULL,
    PRIMARY KEY (pack_digest, store_id, bucket, object),
    FOREIGN KEY (pack_digest) REFERENCES packs(digest) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS databases (
    digest TEXT PRIMARY KEY,
    upload_time INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS database_locations (
    database_digest TEXT NOT NULL,
    store_id TEXT NOT NULL,
    bucket TEXT NOT NULL,
    object TEXT NOT NULL,
    PRIMARY KEY (database_digest, store_id, bucket, object),
    FOREIGN KEY (database_digest) REFERENCES databases(digest) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS xattrs (
    digest TEXT PRIMARY KEY,
    value BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    digest TEXT PRIMARY KEY,
    length INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS file_chunks (
    file_digest TEXT NOT NULL,
    ord INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    chunk_digest TEXT NOT NULL,
    PRIMARY KEY (file_digest, ord),
    FOREIGN KEY (file_digest) REFERENCES files(digest) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS trees (
    digest TEXT PRIMARY KEY,
    file_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tree_entries (
    tree_digest TEXT NOT NULL,
    name TEXT NOT NULL,
    mode INTEGER,
    uid INTEGER,
    user_name TEXT,
    gid INTEGER,
    group_name TEXT,
    ctime INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    reference TEXT NOT NULL,
    xattrs_json TEXT,
    PRIMARY KEY (tree_digest, name),
    FOREIGN KEY (tree_digest) REFERENCES trees(digest) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stores (
    id TEXT PRIMARY KEY,
    store_type TEXT NOT NULL,
    label TEXT NOT NULL,
    retention_kind TEXT NOT NULL,
    retention_days INTEGER
);

CREATE TABLE IF NOT EXISTS store_properties (
    store_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (store_id, key),
    FOREIGN KEY (store_id) REFERENCES stores(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS datasets (
    id TEXT PRIMARY KEY,
    basepath TEXT NOT NULL,
    workspace TEXT NOT NULL,
    snapshot TEXT,
    chunk_size INTEGER NOT NULL,
    pack_size INTEGER NOT NULL,
    retention_kind TEXT NOT NULL,
    retention_param INTEGER
);

CREATE TABLE IF NOT EXISTS dataset_stores (
    dataset_id TEXT NOT NULL,
    ord INTEGER NOT NULL,
    store_id TEXT NOT NULL,
    PRIMARY KEY (dataset_id, ord),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dataset_excludes (
    dataset_id TEXT NOT NULL,
    ord INTEGER NOT NULL,
    pattern TEXT NOT NULL,
    PRIMARY KEY (dataset_id, ord),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dataset_schedules (
    dataset_id TEXT NOT NULL,
    ord INTEGER NOT NULL,
    schedule_json TEXT NOT NULL,
    PRIMARY KEY (dataset_id, ord),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS snapshots (
    digest TEXT PRIMARY KEY,
    parent TEXT,
    tree TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    end_time INTEGER,
    file_counts_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS buckets (
    name TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
);
"#;

/// Implementation of the entity data source backed by SQLite.
pub struct SQLiteEntityDataSource {
    conn: Mutex<Connection>,
    /// Directory containing the SQLite database file. Returned from
    /// `get_db_path()` so callers see the same kind of value as RocksDB
    /// returns (the directory wrapping the on-disk state).
    dir_path: PathBuf,
}

impl SQLiteEntityDataSource {
    pub fn new<P: AsRef<Path>>(dir_path: P) -> Result<Self, Error> {
        use anyhow::Context;
        let dir = dir_path.as_ref();
        std::fs::create_dir_all(dir).with_context(|| {
            format!(
                "SQLiteEntityDataSource::new fs::create_dir_all({})",
                dir.display()
            )
        })?;
        let db_file = dir.join(DB_FILENAME);
        let conn = Connection::open(&db_file)
            .with_context(|| format!("SQLite::open({})", db_file.display()))?;
        configure_connection(&conn)?;
        conn.execute_batch(SCHEMA_DDL)?;
        Ok(Self {
            conn: Mutex::new(conn),
            dir_path: dir.to_path_buf(),
        })
    }

    fn db_file(&self) -> PathBuf {
        self.dir_path.join(DB_FILENAME)
    }
}

/// Apply the per-connection PRAGMAs used by every connection this module
/// opens. `busy_timeout` is critical: multiple `SQLiteEntityDataSource`
/// instances (one per long-lived consumer such as the scheduler and leader)
/// share the same on-disk database file via independent connections, so
/// concurrent writes will block until the lock is available rather than
/// returning `SQLITE_BUSY` immediately.
fn configure_connection(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;\n\
         PRAGMA synchronous = NORMAL;\n\
         PRAGMA foreign_keys = ON;\n\
         PRAGMA busy_timeout = 5000;",
    )?;
    Ok(())
}

// ---------- Conversion helpers ----------

fn dt_to_millis(dt: &DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

fn millis_to_dt(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap_or_default()
}

fn parse_checksum(s: &str) -> Result<Checksum, Error> {
    Checksum::from_str(s)
}

fn pack_retention_to_columns(retention: &PackRetention) -> (&'static str, Option<i64>) {
    match retention {
        PackRetention::ALL => ("ALL", None),
        PackRetention::DAYS(n) => ("DAYS", Some(*n as i64)),
    }
}

fn pack_retention_from_columns(kind: &str, days: Option<i64>) -> Result<PackRetention, Error> {
    match kind {
        "ALL" => Ok(PackRetention::ALL),
        "DAYS" => Ok(PackRetention::DAYS(days.unwrap_or(0) as u16)),
        other => Err(anyhow!("unrecognized pack retention kind: {}", other)),
    }
}

fn snapshot_retention_to_columns(retention: &SnapshotRetention) -> (&'static str, Option<i64>) {
    match retention {
        SnapshotRetention::ALL => ("ALL", None),
        SnapshotRetention::COUNT(n) => ("COUNT", Some(*n as i64)),
        SnapshotRetention::DAYS(n) => ("DAYS", Some(*n as i64)),
        SnapshotRetention::AUTO => ("AUTO", None),
    }
}

fn snapshot_retention_from_columns(
    kind: &str,
    param: Option<i64>,
) -> Result<SnapshotRetention, Error> {
    match kind {
        "ALL" => Ok(SnapshotRetention::ALL),
        "COUNT" => Ok(SnapshotRetention::COUNT(param.unwrap_or(0) as u16)),
        "DAYS" => Ok(SnapshotRetention::DAYS(param.unwrap_or(0) as u16)),
        "AUTO" => Ok(SnapshotRetention::AUTO),
        other => Err(anyhow!("unrecognized snapshot retention kind: {}", other)),
    }
}

fn bucket_policy_to_columns(
    policy: &Option<BucketNamingPolicy>,
) -> (Option<&'static str>, Option<i64>, Option<i64>) {
    match policy {
        None => (None, None, None),
        Some(BucketNamingPolicy::RandomPool(n)) => (Some("RandomPool"), Some(*n as i64), None),
        Some(BucketNamingPolicy::Scheduled(n)) => (Some("Scheduled"), Some(*n as i64), None),
        Some(BucketNamingPolicy::ScheduledRandomPool { days, limit }) => (
            Some("ScheduledRandomPool"),
            Some(*days as i64),
            Some(*limit as i64),
        ),
    }
}

fn bucket_policy_from_columns(
    kind: Option<String>,
    p1: Option<i64>,
    p2: Option<i64>,
) -> Result<Option<BucketNamingPolicy>, Error> {
    match kind.as_deref() {
        None => Ok(None),
        Some("RandomPool") => Ok(Some(BucketNamingPolicy::RandomPool(
            p1.unwrap_or(0) as usize
        ))),
        Some("Scheduled") => Ok(Some(BucketNamingPolicy::Scheduled(p1.unwrap_or(0) as usize))),
        Some("ScheduledRandomPool") => Ok(Some(BucketNamingPolicy::ScheduledRandomPool {
            days: p1.unwrap_or(0) as usize,
            limit: p2.unwrap_or(0) as usize,
        })),
        Some(other) => Err(anyhow!("unrecognized bucket policy kind: {}", other)),
    }
}

fn xattrs_to_json(xattrs: &HashMap<String, Checksum>) -> Result<Option<String>, Error> {
    if xattrs.is_empty() {
        return Ok(None);
    }
    let map: HashMap<&String, String> = xattrs.iter().map(|(k, v)| (k, v.to_string())).collect();
    Ok(Some(serde_json::to_string(&map)?))
}

fn xattrs_from_json(s: Option<String>) -> Result<HashMap<String, Checksum>, Error> {
    match s {
        None => Ok(HashMap::new()),
        Some(text) => {
            let map: HashMap<String, String> = serde_json::from_str(&text)?;
            let mut result: HashMap<String, Checksum> = HashMap::new();
            for (k, v) in map {
                result.insert(k, parse_checksum(&v)?);
            }
            Ok(result)
        }
    }
}

fn read_pack_locations(conn: &Connection, table: &str, digest: &str) -> Result<Vec<PackLocation>, Error> {
    let sql = format!(
        "SELECT store_id, bucket, object FROM {} WHERE {} = ?1",
        table,
        if table == "pack_locations" {
            "pack_digest"
        } else {
            "database_digest"
        }
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![digest], |row| {
        Ok(PackLocation {
            store: row.get::<_, String>(0)?,
            bucket: row.get::<_, String>(1)?,
            object: row.get::<_, String>(2)?,
        })
    })?;
    let mut locations: Vec<PackLocation> = Vec::new();
    for row in rows {
        locations.push(row?);
    }
    Ok(locations)
}

// ---------- EntityDataSource impl ----------

impl EntityDataSource for SQLiteEntityDataSource {
    fn get_configuration(&self) -> Result<Option<Configuration>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT hostname, username, computer_id, timezone, \
             bucket_policy_kind, bucket_policy_param1, bucket_policy_param2 \
             FROM configuration WHERE id = 0",
        )?;
        let row = stmt
            .query_row([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                ))
            })
            .optional()?;
        match row {
            None => Ok(None),
            Some((hostname, username, computer_id, timezone, kind, p1, p2)) => {
                let bucket_naming = bucket_policy_from_columns(kind, p1, p2)?;
                Ok(Some(Configuration {
                    hostname,
                    username,
                    computer_id,
                    bucket_naming,
                    timezone,
                }))
            }
        }
    }

    fn put_configuration(&self, config: &Configuration) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        let (kind, p1, p2) = bucket_policy_to_columns(&config.bucket_naming);
        conn.execute(
            "INSERT INTO configuration \
             (id, hostname, username, computer_id, timezone, \
              bucket_policy_kind, bucket_policy_param1, bucket_policy_param2) \
             VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6, ?7) \
             ON CONFLICT(id) DO UPDATE SET \
              hostname = excluded.hostname, \
              username = excluded.username, \
              computer_id = excluded.computer_id, \
              timezone = excluded.timezone, \
              bucket_policy_kind = excluded.bucket_policy_kind, \
              bucket_policy_param1 = excluded.bucket_policy_param1, \
              bucket_policy_param2 = excluded.bucket_policy_param2",
            params![
                config.hostname,
                config.username,
                config.computer_id,
                config.timezone,
                kind,
                p1,
                p2,
            ],
        )?;
        Ok(())
    }

    fn insert_chunk(&self, chunk: &Chunk) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        let packfile = chunk.packfile.as_ref().map(|c| c.to_string());
        conn.execute(
            "INSERT OR IGNORE INTO chunks (digest, packfile) VALUES (?1, ?2)",
            params![chunk.digest.to_string(), packfile],
        )?;
        Ok(())
    }

    fn get_chunk(&self, digest: &Checksum) -> Result<Option<Chunk>, Error> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT packfile FROM chunks WHERE digest = ?1",
                params![digest.to_string()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        match row {
            None => Ok(None),
            Some(packfile_str) => {
                let packfile = match packfile_str {
                    Some(s) => Some(parse_checksum(&s)?),
                    None => None,
                };
                Ok(Some(Chunk {
                    digest: digest.clone(),
                    offset: 0,
                    length: 0,
                    filepath: None,
                    packfile,
                }))
            }
        }
    }

    fn get_all_chunk_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT digest FROM chunks")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
        for row in rows {
            digests.push(row?);
        }
        Ok(digests)
    }

    fn delete_chunk(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM chunks WHERE digest = ?1", params![id])?;
        Ok(())
    }

    fn insert_pack(&self, pack: &Pack) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO packs (digest, upload_time) VALUES (?1, ?2)",
            params![pack.digest.to_string(), dt_to_millis(&pack.upload_time)],
        )?;
        if inserted == 1 {
            for loc in &pack.locations {
                tx.execute(
                    "INSERT INTO pack_locations (pack_digest, store_id, bucket, object) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![pack.digest.to_string(), loc.store, loc.bucket, loc.object],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn put_pack(&self, pack: &Pack) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM packs WHERE digest = ?1",
            params![pack.digest.to_string()],
        )?;
        tx.execute(
            "INSERT INTO packs (digest, upload_time) VALUES (?1, ?2)",
            params![pack.digest.to_string(), dt_to_millis(&pack.upload_time)],
        )?;
        for loc in &pack.locations {
            tx.execute(
                "INSERT INTO pack_locations (pack_digest, store_id, bucket, object) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![pack.digest.to_string(), loc.store, loc.bucket, loc.object],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn get_pack(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        let conn = self.conn.lock().unwrap();
        let digest_str = digest.to_string();
        let mut stmt = conn.prepare("SELECT digest, upload_time FROM packs WHERE digest = ?1")?;
        let row = stmt
            .query_row(params![digest_str.clone()], |row| {
                let digest: String = row.get(0)?;
                let upload_time: i64 = row.get(1)?;
                Ok((digest, upload_time))
            })
            .optional()?;
        match row {
            None => Ok(None),
            Some((digest_text, upload_time)) => {
                let locations = read_pack_locations(&conn, "pack_locations", &digest_text)?;
                Ok(Some(Pack {
                    digest: parse_checksum(&digest_text)?,
                    locations,
                    upload_time: millis_to_dt(upload_time),
                }))
            }
        }
    }

    fn get_all_pack_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT digest FROM packs")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
        for row in rows {
            digests.push(row?);
        }
        Ok(digests)
    }

    fn delete_pack(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM packs WHERE digest = ?1", params![id])?;
        Ok(())
    }

    fn insert_database(&self, pack: &Pack) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO databases (digest, upload_time) VALUES (?1, ?2)",
            params![pack.digest.to_string(), dt_to_millis(&pack.upload_time)],
        )?;
        if inserted == 1 {
            for loc in &pack.locations {
                tx.execute(
                    "INSERT INTO database_locations (database_digest, store_id, bucket, object) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![pack.digest.to_string(), loc.store, loc.bucket, loc.object],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn put_database(&self, pack: &Pack) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM databases WHERE digest = ?1",
            params![pack.digest.to_string()],
        )?;
        tx.execute(
            "INSERT INTO databases (digest, upload_time) VALUES (?1, ?2)",
            params![pack.digest.to_string(), dt_to_millis(&pack.upload_time)],
        )?;
        for loc in &pack.locations {
            tx.execute(
                "INSERT INTO database_locations (database_digest, store_id, bucket, object) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![pack.digest.to_string(), loc.store, loc.bucket, loc.object],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn get_database(&self, digest: &Checksum) -> Result<Option<Pack>, Error> {
        let conn = self.conn.lock().unwrap();
        let digest_str = digest.to_string();
        let mut stmt =
            conn.prepare("SELECT digest, upload_time FROM databases WHERE digest = ?1")?;
        let row = stmt
            .query_row(params![digest_str.clone()], |row| {
                let digest: String = row.get(0)?;
                let upload_time: i64 = row.get(1)?;
                Ok((digest, upload_time))
            })
            .optional()?;
        match row {
            None => Ok(None),
            Some((digest_text, upload_time)) => {
                let locations = read_pack_locations(&conn, "database_locations", &digest_text)?;
                Ok(Some(Pack {
                    digest: parse_checksum(&digest_text)?,
                    locations,
                    upload_time: millis_to_dt(upload_time),
                }))
            }
        }
    }

    fn get_databases(&self) -> Result<Vec<Pack>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT digest, upload_time FROM databases")?;
        let rows: Vec<(String, i64)> = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut packs: Vec<Pack> = Vec::with_capacity(rows.len());
        for (digest_text, upload_time) in rows {
            let locations = read_pack_locations(&conn, "database_locations", &digest_text)?;
            packs.push(Pack {
                digest: parse_checksum(&digest_text)?,
                locations,
                upload_time: millis_to_dt(upload_time),
            });
        }
        Ok(packs)
    }

    fn delete_database(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM databases WHERE digest = ?1", params![id])?;
        Ok(())
    }

    fn insert_xattr(&self, digest: &Checksum, xattr: &[u8]) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO xattrs (digest, value) VALUES (?1, ?2)",
            params![digest.to_string(), xattr],
        )?;
        Ok(())
    }

    fn get_xattr(&self, digest: &Checksum) -> Result<Option<Vec<u8>>, Error> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT value FROM xattrs WHERE digest = ?1",
                params![digest.to_string()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        Ok(row)
    }

    fn get_all_xattr_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT digest FROM xattrs")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
        for row in rows {
            digests.push(row?);
        }
        Ok(digests)
    }

    fn delete_xattr(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM xattrs WHERE digest = ?1", params![id])?;
        Ok(())
    }

    fn insert_file(&self, file: &File) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO files (digest, length) VALUES (?1, ?2)",
            params![file.digest.to_string(), file.length as i64],
        )?;
        if inserted == 1 {
            for (idx, (offset, chunk_digest)) in file.chunks.iter().enumerate() {
                tx.execute(
                    "INSERT INTO file_chunks (file_digest, ord, offset, chunk_digest) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        file.digest.to_string(),
                        idx as i64,
                        *offset as i64,
                        chunk_digest.to_string(),
                    ],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn get_file(&self, digest: &Checksum) -> Result<Option<File>, Error> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT length FROM files WHERE digest = ?1",
                params![digest.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let length = match row {
            None => return Ok(None),
            Some(v) => v,
        };
        let mut stmt = conn.prepare(
            "SELECT offset, chunk_digest FROM file_chunks \
             WHERE file_digest = ?1 ORDER BY ord",
        )?;
        let rows = stmt.query_map(params![digest.to_string()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut chunks: Vec<(u64, Checksum)> = Vec::new();
        for row in rows {
            let (offset, chunk_text) = row?;
            chunks.push((offset as u64, parse_checksum(&chunk_text)?));
        }
        Ok(Some(File {
            digest: digest.clone(),
            length: length as u64,
            chunks,
        }))
    }

    fn get_all_file_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT digest FROM files")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
        for row in rows {
            digests.push(row?);
        }
        Ok(digests)
    }

    fn delete_file(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM files WHERE digest = ?1", params![id])?;
        Ok(())
    }

    fn insert_tree(&self, tree: &Tree) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO trees (digest, file_count) VALUES (?1, ?2)",
            params![tree.digest.to_string(), tree.file_count as i64],
        )?;
        if inserted == 1 {
            for entry in &tree.entries {
                let xattrs_json = xattrs_to_json(&entry.xattrs)?;
                tx.execute(
                    "INSERT INTO tree_entries \
                     (tree_digest, name, mode, uid, user_name, gid, group_name, \
                      ctime, mtime, reference, xattrs_json) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        tree.digest.to_string(),
                        entry.name,
                        entry.mode.map(|m| m as i64),
                        entry.uid.map(|u| u as i64),
                        entry.user,
                        entry.gid.map(|g| g as i64),
                        entry.group,
                        dt_to_millis(&entry.ctime),
                        dt_to_millis(&entry.mtime),
                        entry.reference.to_string(),
                        xattrs_json,
                    ],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn get_tree(&self, digest: &Checksum) -> Result<Option<Tree>, Error> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT file_count FROM trees WHERE digest = ?1",
                params![digest.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let file_count = match row {
            None => return Ok(None),
            Some(v) => v as u32,
        };
        let mut stmt = conn.prepare(
            "SELECT name, mode, uid, user_name, gid, group_name, ctime, mtime, \
             reference, xattrs_json FROM tree_entries \
             WHERE tree_digest = ?1 ORDER BY name",
        )?;
        let entries_iter = stmt.query_map(params![digest.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,                 // name
                row.get::<_, Option<i64>>(1)?,            // mode
                row.get::<_, Option<i64>>(2)?,            // uid
                row.get::<_, Option<String>>(3)?,         // user
                row.get::<_, Option<i64>>(4)?,            // gid
                row.get::<_, Option<String>>(5)?,         // group
                row.get::<_, i64>(6)?,                    // ctime
                row.get::<_, i64>(7)?,                    // mtime
                row.get::<_, String>(8)?,                 // reference
                row.get::<_, Option<String>>(9)?,         // xattrs_json
            ))
        })?;
        let mut entries: Vec<TreeEntry> = Vec::new();
        for row in entries_iter {
            let (name, mode, uid, user, gid, group, ctime, mtime, reference, xattrs_json) = row?;
            entries.push(TreeEntry {
                name,
                mode: mode.map(|v| v as u32),
                uid: uid.map(|v| v as u32),
                user,
                gid: gid.map(|v| v as u32),
                group,
                ctime: millis_to_dt(ctime),
                mtime: millis_to_dt(mtime),
                reference: TreeReference::from_str(&reference)?,
                xattrs: xattrs_from_json(xattrs_json)?,
            });
        }
        Ok(Some(Tree {
            digest: digest.clone(),
            entries,
            file_count,
        }))
    }

    fn get_all_tree_digests(&self) -> Result<HashedArrayTree<String>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT digest FROM trees")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut digests: HashedArrayTree<String> = HashedArrayTree::new();
        for row in rows {
            digests.push(row?);
        }
        Ok(digests)
    }

    fn delete_tree(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM trees WHERE digest = ?1", params![id])?;
        Ok(())
    }

    fn put_store(&self, store: &Store) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM stores WHERE id = ?1", params![store.id])?;
        let (kind, days) = pack_retention_to_columns(&store.retention);
        tx.execute(
            "INSERT INTO stores (id, store_type, label, retention_kind, retention_days) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![store.id, store.store_type.to_string(), store.label, kind, days],
        )?;
        for (k, v) in &store.properties {
            tx.execute(
                "INSERT INTO store_properties (store_id, key, value) VALUES (?1, ?2, ?3)",
                params![store.id, k, v],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn get_stores(&self) -> Result<Vec<Store>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, store_type, label, retention_kind, retention_days FROM stores",
        )?;
        let rows: Vec<(String, String, String, String, Option<i64>)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut stores: Vec<Store> = Vec::with_capacity(rows.len());
        for (id, st_type, label, kind, days) in rows {
            let mut props_stmt =
                conn.prepare("SELECT key, value FROM store_properties WHERE store_id = ?1")?;
            let mut properties: HashMap<String, String> = HashMap::new();
            let prop_rows = props_stmt.query_map(params![id.clone()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for prop in prop_rows {
                let (k, v) = prop?;
                properties.insert(k, v);
            }
            stores.push(Store {
                id,
                store_type: StoreType::from_str(&st_type)?,
                label,
                properties,
                retention: pack_retention_from_columns(&kind, days)?,
            });
        }
        Ok(stores)
    }

    fn get_store(&self, id: &str) -> Result<Option<Store>, Error> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT store_type, label, retention_kind, retention_days FROM stores \
                 WHERE id = ?1",
                params![id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .optional()?;
        let (st_type, label, kind, days) = match row {
            None => return Ok(None),
            Some(v) => v,
        };
        let mut props_stmt =
            conn.prepare("SELECT key, value FROM store_properties WHERE store_id = ?1")?;
        let mut properties: HashMap<String, String> = HashMap::new();
        let prop_rows = props_stmt.query_map(params![id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for prop in prop_rows {
            let (k, v) = prop?;
            properties.insert(k, v);
        }
        Ok(Some(Store {
            id: id.to_owned(),
            store_type: StoreType::from_str(&st_type)?,
            label,
            properties,
            retention: pack_retention_from_columns(&kind, days)?,
        }))
    }

    fn delete_store(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM stores WHERE id = ?1", params![id])?;
        Ok(())
    }

    fn put_dataset(&self, dataset: &Dataset) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM datasets WHERE id = ?1", params![dataset.id])?;
        let (kind, param) = snapshot_retention_to_columns(&dataset.retention);
        tx.execute(
            "INSERT INTO datasets \
             (id, basepath, workspace, snapshot, chunk_size, pack_size, retention_kind, retention_param) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                dataset.id,
                dataset.basepath.to_string_lossy(),
                dataset.workspace.to_string_lossy(),
                dataset.snapshot.as_ref().map(|c| c.to_string()),
                dataset.chunk_size as i64,
                dataset.pack_size as i64,
                kind,
                param,
            ],
        )?;
        for (idx, store_id) in dataset.stores.iter().enumerate() {
            tx.execute(
                "INSERT INTO dataset_stores (dataset_id, ord, store_id) VALUES (?1, ?2, ?3)",
                params![dataset.id, idx as i64, store_id],
            )?;
        }
        for (idx, pattern) in dataset.excludes.iter().enumerate() {
            tx.execute(
                "INSERT INTO dataset_excludes (dataset_id, ord, pattern) VALUES (?1, ?2, ?3)",
                params![dataset.id, idx as i64, pattern],
            )?;
        }
        for (idx, schedule) in dataset.schedules.iter().enumerate() {
            let json = serde_json::to_string(schedule)?;
            tx.execute(
                "INSERT INTO dataset_schedules (dataset_id, ord, schedule_json) \
                 VALUES (?1, ?2, ?3)",
                params![dataset.id, idx as i64, json],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn get_datasets(&self) -> Result<Vec<Dataset>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id FROM datasets")?;
        let ids: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);
        let mut results: Vec<Dataset> = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(dataset) = read_dataset(&conn, &id)? {
                results.push(dataset);
            }
        }
        Ok(results)
    }

    fn get_dataset(&self, id: &str) -> Result<Option<Dataset>, Error> {
        let conn = self.conn.lock().unwrap();
        read_dataset(&conn, id)
    }

    fn delete_dataset(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM datasets WHERE id = ?1", params![id])?;
        Ok(())
    }

    fn put_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        let counts_json = serde_json::to_string(&snapshot.file_counts)?;
        conn.execute(
            "INSERT INTO snapshots \
             (digest, parent, tree, start_time, end_time, file_counts_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(digest) DO UPDATE SET \
              parent = excluded.parent, \
              tree = excluded.tree, \
              start_time = excluded.start_time, \
              end_time = excluded.end_time, \
              file_counts_json = excluded.file_counts_json",
            params![
                snapshot.digest.to_string(),
                snapshot.parent.as_ref().map(|c| c.to_string()),
                snapshot.tree.to_string(),
                dt_to_millis(&snapshot.start_time),
                snapshot.end_time.as_ref().map(dt_to_millis),
                counts_json,
            ],
        )?;
        Ok(())
    }

    fn get_snapshot(&self, digest: &Checksum) -> Result<Option<Snapshot>, Error> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT parent, tree, start_time, end_time, file_counts_json \
                 FROM snapshots WHERE digest = ?1",
                params![digest.to_string()],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?;
        match row {
            None => Ok(None),
            Some((parent, tree, start_time, end_time, counts_json)) => {
                let parent = match parent {
                    Some(s) => Some(parse_checksum(&s)?),
                    None => None,
                };
                let file_counts: FileCounts = serde_json::from_str(&counts_json)?;
                Ok(Some(Snapshot {
                    digest: digest.clone(),
                    parent,
                    start_time: millis_to_dt(start_time),
                    end_time: end_time.map(millis_to_dt),
                    file_counts,
                    tree: parse_checksum(&tree)?,
                }))
            }
        }
    }

    fn delete_snapshot(&self, id: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM snapshots WHERE digest = ?1", params![id])?;
        Ok(())
    }

    fn get_db_path(&self) -> PathBuf {
        self.dir_path.clone()
    }

    fn create_backup(&self, path: Option<PathBuf>) -> Result<PathBuf, Error> {
        // Mirror RocksDB semantics: when path is None, default to a sibling
        // directory `<dir_path>.backup` containing the backup file. When the
        // caller provides a path, treat it as the destination directory.
        let backup_dir = match path {
            Some(p) => p,
            None => {
                let mut p = self.dir_path.clone();
                let name = p
                    .file_name()
                    .map(|s| s.to_owned())
                    .unwrap_or_else(|| std::ffi::OsString::from("zorigami"));
                let mut new_name = name.clone();
                new_name.push(".backup");
                p.set_file_name(new_name);
                p
            }
        };
        std::fs::create_dir_all(&backup_dir)?;
        let backup_file = backup_dir.join(DB_FILENAME);
        // VACUUM INTO refuses to overwrite an existing file; clear any prior
        // backup so the operation is idempotent.
        let _ = std::fs::remove_file(&backup_file);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "VACUUM INTO ?1",
            params![backup_file.to_string_lossy().as_ref()],
        )?;
        Ok(backup_dir)
    }

    fn restore_from_backup(&self, path: Option<PathBuf>) -> Result<(), Error> {
        let backup_dir = match path {
            Some(p) => p,
            None => {
                let mut p = self.dir_path.clone();
                let name = p
                    .file_name()
                    .map(|s| s.to_owned())
                    .unwrap_or_else(|| std::ffi::OsString::from("zorigami"));
                let mut new_name = name.clone();
                new_name.push(".backup");
                p.set_file_name(new_name);
                p
            }
        };
        let backup_file = backup_dir.join(DB_FILENAME);
        if !backup_file.exists() {
            return Err(anyhow!(
                "SQLite backup file not found at {}",
                backup_file.display()
            ));
        }
        let live_file = self.db_file();
        // Close the live connection by replacing it with one backed by an
        // in-memory database, copy the backup file over the live file, then
        // reopen the live connection. WAL/SHM sidecars must be removed so the
        // restored file is the source of truth.
        let mut conn_guard = self.conn.lock().unwrap();
        *conn_guard = Connection::open_in_memory()?;
        let _ = std::fs::remove_file(live_file.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(live_file.with_extension("sqlite-shm"));
        std::fs::copy(&backup_file, &live_file)?;
        let new_conn = Connection::open(&live_file)?;
        configure_connection(&new_conn)?;
        new_conn.execute_batch(SCHEMA_DDL)?;
        *conn_guard = new_conn;
        Ok(())
    }

    fn get_entity_counts(&self) -> Result<RecordCounts, Error> {
        let conn = self.conn.lock().unwrap();
        let count = |table: &str| -> Result<usize, Error> {
            let n: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |row| row.get(0))?;
            Ok(n as usize)
        };
        Ok(RecordCounts {
            chunk: count("chunks")?,
            dataset: count("datasets")?,
            file: count("files")?,
            pack: count("packs")?,
            snapshot: count("snapshots")?,
            store: count("stores")?,
            tree: count("trees")?,
            xattr: count("xattrs")?,
        })
    }

    fn add_bucket(&self, name: &str) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO buckets (name, created_at) VALUES (?1, ?2)",
            params![name, Utc::now().timestamp_millis()],
        )?;
        Ok(())
    }

    fn count_buckets(&self) -> Result<usize, Error> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM buckets", [], |row| row.get(0))?;
        Ok(n as usize)
    }

    fn get_last_bucket(&self) -> Result<Option<String>, Error> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT name FROM buckets ORDER BY name DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(row)
    }

    fn get_random_bucket(&self) -> Result<Option<String>, Error> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT name FROM buckets ORDER BY random() LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(row)
    }

    fn get_schema_version(&self) -> Result<u32, Error> {
        let conn = self.conn.lock().unwrap();
        let v: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        Ok(v as u32)
    }

    fn set_schema_version(&self, version: u32) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        // PRAGMA user_version does not accept bound parameters.
        conn.execute_batch(&format!("PRAGMA user_version = {};", version))?;
        Ok(())
    }
}

fn read_dataset(conn: &Connection, id: &str) -> Result<Option<Dataset>, Error> {
    let row = conn
        .query_row(
            "SELECT basepath, workspace, snapshot, chunk_size, pack_size, retention_kind, retention_param \
             FROM datasets WHERE id = ?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                ))
            },
        )
        .optional()?;
    let (basepath, workspace, snapshot, chunk_size, pack_size, retention_kind, retention_param) =
        match row {
            None => return Ok(None),
            Some(v) => v,
        };
    let snapshot = match snapshot {
        Some(s) => Some(parse_checksum(&s)?),
        None => None,
    };
    let mut stores_stmt = conn.prepare(
        "SELECT store_id FROM dataset_stores WHERE dataset_id = ?1 ORDER BY ord",
    )?;
    let stores: Vec<String> = stores_stmt
        .query_map(params![id], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut excludes_stmt = conn.prepare(
        "SELECT pattern FROM dataset_excludes WHERE dataset_id = ?1 ORDER BY ord",
    )?;
    let excludes: Vec<String> = excludes_stmt
        .query_map(params![id], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut schedules_stmt = conn.prepare(
        "SELECT schedule_json FROM dataset_schedules WHERE dataset_id = ?1 ORDER BY ord",
    )?;
    let schedule_jsons: Vec<String> = schedules_stmt
        .query_map(params![id], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut schedules: Vec<Schedule> = Vec::with_capacity(schedule_jsons.len());
    for json in schedule_jsons {
        schedules.push(serde_json::from_str(&json)?);
    }
    Ok(Some(Dataset {
        id: id.to_owned(),
        basepath: PathBuf::from(basepath),
        schedules,
        snapshot,
        workspace: PathBuf::from(workspace),
        chunk_size: chunk_size as usize,
        pack_size: pack_size as u64,
        stores,
        excludes,
        retention: snapshot_retention_from_columns(&retention_kind, retention_param)?,
    }))
}
