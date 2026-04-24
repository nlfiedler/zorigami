//
// Copyright (c) 2026 Nathan Fiedler
//

//! SQLite-backed implementation of `ErrorRepository`.
//!
//! Errors from background operations (pruning, test restore, backup, future
//! database scrub) are recorded here so they can be surfaced in the web
//! interface. The store is intentionally separate from the main RocksDB
//! database: this data is structured, low volume, and orthogonal to the core
//! backup/restore workflow.

use crate::domain::entities::{CapturedError, ErrorOperation};
use crate::domain::repositories::ErrorRepository;
use anyhow::{Context, Error, anyhow};
use chrono::{DateTime, Utc};
use log::warn;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS errors (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp  TEXT    NOT NULL,
    operation  TEXT    NOT NULL,
    dataset_id TEXT,
    message    TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_errors_timestamp ON errors(timestamp);
";

/// Interval between opportunistic retention sweeps triggered from inside
/// `record_error`. Chosen to amortize the delete cost across many inserts.
const OPPORTUNISTIC_PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub struct ErrorRepositoryImpl {
    conn: Mutex<Connection>,
    retention_days: u32,
    last_pruned: Mutex<Option<Instant>>,
}

impl ErrorRepositoryImpl {
    /// Open (or create) the SQLite database at the given path and apply the
    /// schema.
    pub fn new<P: AsRef<Path>>(db_path: P, retention_days: u32) -> Result<Self, Error> {
        if let Some(parent) = db_path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "ErrorRepositoryImpl::new create_dir_all({})",
                    parent.display()
                )
            })?;
        }
        let conn = Connection::open(&db_path)
            .with_context(|| format!("open error db {}", db_path.as_ref().display()))?;
        conn.execute_batch(SCHEMA_SQL)?;
        Ok(Self {
            conn: Mutex::new(conn),
            retention_days,
            last_pruned: Mutex::new(None),
        })
    }

    /// Open an in-memory SQLite database; used by the unit tests.
    #[cfg(test)]
    fn in_memory(retention_days: u32) -> Result<Self, Error> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA_SQL)?;
        Ok(Self {
            conn: Mutex::new(conn),
            retention_days,
            last_pruned: Mutex::new(None),
        })
    }

    fn should_prune_now(&self) -> bool {
        let mut guard = self.last_pruned.lock().unwrap();
        match *guard {
            Some(last) if last.elapsed() < OPPORTUNISTIC_PRUNE_INTERVAL => false,
            _ => {
                *guard = Some(Instant::now());
                true
            }
        }
    }
}

impl ErrorRepository for ErrorRepositoryImpl {
    fn record_error(
        &self,
        operation: ErrorOperation,
        dataset_id: Option<String>,
        message: &str,
    ) -> Result<(), Error> {
        let now = Utc::now().to_rfc3339();
        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO errors (timestamp, operation, dataset_id, message) VALUES (?1, ?2, ?3, ?4)",
                params![now, operation.to_string(), dataset_id.as_deref(), message],
            )?;
        }
        if self.should_prune_now()
            && let Err(err) = self.prune_older_than(self.retention_days)
        {
            warn!("opportunistic error-log prune failed: {}", err);
        }
        Ok(())
    }

    fn list_errors(&self, limit: Option<u32>) -> Result<Vec<CapturedError>, Error> {
        let conn = self.conn.lock().unwrap();
        let sql = match limit {
            Some(_) => {
                "SELECT id, timestamp, operation, dataset_id, message \
                 FROM errors ORDER BY timestamp DESC, id DESC LIMIT ?1"
            }
            None => {
                "SELECT id, timestamp, operation, dataset_id, message \
                 FROM errors ORDER BY timestamp DESC, id DESC"
            }
        };
        let mut stmt = conn.prepare(sql)?;
        let map_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<CapturedError> {
            let id: i64 = row.get(0)?;
            let ts: String = row.get(1)?;
            let operation: String = row.get(2)?;
            let dataset_id: Option<String> = row.get(3)?;
            let message: String = row.get(4)?;
            let timestamp = DateTime::parse_from_rfc3339(&ts)
                .map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?
                .with_timezone(&Utc);
            let operation = ErrorOperation::from_str(&operation).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, e.into())
            })?;
            Ok(CapturedError {
                id,
                timestamp,
                operation,
                dataset_id,
                message,
            })
        };
        let rows: Vec<CapturedError> = match limit {
            Some(n) => stmt
                .query_map(params![n as i64], map_row)?
                .collect::<Result<_, _>>()?,
            None => stmt.query_map([], map_row)?.collect::<Result<_, _>>()?,
        };
        Ok(rows)
    }

    fn count_errors(&self) -> Result<u64, Error> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM errors", [], |row| row.get(0))
            .optional()?
            .unwrap_or(0);
        if count < 0 {
            return Err(anyhow!("negative count"));
        }
        Ok(count as u64)
    }

    fn delete_error(&self, id: i64) -> Result<bool, Error> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute("DELETE FROM errors WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    fn clear_all(&self) -> Result<u64, Error> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute("DELETE FROM errors", [])?;
        Ok(affected as u64)
    }

    fn prune_older_than(&self, days: u32) -> Result<u64, Error> {
        let cutoff = Utc::now() - chrono::Duration::days(days as i64);
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute(
            "DELETE FROM errors WHERE timestamp < ?1",
            params![cutoff.to_rfc3339()],
        )?;
        Ok(affected as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_list() {
        let repo = ErrorRepositoryImpl::in_memory(90).unwrap();
        repo.record_error(ErrorOperation::Prune, Some("ds1".into()), "boom")
            .unwrap();
        repo.record_error(ErrorOperation::RestoreTest, None, "nope")
            .unwrap();
        let all = repo.list_errors(None).unwrap();
        assert_eq!(all.len(), 2);
        // Newest first (id DESC tiebreaker since timestamps may be identical
        // in a fast test)
        assert_eq!(all[0].operation, ErrorOperation::RestoreTest);
        assert_eq!(all[0].dataset_id, None);
        assert_eq!(all[0].message, "nope");
        assert_eq!(all[1].operation, ErrorOperation::Prune);
        assert_eq!(all[1].dataset_id.as_deref(), Some("ds1"));
        assert_eq!(repo.count_errors().unwrap(), 2);
    }

    #[test]
    fn test_list_limit() {
        let repo = ErrorRepositoryImpl::in_memory(90).unwrap();
        for i in 0..5 {
            repo.record_error(
                ErrorOperation::Backup,
                Some("ds".into()),
                &format!("err {}", i),
            )
            .unwrap();
        }
        let limited = repo.list_errors(Some(2)).unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn test_delete_and_clear() {
        let repo = ErrorRepositoryImpl::in_memory(90).unwrap();
        repo.record_error(ErrorOperation::Prune, None, "a").unwrap();
        repo.record_error(ErrorOperation::Prune, None, "b").unwrap();
        repo.record_error(ErrorOperation::Prune, None, "c").unwrap();
        let all = repo.list_errors(None).unwrap();
        let doomed = all[0].id;
        assert!(repo.delete_error(doomed).unwrap());
        assert!(!repo.delete_error(doomed).unwrap());
        assert_eq!(repo.count_errors().unwrap(), 2);
        assert_eq!(repo.clear_all().unwrap(), 2);
        assert_eq!(repo.count_errors().unwrap(), 0);
    }

    #[test]
    fn test_prune_older_than() {
        let repo = ErrorRepositoryImpl::in_memory(90).unwrap();
        // Insert a row with an artificially old timestamp by bypassing the
        // repo API.
        {
            let conn = repo.conn.lock().unwrap();
            let old = (Utc::now() - chrono::Duration::days(10)).to_rfc3339();
            conn.execute(
                "INSERT INTO errors (timestamp, operation, dataset_id, message) VALUES (?1, ?2, NULL, ?3)",
                params![old, "Prune", "ancient"],
            )
            .unwrap();
        }
        repo.record_error(ErrorOperation::Prune, None, "fresh")
            .unwrap();
        assert_eq!(repo.count_errors().unwrap(), 2);
        let removed = repo.prune_older_than(5).unwrap();
        assert_eq!(removed, 1);
        let remaining = repo.list_errors(None).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].message, "fresh");
    }
}
