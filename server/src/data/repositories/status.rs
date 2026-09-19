//
// Copyright (c) 2026 Nathan Fiedler
//

//! SQLite-backed implementation of `StatusRepository`.
//!
//! Errors from background operations (pruning, test restore, backup, database
//! scrub, pack pruning, workspace cleanup) are recorded here, alongside the
//! outcome of the most recent run of each periodic task, so that both can be
//! surfaced in the web interface. The store is intentionally separate from the
//! main RocksDB database: this data is structured, low volume, and orthogonal
//! to the core backup/restore workflow.

use crate::domain::entities::{BackgroundOperation, CapturedError, TaskOutcome, TaskRun};
use crate::domain::repositories::StatusRepository;
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

CREATE TABLE IF NOT EXISTS task_runs (
    operation   TEXT    NOT NULL,
    dataset_id  TEXT    NOT NULL DEFAULT '',
    started_at  TEXT    NOT NULL,
    finished_at TEXT    NOT NULL,
    outcome     TEXT    NOT NULL,
    issue_count INTEGER NOT NULL,
    summary     TEXT    NOT NULL,
    PRIMARY KEY (operation, dataset_id)
);
";

/// Interval between opportunistic retention sweeps triggered from inside
/// `record_error`. Chosen to amortize the delete cost across many inserts.
const OPPORTUNISTIC_PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub struct StatusRepositoryImpl {
    conn: Mutex<Connection>,
    retention_days: u32,
    last_pruned: Mutex<Option<Instant>>,
}

impl StatusRepositoryImpl {
    /// Open (or create) the SQLite database at the given path and apply the
    /// schema.
    pub fn new<P: AsRef<Path>>(db_path: P, retention_days: u32) -> Result<Self, Error> {
        if let Some(parent) = db_path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "StatusRepositoryImpl::new create_dir_all({})",
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

impl StatusRepository for StatusRepositoryImpl {
    fn record_error(
        &self,
        operation: BackgroundOperation,
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
            let operation = BackgroundOperation::from_str(&operation).map_err(|e| {
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

    fn record_run(&self, run: &TaskRun) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        // Only the most recent run of each operation is kept, so this replaces
        // any existing row rather than accumulating history. The table never
        // grows beyond one row per operation (per dataset, for those that work
        // a dataset at a time), so it needs no retention sweep of its own.
        conn.execute(
            "INSERT INTO task_runs \
             (operation, dataset_id, started_at, finished_at, outcome, issue_count, summary) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
             ON CONFLICT(operation, dataset_id) DO UPDATE SET \
             started_at = excluded.started_at, finished_at = excluded.finished_at, \
             outcome = excluded.outcome, issue_count = excluded.issue_count, \
             summary = excluded.summary",
            params![
                run.operation.to_string(),
                run.dataset_id.as_deref().unwrap_or(""),
                run.started_at.to_rfc3339(),
                run.finished_at.to_rfc3339(),
                run.outcome.to_string(),
                run.issue_count as i64,
                run.summary,
            ],
        )?;
        Ok(())
    }

    fn list_runs(&self) -> Result<Vec<TaskRun>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT operation, dataset_id, started_at, finished_at, outcome, issue_count, summary \
             FROM task_runs ORDER BY finished_at DESC",
        )?;
        let rows: Vec<TaskRun> = stmt
            .query_map([], |row| {
                let operation: String = row.get(0)?;
                let dataset_id: String = row.get(1)?;
                let started_at: String = row.get(2)?;
                let finished_at: String = row.get(3)?;
                let outcome: String = row.get(4)?;
                let issue_count: i64 = row.get(5)?;
                let summary: String = row.get(6)?;
                let operation = BackgroundOperation::from_str(&operation).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        e.into(),
                    )
                })?;
                let outcome = TaskOutcome::from_str(&outcome).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        e.into(),
                    )
                })?;
                Ok(TaskRun {
                    operation,
                    // the empty string stands in for "no dataset" so that the
                    // primary key works; SQLite treats NULLs as distinct
                    dataset_id: (!dataset_id.is_empty()).then_some(dataset_id),
                    started_at: parse_timestamp(&started_at, 2)?,
                    finished_at: parse_timestamp(&finished_at, 3)?,
                    outcome,
                    issue_count: issue_count.max(0) as u32,
                    summary,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }
}

/// Parse an RFC 3339 timestamp from the given column into UTC.
fn parse_timestamp(value: &str, column: usize) -> rusqlite::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                column,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?
        .with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_list() {
        let repo = StatusRepositoryImpl::in_memory(90).unwrap();
        repo.record_error(BackgroundOperation::Prune, Some("ds1".into()), "boom")
            .unwrap();
        repo.record_error(BackgroundOperation::RestoreTest, None, "nope")
            .unwrap();
        let all = repo.list_errors(None).unwrap();
        assert_eq!(all.len(), 2);
        // Newest first (id DESC tiebreaker since timestamps may be identical
        // in a fast test)
        assert_eq!(all[0].operation, BackgroundOperation::RestoreTest);
        assert_eq!(all[0].dataset_id, None);
        assert_eq!(all[0].message, "nope");
        assert_eq!(all[1].operation, BackgroundOperation::Prune);
        assert_eq!(all[1].dataset_id.as_deref(), Some("ds1"));
        assert_eq!(repo.count_errors().unwrap(), 2);
    }

    #[test]
    fn test_list_limit() {
        let repo = StatusRepositoryImpl::in_memory(90).unwrap();
        for i in 0..5 {
            repo.record_error(
                BackgroundOperation::Backup,
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
        let repo = StatusRepositoryImpl::in_memory(90).unwrap();
        repo.record_error(BackgroundOperation::Prune, None, "a")
            .unwrap();
        repo.record_error(BackgroundOperation::Prune, None, "b")
            .unwrap();
        repo.record_error(BackgroundOperation::Prune, None, "c")
            .unwrap();
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
        let repo = StatusRepositoryImpl::in_memory(90).unwrap();
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
        repo.record_error(BackgroundOperation::Prune, None, "fresh")
            .unwrap();
        assert_eq!(repo.count_errors().unwrap(), 2);
        let removed = repo.prune_older_than(5).unwrap();
        assert_eq!(removed, 1);
        let remaining = repo.list_errors(None).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].message, "fresh");
    }

    fn make_run(operation: BackgroundOperation, outcome: TaskOutcome, summary: &str) -> TaskRun {
        let started_at = Utc::now();
        TaskRun {
            operation,
            dataset_id: None,
            started_at,
            finished_at: started_at + chrono::Duration::seconds(3),
            outcome,
            issue_count: 0,
            summary: summary.into(),
        }
    }

    #[test]
    fn test_record_and_list_runs() {
        let repo = StatusRepositoryImpl::in_memory(90).unwrap();
        assert!(repo.list_runs().unwrap().is_empty());
        repo.record_run(&make_run(
            BackgroundOperation::PackPrune,
            TaskOutcome::Success,
            "deleted 3 packs",
        ))
        .unwrap();
        repo.record_run(&make_run(
            BackgroundOperation::RestoreTest,
            TaskOutcome::Skipped,
            "no eligible file",
        ))
        .unwrap();
        let runs = repo.list_runs().unwrap();
        assert_eq!(runs.len(), 2);
        let pack = runs
            .iter()
            .find(|r| r.operation == BackgroundOperation::PackPrune)
            .unwrap();
        assert_eq!(pack.outcome, TaskOutcome::Success);
        assert_eq!(pack.summary, "deleted 3 packs");
        assert_eq!(pack.dataset_id, None);
        assert_eq!(pack.duration_millis(), 3000);
        let test = runs
            .iter()
            .find(|r| r.operation == BackgroundOperation::RestoreTest)
            .unwrap();
        assert_eq!(test.outcome, TaskOutcome::Skipped);
    }

    #[test]
    fn test_record_run_replaces_previous() {
        let repo = StatusRepositoryImpl::in_memory(90).unwrap();
        repo.record_run(&make_run(
            BackgroundOperation::DatabaseScrub,
            TaskOutcome::Success,
            "first",
        ))
        .unwrap();
        repo.record_run(&make_run(
            BackgroundOperation::DatabaseScrub,
            TaskOutcome::Failed,
            "second",
        ))
        .unwrap();
        let runs = repo.list_runs().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskOutcome::Failed);
        assert_eq!(runs[0].summary, "second");
    }

    #[test]
    fn test_record_run_keyed_by_dataset() {
        let repo = StatusRepositoryImpl::in_memory(90).unwrap();
        // Snapshot pruning runs one dataset at a time, so each dataset keeps
        // its own most recent run rather than overwriting the others.
        for dataset in ["ds1", "ds2"] {
            let mut run = make_run(BackgroundOperation::Prune, TaskOutcome::Success, dataset);
            run.dataset_id = Some(dataset.into());
            repo.record_run(&run).unwrap();
        }
        let runs = repo.list_runs().unwrap();
        assert_eq!(runs.len(), 2);
        let mut ids: Vec<String> = runs.iter().map(|r| r.dataset_id.clone().unwrap()).collect();
        ids.sort();
        assert_eq!(ids, vec!["ds1".to_string(), "ds2".to_string()]);

        // a second run of the same dataset replaces only that dataset's row
        let mut again = make_run(BackgroundOperation::Prune, TaskOutcome::Issues, "again");
        again.dataset_id = Some("ds1".into());
        again.issue_count = 4;
        repo.record_run(&again).unwrap();
        let runs = repo.list_runs().unwrap();
        assert_eq!(runs.len(), 2);
        let ds1 = runs
            .iter()
            .find(|r| r.dataset_id.as_deref() == Some("ds1"))
            .unwrap();
        assert_eq!(ds1.outcome, TaskOutcome::Issues);
        assert_eq!(ds1.issue_count, 4);
        let ds2 = runs
            .iter()
            .find(|r| r.dataset_id.as_deref() == Some("ds2"))
            .unwrap();
        assert_eq!(ds2.outcome, TaskOutcome::Success);
    }

    #[test]
    fn test_runs_survive_reopen() {
        // The point of persisting runs is that they outlive the process, so
        // verify a row written through one connection is read by the next.
        let tmpdir = tempfile::tempdir().unwrap();
        let db_path = tmpdir.path().join("status.db");
        {
            let repo = StatusRepositoryImpl::new(&db_path, 90).unwrap();
            repo.record_run(&make_run(
                BackgroundOperation::WorkspaceCleanup,
                TaskOutcome::Success,
                "removed 2 files",
            ))
            .unwrap();
        }
        let repo = StatusRepositoryImpl::new(&db_path, 90).unwrap();
        let runs = repo.list_runs().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].operation, BackgroundOperation::WorkspaceCleanup);
        assert_eq!(runs[0].summary, "removed 2 files");
    }
}
