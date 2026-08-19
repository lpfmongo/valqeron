//! Cached statements for the `task_execution` table — terminal run history.
//!
//! Rows are write-once: inserted in the same transaction that deletes the
//! queue row, deleted only by retention pruning.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use valqeron_core::{TaskExecution, TaskId};

use crate::sqlite::row::{FromRow, canonical_timestamp};
use crate::sqlite::task_execution::model::ExecutionRow;

const EXECUTION_COLUMNS: &str = "id, kind, outcome, payload, scheduled_at, started_at, \
                                 finished_at, attempts, duration_ms, error, created_at";

pub(crate) fn insert(conn: &Connection, execution: &TaskExecution) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO task_execution (id, kind, outcome, payload, scheduled_at, started_at, \
                                     finished_at, attempts, duration_ms, error, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )?;
    stmt.execute(params![
        execution.id.as_bytes(),
        execution.kind.as_str(),
        execution.outcome.as_str(),
        execution.payload,
        canonical_timestamp(execution.scheduled_at),
        execution.started_at.map(canonical_timestamp),
        canonical_timestamp(execution.finished_at),
        execution.attempts,
        execution
            .duration_ms
            .map(|ms| i64::try_from(ms).unwrap_or(i64::MAX)),
        execution.error,
        canonical_timestamp(execution.created_at),
    ])
}

pub(crate) fn find_by_id(conn: &Connection, id: &TaskId) -> rusqlite::Result<Option<ExecutionRow>> {
    let sql = format!("SELECT {EXECUTION_COLUMNS} FROM task_execution WHERE id = ?1");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_row(params![id.as_bytes()], ExecutionRow::from_row)
        .optional()
}

pub(crate) fn list_recent(conn: &Connection, limit: u32) -> rusqlite::Result<Vec<ExecutionRow>> {
    let sql = format!(
        "SELECT {EXECUTION_COLUMNS} FROM task_execution
         ORDER BY finished_at DESC, id DESC LIMIT ?1"
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_map(params![limit], ExecutionRow::from_row)?
        .collect()
}

pub(crate) fn prune_finished(
    conn: &Connection,
    older_than: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached("DELETE FROM task_execution WHERE finished_at < ?1")?;
    stmt.execute(params![canonical_timestamp(older_than)])
}
