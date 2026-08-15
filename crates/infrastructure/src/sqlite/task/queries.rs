//! Cached statements for the `background_task` table.
//!
//! Time comparisons rely on the canonical persisted timestamp form
//! ([`canonical_timestamp`]): RFC 3339, millisecond precision, Z-suffixed
//! UTC — a uniform format, so lexicographic `TEXT` comparison is time order.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use valqeron_core::{BackgroundTask, TaskId};

use crate::sqlite::row::{FromRow, canonical_timestamp};
use crate::sqlite::task::mapping::status_as_str;
use crate::sqlite::task::model::TaskRow;

const TASK_COLUMNS: &str = "id, kind, status, payload, scheduled_at, started_at, finished_at, \
                            attempts, max_attempts, retry_delay_secs, last_error, created_at, \
                            updated_at, version";

/// Message recorded on rows found `RUNNING` at startup: the previous process
/// stopped (crash or overrun drain) before the run could be completed.
pub(crate) const INTERRUPTED_ERROR: &str =
    "interrupted: the engine stopped while the task was running";

pub(crate) fn insert(conn: &Connection, task: &BackgroundTask) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO background_task (id, kind, status, payload, scheduled_at, started_at, \
                                      finished_at, attempts, max_attempts, retry_delay_secs, \
                                      last_error, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
    )?;
    stmt.execute(params![
        task.id().as_bytes(),
        task.kind().as_str(),
        status_as_str(task.status()),
        task.payload(),
        canonical_timestamp(task.scheduled_at()),
        task.started_at().map(canonical_timestamp),
        task.finished_at().map(canonical_timestamp),
        task.attempts(),
        task.max_attempts(),
        task.retry_delay_secs(),
        task.last_error(),
        canonical_timestamp(task.created_at()),
        canonical_timestamp(task.updated_at()),
    ])
}

pub(crate) fn find_by_id(conn: &Connection, id: &TaskId) -> rusqlite::Result<Option<TaskRow>> {
    let sql = format!("SELECT {TASK_COLUMNS} FROM background_task WHERE id = ?1");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_row(params![id.as_bytes()], TaskRow::from_row)
        .optional()
}

pub(crate) fn list_recent(conn: &Connection, limit: u32) -> rusqlite::Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS} FROM background_task ORDER BY scheduled_at DESC, id DESC LIMIT ?1"
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_map(params![limit], TaskRow::from_row)?.collect()
}

pub(crate) fn exists_active(
    conn: &Connection,
    kind: &valqeron_core::TaskKind,
) -> rusqlite::Result<bool> {
    let mut stmt = conn.prepare_cached(
        "SELECT 1 FROM background_task
         WHERE kind = ?1 AND status IN ('PENDING', 'RUNNING') LIMIT 1",
    )?;
    stmt.query_row(params![kind.as_str()], |_| Ok(()))
        .optional()
        .map(|found| found.is_some())
}

/// Ids of due `PENDING` tasks, oldest due first. Claiming is a separate
/// per-id guarded update; both run under the same writer guard.
pub(crate) fn due_ids(
    conn: &Connection,
    now: DateTime<Utc>,
    limit: u32,
) -> rusqlite::Result<Vec<TaskId>> {
    let mut stmt = conn.prepare_cached(
        "SELECT id FROM background_task
         WHERE status = 'PENDING' AND scheduled_at <= ?1
         ORDER BY scheduled_at, id LIMIT ?2",
    )?;
    stmt.query_map(params![canonical_timestamp(now), limit], |row| {
        let bytes: Vec<u8> = row.get(0)?;
        uuid::Uuid::from_slice(&bytes)
            .map(TaskId::from_uuid)
            .map_err(|e| crate::sqlite::row::conversion_failure(0, rusqlite::types::Type::Blob, e))
    })?
    .collect()
}

/// Claim one due task: `PENDING → RUNNING`, counting the attempt. The status
/// guard makes the claim idempotent against double-dispatch bugs.
pub(crate) fn mark_running(
    conn: &Connection,
    id: &TaskId,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE background_task SET
            status = 'RUNNING',
            attempts = attempts + 1,
            started_at = ?2,
            updated_at = ?2,
            version = version + 1
         WHERE id = ?1 AND status = 'PENDING'",
    )?;
    stmt.execute(params![id.as_bytes(), canonical_timestamp(now)])
}

pub(crate) fn complete_succeeded(
    conn: &Connection,
    id: &TaskId,
    expected_version: u32,
    finished_at: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE background_task SET
            status = 'SUCCEEDED',
            finished_at = ?2,
            updated_at = ?2,
            last_error = NULL,
            version = version + 1
         WHERE id = ?1 AND version = ?3",
    )?;
    stmt.execute(params![
        id.as_bytes(),
        canonical_timestamp(finished_at),
        expected_version,
    ])
}

pub(crate) fn complete_retry(
    conn: &Connection,
    id: &TaskId,
    expected_version: u32,
    error: &str,
    failed_at: DateTime<Utc>,
    retry_at: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE background_task SET
            status = 'PENDING',
            scheduled_at = ?2,
            updated_at = ?3,
            last_error = ?4,
            version = version + 1
         WHERE id = ?1 AND version = ?5",
    )?;
    stmt.execute(params![
        id.as_bytes(),
        canonical_timestamp(retry_at),
        canonical_timestamp(failed_at),
        error,
        expected_version,
    ])
}

pub(crate) fn complete_failed(
    conn: &Connection,
    id: &TaskId,
    expected_version: u32,
    error: &str,
    finished_at: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE background_task SET
            status = 'FAILED',
            finished_at = ?2,
            updated_at = ?2,
            last_error = ?3,
            version = version + 1
         WHERE id = ?1 AND version = ?4",
    )?;
    stmt.execute(params![
        id.as_bytes(),
        canonical_timestamp(finished_at),
        error,
        expected_version,
    ])
}

/// Startup recovery, half 1: orphaned `RUNNING` rows that already spent their
/// final attempt become terminal `FAILED`.
pub(crate) fn fail_exhausted_running(
    conn: &Connection,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE background_task SET
            status = 'FAILED',
            finished_at = ?1,
            updated_at = ?1,
            last_error = ?2,
            version = version + 1
         WHERE status = 'RUNNING' AND attempts >= max_attempts",
    )?;
    stmt.execute(params![canonical_timestamp(now), INTERRUPTED_ERROR])
}

/// Startup recovery, half 2: orphaned `RUNNING` rows with attempts left go
/// back to `PENDING`, due immediately.
pub(crate) fn requeue_interrupted_running(
    conn: &Connection,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE background_task SET
            status = 'PENDING',
            scheduled_at = ?1,
            updated_at = ?1,
            last_error = ?2,
            version = version + 1
         WHERE status = 'RUNNING' AND attempts < max_attempts",
    )?;
    stmt.execute(params![canonical_timestamp(now), INTERRUPTED_ERROR])
}

pub(crate) fn prune_finished(
    conn: &Connection,
    older_than: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "DELETE FROM background_task
         WHERE status IN ('SUCCEEDED', 'FAILED')
           AND finished_at IS NOT NULL
           AND finished_at < ?1",
    )?;
    stmt.execute(params![canonical_timestamp(older_than)])
}

/// Kept for parity with the other adapters' guarded-write disambiguation.
pub(crate) const TASK_VERSION_SQL: &str = "SELECT version FROM background_task WHERE id = ?1";

/// Count of rows per kind, used by tests.
#[cfg(test)]
pub(crate) fn count_by_kind(
    conn: &Connection,
    kind: &valqeron_core::TaskKind,
) -> rusqlite::Result<u32> {
    let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM background_task WHERE kind = ?1")?;
    stmt.query_row(params![kind.as_str()], |row| row.get(0))
}
