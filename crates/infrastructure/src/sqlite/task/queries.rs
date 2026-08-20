//! Cached statements for the `task_queue` table — live work only.
//!
//! Terminal completions are version-guarded DELETEs: the row moves to
//! `task_execution` (inserted by the caller in the same transaction), so
//! the queue never accumulates dead rows and the due-scan index stays hot.
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

const TASK_COLUMNS: &str = "id, kind, status, payload, scheduled_at, started_at, \
                            attempts, max_attempts, retry_delay_secs, last_error, created_at, \
                            updated_at, version";

pub(crate) fn insert(conn: &Connection, task: &BackgroundTask) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO task_queue (id, kind, status, payload, scheduled_at, started_at, \
                                 attempts, max_attempts, retry_delay_secs, \
                                 last_error, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
    )?;
    stmt.execute(params![
        task.id().as_bytes(),
        task.kind().as_str(),
        status_as_str(task.status()),
        task.payload(),
        canonical_timestamp(task.scheduled_at()),
        task.started_at().map(canonical_timestamp),
        task.attempts(),
        task.max_attempts(),
        task.retry_delay_secs(),
        task.last_error(),
        canonical_timestamp(task.created_at()),
        canonical_timestamp(task.updated_at()),
    ])
}

pub(crate) fn find_by_id(conn: &Connection, id: &TaskId) -> rusqlite::Result<Option<TaskRow>> {
    let sql = format!("SELECT {TASK_COLUMNS} FROM task_queue WHERE id = ?1");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_row(params![id.as_bytes()], TaskRow::from_row)
        .optional()
}

/// Every queued row, soonest first.
pub(crate) fn list_queued(conn: &Connection, limit: u32) -> rusqlite::Result<Vec<TaskRow>> {
    let sql = format!("SELECT {TASK_COLUMNS} FROM task_queue ORDER BY scheduled_at, id LIMIT ?1");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_map(params![limit], TaskRow::from_row)?.collect()
}

/// The earliest row of `kind`: the next (or currently running) run.
pub(crate) fn find_active(
    conn: &Connection,
    kind: &valqeron_core::TaskKind,
) -> rusqlite::Result<Option<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS} FROM task_queue
         WHERE kind = ?1
         ORDER BY scheduled_at, id LIMIT 1"
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_row(params![kind.as_str()], TaskRow::from_row)
        .optional()
}

/// The `PENDING` rows of `kind`, oldest first (retired-kind cleanup reads
/// them before deleting).
pub(crate) fn pending_rows(
    conn: &Connection,
    kind: &valqeron_core::TaskKind,
) -> rusqlite::Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS} FROM task_queue
         WHERE kind = ?1 AND status = 'PENDING'
         ORDER BY scheduled_at, id"
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_map(params![kind.as_str()], TaskRow::from_row)?
        .collect()
}

pub(crate) fn delete_pending(
    conn: &Connection,
    kind: &valqeron_core::TaskKind,
) -> rusqlite::Result<usize> {
    let mut stmt =
        conn.prepare_cached("DELETE FROM task_queue WHERE kind = ?1 AND status = 'PENDING'")?;
    stmt.execute(params![kind.as_str()])
}

pub(crate) fn exists_active(
    conn: &Connection,
    kind: &valqeron_core::TaskKind,
) -> rusqlite::Result<bool> {
    let mut stmt = conn.prepare_cached("SELECT 1 FROM task_queue WHERE kind = ?1 LIMIT 1")?;
    stmt.query_row(params![kind.as_str()], |_| Ok(()))
        .optional()
        .map(|found| found.is_some())
}

/// Ids of due `PENDING` tasks, oldest due first. Claiming is a separate
/// per-id guarded update; both run under the same writer guard.
///
/// Kinds an operator disabled in the registry are skipped — their armed
/// rows freeze in place and thaw when the kind is re-enabled. Kinds with
/// no registry row still claim, so an orphaned row fails loudly instead of
/// sitting forever.
pub(crate) fn due_ids(
    conn: &Connection,
    now: DateTime<Utc>,
    limit: u32,
) -> rusqlite::Result<Vec<TaskId>> {
    let mut stmt = conn.prepare_cached(
        "SELECT id FROM task_queue
         WHERE status = 'PENDING' AND scheduled_at <= ?1
           AND kind NOT IN (SELECT kind FROM task_registry WHERE enabled = 0)
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

/// The earliest claimable `PENDING` `scheduled_at` — the dispatcher's sleep
/// watermark. Mirrors the claim's enabled filter so a disabled kind's
/// frozen rows never produce a past watermark (and a hot dispatcher loop).
pub(crate) fn next_due_at(conn: &Connection) -> rusqlite::Result<Option<DateTime<Utc>>> {
    let mut stmt = conn.prepare_cached(
        "SELECT MIN(scheduled_at) AS next_due FROM task_queue
         WHERE status = 'PENDING'
           AND kind NOT IN (SELECT kind FROM task_registry WHERE enabled = 0)",
    )?;
    stmt.query_row([], |row| {
        crate::sqlite::row::column_opt_datetime(row, "next_due")
    })
}

/// Claim one due task: `PENDING → RUNNING`, counting the attempt. The status
/// guard makes the claim idempotent against double-dispatch bugs.
pub(crate) fn mark_running(
    conn: &Connection,
    id: &TaskId,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE task_queue SET
            status = 'RUNNING',
            attempts = attempts + 1,
            started_at = ?2,
            updated_at = ?2,
            version = version + 1
         WHERE id = ?1 AND status = 'PENDING'",
    )?;
    stmt.execute(params![id.as_bytes(), canonical_timestamp(now)])
}

/// Terminal completion: the run leaves the queue. Version-guarded so a
/// stale completion cannot delete a row someone else has since touched.
pub(crate) fn delete_completed(
    conn: &Connection,
    id: &TaskId,
    expected_version: u32,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached("DELETE FROM task_queue WHERE id = ?1 AND version = ?2")?;
    stmt.execute(params![id.as_bytes(), expected_version])
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
        "UPDATE task_queue SET
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

/// Startup recovery, half 1: orphaned `RUNNING` rows with attempts left go
/// back to `PENDING`, due immediately, with the caller's error recorded.
pub(crate) fn requeue_interrupted_running(
    conn: &Connection,
    error: &str,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE task_queue SET
            status = 'PENDING',
            scheduled_at = ?1,
            updated_at = ?1,
            last_error = ?2,
            version = version + 1
         WHERE status = 'RUNNING' AND attempts < max_attempts",
    )?;
    stmt.execute(params![canonical_timestamp(now), error])
}

/// Startup recovery, half 2a: the orphaned `RUNNING` rows already on their
/// final attempt (read before deletion so the caller can record them).
pub(crate) fn exhausted_running_rows(conn: &Connection) -> rusqlite::Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS} FROM task_queue
         WHERE status = 'RUNNING' AND attempts >= max_attempts
         ORDER BY scheduled_at, id"
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_map([], TaskRow::from_row)?.collect()
}

/// Startup recovery, half 2b: drop the exhausted rows read by
/// [`exhausted_running_rows`].
pub(crate) fn delete_exhausted_running(conn: &Connection) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "DELETE FROM task_queue WHERE status = 'RUNNING' AND attempts >= max_attempts",
    )?;
    stmt.execute([])
}

/// Disambiguates a zero-row guarded write: version mismatch vs. missing.
pub(crate) const TASK_VERSION_SQL: &str = "SELECT version FROM task_queue WHERE id = ?1";

/// Count of rows per kind, used by tests.
#[cfg(test)]
pub(crate) fn count_by_kind(
    conn: &Connection,
    kind: &valqeron_core::TaskKind,
) -> rusqlite::Result<u32> {
    let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM task_queue WHERE kind = ?1")?;
    stmt.query_row(params![kind.as_str()], |row| row.get(0))
}
