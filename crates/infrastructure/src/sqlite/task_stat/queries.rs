//! Cached statements for the `task_stat` table — prune-proof per-kind
//! aggregates, folded forward on every terminal run.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use valqeron_core::{ExecutionOutcome, TaskKind};

use crate::sqlite::row::{FromRow, canonical_timestamp};
use crate::sqlite::task_stat::model::StatRow;

const STAT_COLUMNS: &str = "kind, total_runs, total_failures, total_duration_ms, last_run_at, \
                            last_outcome, last_error, last_success_at, last_duration_ms, \
                            updated_at";

pub(crate) fn record_run(
    conn: &Connection,
    kind: &TaskKind,
    outcome: ExecutionOutcome,
    error: Option<&str>,
    duration_ms: Option<u64>,
    at: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let failed = i64::from(matches!(outcome, ExecutionOutcome::Failed));
    let succeeded_at =
        matches!(outcome, ExecutionOutcome::Succeeded).then(|| canonical_timestamp(at));
    let duration = duration_ms.map(|ms| i64::try_from(ms).unwrap_or(i64::MAX));
    let mut stmt = conn.prepare_cached(
        "INSERT INTO task_stat (kind, total_runs, total_failures, total_duration_ms, \
                                last_run_at, last_outcome, last_error, last_success_at, \
                                last_duration_ms, updated_at)
         VALUES (?1, 1, ?2, COALESCE(?3, 0), ?4, ?5, ?6, ?7, ?3, ?4)
         ON CONFLICT(kind) DO UPDATE SET
            total_runs        = total_runs + 1,
            total_failures    = total_failures + ?2,
            total_duration_ms = total_duration_ms + COALESCE(?3, 0),
            last_run_at       = excluded.last_run_at,
            last_outcome      = excluded.last_outcome,
            last_error        = excluded.last_error,
            last_success_at   = COALESCE(excluded.last_success_at, last_success_at),
            last_duration_ms  = excluded.last_duration_ms,
            updated_at        = excluded.updated_at",
    )?;
    stmt.execute(params![
        kind.as_str(),
        failed,
        duration,
        canonical_timestamp(at),
        outcome.as_str(),
        error,
        succeeded_at,
    ])
}

pub(crate) fn get(conn: &Connection, kind: &TaskKind) -> rusqlite::Result<Option<StatRow>> {
    let sql = format!("SELECT {STAT_COLUMNS} FROM task_stat WHERE kind = ?1");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_row(params![kind.as_str()], StatRow::from_row)
        .optional()
}

pub(crate) fn list(conn: &Connection) -> rusqlite::Result<Vec<StatRow>> {
    let sql = format!("SELECT {STAT_COLUMNS} FROM task_stat ORDER BY kind");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_map([], StatRow::from_row)?.collect()
}
