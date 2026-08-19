use std::str::FromStr;

use rusqlite::Row;
use rusqlite::types::Type;
use valqeron_core::{ExecutionOutcome, TaskExecution};

use crate::sqlite::row::{
    FromRow, column_datetime, column_index, column_opt_datetime, conversion_failure,
};
use crate::sqlite::task::mapping::{column_task_id, column_task_kind};

pub(crate) fn column_execution_outcome(
    row: &Row,
    name: &str,
) -> rusqlite::Result<ExecutionOutcome> {
    let raw: String = row.get(name)?;
    ExecutionOutcome::from_str(&raw)
        .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

/// Nullable non-negative column (the schema CHECK guarantees `>= 0`).
fn column_opt_u64(row: &Row, name: &str) -> rusqlite::Result<Option<u64>> {
    let raw: Option<i64> = row.get(name)?;
    Ok(raw.map(|value| u64::try_from(value).unwrap_or(0)))
}

/// One `task_execution` row. The record is plain data, so no snapshot
/// indirection is needed.
#[derive(Debug)]
pub(crate) struct ExecutionRow(pub TaskExecution);

impl ExecutionRow {
    pub(crate) fn into_inner(self) -> TaskExecution {
        self.0
    }
}

impl FromRow for ExecutionRow {
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self(TaskExecution {
            id: column_task_id(row, "id")?,
            kind: column_task_kind(row, "kind")?,
            outcome: column_execution_outcome(row, "outcome")?,
            payload: row.get("payload")?,
            scheduled_at: column_datetime(row, "scheduled_at")?,
            started_at: column_opt_datetime(row, "started_at")?,
            finished_at: column_datetime(row, "finished_at")?,
            attempts: row.get("attempts")?,
            duration_ms: column_opt_u64(row, "duration_ms")?,
            error: row.get("error")?,
            created_at: column_datetime(row, "created_at")?,
        }))
    }
}
