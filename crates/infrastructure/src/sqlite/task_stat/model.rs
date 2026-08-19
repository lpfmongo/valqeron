use std::str::FromStr;

use rusqlite::Row;
use rusqlite::types::Type;
use valqeron_core::{ExecutionOutcome, TaskStats};

use crate::sqlite::row::{
    FromRow, column_datetime, column_index, column_opt_datetime, conversion_failure,
};
use crate::sqlite::task::mapping::column_task_kind;

/// Non-negative counter column (the schema CHECK guarantees `>= 0`).
fn column_u64(row: &Row, name: &str) -> rusqlite::Result<u64> {
    let raw: i64 = row.get(name)?;
    Ok(u64::try_from(raw).unwrap_or(0))
}

/// Nullable non-negative column.
fn column_opt_u64(row: &Row, name: &str) -> rusqlite::Result<Option<u64>> {
    let raw: Option<i64> = row.get(name)?;
    Ok(raw.map(|value| u64::try_from(value).unwrap_or(0)))
}

fn column_opt_outcome(row: &Row, name: &str) -> rusqlite::Result<Option<ExecutionOutcome>> {
    let raw: Option<String> = row.get(name)?;
    raw.map(|s| {
        ExecutionOutcome::from_str(&s)
            .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
    })
    .transpose()
}

/// One `task_stat` row. The aggregates are plain data.
#[derive(Debug)]
pub(crate) struct StatRow(pub TaskStats);

impl StatRow {
    pub(crate) fn into_inner(self) -> TaskStats {
        self.0
    }
}

impl FromRow for StatRow {
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self(TaskStats {
            kind: column_task_kind(row, "kind")?,
            total_runs: column_u64(row, "total_runs")?,
            total_failures: column_u64(row, "total_failures")?,
            total_duration_ms: column_u64(row, "total_duration_ms")?,
            last_run_at: column_opt_datetime(row, "last_run_at")?,
            last_outcome: column_opt_outcome(row, "last_outcome")?,
            last_error: row.get("last_error")?,
            last_success_at: column_opt_datetime(row, "last_success_at")?,
            last_duration_ms: column_opt_u64(row, "last_duration_ms")?,
            updated_at: column_datetime(row, "updated_at")?,
        }))
    }
}
