use rusqlite::Row;
use valqeron_core::TaskRegistrationSnapshot;

use crate::sqlite::row::{FromRow, column_datetime, column_opt_datetime};
use crate::sqlite::task::mapping::column_task_kind;
use crate::sqlite::task_registration::mapping::{
    column_category, column_log_policy, column_opt_run_outcome, column_opt_sync_source,
    column_tier, column_tracking,
};

/// Non-negative counter column (the schema CHECK guarantees `>= 0`).
fn column_u64(row: &Row, name: &str) -> rusqlite::Result<u64> {
    let raw: i64 = row.get(name)?;
    Ok(u64::try_from(raw).unwrap_or(0))
}

/// One `task_registration` row, mapped to the snapshot so the repository
/// can reconstitute the entity without exposing column details.
#[derive(Debug)]
pub(crate) struct RegistrationRow(pub TaskRegistrationSnapshot);

impl RegistrationRow {
    pub(crate) fn into_inner(self) -> TaskRegistrationSnapshot {
        self.0
    }
}

impl FromRow for RegistrationRow {
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self(TaskRegistrationSnapshot {
            kind: column_task_kind(row, "kind")?,
            category: column_category(row, "category")?,
            tier: column_tier(row, "tier")?,
            tracking: column_tracking(row, "tracking")?,
            schedule: row.get("schedule")?,
            source: column_opt_sync_source(row, "source")?,
            log_policy: column_log_policy(row, "log_policy")?,
            config_enabled: row.get("config_enabled")?,
            paused: row.get("paused")?,
            registered: row.get("registered")?,
            last_run_at: column_opt_datetime(row, "last_run_at")?,
            last_outcome: column_opt_run_outcome(row, "last_outcome")?,
            last_error: row.get("last_error")?,
            total_runs: column_u64(row, "total_runs")?,
            total_failures: column_u64(row, "total_failures")?,
            first_registered_at: column_datetime(row, "first_registered_at")?,
            updated_at: column_datetime(row, "updated_at")?,
        }))
    }
}
