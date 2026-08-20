use rusqlite::Row;
use valqeron_core::TaskRegistrationSnapshot;

use crate::sqlite::row::{FromRow, column_datetime};
use crate::sqlite::task::mapping::column_task_kind;
use crate::sqlite::task_registry::mapping::{
    column_category, column_log_policy, column_opt_sync_source, column_tracking, column_trigger,
    settings_from_row,
};

/// One `task_registry` row, mapped to the snapshot so the repository can
/// reconstitute the entity without exposing column details.
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
            trigger: column_trigger(row, "trigger_kind")?,
            tracking: column_tracking(row, "tracking")?,
            schedule: row.get("schedule")?,
            source: column_opt_sync_source(row, "source")?,
            log_policy: column_log_policy(row, "log_policy")?,
            enabled: row.get("enabled")?,
            settings: settings_from_row(row)?,
            registered: row.get("registered")?,
            first_registered_at: column_datetime(row, "first_registered_at")?,
            updated_at: column_datetime(row, "updated_at")?,
        }))
    }
}
