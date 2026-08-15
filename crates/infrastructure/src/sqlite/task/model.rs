use rusqlite::Row;
use valqeron_core::{BackgroundTaskSnapshot, Versioned};

use crate::sqlite::row::{FromRow, column_datetime};
use crate::sqlite::task::mapping::{
    column_opt_datetime, column_task_id, column_task_kind, column_task_status,
};

/// One `background_task` row, mapped to the snapshot so the repository can
/// reconstitute the entity without exposing column details.
#[derive(Debug)]
pub(crate) struct TaskRow(pub Versioned<BackgroundTaskSnapshot>);

impl TaskRow {
    pub(crate) fn into_inner(self) -> Versioned<BackgroundTaskSnapshot> {
        self.0
    }
}

impl FromRow for TaskRow {
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        let snapshot = BackgroundTaskSnapshot {
            id: column_task_id(row, "id")?,
            kind: column_task_kind(row, "kind")?,
            status: column_task_status(row, "status")?,
            payload: row.get("payload")?,
            scheduled_at: column_datetime(row, "scheduled_at")?,
            started_at: column_opt_datetime(row, "started_at")?,
            finished_at: column_opt_datetime(row, "finished_at")?,
            attempts: row.get("attempts")?,
            max_attempts: row.get("max_attempts")?,
            retry_delay_secs: row.get("retry_delay_secs")?,
            last_error: row.get("last_error")?,
            created_at: column_datetime(row, "created_at")?,
            updated_at: column_datetime(row, "updated_at")?,
        };
        let version: u32 = row.get("version")?;

        Ok(Self(Versioned {
            data: snapshot,
            version,
        }))
    }
}
