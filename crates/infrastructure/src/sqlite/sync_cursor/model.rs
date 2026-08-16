use rusqlite::Row;
use valqeron_core::SyncCursorSnapshot;

use crate::sqlite::row::{FromRow, column_datetime, column_opt_datetime};
use crate::sqlite::sync_cursor::mapping::{
    column_naive_date, column_opt_outcome, column_sync_source,
};

/// One `sync_cursor` row, mapped to the snapshot so the repository can
/// reconstitute the entity without exposing column details.
#[derive(Debug)]
pub(crate) struct CursorRow(pub SyncCursorSnapshot);

impl CursorRow {
    pub(crate) fn into_inner(self) -> SyncCursorSnapshot {
        self.0
    }
}

impl FromRow for CursorRow {
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self(SyncCursorSnapshot {
            source: column_sync_source(row, "source")?,
            through_slot: column_datetime(row, "through_slot")?,
            through_target: column_naive_date(row, "through_target")?,
            cooldown_until: column_opt_datetime(row, "cooldown_until")?,
            consecutive_failures: row.get("consecutive_failures")?,
            last_outcome: column_opt_outcome(row, "last_outcome")?,
            last_error: row.get("last_error")?,
            updated_at: column_datetime(row, "updated_at")?,
        }))
    }
}
