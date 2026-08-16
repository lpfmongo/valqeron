//! Cached statements for the `sync_cursor` table.
//!
//! Timestamps use the canonical persisted form ([`canonical_timestamp`]);
//! civil dates use `NaiveDate`'s canonical `YYYY-MM-DD`.

use rusqlite::{Connection, OptionalExtension, params};
use valqeron_core::{SyncCursor, SyncSource};

use crate::sqlite::row::{FromRow, canonical_timestamp};
use crate::sqlite::sync_cursor::mapping::{canonical_date, outcome_as_str};
use crate::sqlite::sync_cursor::model::CursorRow;

const CURSOR_COLUMNS: &str = "source, through_slot, through_target, cooldown_until, \
                              consecutive_failures, last_outcome, last_error, updated_at";

pub(crate) fn get(conn: &Connection, source: &SyncSource) -> rusqlite::Result<Option<CursorRow>> {
    let sql = format!("SELECT {CURSOR_COLUMNS} FROM sync_cursor WHERE source = ?1");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_row(params![source.as_str()], CursorRow::from_row)
        .optional()
}

pub(crate) fn upsert(conn: &Connection, cursor: &SyncCursor) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO sync_cursor (source, through_slot, through_target, cooldown_until, \
                                  consecutive_failures, last_outcome, last_error, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(source) DO UPDATE SET
            through_slot         = excluded.through_slot,
            through_target       = excluded.through_target,
            cooldown_until       = excluded.cooldown_until,
            consecutive_failures = excluded.consecutive_failures,
            last_outcome         = excluded.last_outcome,
            last_error           = excluded.last_error,
            updated_at           = excluded.updated_at",
    )?;
    stmt.execute(params![
        cursor.source().as_str(),
        canonical_timestamp(cursor.through_slot()),
        canonical_date(cursor.through_target()),
        cursor.cooldown_until().map(canonical_timestamp),
        cursor.consecutive_failures(),
        cursor.last_outcome().map(outcome_as_str),
        cursor.last_error(),
        canonical_timestamp(cursor.updated_at()),
    ])
}
