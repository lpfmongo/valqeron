//! Cached statements for the `task_registry` table.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use valqeron_core::{TaskDeclaration, TaskKind};

use crate::sqlite::row::{FromRow, canonical_timestamp};
use crate::sqlite::task_registration::mapping::{
    category_as_str, log_policy_as_str, tracking_as_str, trigger_as_str,
};
use crate::sqlite::task_registration::model::RegistrationRow;

const REGISTRATION_COLUMNS: &str = "kind, category, trigger_kind, tracking, schedule, source, \
                                    log_policy, config_enabled, paused, registered, \
                                    first_registered_at, updated_at";

/// Upsert from a code declaration. On conflict only the declaration columns
/// are rewritten — operator intent (`paused`) survives.
pub(crate) fn declare(
    conn: &Connection,
    declaration: &TaskDeclaration,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO task_registry (kind, category, trigger_kind, tracking, schedule, source, \
                                        log_policy, config_enabled, first_registered_at, \
                                        updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
         ON CONFLICT(kind) DO UPDATE SET
            category       = excluded.category,
            trigger_kind   = excluded.trigger_kind,
            tracking       = excluded.tracking,
            schedule       = excluded.schedule,
            source         = excluded.source,
            log_policy     = excluded.log_policy,
            config_enabled = excluded.config_enabled,
            registered     = 1,
            updated_at     = excluded.updated_at",
    )?;
    stmt.execute(params![
        declaration.kind.as_str(),
        category_as_str(declaration.category),
        trigger_as_str(declaration.trigger),
        tracking_as_str(declaration.tracking),
        declaration.schedule,
        declaration.source.as_ref().map(|s| s.as_str()),
        log_policy_as_str(declaration.log_policy),
        declaration.config_enabled,
        canonical_timestamp(now),
    ])
}

/// The still-registered kinds NOT in `kinds` (about to be retired).
pub(crate) fn registered_kinds_not_in(
    conn: &Connection,
    kinds: &[TaskKind],
) -> rusqlite::Result<Vec<String>> {
    let placeholders = std::iter::repeat_n("?", kinds.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = if kinds.is_empty() {
        "SELECT kind FROM task_registry WHERE registered = 1".to_owned()
    } else {
        format!(
            "SELECT kind FROM task_registry WHERE registered = 1 AND kind NOT IN ({placeholders})"
        )
    };
    let mut stmt = conn.prepare(&sql)?;
    let params = rusqlite::params_from_iter(kinds.iter().map(|k| k.as_str()));
    stmt.query_map(params, |row| row.get::<_, String>(0))?
        .collect()
}

pub(crate) fn retire(conn: &Connection, kind: &str, now: DateTime<Utc>) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE task_registry SET registered = 0, updated_at = ?2 WHERE kind = ?1",
    )?;
    stmt.execute(params![kind, canonical_timestamp(now)])
}

pub(crate) fn get(conn: &Connection, kind: &TaskKind) -> rusqlite::Result<Option<RegistrationRow>> {
    let sql = format!("SELECT {REGISTRATION_COLUMNS} FROM task_registry WHERE kind = ?1");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_row(params![kind.as_str()], RegistrationRow::from_row)
        .optional()
}

pub(crate) fn list(conn: &Connection) -> rusqlite::Result<Vec<RegistrationRow>> {
    let sql = format!("SELECT {REGISTRATION_COLUMNS} FROM task_registry ORDER BY category, kind");
    let mut stmt = conn.prepare_cached(&sql)?;
    stmt.query_map([], RegistrationRow::from_row)?.collect()
}

pub(crate) fn is_paused(conn: &Connection, kind: &TaskKind) -> rusqlite::Result<Option<bool>> {
    let mut stmt = conn.prepare_cached("SELECT paused FROM task_registry WHERE kind = ?1")?;
    stmt.query_row(params![kind.as_str()], |row| row.get(0))
        .optional()
}

pub(crate) fn set_paused(
    conn: &Connection,
    kind: &TaskKind,
    paused: bool,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn
        .prepare_cached("UPDATE task_registry SET paused = ?2, updated_at = ?3 WHERE kind = ?1")?;
    stmt.execute(params![kind.as_str(), paused, canonical_timestamp(now)])
}
