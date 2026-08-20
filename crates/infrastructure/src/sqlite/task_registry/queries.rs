//! Cached statements for the `task_registry` table.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use valqeron_core::{TaskDeclaration, TaskKind, TaskSettings};

use crate::sqlite::row::{FromRow, canonical_timestamp};
use crate::sqlite::task_registry::mapping::{
    at_local_as_str, category_as_str, log_policy_as_str, recurrence_as_str, tracking_as_str,
    trigger_as_str,
};
use crate::sqlite::task_registry::model::RegistrationRow;

const REGISTRATION_COLUMNS: &str = "kind, category, trigger_kind, tracking, schedule, source, \
                                    log_policy, enabled, period_secs, at_local, recurrence, \
                                    cooldown_secs, max_backfill_days, registered, \
                                    first_registered_at, updated_at";

/// Upsert from a code declaration. On conflict only the identity columns
/// are rewritten; each settings column is filled from the declaration only
/// while NULL — operator intent (`enabled`, non-NULL settings) survives.
pub(crate) fn declare(
    conn: &Connection,
    declaration: &TaskDeclaration,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO task_registry (kind, category, trigger_kind, tracking, schedule, source, \
                                        log_policy, period_secs, at_local, recurrence, \
                                        cooldown_secs, max_backfill_days, first_registered_at, \
                                        updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13)
         ON CONFLICT(kind) DO UPDATE SET
            category          = excluded.category,
            trigger_kind      = excluded.trigger_kind,
            tracking          = excluded.tracking,
            schedule          = excluded.schedule,
            source            = excluded.source,
            log_policy        = excluded.log_policy,
            period_secs       = COALESCE(task_registry.period_secs, excluded.period_secs),
            at_local          = COALESCE(task_registry.at_local, excluded.at_local),
            recurrence        = COALESCE(task_registry.recurrence, excluded.recurrence),
            cooldown_secs     = COALESCE(task_registry.cooldown_secs, excluded.cooldown_secs),
            max_backfill_days = COALESCE(task_registry.max_backfill_days, \
                                         excluded.max_backfill_days),
            registered        = 1,
            updated_at        = excluded.updated_at",
    )?;
    stmt.execute(params![
        declaration.kind.as_str(),
        category_as_str(declaration.category),
        trigger_as_str(declaration.trigger),
        tracking_as_str(declaration.tracking),
        declaration.schedule,
        declaration.source.as_ref().map(|s| s.as_str()),
        log_policy_as_str(declaration.log_policy),
        declaration.settings.period_secs,
        at_local_as_str(declaration.settings.at_local),
        recurrence_as_str(declaration.settings.recurrence),
        declaration.settings.cooldown_secs,
        declaration.settings.max_backfill_days,
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

pub(crate) fn is_enabled(conn: &Connection, kind: &TaskKind) -> rusqlite::Result<Option<bool>> {
    let mut stmt = conn.prepare_cached("SELECT enabled FROM task_registry WHERE kind = ?1")?;
    stmt.query_row(params![kind.as_str()], |row| row.get(0))
        .optional()
}

pub(crate) fn set_enabled(
    conn: &Connection,
    kind: &TaskKind,
    enabled: bool,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn
        .prepare_cached("UPDATE task_registry SET enabled = ?2, updated_at = ?3 WHERE kind = ?1")?;
    stmt.execute(params![kind.as_str(), enabled, canonical_timestamp(now)])
}

pub(crate) fn update_settings(
    conn: &Connection,
    kind: &TaskKind,
    settings: &TaskSettings,
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare_cached(
        "UPDATE task_registry SET
            period_secs       = ?2,
            at_local          = ?3,
            recurrence        = ?4,
            cooldown_secs     = ?5,
            max_backfill_days = ?6,
            updated_at        = ?7
         WHERE kind = ?1",
    )?;
    stmt.execute(params![
        kind.as_str(),
        settings.period_secs,
        at_local_as_str(settings.at_local),
        recurrence_as_str(settings.recurrence),
        settings.cooldown_secs,
        settings.max_backfill_days,
        canonical_timestamp(now),
    ])
}
