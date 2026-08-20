use std::str::FromStr;

use chrono::NaiveTime;
use rusqlite::Row;
use rusqlite::types::Type;
use valqeron_core::{
    LogPolicy, Recurrence, SyncSource, TaskCategory, TaskSettings, TaskTracking, TaskTrigger,
};

use crate::sqlite::row::{column_index, conversion_failure};

/// Storage format of the `at_local` column.
const AT_LOCAL_FORMAT: &str = "%H:%M";

pub(crate) fn column_category(row: &Row, name: &str) -> rusqlite::Result<TaskCategory> {
    let raw: String = row.get(name)?;
    TaskCategory::from_str(&raw)
        .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

pub(crate) fn column_trigger(row: &Row, name: &str) -> rusqlite::Result<TaskTrigger> {
    let raw: String = row.get(name)?;
    TaskTrigger::from_str(&raw)
        .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

pub(crate) fn column_tracking(row: &Row, name: &str) -> rusqlite::Result<TaskTracking> {
    let raw: String = row.get(name)?;
    TaskTracking::from_str(&raw)
        .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

pub(crate) fn column_log_policy(row: &Row, name: &str) -> rusqlite::Result<LogPolicy> {
    let raw: String = row.get(name)?;
    LogPolicy::from_str(&raw)
        .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

pub(crate) fn column_opt_sync_source(
    row: &Row,
    name: &str,
) -> rusqlite::Result<Option<SyncSource>> {
    let raw: Option<String> = row.get(name)?;
    raw.map(|s| {
        SyncSource::new(s).map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
    })
    .transpose()
}

/// The settings columns, parsed **leniently**: a value an operator broke by
/// hand must not brick the catalog read — it is logged and treated as unset
/// (code-owned), so the scheduler falls back to the code default.
pub(crate) fn settings_from_row(row: &Row) -> rusqlite::Result<TaskSettings> {
    Ok(TaskSettings {
        period_secs: lenient_u32(row, "period_secs")?,
        at_local: lenient_at_local(row, "at_local")?,
        recurrence: lenient_recurrence(row, "recurrence")?,
        cooldown_secs: lenient_u32(row, "cooldown_secs")?,
        max_backfill_days: lenient_u32(row, "max_backfill_days")?,
    })
}

fn lenient_u32(row: &Row, name: &str) -> rusqlite::Result<Option<u32>> {
    let raw: Option<i64> = row.get(name)?;
    Ok(raw.and_then(|value| match u32::try_from(value) {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::warn!(column = name, value, "task setting out of range; ignored");
            None
        }
    }))
}

fn lenient_at_local(row: &Row, name: &str) -> rusqlite::Result<Option<NaiveTime>> {
    let raw: Option<String> = row.get(name)?;
    Ok(raw.and_then(
        |value| match NaiveTime::parse_from_str(&value, AT_LOCAL_FORMAT) {
            Ok(at) => Some(at),
            Err(_) => {
                tracing::warn!(column = name, value, "task setting unparseable; ignored");
                None
            }
        },
    ))
}

fn lenient_recurrence(row: &Row, name: &str) -> rusqlite::Result<Option<Recurrence>> {
    let raw: Option<String> = row.get(name)?;
    Ok(raw.and_then(|value| match Recurrence::from_str(&value) {
        Ok(recurrence) => Some(recurrence),
        Err(_) => {
            tracing::warn!(column = name, value, "task setting unparseable; ignored");
            None
        }
    }))
}

pub(crate) fn at_local_as_str(at: Option<NaiveTime>) -> Option<String> {
    at.map(|at| at.format(AT_LOCAL_FORMAT).to_string())
}

pub(crate) fn recurrence_as_str(recurrence: Option<Recurrence>) -> Option<String> {
    recurrence.map(|r| r.to_string())
}

pub(crate) fn category_as_str(category: TaskCategory) -> String {
    category.into()
}

pub(crate) fn trigger_as_str(trigger: TaskTrigger) -> String {
    trigger.into()
}

pub(crate) fn tracking_as_str(tracking: TaskTracking) -> String {
    tracking.into()
}

pub(crate) fn log_policy_as_str(policy: LogPolicy) -> String {
    policy.into()
}
