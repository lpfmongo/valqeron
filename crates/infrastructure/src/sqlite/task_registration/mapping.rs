use std::str::FromStr;

use rusqlite::Row;
use rusqlite::types::Type;
use valqeron_core::{LogPolicy, RunOutcome, SyncSource, TaskCategory, TaskTier, TaskTracking};

use crate::sqlite::row::{column_index, conversion_failure};

pub(crate) fn column_category(row: &Row, name: &str) -> rusqlite::Result<TaskCategory> {
    let raw: String = row.get(name)?;
    TaskCategory::from_str(&raw)
        .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

pub(crate) fn column_tier(row: &Row, name: &str) -> rusqlite::Result<TaskTier> {
    let raw: String = row.get(name)?;
    TaskTier::from_str(&raw).map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
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

pub(crate) fn column_opt_run_outcome(
    row: &Row,
    name: &str,
) -> rusqlite::Result<Option<RunOutcome>> {
    let raw: Option<String> = row.get(name)?;
    raw.map(|s| {
        RunOutcome::from_str(&s)
            .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
    })
    .transpose()
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

pub(crate) fn category_as_str(category: TaskCategory) -> String {
    category.into()
}

pub(crate) fn tier_as_str(tier: TaskTier) -> String {
    tier.into()
}

pub(crate) fn tracking_as_str(tracking: TaskTracking) -> String {
    tracking.into()
}

pub(crate) fn log_policy_as_str(policy: LogPolicy) -> String {
    policy.into()
}

pub(crate) fn run_outcome_as_str(outcome: RunOutcome) -> String {
    outcome.into()
}
