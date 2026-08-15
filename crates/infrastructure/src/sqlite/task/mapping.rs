use std::str::FromStr;

use chrono::{DateTime, Utc};
use rusqlite::Row;
use rusqlite::types::Type;
use valqeron_core::{TaskId, TaskKind, TaskStatus};

use crate::sqlite::row::{column_index, column_uuid, conversion_failure};

pub(crate) fn column_task_id(row: &Row, name: &str) -> rusqlite::Result<TaskId> {
    column_uuid(row, name).map(TaskId::from_uuid)
}

pub(crate) fn column_task_kind(row: &Row, name: &str) -> rusqlite::Result<TaskKind> {
    let raw: String = row.get(name)?;
    TaskKind::new(raw).map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

pub(crate) fn column_task_status(row: &Row, name: &str) -> rusqlite::Result<TaskStatus> {
    let raw: String = row.get(name)?;
    TaskStatus::from_str(&raw)
        .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

pub(crate) fn column_opt_datetime(
    row: &Row,
    name: &str,
) -> rusqlite::Result<Option<DateTime<Utc>>> {
    let raw: Option<String> = row.get(name)?;
    raw.map(|s| {
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
    })
    .transpose()
}

pub(crate) fn status_as_str(status: TaskStatus) -> String {
    status.into()
}
