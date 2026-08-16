use std::str::FromStr;

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

pub(crate) fn status_as_str(status: TaskStatus) -> String {
    status.into()
}
