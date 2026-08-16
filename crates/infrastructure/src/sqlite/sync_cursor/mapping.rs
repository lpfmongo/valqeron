use std::str::FromStr;

use chrono::NaiveDate;
use rusqlite::Row;
use rusqlite::types::Type;
use valqeron_core::{SyncOutcomeKind, SyncSource};

use crate::sqlite::row::{column_index, conversion_failure};

pub(crate) fn column_sync_source(row: &Row, name: &str) -> rusqlite::Result<SyncSource> {
    let raw: String = row.get(name)?;
    SyncSource::new(raw).map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

/// Civil dates persist as ISO-8601 `YYYY-MM-DD` (`NaiveDate`'s canonical
/// text form), so lexicographic `TEXT` comparison is date order.
pub(crate) fn column_naive_date(row: &Row, name: &str) -> rusqlite::Result<NaiveDate> {
    let raw: String = row.get(name)?;
    NaiveDate::from_str(&raw)
        .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
}

pub(crate) fn column_opt_outcome(
    row: &Row,
    name: &str,
) -> rusqlite::Result<Option<SyncOutcomeKind>> {
    let raw: Option<String> = row.get(name)?;
    raw.map(|s| {
        SyncOutcomeKind::from_str(&s)
            .map_err(|e| conversion_failure(column_index(row, name), Type::Text, e))
    })
    .transpose()
}

pub(crate) fn outcome_as_str(kind: SyncOutcomeKind) -> String {
    kind.into()
}

pub(crate) fn canonical_date(date: NaiveDate) -> String {
    date.to_string()
}
