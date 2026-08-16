use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::Row;
use rusqlite::types::Type;

pub trait FromRow: Sized {
    fn from_row(row: &Row) -> rusqlite::Result<Self>;
}

pub(crate) fn conversion_failure(
    column: usize,
    kind: Type,
    err: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, kind, Box::new(err))
}

pub(crate) fn column_index(row: &Row, name: &str) -> usize {
    row.as_ref().column_index(name).unwrap_or(0)
}

/// Canonical persisted timestamp form: RFC 3339, millisecond precision,
/// Z-suffixed UTC.
pub(crate) fn canonical_timestamp(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn column_uuid(row: &Row, name: &str) -> rusqlite::Result<uuid::Uuid> {
    let bytes: Vec<u8> = row.get(name)?;
    let idx = column_index(row, name);
    uuid::Uuid::from_slice(&bytes).map_err(|e| conversion_failure(idx, Type::Blob, e))
}

pub(crate) fn column_opt_uuid(row: &Row, name: &str) -> rusqlite::Result<Option<uuid::Uuid>> {
    let bytes: Option<Vec<u8>> = row.get(name)?;
    let idx = column_index(row, name);
    bytes
        .map(|b| uuid::Uuid::from_slice(&b))
        .transpose()
        .map_err(|e| conversion_failure(idx, Type::Blob, e))
}

pub(crate) fn column_datetime(row: &Row, name: &str) -> rusqlite::Result<DateTime<Utc>> {
    let raw: String = row.get(name)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|dt| dt.with_timezone(&Utc))
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
