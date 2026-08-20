use rusqlite::{Connection, TransactionBehavior};

use crate::sqlite::error::SqliteError;

pub const MIGRATIONS: &[&str] = &[include_str!(
    "../../../../migrations/001_create_initial_schema.sql"
)];

pub fn run(connection: &mut Connection) -> Result<(), SqliteError> {
    fn migration_err(source: impl std::error::Error + Send + Sync + 'static) -> SqliteError {
        SqliteError::Migration {
            source: Box::new(source),
        }
    }

    let current_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(migration_err)?;

    if usize::try_from(current_version).unwrap_or(usize::MAX) > MIGRATIONS.len() {
        return Err(SqliteError::UnknownSchemaVersion {
            found: current_version,
            known: MIGRATIONS.len(),
        });
    }

    for (index, sql) in MIGRATIONS.iter().enumerate() {
        let migration_version = i64::try_from(index.saturating_add(1)).unwrap_or(i64::MAX);
        if migration_version <= current_version {
            continue;
        }

        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Exclusive)
            .map_err(migration_err)?;

        tx.execute_batch(sql).map_err(migration_err)?;

        tx.pragma_update(None, "user_version", migration_version)
            .map_err(migration_err)?;

        tx.commit().map_err(migration_err)?;
    }

    Ok(())
}
