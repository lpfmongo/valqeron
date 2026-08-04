use std::path::Path;

use valqeron_core::{Repositories, StorageEngine, StorageError};

use crate::sqlite::connection::{Database, DatabaseConfig, WalCheckpointStats};
use crate::sqlite::issuer::SqliteIssuerRepository;

pub struct SqliteStorageEngine {
    db: Database,
}

impl SqliteStorageEngine {
    pub fn open(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self, StorageError> {
        let db = Database::open_with_config(path, config)?;
        Ok(Self { db })
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        let db = Database::open_in_memory()?;
        Ok(Self { db })
    }

    /// Periodic maintenance for long-lived processes: `PRAGMA optimize` plus
    /// a passive WAL checkpoint (file-backed databases only, `None` for
    /// in-memory databases).
    ///
    /// Safe to call while the engine is in use; the passive checkpoint never
    /// blocks concurrent readers or writers in other processes.
    pub fn run_maintenance(&self) -> Result<Option<WalCheckpointStats>, StorageError> {
        Ok(self.db.run_maintenance()?)
    }
}

impl StorageEngine for SqliteStorageEngine {
    type Issuers = SqliteIssuerRepository;

    fn repositories(&self) -> Repositories<Self> {
        Repositories {
            issuers: SqliteIssuerRepository::new(self.db.handle()),
        }
    }

    fn dry_run<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&Repositories<Self>) -> T,
    {
        self.db.dry_run(|handle| {
            let repositories = Repositories {
                issuers: SqliteIssuerRepository::new(handle.clone()),
            };
            f(&repositories)
        })
    }
}
