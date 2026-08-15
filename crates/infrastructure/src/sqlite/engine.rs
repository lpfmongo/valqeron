use std::path::Path;

use valqeron_core::{Repositories, StorageEngine, StorageError};

use crate::sqlite::database::{Database, DatabaseConfig, WalCheckpointStats};
use crate::sqlite::issuer::SqliteIssuerRepository;
use crate::sqlite::security::SqliteSecurityRepository;
use crate::sqlite::task::SqliteBackgroundTaskRepository;

pub struct SqliteStorageEngine {
    db: Database,
}

impl SqliteStorageEngine {
    pub fn open(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self, StorageError> {
        let db = Database::open_with_config(path, config)?;
        Ok(Self { db })
    }

    /// Periodic maintenance for long-lived processes: `PRAGMA optimize` plus
    /// a passive WAL checkpoint.
    ///
    /// Safe to call while the engine is in use; the passive checkpoint never
    /// blocks concurrent readers or writers in other processes.
    pub fn run_maintenance(&self) -> Result<WalCheckpointStats, StorageError> {
        Ok(self.db.run_maintenance()?)
    }

    /// Number of reader connections in the pool, as configured at open time.
    /// Admission control on top of this engine must size its read
    /// concurrency to exactly this value: more would queue on the pool's
    /// `Condvar`, fewer would idle readers.
    pub fn reader_pool_size(&self) -> usize {
        self.db.reader_pool_size()
    }
}

impl StorageEngine for SqliteStorageEngine {
    type Issuers = SqliteIssuerRepository;
    type Securities = SqliteSecurityRepository;
    type Tasks = SqliteBackgroundTaskRepository;

    fn repositories(&self) -> Repositories<Self> {
        Repositories {
            issuers: SqliteIssuerRepository::new(self.db.handle()),
            securities: SqliteSecurityRepository::new(self.db.handle()),
            tasks: SqliteBackgroundTaskRepository::new(self.db.handle()),
        }
    }

    fn dry_run<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&Repositories<Self>) -> T,
    {
        self.db.dry_run(|handle| {
            let repositories = Repositories {
                issuers: SqliteIssuerRepository::new(handle.clone()),
                securities: SqliteSecurityRepository::new(handle.clone()),
                tasks: SqliteBackgroundTaskRepository::new(handle.clone()),
            };
            f(&repositories)
        })
    }
}
