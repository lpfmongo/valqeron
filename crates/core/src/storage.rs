use crate::issuer::repository::IssuerRepository;
use crate::security::repository::SecurityRepository;
use crate::tasks::repository::{
    BackgroundTaskRepository, SyncCursorRepository, TaskExecutionRepository,
    TaskRegistrationRepository, TaskStatRepository,
};

mod error;

pub use error::{StorageError, StorageFault};

pub struct Repositories<E: StorageEngine> {
    pub issuers: E::Issuers,
    pub securities: E::Securities,
    pub tasks: E::Tasks,
    pub executions: E::Executions,
    pub stats: E::Stats,
    pub cursors: E::Cursors,
    pub registry: E::Registry,
}

pub trait StorageEngine: Sized + Send + Sync {
    type Issuers: IssuerRepository;
    type Securities: SecurityRepository;
    type Tasks: BackgroundTaskRepository;
    type Executions: TaskExecutionRepository;
    type Stats: TaskStatRepository;
    type Cursors: SyncCursorRepository;
    type Registry: TaskRegistrationRepository;

    fn repositories(&self) -> Repositories<Self>;

    fn dry_run<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&Repositories<Self>) -> T;
}

pub struct PersistenceManager<E: StorageEngine> {
    engine: E,
}

impl<E: StorageEngine> PersistenceManager<E> {
    pub fn new(engine: E) -> Self {
        Self { engine }
    }

    pub fn repositories(&self) -> Repositories<E> {
        self.engine.repositories()
    }

    pub fn dry_run<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&Repositories<E>) -> T,
    {
        self.engine.dry_run(f)
    }
}
