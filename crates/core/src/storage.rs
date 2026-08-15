use crate::issuer::repository::IssuerRepository;
use crate::security::repository::SecurityRepository;
use crate::task::repository::BackgroundTaskRepository;

mod error;

pub use error::{StorageError, StorageFault};

pub struct Repositories<E: StorageEngine> {
    pub issuers: E::Issuers,
    pub securities: E::Securities,
    pub tasks: E::Tasks,
}

pub trait StorageEngine: Sized + Send + Sync {
    type Issuers: IssuerRepository;
    type Securities: SecurityRepository;
    type Tasks: BackgroundTaskRepository;

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
