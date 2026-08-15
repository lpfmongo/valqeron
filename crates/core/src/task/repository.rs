use chrono::{DateTime, Utc};
use std::rc::Rc;
use std::sync::Arc;

use crate::common::{RepositoryResult, Versioned, WriteOutcome};
use crate::task::{BackgroundTask, TaskCompletion, TaskId, TaskKind};

/// Persistence port for the engine's background task queue.
///
/// The engine's single-instance lock guarantees exactly one dispatcher, so
/// claiming does not need cross-process leasing — but every state transition
/// is still version-guarded ([`WriteOutcome`]) to keep logic bugs loud.
#[cfg_attr(test, mockall::automock)]
pub trait BackgroundTaskRepository {
    /// Persist a freshly built (`Pending`, zero-attempt) task.
    fn insert(&self, task: &BackgroundTask) -> RepositoryResult<()>;

    fn find_by_id(&self, id: &TaskId) -> RepositoryResult<Option<Versioned<BackgroundTask>>>;

    /// Most recently scheduled tasks first, for observability.
    fn list_recent(&self, limit: u32) -> RepositoryResult<Vec<Versioned<BackgroundTask>>>;

    /// Whether any non-terminal (`Pending`/`Running`) row of `kind` exists.
    /// Periodic schedulers use this as an enqueue gate so one kind never
    /// piles up or runs overlapped.
    fn exists_active(&self, kind: &TaskKind) -> RepositoryResult<bool>;

    /// Atomically claim up to `limit` due `Pending` tasks (oldest due first):
    /// each becomes `Running` with `attempts + 1`, `started_at = now`, and a
    /// bumped version. Returns the claimed rows in their post-claim state.
    fn claim_due(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> RepositoryResult<Vec<Versioned<BackgroundTask>>>;

    /// Record how a claimed run ended (success, retry, or terminal failure).
    fn complete(
        &self,
        id: &TaskId,
        expected_version: u32,
        completion: TaskCompletion,
    ) -> RepositoryResult<WriteOutcome>;

    /// Crash recovery, called once at engine startup (safe under the
    /// single-instance lock): `Running` rows are orphans of a previous
    /// process. Rows with attempts left go back to `Pending` due at `now`;
    /// rows already on their final attempt become terminal `Failed`. Returns
    /// how many rows were touched.
    fn reset_stale_running(&self, now: DateTime<Utc>) -> RepositoryResult<u32>;

    /// Delete terminal (`Succeeded`/`Failed`) rows that finished before
    /// `older_than`. Returns how many rows were removed.
    fn prune_finished(&self, older_than: DateTime<Utc>) -> RepositoryResult<u32>;
}

macro_rules! delegate_background_task_repository {
    ($ty:ty) => {
        impl<R: BackgroundTaskRepository + ?Sized> BackgroundTaskRepository for $ty {
            fn insert(&self, task: &BackgroundTask) -> RepositoryResult<()> {
                (**self).insert(task)
            }
            fn find_by_id(
                &self,
                id: &TaskId,
            ) -> RepositoryResult<Option<Versioned<BackgroundTask>>> {
                (**self).find_by_id(id)
            }
            fn list_recent(&self, limit: u32) -> RepositoryResult<Vec<Versioned<BackgroundTask>>> {
                (**self).list_recent(limit)
            }
            fn exists_active(&self, kind: &TaskKind) -> RepositoryResult<bool> {
                (**self).exists_active(kind)
            }
            fn claim_due(
                &self,
                now: DateTime<Utc>,
                limit: u32,
            ) -> RepositoryResult<Vec<Versioned<BackgroundTask>>> {
                (**self).claim_due(now, limit)
            }
            fn complete(
                &self,
                id: &TaskId,
                expected_version: u32,
                completion: TaskCompletion,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).complete(id, expected_version, completion)
            }
            fn reset_stale_running(&self, now: DateTime<Utc>) -> RepositoryResult<u32> {
                (**self).reset_stale_running(now)
            }
            fn prune_finished(&self, older_than: DateTime<Utc>) -> RepositoryResult<u32> {
                (**self).prune_finished(older_than)
            }
        }
    };
}

delegate_background_task_repository!(Box<R>);
delegate_background_task_repository!(Rc<R>);
delegate_background_task_repository!(Arc<R>);
