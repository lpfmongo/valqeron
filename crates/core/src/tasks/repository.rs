//! The five persistence ports of the tasks domain, plus their mocks and
//! `Box`/`Rc`/`Arc` delegates. A trait change must update the delegate
//! macro alongside the `automock`.

use chrono::{DateTime, Utc};
use std::rc::Rc;
use std::sync::Arc;

use crate::common::{RepositoryResult, Versioned, WriteOutcome};
use crate::tasks::{
    BackgroundTask, ExecutionOutcome, SyncCursor, SyncSource, TaskCompletion, TaskDeclaration,
    TaskExecution, TaskId, TaskKind, TaskRegistration, TaskSettings, TaskStats,
};

// ================ QUEUE ================
/// Persistence port for the engine's live task queue.
///
/// Queue rows are active work only (`Pending`/`Running`); terminal
/// completions delete the row, and the caller records the execution history
/// and stats in the same transaction. The engine's single-instance lock
/// guarantees exactly one dispatcher, so claiming does not need
/// cross-process leasing — but every state transition is still
/// version-guarded ([`WriteOutcome`]) to keep logic bugs loud.
#[cfg_attr(test, mockall::automock)]
pub trait BackgroundTaskRepository {
    /// Persist a freshly built (`Pending`, zero-attempt) task.
    fn insert(&self, task: &BackgroundTask) -> RepositoryResult<()>;

    fn find_by_id(&self, id: &TaskId) -> RepositoryResult<Option<Versioned<BackgroundTask>>>;

    /// Every queued row, soonest first — "what will run next", for
    /// observability.
    fn list_queued(&self, limit: u32) -> RepositoryResult<Vec<Versioned<BackgroundTask>>>;

    /// Whether any row of `kind` exists (every queue row is active).
    /// Periodic schedulers use this as an enqueue gate so one kind never
    /// piles up or runs overlapped.
    fn exists_active(&self, kind: &TaskKind) -> RepositoryResult<bool>;

    /// The earliest row of `kind` — the task's next (or current) run, for
    /// status derivation.
    fn find_active(&self, kind: &TaskKind) -> RepositoryResult<Option<Versioned<BackgroundTask>>>;

    /// Remove and return every `Pending` row of `kind` (retired kinds at
    /// boot reconcile). The caller records the cancellations in the
    /// execution history.
    fn take_pending(&self, kind: &TaskKind) -> RepositoryResult<Vec<BackgroundTask>>;

    /// Atomically claim up to `limit` due `Pending` tasks (oldest due first):
    /// each becomes `Running` with `attempts + 1`, `started_at = now`, and a
    /// bumped version. Returns the claimed rows in their post-claim state.
    fn claim_due(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> RepositoryResult<Vec<Versioned<BackgroundTask>>>;

    /// The earliest `Pending` `scheduled_at` among claimable kinds
    /// (operator-disabled kinds excluded) — the dispatcher's sleep
    /// watermark. `None` when nothing claimable is queued.
    fn next_due_at(&self) -> RepositoryResult<Option<DateTime<Utc>>>;

    /// Record how a claimed run ended: `Terminal` deletes the row (the
    /// caller inserts the history record), `Retry` requeues it.
    fn complete(
        &self,
        id: &TaskId,
        expected_version: u32,
        completion: TaskCompletion,
    ) -> RepositoryResult<WriteOutcome>;

    /// Crash recovery, half 1 (safe under the single-instance lock):
    /// orphaned `Running` rows with attempts left go back to `Pending`, due
    /// at `now`, with `error` recorded. Returns how many were requeued.
    fn requeue_interrupted(&self, error: &str, now: DateTime<Utc>) -> RepositoryResult<u32>;

    /// Crash recovery, half 2: remove and return orphaned `Running` rows
    /// already on their final attempt. The caller records them as failed
    /// executions.
    fn take_exhausted_running(&self, now: DateTime<Utc>) -> RepositoryResult<Vec<BackgroundTask>>;
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
            fn list_queued(&self, limit: u32) -> RepositoryResult<Vec<Versioned<BackgroundTask>>> {
                (**self).list_queued(limit)
            }
            fn exists_active(&self, kind: &TaskKind) -> RepositoryResult<bool> {
                (**self).exists_active(kind)
            }
            fn find_active(
                &self,
                kind: &TaskKind,
            ) -> RepositoryResult<Option<Versioned<BackgroundTask>>> {
                (**self).find_active(kind)
            }
            fn take_pending(&self, kind: &TaskKind) -> RepositoryResult<Vec<BackgroundTask>> {
                (**self).take_pending(kind)
            }
            fn claim_due(
                &self,
                now: DateTime<Utc>,
                limit: u32,
            ) -> RepositoryResult<Vec<Versioned<BackgroundTask>>> {
                (**self).claim_due(now, limit)
            }
            fn next_due_at(&self) -> RepositoryResult<Option<DateTime<Utc>>> {
                (**self).next_due_at()
            }
            fn complete(
                &self,
                id: &TaskId,
                expected_version: u32,
                completion: TaskCompletion,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).complete(id, expected_version, completion)
            }
            fn requeue_interrupted(
                &self,
                error: &str,
                now: DateTime<Utc>,
            ) -> RepositoryResult<u32> {
                (**self).requeue_interrupted(error, now)
            }
            fn take_exhausted_running(
                &self,
                now: DateTime<Utc>,
            ) -> RepositoryResult<Vec<BackgroundTask>> {
                (**self).take_exhausted_running(now)
            }
        }
    };
}

delegate_background_task_repository!(Box<R>);
delegate_background_task_repository!(Rc<R>);
delegate_background_task_repository!(Arc<R>);

// ================ HISTORY & STATS ================
/// Persistence port for the terminal run history.
///
/// Records are write-once: they are inserted in the same transaction that
/// deletes the queue row, and only ever leave through `prune_finished`.
#[cfg_attr(test, mockall::automock)]
pub trait TaskExecutionRepository {
    fn insert(&self, execution: &TaskExecution) -> RepositoryResult<()>;

    fn find_by_id(&self, id: &TaskId) -> RepositoryResult<Option<TaskExecution>>;

    /// Most recently finished runs first, for observability.
    fn list_recent(&self, limit: u32) -> RepositoryResult<Vec<TaskExecution>>;

    /// Delete history rows that finished before `older_than`. Returns how
    /// many rows were removed. The per-kind aggregates survive on
    /// `task_stat`.
    fn prune_finished(&self, older_than: DateTime<Utc>) -> RepositoryResult<u32>;
}

macro_rules! delegate_task_execution_repository {
    ($ty:ty) => {
        impl<R: TaskExecutionRepository + ?Sized> TaskExecutionRepository for $ty {
            fn insert(&self, execution: &TaskExecution) -> RepositoryResult<()> {
                (**self).insert(execution)
            }
            fn find_by_id(&self, id: &TaskId) -> RepositoryResult<Option<TaskExecution>> {
                (**self).find_by_id(id)
            }
            fn list_recent(&self, limit: u32) -> RepositoryResult<Vec<TaskExecution>> {
                (**self).list_recent(limit)
            }
            fn prune_finished(&self, older_than: DateTime<Utc>) -> RepositoryResult<u32> {
                (**self).prune_finished(older_than)
            }
        }
    };
}

delegate_task_execution_repository!(Box<R>);
delegate_task_execution_repository!(Rc<R>);
delegate_task_execution_repository!(Arc<R>);

/// Persistence port for the per-kind run aggregates.
///
/// One row per kind, upserted on every terminal run; never pruned or
/// deleted (retirement keeps it, exactly like the sync cursor).
#[cfg_attr(test, mockall::automock)]
pub trait TaskStatRepository {
    /// Fold one terminal run into the kind's aggregates: bump `total_runs`
    /// (and `total_failures` on `Failed`), refresh the `last_*` columns,
    /// and add a measured duration to the running total.
    fn record_run(
        &self,
        kind: &TaskKind,
        outcome: ExecutionOutcome,
        error: Option<String>,
        duration_ms: Option<u64>,
        at: DateTime<Utc>,
    ) -> RepositoryResult<()>;

    fn get(&self, kind: &TaskKind) -> RepositoryResult<Option<TaskStats>>;

    /// Every stats row, ordered by kind.
    fn list(&self) -> RepositoryResult<Vec<TaskStats>>;
}

macro_rules! delegate_task_stat_repository {
    ($ty:ty) => {
        impl<R: TaskStatRepository + ?Sized> TaskStatRepository for $ty {
            fn record_run(
                &self,
                kind: &TaskKind,
                outcome: ExecutionOutcome,
                error: Option<String>,
                duration_ms: Option<u64>,
                at: DateTime<Utc>,
            ) -> RepositoryResult<()> {
                (**self).record_run(kind, outcome, error, duration_ms, at)
            }
            fn get(&self, kind: &TaskKind) -> RepositoryResult<Option<TaskStats>> {
                (**self).get(kind)
            }
            fn list(&self) -> RepositoryResult<Vec<TaskStats>> {
                (**self).list()
            }
        }
    };
}

delegate_task_stat_repository!(Box<R>);
delegate_task_stat_repository!(Rc<R>);
delegate_task_stat_repository!(Arc<R>);

// ================ CATALOG ================
/// Persistence port for the task catalog.
///
/// One row per kind, written only by the engine's scheduler (single
/// writer under the instance lock), so plain upserts are enough — no
/// optimistic versioning. The `enabled` flag and the settings columns are
/// additionally flipped by operators (SQL today, RPC later); those are
/// narrow updates that cannot conflict with the scheduler's writes. Run
/// aggregates live on `task_stat`, not here.
#[cfg_attr(test, mockall::automock)]
pub trait TaskRegistryRepository {
    /// Upsert a registration from its code declaration. On conflict only
    /// the identity columns (category, trigger, tracking, schedule, source,
    /// log policy, `registered = 1`) are rewritten; each settings column is
    /// filled from the declaration only while NULL. Operator intent
    /// (`enabled`, non-NULL settings) is preserved.
    fn declare(&self, declaration: &TaskDeclaration, now: DateTime<Utc>) -> RepositoryResult<()>;

    /// Mark every currently registered kind NOT in `kinds` as retired.
    /// Returns the kinds that were retired by this call.
    fn retire_missing(
        &self,
        kinds: &[TaskKind],
        now: DateTime<Utc>,
    ) -> RepositoryResult<Vec<TaskKind>>;

    fn get(&self, kind: &TaskKind) -> RepositoryResult<Option<TaskRegistration>>;

    /// Every registration (including retired ones), ordered by category
    /// then kind.
    fn list(&self) -> RepositoryResult<Vec<TaskRegistration>>;

    /// Whether the kind is enabled. Unknown kinds are enabled — the gate
    /// must fail open for rows the reconcile has not written yet.
    fn is_enabled(&self, kind: &TaskKind) -> RepositoryResult<bool>;

    /// Flip the operator enable flag. Returns whether the row existed.
    fn set_enabled(
        &self,
        kind: &TaskKind,
        enabled: bool,
        now: DateTime<Utc>,
    ) -> RepositoryResult<bool>;

    /// Overwrite the settings columns with `settings` as-is (`None`
    /// clears a column back to code-owned; the next boot refills the code
    /// default). Returns whether the row existed.
    fn update_settings(
        &self,
        kind: &TaskKind,
        settings: &TaskSettings,
        now: DateTime<Utc>,
    ) -> RepositoryResult<bool>;
}

macro_rules! delegate_task_registry_repository {
    ($ty:ty) => {
        impl<R: TaskRegistryRepository + ?Sized> TaskRegistryRepository for $ty {
            fn declare(
                &self,
                declaration: &TaskDeclaration,
                now: DateTime<Utc>,
            ) -> RepositoryResult<()> {
                (**self).declare(declaration, now)
            }
            fn retire_missing(
                &self,
                kinds: &[TaskKind],
                now: DateTime<Utc>,
            ) -> RepositoryResult<Vec<TaskKind>> {
                (**self).retire_missing(kinds, now)
            }
            fn get(&self, kind: &TaskKind) -> RepositoryResult<Option<TaskRegistration>> {
                (**self).get(kind)
            }
            fn list(&self) -> RepositoryResult<Vec<TaskRegistration>> {
                (**self).list()
            }
            fn is_enabled(&self, kind: &TaskKind) -> RepositoryResult<bool> {
                (**self).is_enabled(kind)
            }
            fn set_enabled(
                &self,
                kind: &TaskKind,
                enabled: bool,
                now: DateTime<Utc>,
            ) -> RepositoryResult<bool> {
                (**self).set_enabled(kind, enabled, now)
            }
            fn update_settings(
                &self,
                kind: &TaskKind,
                settings: &TaskSettings,
                now: DateTime<Utc>,
            ) -> RepositoryResult<bool> {
                (**self).update_settings(kind, settings, now)
            }
        }
    };
}

delegate_task_registry_repository!(Box<R>);
delegate_task_registry_repository!(Rc<R>);
delegate_task_registry_repository!(Arc<R>);

// ================ SYNC CURSORS ================
/// Persistence port for sync-source progress cursors.
///
/// One row per source, written only by the engine's sync reconciler and
/// completion hook (single writer under the engine's instance lock), so
/// plain upsert semantics are enough — no optimistic versioning.
#[cfg_attr(test, mockall::automock)]
pub trait SyncCursorRepository {
    fn get(&self, source: &SyncSource) -> RepositoryResult<Option<SyncCursor>>;

    /// Insert or fully replace the cursor row for `cursor.source()`.
    fn upsert(&self, cursor: &SyncCursor) -> RepositoryResult<()>;
}

macro_rules! delegate_sync_cursor_repository {
    ($ty:ty) => {
        impl<R: SyncCursorRepository + ?Sized> SyncCursorRepository for $ty {
            fn get(&self, source: &SyncSource) -> RepositoryResult<Option<SyncCursor>> {
                (**self).get(source)
            }
            fn upsert(&self, cursor: &SyncCursor) -> RepositoryResult<()> {
                (**self).upsert(cursor)
            }
        }
    };
}

delegate_sync_cursor_repository!(Box<R>);
delegate_sync_cursor_repository!(Rc<R>);
delegate_sync_cursor_repository!(Arc<R>);
