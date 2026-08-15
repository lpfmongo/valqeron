//! Async facade over the synchronous storage engine.
//!
//! gRPC handlers are async; `StorageEngine`/`IssuerRepository` are blocking by design
//! (engine.md §2.1). Calling the blocking reader pool from a tokio worker thread could stall the
//! runtime — and, because pool checkout parks on a `Condvar`, deadlock it outright when every
//! worker is parked and none is left to release a reader.
//!
//! The facade closes that hole with tokio's built-in bridge: every storage closure runs on the
//! **blocking** pool via `spawn_blocking`, and admission is bounded per lane so backpressure is an
//! explicit typed error rather than an unbounded pile-up of blocked threads.
//!
//! Admission maps 1:1 onto the SQLite resources underneath:
//!
//! - the **read lane** holds as many permits as the reader pool has connections — an admitted
//!   read never waits on the pool's `Condvar`;
//! - the **write lane** holds exactly one permit, mirroring the single writer — an admitted write
//!   never waits on the writer mutex, and queued writes wait as suspended futures (cheap) instead
//!   of blocked threads (burned).
//!
//! Without the split, a burst of writes could hold every slot while serializing on the writer
//! mutex, starving reads whose reader connections sit idle.
//!
//! Closures receive [`Repositories`], never the engine itself: handlers cannot reach
//! [`StorageEngine::dry_run`] directly, so the self-deadlocking nested-dry-run mistake is
//! unrepresentable at this edge — [`AsyncStorage::write`] is the only place that routes through
//! the savepoint. The loom-verified single-writer / reader-pool model in
//! `valqeron-infrastructure` is used exactly as designed — untouched.
//!
//! Shutdown protocol: [`AsyncStorage::close`] rejects new calls, and
//! [`AsyncStorage::wait_idle`] waits for in-flight closures to finish, after
//! which [`AsyncStorage::into_engine`] can reclaim the engine for a
//! deterministic final WAL checkpoint (`Drop`).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use valqeron_core::{Repositories, StorageEngine, StorageError};
use valqeron_infrastructure::{DatabaseConfig, SqliteStorageEngine};

/// How long a caller may wait for an execution slot before the engine reports backpressure. Mirrors
/// the spirit of the storage layer's own 15s progress-handler timeout while failing faster at the
/// queueing stage.
const QUEUE_TIMEOUT: Duration = Duration::from_secs(5);

/// Poll interval for [`AsyncStorage::wait_idle`].
const IDLE_POLL: Duration = Duration::from_millis(25);

#[derive(Debug, thiserror::Error)]
pub enum StorageCallError {
    /// Every execution slot stayed occupied for the whole queue timeout.
    #[error("the engine is overloaded; retry later")]
    Overloaded,

    /// The engine is shutting down and no longer accepts storage work.
    #[error("the engine is shutting down")]
    ShuttingDown,

    /// The blocking task was cancelled or failed to run to completion.
    #[error("storage task failed: {0}")]
    TaskFailed(String),
}

/// Cloneable handle giving async code bounded access to the blocking storage engine.
#[derive(Clone)]
pub struct AsyncStorage {
    engine: Arc<SqliteStorageEngine>,
    read_permits: Arc<Semaphore>,
    write_permits: Arc<Semaphore>,
    max_reads: usize,
    max_writes: usize,
}

/// The SQLite backend has exactly one WAL writer — a fact of the storage
/// engine, not a tunable. The write lane mirrors it.
const WRITE_SLOTS: usize = 1;

impl AsyncStorage {
    /// The single construction path in the engine: open the database
    /// (pragmas + migrations run inside `SqliteStorageEngine::open`, before
    /// any reader exists) and wrap it in the lane-bounded facade in one
    /// step — a raw, ungoverned engine handle never exists outside this
    /// module; the only way it leaves again is [`AsyncStorage::into_engine`]
    /// for the final checkpoint.
    pub fn open(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self, StorageError> {
        Ok(Self::new(SqliteStorageEngine::open(path, config)?))
    }

    /// Wrap an opened engine. Lane sizing is derived, not configured: the
    /// read lane takes exactly [`SqliteStorageEngine::reader_pool_size`]
    /// permits (more would only queue on the pool's own `Condvar`, fewer
    /// would idle readers) and the write lane mirrors the single writer —
    /// a permits/pool mismatch is unrepresentable by construction.
    fn new(engine: SqliteStorageEngine) -> Self {
        let read_slots = engine.reader_pool_size();
        Self {
            engine: Arc::new(engine),
            read_permits: Arc::new(Semaphore::new(read_slots)),
            write_permits: Arc::new(Semaphore::new(WRITE_SLOTS)),
            max_reads: read_slots,
            max_writes: WRITE_SLOTS,
        }
    }

    /// Run a read-only closure against the repositories on tokio's blocking pool, bounded by the
    /// read lane.
    ///
    /// The closure must contain the *whole* read (it may issue several queries); routing a write
    /// through this lane is not a correctness bug — the repositories still serialize it on the
    /// writer — but it distorts admission accounting, so keep intent honest.
    ///
    /// Cancellation note: once the closure starts, it runs to completion even if the caller's
    /// future is dropped — `spawn_blocking` tasks are not cancellable. The permit is released when
    /// the closure finishes.
    pub async fn read<T, F>(&self, operation: &'static str, f: F) -> Result<T, StorageCallError>
    where
        F: FnOnce(&Repositories<SqliteStorageEngine>) -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = acquire_slot(&self.read_permits, operation).await?;
        let engine = Arc::clone(&self.engine);
        let span = tracing::debug_span!("storage_read", operation);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _entered = span.entered();
            f(&engine.repositories())
        })
        .await
        .map_err(|e| StorageCallError::TaskFailed(e.to_string()))
    }

    /// Run a mutating closure against the repositories on tokio's blocking pool, bounded by the
    /// write lane (one permit — the single writer).
    ///
    /// The closure must contain the *whole* domain operation (e.g. `register_issuer`'s
    /// check-then-insert) so multistep services are not split across executions. With
    /// `dry_run = true` the same closure runs inside [`StorageEngine::dry_run`] — a savepoint
    /// that always rolls back; a failure of the savepoint machinery itself surfaces through
    /// `E::from(StorageError)`.
    ///
    /// Cancellation note: once the closure starts, it runs to completion even if the caller's
    /// future is dropped — `spawn_blocking` tasks are not cancellable. The permit is released when
    /// the closure finishes.
    pub async fn write<T, E, F>(
        &self,
        operation: &'static str,
        dry_run: bool,
        f: F,
    ) -> Result<Result<T, E>, StorageCallError>
    where
        F: FnOnce(&Repositories<SqliteStorageEngine>) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<StorageError> + Send + 'static,
    {
        let permit = acquire_slot(&self.write_permits, operation).await?;
        let engine = Arc::clone(&self.engine);
        let span = tracing::debug_span!("storage_write", operation, dry_run);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _entered = span.entered();
            if dry_run {
                engine
                    .dry_run(f)
                    .unwrap_or_else(|savepoint_err| Err(E::from(savepoint_err)))
            } else {
                f(&engine.repositories())
            }
        })
        .await
        .map_err(|e| StorageCallError::TaskFailed(e.to_string()))
    }

    /// Run an engine-level closure (checkpoints, `PRAGMA optimize`) on the write lane.
    ///
    /// This is the only entry point that hands out the engine itself; it exists for background
    /// jobs, not for RPC handlers — domain work goes through [`AsyncStorage::read`] /
    /// [`AsyncStorage::write`].
    pub(crate) async fn maintenance<T, F>(
        &self,
        operation: &'static str,
        f: F,
    ) -> Result<T, StorageCallError>
    where
        F: FnOnce(&SqliteStorageEngine) -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = acquire_slot(&self.write_permits, operation).await?;
        let engine = Arc::clone(&self.engine);
        let span = tracing::debug_span!("storage_maintenance", operation);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _entered = span.entered();
            f(&engine)
        })
        .await
        .map_err(|e| StorageCallError::TaskFailed(e.to_string()))
    }

    /// Stop accepting new storage calls on both lanes. In-flight closures keep running.
    pub fn close(&self) {
        self.read_permits.close();
        self.write_permits.close();
    }

    /// Wait (bounded) until no storage closure is in flight. Returns `true`
    /// when idle, `false` when the deadline expired first.
    pub async fn wait_idle(&self, deadline: Duration) -> bool {
        let wait = async {
            while self.in_flight() > 0 {
                tokio::time::sleep(IDLE_POLL).await;
            }
        };
        tokio::time::timeout(deadline, wait).await.is_ok()
    }

    /// Number of closures currently holding an execution slot on either lane.
    pub fn in_flight(&self) -> usize {
        let reads = self
            .max_reads
            .saturating_sub(self.read_permits.available_permits());
        let writes = self
            .max_writes
            .saturating_sub(self.write_permits.available_permits());
        reads.saturating_add(writes)
    }

    /// Reclaim the engine for a deterministic final checkpoint. Fails when
    /// another clone (or a still-running closure) holds a reference.
    pub fn into_engine(self) -> Result<SqliteStorageEngine, Self> {
        let Self {
            engine,
            read_permits,
            write_permits,
            max_reads,
            max_writes,
        } = self;
        Arc::try_unwrap(engine).map_err(|engine| Self {
            engine,
            read_permits,
            write_permits,
            max_reads,
            max_writes,
        })
    }
}

/// Acquire a permit from one lane, converting the two failure shapes into typed errors: a closed
/// semaphore means shutdown, a full lane for the whole [`QUEUE_TIMEOUT`] means overload.
async fn acquire_slot(
    permits: &Arc<Semaphore>,
    operation: &'static str,
) -> Result<OwnedSemaphorePermit, StorageCallError> {
    let acquire = Arc::clone(permits).acquire_owned();
    match tokio::time::timeout(QUEUE_TIMEOUT, acquire).await {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_closed)) => Err(StorageCallError::ShuttingDown),
        Err(_elapsed) => {
            tracing::warn!(operation, "storage queue full; rejecting call");
            Err(StorageCallError::Overloaded)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use valqeron_core::{IssuerRepository, LoadMode, StorageFault};
    use valqeron_infrastructure::DatabaseConfig;

    /// The `TempDir` holds the database files; keep it alive alongside the
    /// storage handle for the duration of the test. Lane sizing follows the
    /// pool automatically, so tests shape concurrency through the pool size.
    fn storage(reader_pool_size: usize) -> (tempfile::TempDir, AsyncStorage) {
        let dir = tempfile::tempdir().expect("create temp dir for test database");
        let storage = AsyncStorage::open(
            dir.path().join("test.db"),
            DatabaseConfig {
                reader_pool_size,
                ..DatabaseConfig::default()
            },
        )
        .expect("open temp file-backed storage");
        (dir, storage)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn more_concurrent_calls_than_pool_slots_complete() {
        // The deadlock probe: with only 2 runtime workers and a read lane of
        // 2, 16 concurrent reads must complete because the blocking happens
        // on the blocking pool, never on the runtime workers.
        let (_dir, storage) = storage(2);
        let mut handles = Vec::new();
        for _ in 0..16 {
            let s = storage.clone();
            handles.push(tokio::spawn(async move {
                s.read("test.read", |repos| {
                    repos
                        .issuers
                        .list_paged(None, 10, LoadMode::Lazy)
                        .map(|rows| rows.len())
                })
                .await
            }));
        }
        for handle in handles {
            let joined = handle.await.expect("task join");
            let called = joined.expect("no backpressure expected in this test");
            assert!(matches!(called, Ok(0)));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn full_write_lane_surfaces_a_typed_backpressure_error() {
        // One write slot, held by a closure that blocks until we release it.
        let (_dir, storage) = storage(2);
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let blocker = {
            let s = storage.clone();
            tokio::spawn(async move {
                s.maintenance("test.block", move |_| {
                    let _ = release_rx.recv();
                })
                .await
            })
        };

        // Give the blocker time to occupy the slot.
        while storage.in_flight() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // tokio::time::pause() cannot be used with multi_thread; accept the
        // real 5s queue timeout and assert on the boundary that matters
        // (typed error, no hang).
        let attempt = {
            let s = storage.clone();
            tokio::spawn(async move {
                s.write("test.reject", false, |_| Ok::<(), StorageError>(()))
                    .await
            })
        };
        let outcome = attempt.await.expect("join");
        assert!(matches!(outcome, Err(StorageCallError::Overloaded)));

        release_tx.send(()).expect("release blocker");
        let blocked = blocker.await.expect("join blocker");
        assert!(blocked.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_write_lane_still_serves_reads() {
        // The head-of-line probe: the single write slot is held, yet reads
        // keep flowing because admission is per lane, not shared.
        let (_dir, storage) = storage(2);
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let blocker = {
            let s = storage.clone();
            tokio::spawn(async move {
                s.maintenance("test.hold-writer", move |_| {
                    let _ = release_rx.recv();
                })
                .await
            })
        };

        while storage.in_flight() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let read = storage
            .read("test.read-through", |repos| {
                repos
                    .issuers
                    .list_paged(None, 10, LoadMode::Lazy)
                    .map(|rows| rows.len())
            })
            .await;
        assert!(
            matches!(read, Ok(Ok(0))),
            "read must complete while the write lane is saturated"
        );

        release_tx.send(()).expect("release blocker");
        let blocked = blocker.await.expect("join blocker");
        assert!(blocked.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dry_run_routes_through_the_savepoint_and_persists_nothing() {
        let (_dir, storage) = storage(2);

        let cnpj = valqeron_core::Cnpj::new("12.345.678/0001-95").expect("valid cnpj");
        let issuer = valqeron_core::Issuer::builder()
            .cnpj(cnpj)
            .build()
            .expect("valid issuer");
        storage
            .write("test.dry-run", true, move |repos| {
                valqeron_core::register_issuer(&repos.issuers, &issuer)
                    .map_err(|e| StorageError::Fault(StorageFault::new(e.to_string())))
            })
            .await
            .expect("no backpressure")
            .expect("register succeeds inside the savepoint");

        let count = storage
            .read("test.count", |repos| {
                repos
                    .issuers
                    .list_paged(None, 10, LoadMode::Lazy)
                    .map(|rows| rows.len())
            })
            .await
            .expect("no backpressure")
            .expect("list succeeds");
        assert_eq!(count, 0, "dry run must not persist");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_storage_rejects_new_calls_and_drains() {
        let (_dir, storage) = storage(2);
        storage.close();

        let read = storage.read("test.closed-read", |_| ()).await;
        assert!(matches!(read, Err(StorageCallError::ShuttingDown)));
        let write = storage
            .write("test.closed-write", false, |_| Ok::<(), StorageError>(()))
            .await;
        assert!(matches!(write, Err(StorageCallError::ShuttingDown)));

        assert!(storage.wait_idle(Duration::from_secs(1)).await);
        let reclaimed = storage.into_engine();
        assert!(reclaimed.is_ok(), "no other clones may remain");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_caller_does_not_poison_the_facade() {
        let (_dir, storage) = storage(2);
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let s = storage.clone();
        let task = tokio::spawn(async move {
            s.maintenance("test.cancelled", move |_| {
                let _ = release_rx.recv();
            })
            .await
        });

        while storage.in_flight() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Cancel the awaiting future; the blocking closure keeps running.
        task.abort();
        release_tx.send(()).expect("release closure");

        // The slot must come back and the facade must keep serving.
        assert!(storage.wait_idle(Duration::from_secs(5)).await);
        let after = storage.maintenance("test.after-cancel", |_| 7).await;
        assert!(matches!(after, Ok(7)));
    }
}
