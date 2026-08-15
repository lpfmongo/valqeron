//! `BackgroundTasksManager` — the engine's background work: registration,
//! scheduling, durable queueing, dispatch, retry, crash recovery, and
//! shutdown draining.
//!
//! Two execution planes share one handler registry (`kind → handler`):
//!
//! - **Periodic schedules** own a ticker each (±10% optional jitter, missed
//!   ticks skipped). A `Durable` tick *enqueues a `background_task` row* and
//!   wakes the dispatcher — every run leaves queryable history. An
//!   `Ephemeral` tick runs its handler inline with no persistence (pure
//!   liveness work like the heartbeat). Durable ticks are gated on
//!   `exists_active`, so one kind never piles up or runs overlapped —
//!   the same guarantee the old in-memory runner gave.
//! - **The dispatcher** claims due `PENDING` rows in batches (woken by
//!   enqueues via `Notify`, with a fallback poll for time-scheduled retries),
//!   executes them with bounded concurrency, and records the outcome:
//!   success, retry with capped exponential backoff, or terminal failure.
//!   Rows claimed by a previous process (`RUNNING` at boot) are recovered at
//!   startup: requeued while attempts remain, failed otherwise.
//!
//! Shutdown mirrors the old `JobSet`: a `watch` flip stops every ticker and
//! the dispatcher, and a bounded drain waits for bodies still in flight —
//! an execution cut off mid-run is exactly the crash-recovery case.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use valqeron_core::{
    BackgroundTask, BackgroundTaskRepository, StorageError, TaskCompletion, TaskId, TaskKind,
    Versioned, WriteOutcome,
};

use crate::storage::AsyncStorage;

// ================ TUNING ================
/// Fallback dispatcher wake-up: covers rows that become due by clock
/// (retries, future schedules) rather than by an in-process enqueue.
const DISPATCH_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Due rows claimed per write-lane call.
const CLAIM_BATCH: u32 = 8;

/// Concurrent handler executions per claimed batch. Handlers ultimately
/// serialize on the storage lanes anyway; this only bounds task-level
/// parallelism.
const EXECUTION_CONCURRENCY: usize = 2;

// ================ FAILURE ================
/// What a handler reports when a run fails; recorded as the row's
/// `last_error` and fed into the retry decision.
#[derive(Debug)]
pub(crate) struct TaskFailure(String);

impl TaskFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for TaskFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ================ REGISTRY ================
/// Execution context handed to a handler: the storage facade plus the row's
/// payload (ephemeral runs carry no payload).
pub(crate) struct TaskContext {
    pub storage: AsyncStorage,
    /// The claimed row's payload. No built-in handler consumes one yet; the
    /// field is the contract for future payload-carrying tasks.
    #[expect(dead_code, reason = "handler contract; first payload consumer pending")]
    pub payload: Option<String>,
}

type TaskFuture = Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send>>;
type HandlerFn = Arc<dyn Fn(TaskContext) -> TaskFuture + Send + Sync>;

/// Whether a periodic schedule's runs are persisted as `background_task`
/// rows (history, retries visible) or executed purely in memory.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Tracking {
    Durable,
    Ephemeral,
}

/// One periodic schedule, dispatching to the handler registered under `kind`.
pub(crate) struct PeriodicSpec {
    pub kind: &'static str,
    pub period: Duration,
    /// ±10% period jitter so periodic jobs do not synchronize.
    pub jitter: bool,
    pub tracking: Tracking,
}

/// How an explicitly enqueued task retries.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    pub max_attempts: u32,
    pub retry_delay_secs: u32,
}

impl RetryPolicy {
    /// One attempt, no retry — the policy of periodic runs, whose "retry"
    /// is simply the next tick.
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            retry_delay_secs: 0,
        }
    }
}

// ================ BUILDER ================
#[derive(Default)]
pub(crate) struct BackgroundTasksBuilder {
    handlers: HashMap<&'static str, HandlerFn>,
    periodics: Vec<PeriodicSpec>,
}

impl BackgroundTasksBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the handler executed for every task row (or ephemeral tick)
    /// of `kind`.
    pub fn handler<F, Fut>(mut self, kind: &'static str, f: F) -> Self
    where
        F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), TaskFailure>> + Send + 'static,
    {
        self.handlers
            .insert(kind, Arc::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    pub fn periodic(mut self, spec: PeriodicSpec) -> Self {
        self.periodics.push(spec);
        self
    }

    /// Run crash recovery, then spawn the tickers and the dispatcher.
    pub async fn start(self, storage: AsyncStorage) -> BackgroundTasksManager {
        let inner = Arc::new(Inner {
            storage,
            handlers: self.handlers,
            wake: Notify::new(),
        });

        recover_stale_running(&inner).await;

        let (shutdown, _) = tokio::sync::watch::channel(false);
        let mut tasks = JoinSet::new();

        for spec in self.periodics {
            if !inner.handlers.contains_key(spec.kind) {
                tracing::error!(
                    kind = spec.kind,
                    "periodic schedule references an unregistered handler; skipping"
                );
                continue;
            }
            spawn_periodic(&mut tasks, Arc::clone(&inner), shutdown.subscribe(), spec);
        }

        spawn_dispatcher(&mut tasks, Arc::clone(&inner), shutdown.subscribe());

        BackgroundTasksManager { shutdown, tasks }
    }
}

// ================ THE MANAGER ================
/// Owns the spawned tickers + dispatcher and their shared shutdown signal.
pub(crate) struct BackgroundTasksManager {
    shutdown: tokio::sync::watch::Sender<bool>,
    tasks: JoinSet<()>,
}

impl BackgroundTasksManager {
    pub fn builder() -> BackgroundTasksBuilder {
        BackgroundTasksBuilder::new()
    }

    /// Stop every ticker and the dispatcher, then wait (bounded) for bodies
    /// still in flight. Returns `true` when everything exited within the
    /// deadline; a run cut off by the deadline is recovered at next boot.
    pub async fn drain(mut self, deadline: Duration) -> bool {
        let _ = self.shutdown.send(true);
        let all_done = async { while self.tasks.join_next().await.is_some() {} };
        tokio::time::timeout(deadline, all_done).await.is_ok()
    }
}

// ================ SHARED STATE ================
struct Inner {
    storage: AsyncStorage,
    handlers: HashMap<&'static str, HandlerFn>,
    /// Wakes the dispatcher immediately on enqueue instead of waiting for
    /// the fallback poll.
    wake: Notify,
}

// ================ RECOVERY ================
/// `RUNNING` rows at startup are orphans of a previous process (the
/// single-instance lock guarantees no live owner). Requeue or fail them.
async fn recover_stale_running(inner: &Arc<Inner>) {
    let now = Utc::now();
    let recovered = inner
        .storage
        .write("task_recovery", false, move |repos| {
            repos
                .tasks
                .reset_stale_running(now)
                .map_err(StorageError::from)
        })
        .await;

    match recovered {
        Ok(Ok(0)) => {}
        Ok(Ok(count)) => tracing::info!(
            target: "valqeron::audit",
            operation = "task_recovery",
            recovered = count,
            "requeued/failed background tasks left running by a previous process"
        ),
        Ok(Err(e)) => tracing::warn!(error = %e, "background task recovery failed"),
        Err(e) => tracing::warn!(error = %e, "background task recovery not executed"),
    }
}

// ================ ENQUEUE ================
/// Insert one `PENDING` row and wake the dispatcher. The task id is returned
/// for observability/logging.
async fn enqueue_task(
    inner: &Arc<Inner>,
    kind: &'static str,
    payload: Option<String>,
    retry: RetryPolicy,
) -> Result<TaskId, String> {
    let task_kind = TaskKind::new(kind).map_err(|e| e.to_string())?;
    let mut builder = BackgroundTask::builder()
        .kind(task_kind)
        .max_attempts(retry.max_attempts)
        .retry_delay_secs(retry.retry_delay_secs);
    if let Some(payload) = payload {
        builder = builder.payload(payload);
    }
    let task = builder.build().map_err(|e| e.to_string())?;

    let id = *task.id();
    let inserted = inner
        .storage
        .write("task_enqueue", false, move |repos| {
            repos.tasks.insert(&task).map_err(StorageError::from)
        })
        .await;

    match inserted {
        Ok(Ok(())) => {
            inner.wake.notify_one();
            Ok(id)
        }
        Ok(Err(e)) => Err(e.to_string()),
        Err(e) => Err(e.to_string()),
    }
}

// ================ PERIODIC TICKERS ================
fn spawn_periodic(
    tasks: &mut JoinSet<()>,
    inner: Arc<Inner>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    spec: PeriodicSpec,
) {
    tasks.spawn(async move {
        let period = if spec.jitter {
            jittered(spec.period)
        } else {
            spec.period
        };
        let first_tick = tokio::time::Instant::now()
            .checked_add(period)
            .unwrap_or_else(tokio::time::Instant::now);
        let mut ticker = tokio::time::interval_at(first_tick, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    tracing::debug!(kind = spec.kind, "periodic schedule stopped");
                    break;
                }
                _ = ticker.tick() => match spec.tracking {
                    Tracking::Durable => durable_tick(&inner, spec.kind).await,
                    Tracking::Ephemeral => ephemeral_tick(&inner, spec.kind).await,
                },
            }
        }
    });
}

/// Enqueue one durable run of `kind`, unless a previous run is still active
/// (pending or running) — the no-overlap/no-pileup guarantee.
async fn durable_tick(inner: &Arc<Inner>, kind: &'static str) {
    let Ok(task_kind) = TaskKind::new(kind) else {
        tracing::error!(kind, "invalid periodic task kind");
        return;
    };
    let gate = inner
        .storage
        .read("task_gate", move |repos| {
            repos.tasks.exists_active(&task_kind)
        })
        .await;
    match gate {
        Ok(Ok(true)) => {
            tracing::debug!(kind, "previous run still active; skipping this tick");
            return;
        }
        Ok(Ok(false)) => {}
        Ok(Err(e)) => {
            tracing::warn!(kind, error = %e, "enqueue gate check failed; skipping tick");
            return;
        }
        Err(e) => {
            tracing::warn!(kind, error = %e, "enqueue gate check not executed; skipping tick");
            return;
        }
    }

    if let Err(e) = enqueue_task(inner, kind, None, RetryPolicy::none()).await {
        tracing::warn!(kind, error = %e, "failed to enqueue periodic task");
    }
}

/// Run the handler inline, leaving no row behind. The body is awaited on the
/// ticker task itself, so ephemeral runs of one kind never overlap.
async fn ephemeral_tick(inner: &Arc<Inner>, kind: &'static str) {
    let Some(handler) = inner.handlers.get(kind) else {
        return; // Unreachable: registration is validated at start().
    };
    let ctx = TaskContext {
        storage: inner.storage.clone(),
        payload: None,
    };
    if let Err(e) = handler(ctx).await {
        tracing::warn!(kind, error = %e, "ephemeral background run failed");
    }
}

// ================ DISPATCHER ================
fn spawn_dispatcher(
    tasks: &mut JoinSet<()>,
    inner: Arc<Inner>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    tracing::debug!("task dispatcher stopped");
                    break;
                }
                _ = inner.wake.notified() => {}
                _ = tokio::time::sleep(DISPATCH_POLL_INTERVAL) => {}
            }
            drain_due(&inner).await;
        }
    });
}

/// Claim and execute due tasks until the queue has none left. Zero-delay
/// retries become due immediately and are picked up by the next claim in
/// this same drain, bounded by each task's attempt budget.
async fn drain_due(inner: &Arc<Inner>) {
    loop {
        let now = Utc::now();
        let claimed = inner
            .storage
            .write("task_claim", false, move |repos| {
                repos
                    .tasks
                    .claim_due(now, CLAIM_BATCH)
                    .map_err(StorageError::from)
            })
            .await;

        let claimed = match claimed {
            Ok(Ok(claimed)) => claimed,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "claiming due background tasks failed");
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "claiming due background tasks not executed");
                return;
            }
        };
        if claimed.is_empty() {
            return;
        }

        // Execute the batch with bounded parallelism; all executions finish
        // (and record their outcome) before the next claim.
        let semaphore = Arc::new(tokio::sync::Semaphore::new(EXECUTION_CONCURRENCY));
        let mut executions = JoinSet::new();
        for task in claimed {
            let inner = Arc::clone(inner);
            let semaphore = Arc::clone(&semaphore);
            executions.spawn(async move {
                let Ok(_permit) = semaphore.acquire().await else {
                    return;
                };
                execute_one(&inner, task).await;
            });
        }
        while executions.join_next().await.is_some() {}
    }
}

/// Run one claimed task through its handler and record the outcome.
async fn execute_one(inner: &Arc<Inner>, task: Versioned<BackgroundTask>) {
    let Versioned {
        data: task,
        version,
    } = task;
    let kind = task.kind().as_str().to_owned();
    let id = *task.id();
    let attempt = task.attempts();

    let result = match inner.handlers.get(kind.as_str()) {
        Some(handler) => {
            let ctx = TaskContext {
                storage: inner.storage.clone(),
                payload: task.payload().map(str::to_owned),
            };
            let started = std::time::Instant::now();
            let outcome = handler(ctx).await;
            (outcome, started.elapsed())
        }
        None => (
            Err(TaskFailure::new(format!(
                "no handler registered for kind {kind:?}"
            ))),
            Duration::ZERO,
        ),
    };

    let now = Utc::now();
    let completion = match result {
        (Ok(()), duration) => {
            tracing::info!(
                target: "valqeron::audit",
                operation = "task_run",
                kind = %kind,
                task_id = %id.value(),
                attempt,
                duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                outcome = "succeeded",
                "background task run finished"
            );
            TaskCompletion::Succeeded { finished_at: now }
        }
        (Err(failure), duration) => {
            let error = failure.to_string();
            let (completion, label) = if task.can_retry() {
                (
                    TaskCompletion::Retry {
                        error: error.clone(),
                        failed_at: now,
                        retry_at: task.next_retry_at(now),
                    },
                    "retry",
                )
            } else {
                (
                    TaskCompletion::Failed {
                        error: error.clone(),
                        finished_at: now,
                    },
                    "failed",
                )
            };
            tracing::warn!(
                target: "valqeron::audit",
                operation = "task_run",
                kind = %kind,
                task_id = %id.value(),
                attempt,
                max_attempts = task.max_attempts(),
                duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                outcome = label,
                error = %error,
                "background task run finished"
            );
            completion
        }
    };

    let recorded = inner
        .storage
        .write("task_complete", false, move |repos| {
            repos
                .tasks
                .complete(&id, version, completion)
                .map_err(StorageError::from)
        })
        .await;
    match recorded {
        Ok(Ok(WriteOutcome::Applied)) => {}
        Ok(Ok(other)) => tracing::warn!(
            kind = %kind,
            task_id = %id.value(),
            ?other,
            "background task completion did not apply"
        ),
        Ok(Err(e)) => tracing::warn!(
            kind = %kind,
            task_id = %id.value(),
            error = %e,
            "recording background task completion failed"
        ),
        Err(e) => tracing::warn!(
            kind = %kind,
            task_id = %id.value(),
            error = %e,
            "recording background task completion not executed"
        ),
    }
}

// ================ JITTER ================
/// Scale a period into 90%..=110% using clock sub-second noise — enough to
/// desynchronize periodic jobs without pulling in an RNG dependency.
fn jittered(base: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let percent = u64::from(90u32.saturating_add(nanos.checked_rem(21).unwrap_or(0)));
    let millis = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
    let scaled = millis
        .saturating_mul(percent)
        .checked_div(100)
        .unwrap_or(millis);
    if scaled == 0 {
        base
    } else {
        Duration::from_millis(scaled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use valqeron_core::TaskStatus;
    use valqeron_infrastructure::{DatabaseConfig, SqliteStorageEngine};

    fn storage() -> (tempfile::TempDir, AsyncStorage) {
        let dir = tempfile::tempdir().expect("create temp dir for test database");
        let engine = SqliteStorageEngine::open(
            dir.path().join("tasks.db"),
            DatabaseConfig {
                reader_pool_size: 2,
                ..DatabaseConfig::default()
            },
        )
        .expect("open temp engine");
        (dir, AsyncStorage::new(engine))
    }

    async fn recent_tasks(storage: &AsyncStorage) -> Vec<Versioned<BackgroundTask>> {
        storage
            .read("test.recent", |repos| repos.tasks.list_recent(50))
            .await
            .expect("no backpressure")
            .expect("list_recent succeeds")
    }
    async fn wait_until(deadline_secs: u64, mut probe: impl AsyncFnMut() -> bool) {
        let wait = async {
            while !probe().await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let met = tokio::time::timeout(Duration::from_secs(deadline_secs), wait).await;
        assert!(met.is_ok(), "condition not reached within {deadline_secs}s");
    }

    #[test]
    fn jitter_stays_within_ten_percent() {
        let base = Duration::from_secs(3600);
        for _ in 0..50 {
            let j = jittered(base);
            assert!(
                j >= Duration::from_secs(3240) && j <= Duration::from_secs(3960),
                "jittered value out of ±10% envelope: {j:?}"
            );
        }
    }

    #[test]
    fn jitter_never_zeroes_a_tiny_period() {
        assert!(jittered(Duration::from_millis(1)) > Duration::ZERO);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_periodic_enqueues_executes_and_records_history() {
        let (_dir, storage) = storage();
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);

        let manager = BackgroundTasksManager::builder()
            .handler("test_durable", move |_ctx| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .periodic(PeriodicSpec {
                kind: "test_durable",
                period: Duration::from_millis(20),
                jitter: false,
                tracking: Tracking::Durable,
            })
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 2).await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        let rows = recent_tasks(&storage).await;
        assert!(!rows.is_empty(), "durable runs must leave history rows");
        assert!(
            rows.iter()
                .all(|t| t.data.kind().as_str() == "test_durable"),
            "only the durable kind is recorded"
        );
        assert!(
            rows.iter()
                .any(|t| t.data.status() == TaskStatus::Succeeded),
            "at least one recorded run succeeded: {rows:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ephemeral_periodic_runs_without_rows() {
        let (_dir, storage) = storage();
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);

        let manager = BackgroundTasksManager::builder()
            .handler("test_ephemeral", move |_ctx| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .periodic(PeriodicSpec {
                kind: "test_ephemeral",
                period: Duration::from_millis(10),
                jitter: false,
                tracking: Tracking::Ephemeral,
            })
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 3).await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        assert!(
            recent_tasks(&storage).await.is_empty(),
            "ephemeral runs must not persist rows"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failing_enqueued_task_retries_until_it_succeeds() {
        let (_dir, storage) = storage();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);

        let manager = BackgroundTasksManager::builder()
            .handler("test_retry", move |_ctx| {
                let counter = Arc::clone(&counter);
                async move {
                    // Fail the first two attempts, succeed on the third.
                    if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err(TaskFailure::new("transient"))
                    } else {
                        Ok(())
                    }
                }
            })
            .start(storage.clone())
            .await;

        // Reach inside: enqueue one task with a 3-attempt budget.
        let inner = Arc::new(Inner {
            storage: storage.clone(),
            handlers: HashMap::new(),
            wake: Notify::new(),
        });
        let id = enqueue_task(
            &inner,
            "test_retry",
            Some(r#"{"n":1}"#.to_string()),
            RetryPolicy {
                max_attempts: 3,
                retry_delay_secs: 0,
            },
        )
        .await
        .expect("enqueue succeeds");

        let probe_storage = storage.clone();
        wait_until(10, async || {
            probe_storage
                .read("test.find", move |repos| repos.tasks.find_by_id(&id))
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
                .is_some_and(|t| t.data.status() == TaskStatus::Succeeded)
        })
        .await;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "two failures + one success"
        );
        let row = storage
            .read("test.final", move |repos| repos.tasks.find_by_id(&id))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(row.data.attempts(), 3);
        assert_eq!(row.data.payload(), Some(r#"{"n":1}"#));
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exhausted_retries_end_terminally_failed() {
        let (_dir, storage) = storage();

        let manager = BackgroundTasksManager::builder()
            .handler("test_fatal", |_ctx| async {
                Err(TaskFailure::new("always broken"))
            })
            .start(storage.clone())
            .await;

        let inner = Arc::new(Inner {
            storage: storage.clone(),
            handlers: HashMap::new(),
            wake: Notify::new(),
        });
        let id = enqueue_task(
            &inner,
            "test_fatal",
            None,
            RetryPolicy {
                max_attempts: 2,
                retry_delay_secs: 0,
            },
        )
        .await
        .expect("enqueue succeeds");

        let probe_storage = storage.clone();
        wait_until(10, async || {
            probe_storage
                .read("test.find", move |repos| repos.tasks.find_by_id(&id))
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
                .is_some_and(|t| t.data.status() == TaskStatus::Failed)
        })
        .await;

        let row = storage
            .read("test.final", move |repos| repos.tasks.find_by_id(&id))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(row.data.attempts(), 2, "the full budget was spent");
        assert_eq!(row.data.last_error(), Some("always broken"));
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_kind_fails_terminally_with_a_named_error() {
        let (_dir, storage) = storage();
        let manager = BackgroundTasksManager::builder()
            .start(storage.clone())
            .await;

        let inner = Arc::new(Inner {
            storage: storage.clone(),
            handlers: HashMap::new(),
            wake: Notify::new(),
        });
        let id = enqueue_task(&inner, "test_unregistered", None, RetryPolicy::none())
            .await
            .expect("enqueue succeeds");

        let probe_storage = storage.clone();
        wait_until(10, async || {
            probe_storage
                .read("test.find", move |repos| repos.tasks.find_by_id(&id))
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
                .is_some_and(|t| t.data.status() == TaskStatus::Failed)
        })
        .await;

        let row = storage
            .read("test.final", move |repos| repos.tasks.find_by_id(&id))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            row.data
                .last_error()
                .is_some_and(|e| e.contains("no handler registered")),
            "error names the missing handler: {:?}",
            row.data.last_error()
        );
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_recovers_rows_left_running_by_a_previous_process() {
        let (_dir, storage) = storage();

        // Simulate a crash: a claimed (RUNNING) row nobody will complete.
        let task = BackgroundTask::builder()
            .kind(TaskKind::new("test_recovered").unwrap())
            .max_attempts(2)
            .build()
            .unwrap();
        let id = *task.id();
        storage
            .write("test.seed", false, move |repos| {
                repos.tasks.insert(&task).map_err(StorageError::from)?;
                let claimed = repos
                    .tasks
                    .claim_due(Utc::now(), 8)
                    .map_err(StorageError::from)?;
                assert_eq!(claimed.len(), 1);
                Ok::<_, StorageError>(())
            })
            .await
            .expect("no backpressure")
            .expect("seed succeeds");

        // A new manager must requeue and execute it.
        let manager = BackgroundTasksManager::builder()
            .handler("test_recovered", |_ctx| async { Ok(()) })
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(10, async || {
            probe_storage
                .read("test.find", move |repos| repos.tasks.find_by_id(&id))
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
                .is_some_and(|t| t.data.status() == TaskStatus::Succeeded)
        })
        .await;

        let row = storage
            .read("test.final", move |repos| repos.tasks.find_by_id(&id))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            row.data.attempts(),
            2,
            "interrupted attempt + recovered run"
        );
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_ticks_do_not_pile_up_while_a_run_is_active() {
        let (_dir, storage) = storage();
        let release = Arc::new(Notify::new());
        let releaser = Arc::clone(&release);
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);

        // Handler blocks until released; ticks fire every 10ms meanwhile.
        let manager = BackgroundTasksManager::builder()
            .handler("test_slow", move |_ctx| {
                let release = Arc::clone(&releaser);
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok(())
                }
            })
            .periodic(PeriodicSpec {
                kind: "test_slow",
                period: Duration::from_millis(10),
                jitter: false,
                tracking: Tracking::Durable,
            })
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 1).await;
        // Let several periods elapse while the first run is still active.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let rows = recent_tasks(&storage).await;
        assert_eq!(
            rows.len(),
            1,
            "the gate must prevent same-kind pileup: {rows:?}"
        );

        release.notify_waiters();
        assert!(manager.drain(Duration::from_secs(2)).await);
    }
}
