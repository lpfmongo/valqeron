//! The background task manager — a generic kernel with no tier- or
//! task-specific knowledge.
//!
//! Everything the manager knows about a task is its [`TaskSpec`]: kind,
//! category, plane configuration, log policy — plus an opaque handler
//! (`TaskContext → TaskOutcome`). Tier semantics (intervals, wall-clock
//! recurrence, cursor-driven sync) live behind the [`plane::Plane`] trait;
//! task implementations live in `crate::jobs`. The manager orchestrates:
//!
//! - **the catalog** — at boot every registration is upserted into
//!   `task_registration` (preserving operator intent and the run summary),
//!   kinds gone from code are retired and their pending rows cancelled;
//! - **seeding loops** — one per registration; each pass runs the plane's
//!   reconcile inside a single write transaction, behind the operator
//!   `paused` gate (ephemeral inline ticks deliberately ignore `paused`:
//!   pausing liveness work like the systemd watchdog would get the engine
//!   killed);
//! - **the dispatcher** — claims due `PENDING` rows in batches, executes
//!   them with bounded concurrency inside `task_run{kind, category}` spans,
//!   and records the outcome plus the registration's prune-proof run
//!   summary in one transaction;
//! - **crash recovery and shutdown draining** — unchanged from the original
//!   design: `RUNNING` rows at boot are requeued or failed, and a `watch`
//!   flip stops every loop with a bounded drain.

pub(crate) mod plane;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::Instrument;
use valqeron_core::{
    BackgroundTask, BackgroundTaskRepository, LogPolicy, RunOutcome, StorageError, TaskCategory,
    TaskCompletion, TaskDeclaration, TaskKind, TaskRegistrationRepository, Versioned, WriteOutcome,
};

use crate::storage::AsyncStorage;
use crate::tasks::plane::{
    Plane, PlaneConfig, RunWindow, SeedPass, TaskContext, TaskFailure, TaskOutcome, TickMode,
};

// ================ TUNING ================
/// Fallback dispatcher wake-up: covers rows that become due by clock
/// (retries, future schedules) rather than by an in-process seed.
const DISPATCH_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Due rows claimed per write-lane call.
const CLAIM_BATCH: u32 = 8;

/// Concurrent handler executions per claimed batch. Handlers ultimately
/// serialize on the storage lanes anyway; this only bounds task-level
/// parallelism.
const EXECUTION_CONCURRENCY: usize = 2;

/// Error recorded on pending rows of kinds that disappeared from code.
const RETIRED_ERROR: &str = "retired: kind no longer registered";

// ================ REGISTRATION ================
/// Everything the manager may know about one task.
pub(crate) struct TaskSpec {
    pub kind: &'static str,
    pub category: TaskCategory,
    pub plane: PlaneConfig,
    pub log_policy: LogPolicy,
}

type TaskFuture = Pin<Box<dyn Future<Output = TaskOutcome> + Send>>;
type HandlerFn = Arc<dyn Fn(TaskContext) -> TaskFuture + Send + Sync>;

struct Registration {
    category: TaskCategory,
    log_policy: LogPolicy,
    handler: HandlerFn,
    plane: Arc<dyn Plane>,
}

// ================ BUILDER ================
#[derive(Default)]
pub(crate) struct BackgroundTasksBuilder {
    specs: Vec<(TaskSpec, HandlerFn)>,
    disabled: Vec<TaskDeclaration>,
}

impl BackgroundTasksBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one task: its spec and its handler.
    pub fn register<F, Fut>(mut self, spec: TaskSpec, f: F) -> Self
    where
        F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskOutcome> + Send + 'static,
    {
        self.specs
            .push((spec, Arc::new(move |ctx| Box::pin(f(ctx)))));
        self
    }

    /// Record a task that exists in code but is configured off for this
    /// boot — visible in the catalog as `disabled` instead of silently
    /// absent. The declaration must carry `config_enabled = false`.
    pub fn declare_disabled(mut self, declaration: TaskDeclaration) -> Self {
        self.disabled.push(declaration);
        self
    }

    /// Run crash recovery and the catalog reconcile, then spawn the seeder
    /// loops and the dispatcher. A kind registered twice is rejected with
    /// an error log — two planes would race to seed it.
    pub async fn start(self, storage: AsyncStorage) -> BackgroundTasksManager {
        let mut registrations: HashMap<&'static str, Arc<Registration>> = HashMap::new();
        let mut declarations: Vec<TaskDeclaration> = Vec::new();

        for (spec, handler) in self.specs {
            if registrations.contains_key(spec.kind) {
                tracing::error!(
                    kind = spec.kind,
                    "task kind registered twice; skipping the later registration"
                );
                continue;
            }
            let Ok(kind) = TaskKind::new(spec.kind) else {
                tracing::error!(kind = spec.kind, "invalid task kind; skipping registration");
                continue;
            };
            declarations.push(TaskDeclaration {
                kind,
                category: spec.category,
                tier: spec.plane.tier(),
                tracking: spec.plane.tracking(),
                schedule: spec.plane.descriptor(),
                source: spec.plane.source(),
                log_policy: spec.log_policy,
                config_enabled: true,
            });
            let registration = Registration {
                category: spec.category,
                log_policy: spec.log_policy,
                handler,
                plane: plane::build(spec.kind, spec.plane),
            };
            registrations.insert(spec.kind, Arc::new(registration));
        }

        for declaration in self.disabled {
            let conflicts = registrations.contains_key(declaration.kind.as_str())
                || declarations.iter().any(|d| d.kind == declaration.kind);
            if conflicts {
                tracing::error!(
                    kind = declaration.kind.as_str(),
                    "disabled declaration conflicts with an active registration; skipping"
                );
                continue;
            }
            declarations.push(declaration);
        }

        let inner = Arc::new(Inner {
            storage,
            registrations,
            wake: Notify::new(),
        });

        recover_stale_running(&inner).await;
        reconcile_registry(&inner, declarations).await;

        let (shutdown, _) = tokio::sync::watch::channel(false);
        let mut tasks = JoinSet::new();

        for (kind, registration) in &inner.registrations {
            spawn_seeder(
                &mut tasks,
                Arc::clone(&inner),
                shutdown.subscribe(),
                kind,
                Arc::clone(registration),
            );
        }
        spawn_dispatcher(&mut tasks, Arc::clone(&inner), shutdown.subscribe());

        BackgroundTasksManager {
            shutdown,
            tasks,
            inner,
        }
    }
}

// ================ THE MANAGER ================
/// Owns the spawned seeders + dispatcher and their shared shutdown signal.
pub(crate) struct BackgroundTasksManager {
    shutdown: tokio::sync::watch::Sender<bool>,
    tasks: JoinSet<()>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by the test-only `kick` hook")
    )]
    inner: Arc<Inner>,
}

impl BackgroundTasksManager {
    pub fn builder() -> BackgroundTasksBuilder {
        BackgroundTasksBuilder::new()
    }

    /// Stop every seeder and the dispatcher, then wait (bounded) for bodies
    /// still in flight. Returns `true` when everything exited within the
    /// deadline; a run cut off by the deadline is recovered at next boot.
    pub async fn drain(mut self, deadline: Duration) -> bool {
        let _ = self.shutdown.send(true);
        let all_done = async { while self.tasks.join_next().await.is_some() {} };
        tokio::time::timeout(deadline, all_done).await.is_ok()
    }

    /// Test hook: wake a kind's seeder immediately (e.g. after flipping the
    /// paused flag) instead of waiting for its fallback tick.
    #[cfg(test)]
    pub fn kick(&self, kind: &str) {
        if let Some(registration) = self.inner.registrations.get(kind) {
            registration.plane.wake();
        }
    }
}

// ================ SHARED STATE ================
struct Inner {
    storage: AsyncStorage,
    registrations: HashMap<&'static str, Arc<Registration>>,
    /// Wakes the dispatcher immediately on seed instead of waiting for the
    /// fallback poll.
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

// ================ CATALOG RECONCILE ================
/// Upsert every code declaration into the catalog, retire kinds that
/// disappeared from code, and cancel their leftover pending rows — one
/// transaction, run once at boot (after crash recovery, so a retired
/// kind's requeued rows are cancelled too).
async fn reconcile_registry(inner: &Arc<Inner>, declarations: Vec<TaskDeclaration>) {
    let now = Utc::now();
    let outcome = inner
        .storage
        .write("task_registry_reconcile", false, move |repos| {
            for declaration in &declarations {
                repos.registry.declare(declaration, now)?;
            }
            let kinds: Vec<TaskKind> = declarations.iter().map(|d| d.kind.clone()).collect();
            let retired = repos.registry.retire_missing(&kinds, now)?;
            let mut cancelled: u32 = 0;
            for kind in &retired {
                cancelled =
                    cancelled.saturating_add(repos.tasks.fail_pending(kind, RETIRED_ERROR, now)?);
            }
            Ok::<_, StorageError>((declarations.len(), retired, cancelled))
        })
        .await;

    match outcome {
        Ok(Ok((declared, retired, cancelled_rows))) => {
            let retired_kinds: Vec<&str> = retired.iter().map(TaskKind::as_str).collect();
            tracing::info!(
                target: "valqeron::audit",
                operation = "task_registry_reconcile",
                declared,
                retired = retired.len(),
                cancelled_rows,
                retired_kinds = ?retired_kinds,
                "task catalog reconciled"
            );
        }
        Ok(Err(e)) => tracing::warn!(error = %e, "task catalog reconcile failed"),
        Err(e) => tracing::warn!(error = %e, "task catalog reconcile not executed"),
    }
}

// ================ SEEDER LOOPS ================
/// One loop per registration: ticks at the plane's cadence, wakes early on
/// run completions, and delegates each pass to the plane.
fn spawn_seeder(
    tasks: &mut JoinSet<()>,
    inner: Arc<Inner>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    kind: &'static str,
    registration: Arc<Registration>,
) {
    tasks.spawn(async move {
        let (first_tick, period) = registration.plane.cadence();
        let mut ticker = tokio::time::interval_at(first_tick, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    tracing::debug!(kind, "task seeder stopped");
                    break;
                }
                _ = registration.plane.wake_notified() => {}
                _ = ticker.tick() => {}
            }
            match registration.plane.mode() {
                TickMode::Seed => seed_pass(&inner, kind, &registration).await,
                TickMode::Inline => inline_run(&inner, kind, &registration).await,
            }
        }
    });
}

/// One durable seeding pass: the operator pause gate and the plane's
/// reconcile share a single write transaction, so the gate cannot race the
/// insert.
async fn seed_pass(inner: &Arc<Inner>, kind: &'static str, registration: &Arc<Registration>) {
    let Ok(task_kind) = TaskKind::new(kind) else {
        return; // Unreachable: validated at registration.
    };
    let plane = Arc::clone(&registration.plane);
    let now = Utc::now();
    let pass = inner
        .storage
        .write("task_seed_pass", false, move |repos| {
            if repos.registry.is_paused(&task_kind)? {
                return Ok(SeedPass::Paused);
            }
            plane.reconcile(repos, now)
        })
        .await;

    match pass {
        Ok(Ok(SeedPass::Seeded)) => inner.wake.notify_one(),
        Ok(Ok(SeedPass::Paused)) => {
            tracing::debug!(kind, "task is paused; seeding skipped");
        }
        Ok(Ok(SeedPass::Idle)) => {}
        Ok(Err(e)) => tracing::warn!(kind, error = %e, "task seeding pass failed"),
        Err(e) => tracing::warn!(kind, error = %e, "task seeding pass not executed"),
    }
}

/// Run the handler inline, leaving no row behind. The body is awaited on
/// the seeder task itself, so ephemeral runs of one kind never overlap.
/// Deliberately not gated on `paused`: ephemeral work is liveness work.
async fn inline_run(inner: &Arc<Inner>, kind: &'static str, registration: &Arc<Registration>) {
    let ctx = TaskContext {
        storage: inner.storage.clone(),
        window: RunWindow::None,
    };
    let span = tracing::info_span!("task_run", kind, category = registration.category.as_str());
    match (registration.handler)(ctx).instrument(span).await {
        TaskOutcome::Done => {}
        TaskOutcome::NotReady { retry_after_secs } => {
            tracing::debug!(kind, retry_after_secs, "ephemeral run reported not-ready");
        }
        TaskOutcome::Failed(e) => {
            tracing::warn!(kind, error = %e, "ephemeral background run failed");
        }
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

/// Whether the manager emits the run-finished line for a successful run.
/// Failures are always logged.
fn should_log_run(policy: Option<LogPolicy>, succeeded: bool) -> bool {
    !(succeeded && policy == Some(LogPolicy::FailuresOnly))
}

/// Run one claimed task: parse its window through the plane, execute the
/// handler inside a `task_run` span, let the plane interpret the outcome,
/// then record completion + the catalog run summary in one transaction and
/// fire the plane hooks.
async fn execute_one(inner: &Arc<Inner>, task: Versioned<BackgroundTask>) {
    let Versioned {
        data: task,
        version,
    } = task;
    let kind = task.kind().as_str().to_owned();
    let id = *task.id();
    let attempt = task.attempts();
    let payload = task.payload().map(str::to_owned);

    let registration = inner.registrations.get(kind.as_str()).map(Arc::clone);

    let (result, duration) = match &registration {
        None => (
            Err(TaskFailure::new(format!(
                "no handler registered for kind {kind:?}"
            ))),
            Duration::ZERO,
        ),
        Some(registration) => match registration.plane.window_for(payload.as_deref()) {
            Err(message) => (Err(TaskFailure::new(message)), Duration::ZERO),
            Ok(window) => {
                let ctx = TaskContext {
                    storage: inner.storage.clone(),
                    window,
                };
                let span = tracing::info_span!(
                    "task_run",
                    kind = %kind,
                    category = registration.category.as_str()
                );
                let started = std::time::Instant::now();
                let outcome = (registration.handler)(ctx).instrument(span).await;
                let verdict = registration
                    .plane
                    .interpret(&inner.storage, window, outcome)
                    .await;
                (verdict, started.elapsed())
            }
        },
    };

    let now = Utc::now();
    let completion = match result {
        Ok(()) => {
            let policy = registration.as_ref().map(|r| r.log_policy);
            if should_log_run(policy, true) {
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
            }
            TaskCompletion::Succeeded { finished_at: now }
        }
        Err(failure) => {
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

    // The catalog's run summary records *terminal* completions of
    // registered kinds; the plane hooks need the terminal error before
    // `completion` moves into the recording closure.
    let terminal_error = match &completion {
        TaskCompletion::Failed { error, .. } => Some(error.clone()),
        _ => None,
    };
    let run_record: Option<(TaskKind, RunOutcome, Option<String>)> = match &completion {
        TaskCompletion::Succeeded { .. } if registration.is_some() => TaskKind::new(&kind)
            .ok()
            .map(|k| (k, RunOutcome::Succeeded, None)),
        TaskCompletion::Failed { error, .. } if registration.is_some() => TaskKind::new(&kind)
            .ok()
            .map(|k| (k, RunOutcome::Failed, Some(error.clone()))),
        _ => None,
    };

    let recorded = inner
        .storage
        .write("task_complete", false, move |repos| {
            let outcome = repos
                .tasks
                .complete(&id, version, completion)
                .map_err(StorageError::from)?;
            if let Some((record_kind, run_outcome, error)) = run_record {
                repos
                    .registry
                    .record_run(&record_kind, run_outcome, error, now)
                    .map_err(StorageError::from)?;
            }
            Ok::<_, StorageError>(outcome)
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

    // Plane hooks: terminal-failure side effects (sync cursor cooldown),
    // then re-arm the seeder without waiting for its fallback tick.
    if let Some(registration) = registration {
        if let Some(error) = terminal_error {
            registration.plane.on_terminal(&inner.storage, error).await;
        }
        registration.plane.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::plane::Tracking;
    use chrono::{DateTime, NaiveTime};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use valqeron_core::{
        DerivedTaskStatus, MarketCalendar, Recurrence, Schedule, SyncCursorRepository, TaskId,
        TaskStatus, TaskTier, TaskTracking, list_task_statuses,
    };
    use valqeron_infrastructure::DatabaseConfig;

    fn storage() -> (tempfile::TempDir, AsyncStorage) {
        let dir = tempfile::tempdir().expect("create temp dir for test database");
        let storage = AsyncStorage::open(
            dir.path().join("tasks.db"),
            DatabaseConfig {
                reader_pool_size: 2,
                ..DatabaseConfig::default()
            },
        )
        .expect("open temp storage");
        (dir, storage)
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

    async fn recent_tasks(storage: &AsyncStorage) -> Vec<Versioned<BackgroundTask>> {
        storage
            .read("test.recent", |repos| repos.tasks.list_recent(50))
            .await
            .expect("no backpressure")
            .expect("list_recent succeeds")
    }

    async fn registration_row(
        storage: &AsyncStorage,
        kind: &str,
    ) -> Option<valqeron_core::TaskRegistration> {
        let kind = TaskKind::new(kind).unwrap();
        storage
            .read("test.registration", move |repos| repos.registry.get(&kind))
            .await
            .expect("no backpressure")
            .expect("get succeeds")
    }

    /// Insert one row directly, exercising the dispatcher without a seeder.
    async fn seed_task(
        storage: &AsyncStorage,
        kind: &str,
        scheduled_at: Option<DateTime<Utc>>,
        max_attempts: u32,
    ) -> TaskId {
        let kind = TaskKind::new(kind).unwrap();
        let mut builder = BackgroundTask::builder()
            .kind(kind)
            .max_attempts(max_attempts);
        if let Some(at) = scheduled_at {
            builder = builder.scheduled_at(at);
        }
        let task = builder.build().unwrap();
        let id = *task.id();
        storage
            .write("test.seed", false, move |repos| {
                repos.tasks.insert(&task).map_err(StorageError::from)
            })
            .await
            .expect("no backpressure")
            .expect("insert succeeds");
        id
    }

    fn interval_spec(kind: &'static str, period_ms: u64, tracking: Tracking) -> TaskSpec {
        TaskSpec {
            kind,
            category: TaskCategory::EngineSystem,
            plane: PlaneConfig::Interval {
                period: Duration::from_millis(period_ms),
                jitter: false,
                tracking,
            },
            log_policy: LogPolicy::All,
        }
    }

    fn daily_utc_schedule() -> Schedule {
        Schedule::new(
            MarketCalendar::UTC,
            NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
            Recurrence::Daily,
        )
    }

    fn recurring_spec(kind: &'static str) -> TaskSpec {
        TaskSpec {
            kind,
            category: TaskCategory::EngineSystem,
            plane: PlaneConfig::Recurring {
                schedule: daily_utc_schedule(),
                retry: plane::RetryPolicy::none(),
            },
            log_policy: LogPolicy::All,
        }
    }

    #[test]
    fn log_policy_gates_only_successful_runs() {
        assert!(should_log_run(Some(LogPolicy::All), true));
        assert!(should_log_run(Some(LogPolicy::All), false));
        assert!(!should_log_run(Some(LogPolicy::FailuresOnly), true));
        assert!(should_log_run(Some(LogPolicy::FailuresOnly), false));
        assert!(should_log_run(None, true), "unregistered kinds always log");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_interval_seeds_executes_and_records_history() {
        let (_dir, storage) = storage();
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);

        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_durable", 20, Tracking::Durable),
                move |_ctx| {
                    let counter = Arc::clone(&counter);
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        TaskOutcome::Done
                    }
                },
            )
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
    async fn ephemeral_interval_runs_without_rows_or_catalog_stats() {
        let (_dir, storage) = storage();
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);

        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_ephemeral", 10, Tracking::Ephemeral),
                move |_ctx| {
                    let counter = Arc::clone(&counter);
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        TaskOutcome::Done
                    }
                },
            )
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 3).await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        assert!(
            recent_tasks(&storage).await.is_empty(),
            "ephemeral runs must not persist rows"
        );
        let registration = registration_row(&storage, "test_ephemeral")
            .await
            .expect("cataloged");
        assert_eq!(registration.total_runs(), 0, "no per-run stats");
        assert_eq!(registration.last_outcome(), None);
        assert_eq!(registration.tracking(), TaskTracking::Ephemeral);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failing_task_retries_until_it_succeeds() {
        let (_dir, storage) = storage();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);

        // Long-period registration: the handler is registered, the seeder
        // never interferes; the row is seeded directly.
        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_retry", 600_000, Tracking::Durable),
                move |_ctx| {
                    let counter = Arc::clone(&counter);
                    async move {
                        if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                            TaskOutcome::Failed("transient".into())
                        } else {
                            TaskOutcome::Done
                        }
                    }
                },
            )
            .start(storage.clone())
            .await;

        let id = seed_task(&storage, "test_retry", None, 3).await;

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
        assert!(manager.drain(Duration::from_secs(2)).await);

        // Only the terminal completion reaches the catalog summary.
        let registration = registration_row(&storage, "test_retry").await.unwrap();
        assert_eq!(registration.total_runs(), 1);
        assert_eq!(registration.total_failures(), 0);
        assert_eq!(registration.last_outcome(), Some(RunOutcome::Succeeded));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exhausted_retries_end_terminally_failed_and_counted() {
        let (_dir, storage) = storage();

        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_fatal", 600_000, Tracking::Durable),
                |_ctx| async { TaskOutcome::Failed("always broken".into()) },
            )
            .start(storage.clone())
            .await;

        let id = seed_task(&storage, "test_fatal", None, 2).await;

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

        let registration = registration_row(&storage, "test_fatal").await.unwrap();
        assert_eq!(registration.total_runs(), 1);
        assert_eq!(registration.total_failures(), 1);
        assert_eq!(registration.last_outcome(), Some(RunOutcome::Failed));
        assert_eq!(registration.last_error(), Some("always broken"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_kind_fails_terminally_with_a_named_error() {
        let (_dir, storage) = storage();
        let manager = BackgroundTasksManager::builder()
            .start(storage.clone())
            .await;

        let id = seed_task(&storage, "test_unregistered", None, 1).await;

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
        let id = seed_task(&storage, "test_recovered", None, 2).await;
        storage
            .write("test.claim", false, move |repos| {
                let claimed = repos
                    .tasks
                    .claim_due(Utc::now(), 8)
                    .map_err(StorageError::from)?;
                assert_eq!(claimed.len(), 1);
                Ok::<_, StorageError>(())
            })
            .await
            .expect("no backpressure")
            .expect("claim succeeds");

        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_recovered", 600_000, Tracking::Durable),
                |_ctx| async { TaskOutcome::Done },
            )
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

        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_slow", 10, Tracking::Durable),
                move |_ctx| {
                    let release = Arc::clone(&releaser);
                    let counter = Arc::clone(&counter);
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        release.notified().await;
                        TaskOutcome::Done
                    }
                },
            )
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recurring_seeds_the_next_occurrence_as_a_future_row() {
        let (_dir, storage) = storage();
        let manager = BackgroundTasksManager::builder()
            .register(recurring_spec("test_recurring"), |_ctx| async {
                TaskOutcome::Done
            })
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(5, async || !recent_tasks(&probe_storage).await.is_empty()).await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        let rows = recent_tasks(&storage).await;
        assert_eq!(rows.len(), 1, "exactly one seeded row: {rows:?}");
        assert_eq!(rows[0].data.status(), TaskStatus::Pending);
        assert!(
            rows[0].data.scheduled_at() > Utc::now(),
            "the row is a future wall-clock alarm, not an immediate run"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn past_due_recurring_row_from_a_previous_boot_runs_once() {
        let (_dir, storage) = storage();
        let past_due = Utc::now()
            .checked_sub_signed(chrono::Duration::hours(1))
            .expect("representable");
        seed_task(&storage, "test_recurring", Some(past_due), 1).await;

        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);
        let manager = BackgroundTasksManager::builder()
            .register(recurring_spec("test_recurring"), move |_ctx| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    TaskOutcome::Done
                }
            })
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 1).await;
        let probe_storage = storage.clone();
        wait_until(5, async || {
            recent_tasks(&probe_storage)
                .await
                .iter()
                .any(|t| t.data.status() == TaskStatus::Pending)
        })
        .await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        assert_eq!(runs.load(Ordering::SeqCst), 1, "the missed slot ran once");
        let rows = recent_tasks(&storage).await;
        let succeeded = rows
            .iter()
            .filter(|t| t.data.status() == TaskStatus::Succeeded)
            .count();
        let future_pending = rows
            .iter()
            .filter(|t| t.data.status() == TaskStatus::Pending)
            .filter(|t| t.data.scheduled_at() > Utc::now())
            .count();
        assert_eq!(succeeded, 1);
        assert_eq!(future_pending, 1, "the next occurrence is seeded ahead");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn duplicate_kind_registration_is_skipped() {
        let (_dir, storage) = storage();
        let first = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(AtomicUsize::new(0));
        let first_counter = Arc::clone(&first);
        let second_counter = Arc::clone(&second);

        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_dup", 10, Tracking::Ephemeral),
                move |_ctx| {
                    let counter = Arc::clone(&first_counter);
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        TaskOutcome::Done
                    }
                },
            )
            .register(
                interval_spec("test_dup", 10, Tracking::Ephemeral),
                move |_ctx| {
                    let counter = Arc::clone(&second_counter);
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        TaskOutcome::Done
                    }
                },
            )
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&first);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 2).await;
        assert!(manager.drain(Duration::from_secs(2)).await);
        assert_eq!(second.load(Ordering::SeqCst), 0, "the duplicate never runs");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn boot_reconcile_registers_the_catalog() {
        let (_dir, storage) = storage();
        let disabled_declaration = TaskDeclaration {
            kind: TaskKind::new("test_disabled_source").unwrap(),
            category: TaskCategory::FinanceDataSync,
            tier: TaskTier::Sync,
            tracking: TaskTracking::Durable,
            schedule: "sync:daily@07:00-03:00".into(),
            source: Some(valqeron_core::SyncSource::new("disabled_src").unwrap()),
            log_policy: LogPolicy::All,
            config_enabled: false,
        };
        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_a", 600_000, Tracking::Durable),
                |_| async { TaskOutcome::Done },
            )
            .register(
                interval_spec("test_b", 600_000, Tracking::Ephemeral),
                |_| async { TaskOutcome::Done },
            )
            .register(recurring_spec("test_c"), |_| async { TaskOutcome::Done })
            .declare_disabled(disabled_declaration)
            .start(storage.clone())
            .await;

        let listed = storage
            .read("test.list", |repos| repos.registry.list())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(listed.len(), 4);

        let a = registration_row(&storage, "test_a").await.unwrap();
        assert_eq!(a.tier(), TaskTier::Interval);
        assert_eq!(a.tracking(), TaskTracking::Durable);
        assert_eq!(a.schedule(), "interval:600s");
        assert!(a.config_enabled());

        let c = registration_row(&storage, "test_c").await.unwrap();
        assert_eq!(c.tier(), TaskTier::Recurring);
        assert_eq!(c.schedule(), "recurring:daily@03:00+00:00");

        let disabled = registration_row(&storage, "test_disabled_source")
            .await
            .unwrap();
        assert!(!disabled.config_enabled());
        assert!(disabled.registered());
        assert_eq!(
            disabled.source().map(|s| s.as_str().to_owned()),
            Some("disabled_src".into())
        );
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retired_kinds_are_marked_and_their_pending_rows_cancelled() {
        let (_dir, storage) = storage();

        // A previous boot: kind existed, was cataloged, and left a future
        // pending row behind.
        let old_declaration = TaskDeclaration {
            kind: TaskKind::new("test_old_kind").unwrap(),
            category: TaskCategory::Other,
            tier: TaskTier::Recurring,
            tracking: TaskTracking::Durable,
            schedule: "recurring:daily@03:00+00:00".into(),
            source: None,
            log_policy: LogPolicy::All,
            config_enabled: true,
        };
        storage
            .write("test.declare_old", false, move |repos| {
                repos
                    .registry
                    .declare(&old_declaration, Utc::now())
                    .map_err(StorageError::from)
            })
            .await
            .unwrap()
            .unwrap();
        let future = Utc::now()
            .checked_add_signed(chrono::Duration::hours(6))
            .expect("representable");
        let id = seed_task(&storage, "test_old_kind", Some(future), 1).await;

        // The new boot no longer registers it.
        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_current", 600_000, Tracking::Durable),
                |_| async { TaskOutcome::Done },
            )
            .start(storage.clone())
            .await;

        let registration = registration_row(&storage, "test_old_kind").await.unwrap();
        assert!(!registration.registered(), "retired at reconcile");

        let row = storage
            .read("test.row", move |repos| repos.tasks.find_by_id(&id))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(row.data.status(), TaskStatus::Failed);
        assert_eq!(row.data.last_error(), Some(RETIRED_ERROR));
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn paused_kind_seeds_nothing_until_resumed() {
        let (_dir, storage) = storage();

        // Catalog the kind and pause it before the manager boots; the boot
        // reconcile must preserve the pause.
        let declaration = TaskDeclaration {
            kind: TaskKind::new("test_pausable").unwrap(),
            category: TaskCategory::Other,
            tier: TaskTier::Interval,
            tracking: TaskTracking::Durable,
            schedule: "interval:0s".into(),
            source: None,
            log_policy: LogPolicy::All,
            config_enabled: true,
        };
        storage
            .write("test.pause", false, move |repos| {
                let now = Utc::now();
                repos.registry.declare(&declaration, now)?;
                let kind = TaskKind::new("test_pausable").map_err(|e| {
                    StorageError::Fault(valqeron_core::StorageFault::new(e.to_string()))
                })?;
                repos.registry.set_paused(&kind, true, now)?;
                Ok::<_, StorageError>(())
            })
            .await
            .unwrap()
            .unwrap();

        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);
        let manager = BackgroundTasksManager::builder()
            .register(
                interval_spec("test_pausable", 20, Tracking::Durable),
                move |_ctx| {
                    let counter = Arc::clone(&counter);
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        TaskOutcome::Done
                    }
                },
            )
            .start(storage.clone())
            .await;

        // Several ticks pass; the paused gate must hold.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0, "paused: nothing runs");
        assert!(recent_tasks(&storage).await.is_empty(), "nothing seeded");

        // Unpause: the next tick seeds and the dispatcher runs it.
        storage
            .write("test.unpause", false, move |repos| {
                let kind = TaskKind::new("test_pausable").map_err(|e| {
                    StorageError::Fault(valqeron_core::StorageFault::new(e.to_string()))
                })?;
                repos.registry.set_paused(&kind, false, Utc::now())?;
                Ok::<_, StorageError>(())
            })
            .await
            .unwrap()
            .unwrap();

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 1).await;
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_read_model_reports_the_live_manager() {
        let (_dir, storage) = storage();
        let manager = BackgroundTasksManager::builder()
            .register(recurring_spec("test_status_recurring"), |_| async {
                TaskOutcome::Done
            })
            .register(
                interval_spec("test_status_ephemeral", 600_000, Tracking::Ephemeral),
                |_| async { TaskOutcome::Done },
            )
            .start(storage.clone())
            .await;

        // Wait for the recurring seeder to arm its future row.
        let probe_storage = storage.clone();
        wait_until(5, async || !recent_tasks(&probe_storage).await.is_empty()).await;

        let entries = storage
            .read("test.status", |repos| {
                list_task_statuses(&repos.registry, &repos.tasks, &repos.cursors, 5, Utc::now())
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entries.len(), 2);

        let recurring = entries
            .iter()
            .find(|e| e.registration.kind().as_str() == "test_status_recurring")
            .expect("cataloged");
        assert_eq!(recurring.status, DerivedTaskStatus::Waiting);
        assert!(recurring.next_run_at.is_some_and(|at| at > Utc::now()));

        let ephemeral = entries
            .iter()
            .find(|e| e.registration.kind().as_str() == "test_status_ephemeral")
            .expect("cataloged");
        assert_eq!(ephemeral.status, DerivedTaskStatus::Idle);
        assert_eq!(ephemeral.next_run_at, None);

        assert!(manager.drain(Duration::from_secs(2)).await);

        // Suppress unused-import lint for SyncCursorRepository via a probe
        // read (two-source coverage lives in the sync plane tests).
        let cursor = storage
            .read("test.cursor", |repos| {
                repos
                    .cursors
                    .get(&valqeron_core::SyncSource::new("none").unwrap())
            })
            .await
            .unwrap()
            .unwrap();
        assert!(cursor.is_none());
    }
}
