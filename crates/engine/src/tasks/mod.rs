//! The background-tasks feature, behind one facade.
//!
//! `crate::tasks` is the feature's only door: jobs and the engine
//! composition see [`BackgroundTasks`] (the runtime handle),
//! [`BackgroundTasksBuilder`] (registration), [`TaskDefinition`] with one
//! typed builder per trigger kind, and the handler contract
//! ([`TaskHandler`], [`TaskContext`], [`TaskOutcome`], [`RunWindow`]).
//! Trigger semantics (monotonic intervals, wall-clock recurrence,
//! cursor-driven sync) stay private behind the `trigger` module.
//!
//! Inside, three runtime roles — types, not layers of files:
//!
//! - [`BackgroundTasks`] / [`BackgroundTasksBuilder`] — the **manager**:
//!   validates registrations, reconciles the catalog at boot (declare,
//!   retire, cancel), runs crash recovery, and owns the workers;
//! - [`TaskWorkerManager`] — the **workers**: one seeder loop per kind
//!   (individually stoppable) plus the single dispatcher, with a bounded
//!   global drain;
//! - [`TaskContextRunner`] — the **runner**: owns the storage gateway and
//!   the execution of one pass or one run — seeding transactions behind the
//!   `paused` gate, claiming, handler execution inside `task_run` spans,
//!   and the terminal move (queue delete + history insert + stats fold in
//!   one transaction).

mod task;
mod trigger;

pub(crate) use task::TaskDefinition;
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "the facade's nameable surface; call sites reach builders through TaskDefinition constructors and implement TaskHandler on structs"
    )
)]
pub(crate) use task::{IntervalTaskBuilder, RecurringTaskBuilder, SyncTaskBuilder, TaskHandler};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "part of the TaskHandler contract for struct implementors"
    )
)]
pub(crate) use trigger::BoxFuture;
pub(crate) use trigger::{RetryPolicy, RunWindow, TaskContext, TaskOutcome};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::Instrument;
use valqeron_core::{
    BackgroundTask, BackgroundTaskRepository, ExecutionOutcome, LogPolicy, StorageError,
    StorageFault, TaskCompletion, TaskDeclaration, TaskExecution, TaskExecutionRepository,
    TaskKind, TaskRegistrationRepository, TaskStatRepository, TaskStatusEntry, Versioned,
    WriteOutcome, list_task_statuses,
};

use crate::storage::AsyncStorage;
use crate::tasks::task::Registration;
use crate::tasks::trigger::sync::ESCALATE_AFTER_FAILURES;
use crate::tasks::trigger::{Interpretation, SeedPass, TaskFailure, TickMode};

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

/// Error recorded on rows found `RUNNING` at startup: the previous process
/// stopped (crash or overrun drain) before the run could complete.
pub(crate) const INTERRUPTED_ERROR: &str =
    "interrupted: the engine stopped while the task was running";

// ================ BUILDER ================
#[derive(Default)]
pub(crate) struct BackgroundTasksBuilder {
    definitions: Vec<TaskDefinition>,
    disabled: Vec<TaskDeclaration>,
}

impl BackgroundTasksBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one task.
    pub fn task(mut self, definition: TaskDefinition) -> Self {
        self.definitions.push(definition);
        self
    }

    /// Record a task that exists in code but is configured off for this
    /// boot — visible in the catalog as `disabled` instead of silently
    /// absent. Produce the declaration with a builder's
    /// [`disabled`](SyncTaskBuilder::disabled) terminal; `None` (an invalid
    /// kind) is skipped, already logged by the terminal.
    pub fn declare(mut self, declaration: Option<TaskDeclaration>) -> Self {
        if let Some(declaration) = declaration {
            self.disabled.push(declaration);
        }
        self
    }

    /// Run crash recovery and the catalog reconcile, then start the
    /// workers. A kind registered twice is rejected with an error log — two
    /// triggers would race to seed it.
    pub async fn start(self, storage: AsyncStorage) -> BackgroundTasks {
        let mut registrations: HashMap<&'static str, Arc<Registration>> = HashMap::new();
        let mut declarations: Vec<TaskDeclaration> = Vec::new();

        for definition in self.definitions {
            let kind = definition.kind;
            if registrations.contains_key(kind) {
                tracing::error!(
                    kind,
                    "task kind registered twice; skipping the later registration"
                );
                continue;
            }
            let Some((declaration, registration)) = definition.into_parts() else {
                tracing::error!(kind, "invalid task kind; skipping registration");
                continue;
            };
            declarations.push(declaration);
            registrations.insert(kind, Arc::new(registration));
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

        let runner = Arc::new(TaskContextRunner {
            storage,
            registrations,
            wake: Notify::new(),
        });

        runner.recover_stale_running().await;
        runner.reconcile_registry(declarations).await;

        let workers = TaskWorkerManager::launch(Arc::clone(&runner));
        BackgroundTasks { runner, workers }
    }
}

// ================ THE FEATURE HANDLE ================
/// The running background-tasks feature: registration via
/// [`BackgroundTasks::builder`], a bounded [`drain`](Self::drain) for
/// shutdown, per-task worker control, and the status read model.
pub(crate) struct BackgroundTasks {
    runner: Arc<TaskContextRunner>,
    workers: TaskWorkerManager,
}

impl BackgroundTasks {
    pub fn builder() -> BackgroundTasksBuilder {
        BackgroundTasksBuilder::new()
    }

    /// Stop every seeder and the dispatcher, then wait (bounded) for bodies
    /// still in flight. Returns `true` when everything exited within the
    /// deadline; a run cut off by the deadline is recovered at next boot.
    pub async fn drain(self, deadline: Duration) -> bool {
        self.workers.drain(deadline).await
    }

    /// Stop one kind's seeder: in-memory runtime control, unlike the
    /// persisted `paused` flag — and with the same semantics (an
    /// already-armed row still dispatches). Returns whether a running
    /// seeder was stopped.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "runtime task control; first RPC consumer is the pause endpoint"
        )
    )]
    pub fn stop(&self, kind: &str) -> bool {
        self.workers.stop(kind)
    }

    /// Restart a stopped kind's seeder. Returns whether a seeder was
    /// (re)started.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "runtime task control; first RPC consumer is the pause endpoint"
        )
    )]
    pub fn start(&self, kind: &str) -> bool {
        self.workers.start(kind)
    }

    /// Wake a kind's seeder immediately (e.g. after flipping the paused
    /// flag) instead of waiting for its fallback tick. Returns whether the
    /// kind is registered.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "runtime task control; first RPC consumer is the pause endpoint"
        )
    )]
    pub fn kick(&self, kind: &str) -> bool {
        match self.runner.registrations.get(kind) {
            Some(registration) => {
                registration.trigger.wake();
                true
            }
            None => false,
        }
    }

    /// The assembled status of every cataloged task — derived fresh from
    /// the catalog, the queue, the stats, and the cursors.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "status read model; first consumer is the ListTasks RPC"
        )
    )]
    pub async fn statuses(&self) -> Result<Vec<TaskStatusEntry>, StorageError> {
        let now = Utc::now();
        self.runner
            .storage
            .read("task_statuses", move |repos| {
                list_task_statuses(
                    &repos.registry,
                    &repos.tasks,
                    &repos.stats,
                    &repos.cursors,
                    ESCALATE_AFTER_FAILURES,
                    now,
                )
                .map_err(StorageError::from)
            })
            .await
            .map_err(|e| StorageError::Fault(StorageFault::new(e.to_string())))?
    }
}

// ================ THE RUNNER ================
/// Owns the storage gateway and executes one unit of work at a time: a
/// seeding pass, an inline run, or a claimed run's full path. This is the
/// only type that knows both the handlers and where they execute.
struct TaskContextRunner {
    storage: AsyncStorage,
    registrations: HashMap<&'static str, Arc<Registration>>,
    /// Wakes the dispatcher immediately on seed instead of waiting for the
    /// fallback poll.
    wake: Notify,
}

impl TaskContextRunner {
    // ---------------- boot ----------------

    /// `RUNNING` rows at startup are orphans of a previous process (the
    /// single-instance lock guarantees no live owner). Rows with attempts
    /// left are requeued; exhausted rows move to the execution history as
    /// failed — and they count in the stats.
    async fn recover_stale_running(&self) {
        let now = Utc::now();
        let recovered = self
            .storage
            .write("task_recovery", false, move |repos| {
                let requeued = repos.tasks.requeue_interrupted(INTERRUPTED_ERROR, now)?;
                let exhausted = repos.tasks.take_exhausted_running(now)?;
                for task in &exhausted {
                    let execution = TaskExecution::from_task(
                        task,
                        ExecutionOutcome::Failed,
                        Some(INTERRUPTED_ERROR.to_owned()),
                        None,
                        now,
                    );
                    repos.executions.insert(&execution)?;
                    repos.stats.record_run(
                        task.kind(),
                        ExecutionOutcome::Failed,
                        Some(INTERRUPTED_ERROR.to_owned()),
                        None,
                        now,
                    )?;
                }
                let failed = u32::try_from(exhausted.len()).unwrap_or(u32::MAX);
                Ok::<_, StorageError>(requeued.saturating_add(failed))
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

    /// Upsert every code declaration into the catalog, retire kinds that
    /// disappeared from code, and cancel their leftover pending rows — one
    /// transaction, run once at boot (after crash recovery, so a retired
    /// kind's requeued rows are cancelled too).
    async fn reconcile_registry(&self, declarations: Vec<TaskDeclaration>) {
        let now = Utc::now();
        let outcome = self
            .storage
            .write("task_registry_reconcile", false, move |repos| {
                for declaration in &declarations {
                    repos.registry.declare(declaration, now)?;
                }
                let kinds: Vec<TaskKind> = declarations.iter().map(|d| d.kind.clone()).collect();
                let retired = repos.registry.retire_missing(&kinds, now)?;
                let mut cancelled: u32 = 0;
                for kind in &retired {
                    // Cancelled rows go to the history (audit trail) but not
                    // to the stats: a row that never ran is not a run.
                    let taken = repos.tasks.take_pending(kind)?;
                    for task in &taken {
                        let execution = TaskExecution::from_task(
                            task,
                            ExecutionOutcome::Failed,
                            Some(RETIRED_ERROR.to_owned()),
                            None,
                            now,
                        );
                        repos.executions.insert(&execution)?;
                    }
                    cancelled =
                        cancelled.saturating_add(u32::try_from(taken.len()).unwrap_or(u32::MAX));
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

    // ---------------- seeding ----------------

    /// One durable seeding pass: the operator pause gate and the trigger's
    /// reconcile share a single write transaction, so the gate cannot race
    /// the insert.
    async fn seed_pass(&self, kind: &'static str, registration: &Arc<Registration>) {
        let Ok(task_kind) = TaskKind::new(kind) else {
            return; // Unreachable: validated at registration.
        };
        let trigger = Arc::clone(&registration.trigger);
        let now = Utc::now();
        let pass = self
            .storage
            .write("task_seed_pass", false, move |repos| {
                if repos.registry.is_paused(&task_kind)? {
                    return Ok(SeedPass::Paused);
                }
                trigger.reconcile(repos, now)
            })
            .await;

        match pass {
            Ok(Ok(SeedPass::Seeded)) => self.wake.notify_one(),
            Ok(Ok(SeedPass::Paused)) => {
                tracing::debug!(kind, "task is paused; seeding skipped");
            }
            Ok(Ok(SeedPass::Idle)) => {}
            Ok(Err(e)) => tracing::warn!(kind, error = %e, "task seeding pass failed"),
            Err(e) => tracing::warn!(kind, error = %e, "task seeding pass not executed"),
        }
    }

    /// Run the handler inline, leaving no row behind. The body is awaited
    /// on the seeder task itself, so ephemeral runs of one kind never
    /// overlap. Deliberately not gated on `paused`: ephemeral work is
    /// liveness work.
    async fn inline_run(&self, kind: &'static str, registration: &Arc<Registration>) {
        let ctx = TaskContext {
            storage: self.storage.clone(),
            window: RunWindow::None,
        };
        let span = tracing::info_span!("task_run", kind, category = registration.category.as_str());
        match registration.handler.run(ctx).instrument(span).await {
            TaskOutcome::Done => {}
            TaskOutcome::NotReady { retry_after_secs } => {
                tracing::debug!(kind, retry_after_secs, "ephemeral run reported not-ready");
            }
            TaskOutcome::Failed(e) => {
                tracing::warn!(kind, error = %e, "ephemeral background run failed");
            }
        }
    }

    // ---------------- dispatching ----------------

    /// Claim and execute due tasks until the queue has none left.
    /// Zero-delay retries become due immediately and are picked up by the
    /// next claim in this same drain, bounded by each task's attempt
    /// budget.
    async fn drain_due(runner: &Arc<Self>) {
        loop {
            let now = Utc::now();
            let claimed = runner
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

            // Execute the batch with bounded parallelism; all executions
            // finish (and record their outcome) before the next claim.
            let semaphore = Arc::new(tokio::sync::Semaphore::new(EXECUTION_CONCURRENCY));
            let mut executions = tokio::task::JoinSet::new();
            for task in claimed {
                let runner = Arc::clone(runner);
                let semaphore = Arc::clone(&semaphore);
                executions.spawn(async move {
                    let Ok(_permit) = semaphore.acquire().await else {
                        return;
                    };
                    runner.execute_one(task).await;
                });
            }
            while executions.join_next().await.is_some() {}
        }
    }

    /// Run one claimed task: parse its window through the trigger, execute
    /// the handler inside a `task_run` span, let the trigger interpret the
    /// outcome, then record the completion in one transaction — a terminal
    /// run *moves* (queue delete + execution insert + stats fold) — and
    /// fire the trigger hooks.
    async fn execute_one(&self, task: Versioned<BackgroundTask>) {
        let Versioned {
            data: task,
            version,
        } = task;
        let kind = task.kind().as_str().to_owned();
        let id = *task.id();
        let attempt = task.attempts();
        let payload = task.payload().map(str::to_owned);

        let registration = self.registrations.get(kind.as_str()).map(Arc::clone);

        // `duration` is `Some` only when a handler actually executed.
        let (result, duration) = match &registration {
            None => (
                Err(TaskFailure::new(format!(
                    "no handler registered for kind {kind:?}"
                ))),
                None,
            ),
            Some(registration) => match registration.trigger.window_for(payload.as_deref()) {
                Err(message) => (Err(TaskFailure::new(message)), None),
                Ok(window) => {
                    let ctx = TaskContext {
                        storage: self.storage.clone(),
                        window,
                    };
                    let span = tracing::info_span!(
                        "task_run",
                        kind = %kind,
                        category = registration.category.as_str()
                    );
                    let started = std::time::Instant::now();
                    let outcome = registration.handler.run(ctx).instrument(span).await;
                    let verdict = registration
                        .trigger
                        .interpret(&self.storage, window, outcome)
                        .await;
                    (verdict, Some(started.elapsed()))
                }
            },
        };

        let now = Utc::now();
        let duration_ms =
            duration.map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
        let logged_duration = duration_ms.unwrap_or(0);
        let completion = match result {
            Ok(interpretation) => {
                let outcome = match interpretation {
                    Interpretation::Completed => ExecutionOutcome::Succeeded,
                    Interpretation::NotReady => ExecutionOutcome::NotReady,
                };
                let policy = registration.as_ref().map(|r| r.log_policy);
                if should_log_run(policy, true) {
                    tracing::info!(
                        target: "valqeron::audit",
                        operation = "task_run",
                        kind = %kind,
                        task_id = %id.value(),
                        attempt,
                        duration_ms = logged_duration,
                        outcome = match outcome {
                            ExecutionOutcome::NotReady => "not_ready",
                            _ => "succeeded",
                        },
                        "background task run finished"
                    );
                }
                TaskCompletion::Terminal {
                    outcome,
                    error: None,
                    finished_at: now,
                }
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
                        TaskCompletion::Terminal {
                            outcome: ExecutionOutcome::Failed,
                            error: Some(error.clone()),
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
                    duration_ms = logged_duration,
                    outcome = label,
                    error = %error,
                    "background task run finished"
                );
                completion
            }
        };

        // A terminal completion moves the run: queue delete, history
        // insert, stats fold — one transaction, applied only when the
        // guarded delete matched. Stats are recorded for registered kinds
        // only (an unknown kind's row is history, not a task's track
        // record); the trigger hooks need the terminal error before
        // `completion` moves into the closure.
        let terminal_error = match &completion {
            TaskCompletion::Terminal {
                outcome: ExecutionOutcome::Failed,
                error,
                ..
            } => error.clone(),
            _ => None,
        };
        let execution = match &completion {
            TaskCompletion::Terminal { outcome, error, .. } => Some(TaskExecution::from_task(
                &task,
                *outcome,
                error.clone(),
                duration_ms,
                now,
            )),
            TaskCompletion::Retry { .. } => None,
        };
        let stat_recorded = registration.is_some();

        let recorded = self
            .storage
            .write("task_complete", false, move |repos| {
                let outcome = repos
                    .tasks
                    .complete(&id, version, completion)
                    .map_err(StorageError::from)?;
                if let (WriteOutcome::Applied, Some(execution)) = (&outcome, execution) {
                    repos.executions.insert(&execution)?;
                    if stat_recorded {
                        repos.stats.record_run(
                            &execution.kind,
                            execution.outcome,
                            execution.error.clone(),
                            execution.duration_ms,
                            now,
                        )?;
                    }
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

        // Trigger hooks: terminal-failure side effects (sync cursor
        // cooldown), then re-arm the seeder without waiting for its
        // fallback tick.
        if let Some(registration) = registration {
            if let Some(error) = terminal_error {
                registration.trigger.on_terminal(&self.storage, error).await;
            }
            registration.trigger.wake();
        }
    }
}

/// Whether the manager emits the run-finished line for a successful run.
/// Failures are always logged.
fn should_log_run(policy: Option<LogPolicy>, succeeded: bool) -> bool {
    !(succeeded && policy == Some(LogPolicy::FailuresOnly))
}

// ================ THE WORKERS ================
/// One stoppable seeder loop per registered kind.
struct SeederWorker {
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// Owns the seeder loops and the dispatcher: spawning, per-kind stop and
/// restart, and the bounded global drain.
struct TaskWorkerManager {
    runner: Arc<TaskContextRunner>,
    seeders: Mutex<HashMap<&'static str, SeederWorker>>,
    dispatcher_shutdown: watch::Sender<bool>,
    dispatcher: JoinHandle<()>,
}

impl TaskWorkerManager {
    /// Spawn one seeder per registration and the dispatcher.
    fn launch(runner: Arc<TaskContextRunner>) -> Self {
        let (dispatcher_shutdown, _) = watch::channel(false);
        let dispatcher =
            Self::spawn_dispatcher(Arc::clone(&runner), dispatcher_shutdown.subscribe());

        let workers = Self {
            seeders: Mutex::new(HashMap::new()),
            dispatcher_shutdown,
            dispatcher,
            runner,
        };
        for (kind, registration) in &workers.runner.registrations {
            let worker = Self::spawn_seeder(&workers.runner, kind, Arc::clone(registration));
            if let Ok(mut seeders) = workers.seeders.lock() {
                seeders.insert(kind, worker);
            }
        }
        workers
    }

    /// One loop per registration: ticks at the trigger's cadence, wakes
    /// early on run completions, and delegates each pass to the runner.
    fn spawn_seeder(
        runner: &Arc<TaskContextRunner>,
        kind: &'static str,
        registration: Arc<Registration>,
    ) -> SeederWorker {
        let (shutdown, mut stopped) = watch::channel(false);
        let runner = Arc::clone(runner);
        let handle = tokio::spawn(async move {
            let (first_tick, period) = registration.trigger.cadence();
            let mut ticker = tokio::time::interval_at(first_tick, period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = stopped.changed() => {
                        tracing::debug!(kind, "task seeder stopped");
                        break;
                    }
                    _ = registration.trigger.wake_notified() => {}
                    _ = ticker.tick() => {}
                }
                match registration.trigger.mode() {
                    TickMode::Seed => runner.seed_pass(kind, &registration).await,
                    TickMode::Inline => runner.inline_run(kind, &registration).await,
                }
            }
        });
        SeederWorker { shutdown, handle }
    }

    fn spawn_dispatcher(
        runner: Arc<TaskContextRunner>,
        mut shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        tracing::debug!("task dispatcher stopped");
                        break;
                    }
                    _ = runner.wake.notified() => {}
                    _ = tokio::time::sleep(DISPATCH_POLL_INTERVAL) => {}
                }
                TaskContextRunner::drain_due(&runner).await;
            }
        })
    }

    /// Stop one kind's seeder loop. Returns whether a running seeder was
    /// stopped.
    fn stop(&self, kind: &str) -> bool {
        let Ok(seeders) = self.seeders.lock() else {
            return false;
        };
        match seeders.get(kind) {
            Some(worker) if !*worker.shutdown.borrow() => worker.shutdown.send(true).is_ok(),
            _ => false,
        }
    }

    /// Restart a stopped kind's seeder. Returns whether a seeder was
    /// (re)started; a kind that is unknown or still running is left alone.
    fn start(&self, kind: &str) -> bool {
        let Some((registered_kind, registration)) = self
            .runner
            .registrations
            .get_key_value(kind)
            .map(|(k, v)| (*k, Arc::clone(v)))
        else {
            return false;
        };
        let Ok(mut seeders) = self.seeders.lock() else {
            return false;
        };
        if seeders
            .get(registered_kind)
            .is_some_and(|worker| !*worker.shutdown.borrow())
        {
            return false; // Still running.
        }
        let worker = Self::spawn_seeder(&self.runner, registered_kind, registration);
        seeders.insert(registered_kind, worker);
        true
    }

    /// Stop everything and wait (bounded) for the loops to exit.
    async fn drain(self, deadline: Duration) -> bool {
        let _ = self.dispatcher_shutdown.send(true);
        let mut handles = vec![self.dispatcher];
        if let Ok(mut seeders) = self.seeders.lock() {
            for (_, worker) in seeders.drain() {
                let _ = worker.shutdown.send(true);
                handles.push(worker.handle);
            }
        }
        let all_done = async {
            for handle in handles {
                let _ = handle.await;
            }
        };
        tokio::time::timeout(deadline, all_done).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, NaiveTime};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use valqeron_core::{
        DerivedTaskStatus, MarketCalendar, Recurrence, Schedule, SyncCursorRepository,
        TaskCategory, TaskId, TaskStats, TaskStatus, TaskTracking, TaskTrigger,
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

    async fn queued_tasks(storage: &AsyncStorage) -> Vec<Versioned<BackgroundTask>> {
        storage
            .read("test.queued", |repos| repos.tasks.list_queued(50))
            .await
            .expect("no backpressure")
            .expect("list_queued succeeds")
    }

    async fn recent_executions(storage: &AsyncStorage) -> Vec<TaskExecution> {
        storage
            .read("test.executions", |repos| repos.executions.list_recent(50))
            .await
            .expect("no backpressure")
            .expect("list_recent succeeds")
    }

    async fn execution_row(storage: &AsyncStorage, id: TaskId) -> Option<TaskExecution> {
        storage
            .read("test.execution", move |repos| {
                repos.executions.find_by_id(&id)
            })
            .await
            .expect("no backpressure")
            .expect("find_by_id succeeds")
    }

    async fn stats_row(storage: &AsyncStorage, kind: &str) -> Option<TaskStats> {
        let kind = TaskKind::new(kind).unwrap();
        storage
            .read("test.stats", move |repos| repos.stats.get(&kind))
            .await
            .expect("no backpressure")
            .expect("get succeeds")
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

    fn interval(kind: &'static str, period_ms: u64) -> IntervalTaskBuilder {
        TaskDefinition::interval(kind, Duration::from_millis(period_ms))
            .category(TaskCategory::EngineSystem)
            .no_jitter()
    }

    fn daily_utc_schedule() -> Schedule {
        Schedule::new(
            MarketCalendar::UTC,
            NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
            Recurrence::Daily,
        )
    }

    fn recurring(kind: &'static str) -> RecurringTaskBuilder {
        TaskDefinition::recurring(kind, daily_utc_schedule()).category(TaskCategory::EngineSystem)
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

        let manager = BackgroundTasks::builder()
            .task(interval("test_durable", 20).run(move |_ctx: TaskContext| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    TaskOutcome::Done
                }
            }))
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 2).await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        let rows = recent_executions(&storage).await;
        assert!(!rows.is_empty(), "durable runs must leave history rows");
        assert!(
            rows.iter().all(|e| e.kind.as_str() == "test_durable"),
            "only the durable kind is recorded"
        );
        assert!(
            rows.iter()
                .any(|e| e.outcome == ExecutionOutcome::Succeeded),
            "at least one recorded run succeeded: {rows:?}"
        );
        assert!(
            rows.iter().all(|e| e.duration_ms.is_some()),
            "executed runs carry a measured duration"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ephemeral_interval_runs_without_rows_or_catalog_stats() {
        let (_dir, storage) = storage();
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);

        let manager = BackgroundTasks::builder()
            .task(
                interval("test_ephemeral", 10)
                    .ephemeral()
                    .run(move |_ctx: TaskContext| {
                        let counter = Arc::clone(&counter);
                        async move {
                            counter.fetch_add(1, Ordering::SeqCst);
                            TaskOutcome::Done
                        }
                    }),
            )
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 3).await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        assert!(
            queued_tasks(&storage).await.is_empty(),
            "ephemeral runs must not queue rows"
        );
        assert!(
            recent_executions(&storage).await.is_empty(),
            "ephemeral runs must not record history"
        );
        assert!(
            stats_row(&storage, "test_ephemeral").await.is_none(),
            "no per-run stats"
        );
        let registration = registration_row(&storage, "test_ephemeral")
            .await
            .expect("cataloged");
        assert_eq!(registration.tracking(), TaskTracking::Ephemeral);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failing_task_retries_until_it_succeeds() {
        let (_dir, storage) = storage();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);

        // Long-period registration: the handler is registered, the seeder
        // never interferes; the row is seeded directly.
        let manager = BackgroundTasks::builder()
            .task(
                interval("test_retry", 600_000).run(move |_ctx: TaskContext| {
                    let counter = Arc::clone(&counter);
                    async move {
                        if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                            TaskOutcome::Failed("transient".into())
                        } else {
                            TaskOutcome::Done
                        }
                    }
                }),
            )
            .start(storage.clone())
            .await;

        let id = seed_task(&storage, "test_retry", None, 3).await;

        let probe_storage = storage.clone();
        wait_until(10, async || {
            execution_row(&probe_storage, id)
                .await
                .is_some_and(|e| e.outcome == ExecutionOutcome::Succeeded)
        })
        .await;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "two failures + one success"
        );
        let execution = execution_row(&storage, id).await.unwrap();
        assert_eq!(execution.attempts, 3);
        assert!(
            storage
                .read("test.gone", move |repos| repos.tasks.find_by_id(&id))
                .await
                .unwrap()
                .unwrap()
                .is_none(),
            "the terminal run left the queue"
        );
        assert!(manager.drain(Duration::from_secs(2)).await);

        // Only the terminal completion reaches the stats.
        let stats = stats_row(&storage, "test_retry").await.unwrap();
        assert_eq!(stats.total_runs, 1);
        assert_eq!(stats.total_failures, 0);
        assert_eq!(stats.last_outcome, Some(ExecutionOutcome::Succeeded));
        assert!(stats.last_success_at.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exhausted_retries_end_terminally_failed_and_counted() {
        let (_dir, storage) = storage();

        let manager = BackgroundTasks::builder()
            .task(
                interval("test_fatal", 600_000)
                    .run(|_ctx: TaskContext| async { TaskOutcome::Failed("always broken".into()) }),
            )
            .start(storage.clone())
            .await;

        let id = seed_task(&storage, "test_fatal", None, 2).await;

        let probe_storage = storage.clone();
        wait_until(10, async || {
            execution_row(&probe_storage, id)
                .await
                .is_some_and(|e| e.outcome == ExecutionOutcome::Failed)
        })
        .await;

        let execution = execution_row(&storage, id).await.unwrap();
        assert_eq!(execution.attempts, 2, "the full budget was spent");
        assert_eq!(execution.error.as_deref(), Some("always broken"));
        assert!(manager.drain(Duration::from_secs(2)).await);

        let stats = stats_row(&storage, "test_fatal").await.unwrap();
        assert_eq!(stats.total_runs, 1);
        assert_eq!(stats.total_failures, 1);
        assert_eq!(stats.last_outcome, Some(ExecutionOutcome::Failed));
        assert_eq!(stats.last_error.as_deref(), Some("always broken"));
        assert_eq!(stats.last_success_at, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_kind_fails_terminally_with_a_named_error() {
        let (_dir, storage) = storage();
        let manager = BackgroundTasks::builder().start(storage.clone()).await;

        let id = seed_task(&storage, "test_unregistered", None, 1).await;

        let probe_storage = storage.clone();
        wait_until(10, async || {
            execution_row(&probe_storage, id)
                .await
                .is_some_and(|e| e.outcome == ExecutionOutcome::Failed)
        })
        .await;

        let execution = execution_row(&storage, id).await.unwrap();
        assert!(
            execution
                .error
                .as_deref()
                .is_some_and(|e| e.contains("no handler registered")),
            "error names the missing handler: {:?}",
            execution.error
        );
        assert_eq!(execution.duration_ms, None, "no handler ever executed");
        assert!(
            stats_row(&storage, "test_unregistered").await.is_none(),
            "unregistered kinds are history, not a task's track record"
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

        let manager = BackgroundTasks::builder()
            .task(
                interval("test_recovered", 600_000)
                    .run(|_ctx: TaskContext| async { TaskOutcome::Done }),
            )
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(10, async || {
            execution_row(&probe_storage, id)
                .await
                .is_some_and(|e| e.outcome == ExecutionOutcome::Succeeded)
        })
        .await;

        let execution = execution_row(&storage, id).await.unwrap();
        assert_eq!(execution.attempts, 2, "interrupted attempt + recovered run");
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_ticks_do_not_pile_up_while_a_run_is_active() {
        let (_dir, storage) = storage();
        let release = Arc::new(Notify::new());
        let releaser = Arc::clone(&release);
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&runs);

        let manager = BackgroundTasks::builder()
            .task(interval("test_slow", 10).run(move |_ctx: TaskContext| {
                let release = Arc::clone(&releaser);
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    TaskOutcome::Done
                }
            }))
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 1).await;
        // Let several periods elapse while the first run is still active.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let rows = queued_tasks(&storage).await;
        assert_eq!(
            rows.len(),
            1,
            "the gate must prevent same-kind pileup: {rows:?}"
        );
        assert!(
            recent_executions(&storage).await.is_empty(),
            "the held run has not completed"
        );

        release.notify_waiters();
        assert!(manager.drain(Duration::from_secs(2)).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recurring_seeds_the_next_occurrence_as_a_future_row() {
        let (_dir, storage) = storage();
        let manager = BackgroundTasks::builder()
            .task(recurring("test_recurring").run(|_ctx: TaskContext| async { TaskOutcome::Done }))
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(5, async || !queued_tasks(&probe_storage).await.is_empty()).await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        let rows = queued_tasks(&storage).await;
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
        let manager = BackgroundTasks::builder()
            .task(recurring("test_recurring").run(move |_ctx: TaskContext| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    TaskOutcome::Done
                }
            }))
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 1).await;
        let probe_storage = storage.clone();
        wait_until(5, async || !queued_tasks(&probe_storage).await.is_empty()).await;
        assert!(manager.drain(Duration::from_secs(2)).await);

        assert_eq!(runs.load(Ordering::SeqCst), 1, "the missed slot ran once");
        let succeeded = recent_executions(&storage)
            .await
            .iter()
            .filter(|e| e.outcome == ExecutionOutcome::Succeeded)
            .count();
        let future_pending = queued_tasks(&storage)
            .await
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

        let manager = BackgroundTasks::builder()
            .task(
                interval("test_dup", 10)
                    .ephemeral()
                    .run(move |_ctx: TaskContext| {
                        let counter = Arc::clone(&first_counter);
                        async move {
                            counter.fetch_add(1, Ordering::SeqCst);
                            TaskOutcome::Done
                        }
                    }),
            )
            .task(
                interval("test_dup", 10)
                    .ephemeral()
                    .run(move |_ctx: TaskContext| {
                        let counter = Arc::clone(&second_counter);
                        async move {
                            counter.fetch_add(1, Ordering::SeqCst);
                            TaskOutcome::Done
                        }
                    }),
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
        let disabled_declaration = TaskDefinition::sync(
            "test_disabled_source",
            valqeron_core::SyncSource::new("disabled_src").unwrap(),
            Schedule::new(
                MarketCalendar::B3,
                NaiveTime::from_hms_opt(7, 0, 0).unwrap(),
                Recurrence::Daily,
            ),
        )
        .disabled();
        let manager = BackgroundTasks::builder()
            .task(interval("test_a", 600_000).run(|_: TaskContext| async { TaskOutcome::Done }))
            .task(
                interval("test_b", 600_000)
                    .ephemeral()
                    .run(|_: TaskContext| async { TaskOutcome::Done }),
            )
            .task(recurring("test_c").run(|_: TaskContext| async { TaskOutcome::Done }))
            .declare(disabled_declaration)
            .start(storage.clone())
            .await;

        let listed = storage
            .read("test.list", |repos| repos.registry.list())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(listed.len(), 4);

        let a = registration_row(&storage, "test_a").await.unwrap();
        assert_eq!(a.trigger(), TaskTrigger::Interval);
        assert_eq!(a.tracking(), TaskTracking::Durable);
        assert_eq!(a.schedule(), "interval:600s");
        assert!(a.config_enabled());

        let c = registration_row(&storage, "test_c").await.unwrap();
        assert_eq!(c.trigger(), TaskTrigger::Recurring);
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
            trigger: TaskTrigger::Recurring,
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
        let manager = BackgroundTasks::builder()
            .task(
                interval("test_current", 600_000).run(|_: TaskContext| async { TaskOutcome::Done }),
            )
            .start(storage.clone())
            .await;

        let registration = registration_row(&storage, "test_old_kind").await.unwrap();
        assert!(!registration.registered(), "retired at reconcile");

        let execution = execution_row(&storage, id).await.expect("history row");
        assert_eq!(execution.outcome, ExecutionOutcome::Failed);
        assert_eq!(execution.error.as_deref(), Some(RETIRED_ERROR));
        assert!(
            storage
                .read("test.row", move |repos| repos.tasks.find_by_id(&id))
                .await
                .unwrap()
                .unwrap()
                .is_none(),
            "the cancelled row left the queue"
        );
        assert!(
            stats_row(&storage, "test_old_kind").await.is_none(),
            "a cancelled row never ran, so it is not a run"
        );
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
            trigger: TaskTrigger::Interval,
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
        let manager = BackgroundTasks::builder()
            .task(interval("test_pausable", 20).run(move |_ctx: TaskContext| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    TaskOutcome::Done
                }
            }))
            .start(storage.clone())
            .await;

        // Several ticks pass; the paused gate must hold.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0, "paused: nothing runs");
        assert!(queued_tasks(&storage).await.is_empty(), "nothing seeded");

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
        let manager = BackgroundTasks::builder()
            .task(
                recurring("test_status_recurring")
                    .run(|_: TaskContext| async { TaskOutcome::Done }),
            )
            .task(
                interval("test_status_ephemeral", 600_000)
                    .ephemeral()
                    .run(|_: TaskContext| async { TaskOutcome::Done }),
            )
            .start(storage.clone())
            .await;

        // Wait for the recurring seeder to arm its future row.
        let probe_storage = storage.clone();
        wait_until(5, async || !queued_tasks(&probe_storage).await.is_empty()).await;

        let entries = manager.statuses().await.unwrap();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_handles_stop_and_restart_one_kind() {
        let (_dir, storage) = storage();

        // A struct handler exercises the trait half of the contract; the
        // closures everywhere else exercise the blanket impl.
        struct Counting(Arc<AtomicUsize>);
        impl TaskHandler for Counting {
            fn run<'a>(&'a self, _ctx: TaskContext) -> BoxFuture<'a, TaskOutcome> {
                let counter = Arc::clone(&self.0);
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    TaskOutcome::Done
                })
            }
        }

        let runs = Arc::new(AtomicUsize::new(0));
        let manager = BackgroundTasks::builder()
            .task(interval("test_stoppable", 50).run(Counting(Arc::clone(&runs))))
            .start(storage.clone())
            .await;

        // Stop before the first tick (50ms out): the seeder never seeds.
        assert!(manager.stop("test_stoppable"), "running seeder stops");
        assert!(!manager.stop("test_stoppable"), "stopping twice is a no-op");
        assert!(
            !manager.stop("test_unknown"),
            "unknown kinds have no worker"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0, "stopped: nothing ran");
        assert!(
            queued_tasks(&storage).await.is_empty(),
            "stopped: nothing seeded"
        );

        // Restart: seeding resumes on a fresh loop.
        assert!(!manager.start("test_unknown"), "unknown kinds cannot start");
        assert!(manager.start("test_stoppable"), "stopped seeder restarts");
        assert!(
            !manager.start("test_stoppable"),
            "a running seeder is left alone"
        );
        let probe = Arc::clone(&runs);
        wait_until(5, async || probe.load(Ordering::SeqCst) >= 1).await;

        assert!(manager.kick("test_stoppable"), "registered kinds kick");
        assert!(!manager.kick("test_unknown"));
        assert!(manager.drain(Duration::from_secs(2)).await);
    }
}
