//! ENGINE_SYSTEM tasks: the engine's own housekeeping and liveness work.

use std::time::{Duration, Instant};

use chrono::NaiveTime;
use tokio::sync::watch;
use valqeron_core::{
    MarketCalendar, Recurrence, Schedule, StorageError, TaskCategory, TaskExecutionRepository,
};
use valqeron_infrastructure::SqliteStorageEngine;

use crate::lifecycle::LifecycleState;
use crate::scheduler::{SchedulerBuilder, TaskContext, TaskDefinition, TaskOutcome};

/// Task kinds this module registers — also the `kind` values persisted in
/// the task tables (the ephemeral kinds never persist rows).
pub(crate) const DB_MAINTENANCE_TASK: &str = "db_maintenance";
pub(crate) const HEARTBEAT_TASK: &str = "heartbeat";
pub(crate) const TASK_PRUNE_TASK: &str = "task_prune";
pub(crate) const SD_WATCHDOG_TASK: &str = "sd_watchdog";

/// Code-default periods, seeded into the registry on first boot; the
/// registry's `period_secs` (operator-tunable) is what actually runs.
/// Maintenance every 8 hours: a WAL checkpoint + optimize three times a
/// day is plenty for a reference-data write load.
const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(28_800);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300);

/// How long execution-history rows are kept before `task_prune` deletes
/// them.
const TASK_RETENTION_DAYS: i64 = 7;

/// Wall-clock time of day (UTC) `task_prune` recurs at. Recurring — not a
/// monotonic 24h ticker, which restarts from zero on every boot and never
/// fired on machines restarting daily. Business-day recurrence means
/// weekend prunes wait for Monday, which the retention window tolerates.
const TASK_PRUNE_AT: (u32, u32) = (3, 0);

fn task_prune_at() -> NaiveTime {
    let (h, m) = TASK_PRUNE_AT;
    NaiveTime::from_hms_opt(h, m, 0).unwrap_or_default()
}

/// Register the engine's built-in system tasks: durable database
/// maintenance, the wall-clock history prune, the ephemeral heartbeat, and
/// (under a systemd watchdog) the ephemeral watchdog ping.
pub(crate) fn register(
    builder: SchedulerBuilder,
    started: Instant,
    state: watch::Receiver<LifecycleState>,
) -> SchedulerBuilder {
    let builder = builder
        .task(
            TaskDefinition::interval(DB_MAINTENANCE_TASK, DEFAULT_MAINTENANCE_INTERVAL)
                .category(TaskCategory::EngineSystem)
                .run(|ctx: TaskContext| async move {
                    match ctx
                        .storage
                        .maintenance(DB_MAINTENANCE_TASK, run_maintenance)
                        .await
                    {
                        Ok(Ok(())) => TaskOutcome::Done,
                        Ok(Err(e)) => TaskOutcome::Failed(e),
                        Err(e) => TaskOutcome::Failed(e.to_string()),
                    }
                }),
        )
        .task(
            TaskDefinition::interval(HEARTBEAT_TASK, DEFAULT_HEARTBEAT_INTERVAL)
                .category(TaskCategory::EngineSystem)
                .ephemeral()
                .run(move |_ctx: TaskContext| {
                    let state = state.clone();
                    async move {
                        let current = *state.borrow();
                        tracing::debug!(
                            task = "heartbeat",
                            state = current.as_str(),
                            uptime_secs = started.elapsed().as_secs(),
                            "engine alive"
                        );
                        TaskOutcome::Done
                    }
                }),
        )
        .task(
            TaskDefinition::recurring(
                TASK_PRUNE_TASK,
                Schedule::new(MarketCalendar::UTC, task_prune_at(), Recurrence::Daily),
            )
            .category(TaskCategory::EngineSystem)
            .run(|ctx: TaskContext| async move {
                let Some(cutoff) = chrono::Utc::now()
                    .checked_sub_signed(chrono::Duration::days(TASK_RETENTION_DAYS))
                else {
                    // Unrepresentable retention window; nothing sane to prune.
                    return TaskOutcome::Done;
                };
                let pruned = ctx
                    .storage
                    .write(TASK_PRUNE_TASK, false, move |repos| {
                        repos
                            .executions
                            .prune_finished(cutoff)
                            .map_err(StorageError::from)
                    })
                    .await;
                match pruned {
                    Ok(Ok(removed)) => {
                        tracing::info!(
                            target: "valqeron::audit",
                            operation = "task_prune",
                            removed,
                            retention_days = TASK_RETENTION_DAYS,
                            "pruned terminal background task executions"
                        );
                        TaskOutcome::Done
                    }
                    Ok(Err(e)) => TaskOutcome::Failed(e.to_string()),
                    Err(e) => TaskOutcome::Failed(e.to_string()),
                }
            }),
        );

    // Under a systemd watchdog (WatchdogSec= in the unit), ping WATCHDOG=1
    // at half the configured interval so a hung engine — not just a dead
    // one — gets detected and restarted. No-op everywhere else. The
    // period follows the systemd contract at every boot, so it is pinned:
    // never captured in the registry, never operator-tunable.
    let Some(interval) = crate::notify::watchdog_interval() else {
        return builder;
    };
    let period = interval.checked_div(2).unwrap_or(interval);
    tracing::info!(
        interval_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
        ping_every_ms = u64::try_from(period.as_millis()).unwrap_or(u64::MAX),
        "systemd watchdog armed; pinging at half the interval"
    );
    builder.task(
        TaskDefinition::interval(SD_WATCHDOG_TASK, period)
            .category(TaskCategory::EngineSystem)
            .ephemeral()
            .pinned()
            .run(|_ctx: TaskContext| async {
                crate::notify::notify_watchdog();
                TaskOutcome::Done
            }),
    )
}

/// One maintenance run, executed through the storage facade — never on a
/// runtime thread. Outcomes are logged here; the returned result feeds the
/// durable task record. Failures are retried at the next periodic tick and
/// must not take the daemon down.
fn run_maintenance(engine: &SqliteStorageEngine) -> Result<(), String> {
    let started = Instant::now();
    match engine.run_maintenance() {
        Ok(stats) => {
            tracing::info!(
                target: "valqeron::audit",
                operation = "db_maintenance",
                busy = stats.busy,
                wal_frames = stats.log_frames,
                checkpointed_frames = stats.checkpointed_frames,
                duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "maintenance completed"
            );
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                target: "valqeron::audit",
                operation = "db_maintenance",
                error = %e,
                "maintenance failed"
            );
            Err(e.to_string())
        }
    }
}
