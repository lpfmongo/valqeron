//! The recurring trigger: wall-clock business-day occurrences, always
//! durable. The next occurrence is seeded ahead as a future `PENDING` row —
//! the row itself is the alarm, so it survives restarts and suspend, and a
//! slot the engine was down for runs once at the next boot (unlike an
//! interval ticker, which restarts from zero and starves on machines that
//! restart more often than the period).

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use tokio::sync::Notify;
use valqeron_core::{
    BackgroundTask, BackgroundTaskRepository, Repositories, Schedule, StorageError, StorageFault,
    TaskKind,
};
use valqeron_infrastructure::SqliteStorageEngine;

use crate::scheduler::trigger::{
    BoxFuture, Interpretation, RetryPolicy, RunWindow, SEED_FALLBACK_INTERVAL, SeedPass,
    TaskFailure, TaskOutcome, TickMode, Trigger,
};
use crate::storage::AsyncStorage;

pub(crate) struct RecurringTrigger {
    kind: &'static str,
    schedule: Schedule,
    retry: RetryPolicy,
    /// Run completions wake the seeder so the next occurrence is re-armed
    /// immediately instead of waiting for the fallback tick.
    wake: Notify,
}

impl RecurringTrigger {
    pub(crate) fn new(kind: &'static str, schedule: Schedule, retry: RetryPolicy) -> Self {
        Self {
            kind,
            schedule,
            retry,
            wake: Notify::new(),
        }
    }
}

impl Trigger for RecurringTrigger {
    fn cadence(&self) -> (tokio::time::Instant, Duration) {
        // First pass immediately: reseeding after a restart must not wait.
        (tokio::time::Instant::now(), SEED_FALLBACK_INTERVAL)
    }

    fn mode(&self) -> TickMode {
        TickMode::Seed
    }

    /// Seed the next occurrence as a future row, unless one is already
    /// pending or running. No cursor and no catch-up beyond the seeded row
    /// itself — a slot missed while the engine was down is simply a
    /// past-due row that runs once at boot.
    fn reconcile(
        &self,
        repos: &Repositories<SqliteStorageEngine>,
        now: DateTime<Utc>,
    ) -> Result<SeedPass, StorageError> {
        let kind = TaskKind::new(self.kind)
            .map_err(|e| StorageError::Fault(StorageFault::new(e.to_string())))?;
        if repos.tasks.exists_active(&kind)? {
            // The armed row is the dispatcher's alarm; the seeder has
            // nothing clock-driven pending until the completion wake.
            return Ok(SeedPass::Idle { next_pass_at: None });
        }
        let Some(slot) = self.schedule.next_occurrence_after(now) else {
            return Err(StorageError::Fault(StorageFault::new(format!(
                "recurring schedule for {:?} produced no occurrence",
                self.kind
            ))));
        };
        let task = BackgroundTask::builder()
            .kind(kind)
            .scheduled_at(slot)
            .max_attempts(self.retry.max_attempts)
            .retry_delay_secs(self.retry.retry_delay_secs)
            .build()
            .map_err(|e| StorageError::Fault(StorageFault::new(e.to_string())))?;
        let task_id = *task.id();
        repos.tasks.insert(&task)?;
        tracing::info!(
            target: "valqeron::audit",
            operation = "task_seed",
            kind = self.kind,
            task_id = %task_id.value(),
            scheduled_at = %slot.to_rfc3339_opts(SecondsFormat::Millis, true),
            "seeded the next recurring run"
        );
        Ok(SeedPass::Seeded)
    }

    fn window_for(&self, _payload: Option<&str>) -> Result<RunWindow, String> {
        Ok(RunWindow::None)
    }

    fn interpret<'a>(
        &'a self,
        _storage: &'a AsyncStorage,
        _window: RunWindow,
        outcome: TaskOutcome,
    ) -> BoxFuture<'a, Result<Interpretation, TaskFailure>> {
        Box::pin(async move {
            match outcome {
                TaskOutcome::Done => Ok(Interpretation::Completed),
                TaskOutcome::NotReady { retry_after_secs } => {
                    tracing::info!(
                        kind = self.kind,
                        retry_after_secs,
                        "run reported not-ready; the next occurrence retries"
                    );
                    Ok(Interpretation::NotReady)
                }
                TaskOutcome::Failed(error) => Err(TaskFailure::new(error)),
            }
        })
    }

    fn on_terminal<'a>(&'a self, _storage: &'a AsyncStorage, _error: String) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn wake(&self) {
        self.wake.notify_one();
    }

    fn wake_notified<'a>(&'a self) -> BoxFuture<'a, ()> {
        Box::pin(self.wake.notified())
    }
}
