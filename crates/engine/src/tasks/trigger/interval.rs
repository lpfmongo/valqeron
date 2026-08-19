//! The interval trigger: monotonic ticks since boot, for liveness and
//! housekeeping work where "every N seconds of uptime" is the right
//! semantic. Durable ticks seed an immediately-due row (gated so one kind
//! never piles up); ephemeral ticks run inline with no persistence.

use std::time::Duration;

use chrono::{DateTime, Utc};
use valqeron_core::{
    BackgroundTask, BackgroundTaskRepository, Repositories, StorageError, StorageFault, TaskKind,
};
use valqeron_infrastructure::SqliteStorageEngine;

use crate::storage::AsyncStorage;
use crate::tasks::trigger::{
    BoxFuture, Interpretation, RetryPolicy, RunWindow, SeedPass, TaskFailure, TaskOutcome,
    TickMode, Tracking, Trigger,
};

pub(crate) struct IntervalTrigger {
    kind: &'static str,
    period: Duration,
    jitter: bool,
    tracking: Tracking,
}

impl IntervalTrigger {
    pub(crate) fn new(
        kind: &'static str,
        period: Duration,
        jitter: bool,
        tracking: Tracking,
    ) -> Self {
        Self {
            kind,
            period,
            jitter,
            tracking,
        }
    }
}

impl Trigger for IntervalTrigger {
    fn cadence(&self) -> (tokio::time::Instant, Duration) {
        let period = if self.jitter {
            jittered(self.period)
        } else {
            self.period
        };
        let first = tokio::time::Instant::now()
            .checked_add(period)
            .unwrap_or_else(tokio::time::Instant::now);
        (first, period)
    }

    fn mode(&self) -> TickMode {
        match self.tracking {
            Tracking::Durable => TickMode::Seed,
            Tracking::Ephemeral => TickMode::Inline,
        }
    }

    /// Seed one immediately-due run of `kind`, unless a previous run is
    /// still active (pending or running) — the no-overlap/no-pileup
    /// guarantee.
    fn reconcile(
        &self,
        repos: &Repositories<SqliteStorageEngine>,
        _now: DateTime<Utc>,
    ) -> Result<SeedPass, StorageError> {
        let kind = TaskKind::new(self.kind)
            .map_err(|e| StorageError::Fault(StorageFault::new(e.to_string())))?;
        if repos.tasks.exists_active(&kind)? {
            return Ok(SeedPass::Idle);
        }
        let retry = RetryPolicy::none();
        let task = BackgroundTask::builder()
            .kind(kind)
            .max_attempts(retry.max_attempts)
            .retry_delay_secs(retry.retry_delay_secs)
            .build()
            .map_err(|e| StorageError::Fault(StorageFault::new(e.to_string())))?;
        repos.tasks.insert(&task)?;
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
                    // No cursor to hold: the retry is simply the next tick.
                    tracing::info!(
                        kind = self.kind,
                        retry_after_secs,
                        "run reported not-ready; the next interval tick retries"
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

    fn wake(&self) {}

    fn wake_notified<'a>(&'a self) -> BoxFuture<'a, ()> {
        Box::pin(std::future::pending())
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

    #[test]
    fn modes_follow_tracking() {
        let durable = IntervalTrigger::new("t", Duration::from_secs(1), false, Tracking::Durable);
        assert_eq!(durable.mode(), TickMode::Seed);
        let ephemeral =
            IntervalTrigger::new("t", Duration::from_secs(1), false, Tracking::Ephemeral);
        assert_eq!(ephemeral.mode(), TickMode::Inline);
        assert!(matches!(ephemeral.window_for(None), Ok(RunWindow::None)));
    }
}
