//! Execution triggers: the tier semantics behind background tasks.
//!
//! The task manager is a generic kernel — catalog, seeding loops, dispatch,
//! completion recording. Everything a *tier* means (monotonic intervals,
//! wall-clock recurrence, cursor-driven sync with catch-up) lives behind
//! the [`Trigger`] trait in this module tree, and everything a *task* means
//! lives behind the handler contract ([`TaskContext`] → [`TaskOutcome`]).
//! The manager never sees cursors, payload formats, or tier state.

pub(crate) mod interval;
pub(crate) mod recurring;
pub(crate) mod sync;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use valqeron_core::{
    CooldownPolicy, Repositories, Schedule, StorageError, SyncSource, TargetPeriod, TaskSettings,
    TaskTracking, TaskTrigger,
};
use valqeron_infrastructure::SqliteStorageEngine;

use crate::storage::AsyncStorage;

/// Fallback cadence of the wall-clock seeders (recurring + sync): the first
/// pass runs immediately at boot, run completions wake them directly, and
/// cooldown expiries are precise sleeps from the pass's own hint
/// ([`SeedPass::Idle`]) — this cap only self-heals whatever slips through
/// (a transiently failed pass, a missed edge). One hour aligns with core's
/// `MAX_COOLDOWN`, so every possible cooldown hint fits inside a single
/// sleep; wall-clock work is daily-grained, and the next occurrence is
/// armed as a durable row long before it is due, so an hour of worst-case
/// seeding lateness is immaterial.
pub(crate) const SEED_FALLBACK_INTERVAL: Duration = Duration::from_secs(3600);

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ================ HANDLER CONTRACT ================
/// Execution context handed to a handler. The manager and the triggers parse
/// everything; handlers receive typed values only.
pub(crate) struct TaskContext {
    pub storage: AsyncStorage,
    pub window: RunWindow,
}

/// What span of work this run is responsible for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunWindow {
    /// No window semantics (interval and recurring tiers).
    None,
    /// Sync tier: the occurrence this run belongs to and the civil dates it
    /// must cover — from the row's payload, never from `now`, so a catch-up
    /// run executing days late still targets its own period.
    Period {
        slot: DateTime<Utc>,
        target: TargetPeriod,
    },
}

/// How a handler reports one run. One vocabulary for every tier; each
/// trigger interprets it (sync holds its cursor on `NotReady`, the simpler
/// tiers just log — their retry is the next occurrence).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskOutcome {
    Done,
    /// The work cannot proceed *yet* — not an error.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "handler vocabulary; first NotReady producer arrives with real ingestion"
        )
    )]
    NotReady {
        retry_after_secs: u32,
    },
    Failed(String),
}

// ================ INTERPRETATION ================
/// What a completed handler run meant, after the trigger applied its side
/// effects — this is what the execution history records. `NotReady` is a
/// first-class result ("waited" is not "worked"), never a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interpretation {
    /// The run did its work (the sync trigger advanced its cursor).
    Completed,
    /// The run could not proceed yet (the sync trigger held its cursor and
    /// set a cooldown).
    NotReady,
}

// ================ FAILURE ================
/// What a run reports when it fails; recorded as the row's `last_error`
/// and fed into the retry decision.
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

// ================ REGISTRATION CONFIG ================
/// Whether a trigger's runs are persisted as `background_task` rows
/// (history, retries visible) or executed purely in memory.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Tracking {
    Durable,
    Ephemeral,
}

/// How a durable run retries within one occurrence.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    pub max_attempts: u32,
    pub retry_delay_secs: u32,
}

impl RetryPolicy {
    /// One attempt, no retry — for work whose "retry" is simply the next
    /// occurrence.
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            retry_delay_secs: 0,
        }
    }
}

/// Everything the manager needs to know about a task's tier — and nothing
/// more.
#[derive(Clone)]
pub(crate) enum TriggerConfig {
    /// Monotonic interval since boot (liveness and housekeeping work).
    Interval {
        period: Duration,
        /// ±10% period jitter so periodic tasks do not synchronize.
        jitter: bool,
        tracking: Tracking,
    },
    /// Wall-clock business-day recurrence; the next occurrence is always
    /// seeded ahead as a durable row (the row is the alarm).
    Recurring {
        schedule: Schedule,
        retry: RetryPolicy,
    },
    /// Cursor-driven recurrence with sequential catch-up, cooldowns, and
    /// halt-on-failure semantics.
    Sync {
        source: SyncSource,
        schedule: Schedule,
        retry: RetryPolicy,
        cooldown: CooldownPolicy,
        /// Bound on unattended catch-up, in business days; beyond it the
        /// cursor skips ahead to the most recent occurrence with a warning.
        max_backfill_days: u32,
    },
}

impl TriggerConfig {
    pub fn trigger_kind(&self) -> TaskTrigger {
        match self {
            TriggerConfig::Interval { .. } => TaskTrigger::Interval,
            TriggerConfig::Recurring { .. } => TaskTrigger::Recurring,
            TriggerConfig::Sync { .. } => TaskTrigger::Sync,
        }
    }

    pub fn tracking(&self) -> TaskTracking {
        match self {
            TriggerConfig::Interval {
                tracking: Tracking::Ephemeral,
                ..
            } => TaskTracking::Ephemeral,
            _ => TaskTracking::Durable,
        }
    }

    pub fn source(&self) -> Option<SyncSource> {
        match self {
            TriggerConfig::Sync { source, .. } => Some(source.clone()),
            _ => None,
        }
    }

    /// Canonical schedule descriptor for the catalog, display-only.
    pub fn descriptor(&self) -> String {
        match self {
            TriggerConfig::Interval { period, jitter, .. } => {
                let secs = period.as_secs();
                if *jitter {
                    format!("interval:{secs}s±10%")
                } else {
                    format!("interval:{secs}s")
                }
            }
            TriggerConfig::Recurring { schedule, .. } => {
                format!("recurring:{}", schedule.descriptor())
            }
            TriggerConfig::Sync { schedule, .. } => format!("sync:{}", schedule.descriptor()),
        }
    }

    /// The code config with the registry's stored settings folded in:
    /// every `Some` overrides the corresponding knob, every `None` keeps
    /// the code value. Calendars, jitter, tracking, and retry policies are
    /// never DB-tunable.
    pub fn with_settings(self, settings: &TaskSettings) -> Self {
        match self {
            TriggerConfig::Interval {
                period,
                jitter,
                tracking,
            } => TriggerConfig::Interval {
                period: settings
                    .period_secs
                    .map(|secs| Duration::from_secs(u64::from(secs)))
                    .unwrap_or(period),
                jitter,
                tracking,
            },
            TriggerConfig::Recurring { schedule, retry } => TriggerConfig::Recurring {
                schedule: schedule_with(schedule, settings),
                retry,
            },
            TriggerConfig::Sync {
                source,
                schedule,
                retry,
                cooldown,
                max_backfill_days,
            } => TriggerConfig::Sync {
                source,
                schedule: schedule_with(schedule, settings),
                retry,
                cooldown: settings
                    .cooldown_secs
                    .map(CooldownPolicy::new)
                    .unwrap_or(cooldown),
                max_backfill_days: settings.max_backfill_days.unwrap_or(max_backfill_days),
            },
        }
    }
}

/// `schedule` with the stored time-of-day/recurrence overrides applied;
/// the market calendar is identity, never a setting.
fn schedule_with(schedule: Schedule, settings: &TaskSettings) -> Schedule {
    Schedule::new(
        *schedule.calendar(),
        settings.at_local.unwrap_or_else(|| schedule.at()),
        settings.recurrence.unwrap_or_else(|| schedule.recurrence()),
    )
}

// ================ SEEDING ================
/// How a trigger's seeder ticks are executed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TickMode {
    /// Each tick may seed a durable row (inside one write transaction).
    Seed,
    /// Each tick runs the handler inline, leaving no row behind.
    Inline,
}

/// What one seeding pass decided — for the manager's generic logging and
/// the seeder's next sleep. (The operator enable gate lives in the
/// manager's memory and never reaches the triggers: a disabled kind's pass
/// is skipped entirely.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeedPass {
    /// A row was inserted; the dispatcher should wake.
    Seeded,
    /// Nothing seeded. `next_pass_at` is the trigger's own clock edge (a
    /// cooldown expiry, an advanced-past slot to retry immediately);
    /// `None` means nothing is pending before the next wake or the
    /// fallback tick.
    Idle { next_pass_at: Option<DateTime<Utc>> },
}

// ================ THE TRAIT ================
/// One tier's scheduling semantics. Implementations own their state
/// (the sync trigger owns the cursor and the payload format); the manager
/// only orchestrates.
pub(crate) trait Trigger: Send + Sync {
    /// Seeder loop cadence: (first tick, period), computed at spawn time.
    fn cadence(&self) -> (tokio::time::Instant, Duration);

    fn mode(&self) -> TickMode;

    /// One seeding pass, executed inside the manager's single write-lane
    /// transaction (after the manager's paused gate) — gate and insert
    /// cannot race.
    fn reconcile(
        &self,
        repos: &Repositories<SqliteStorageEngine>,
        now: DateTime<Utc>,
    ) -> Result<SeedPass, StorageError>;

    /// Parse a claimed row's payload into the handler's window.
    fn window_for(&self, payload: Option<&str>) -> Result<RunWindow, String>;

    /// Interpret the handler's outcome, applying tier side effects (the
    /// sync trigger advances or holds its cursor here). `Ok` carries what the
    /// execution history should record; `Err` feeds the dispatcher's retry
    /// machinery.
    fn interpret<'a>(
        &'a self,
        storage: &'a AsyncStorage,
        window: RunWindow,
        outcome: TaskOutcome,
    ) -> BoxFuture<'a, Result<Interpretation, TaskFailure>>;

    /// Called after a run's *final* attempt failed and was recorded.
    fn on_terminal<'a>(&'a self, storage: &'a AsyncStorage, error: String) -> BoxFuture<'a, ()>;

    /// Wake the seeder (called by the manager on every run completion).
    fn wake(&self);

    /// Resolves when the seeder should re-evaluate ahead of its ticker;
    /// triggers without wake-ups never resolve.
    fn wake_notified<'a>(&'a self) -> BoxFuture<'a, ()>;
}

/// Build the trigger runtime for one registration.
pub(crate) fn build(kind: &'static str, config: TriggerConfig) -> Arc<dyn Trigger> {
    match config {
        TriggerConfig::Interval {
            period,
            jitter,
            tracking,
        } => Arc::new(interval::IntervalTrigger::new(
            kind, period, jitter, tracking,
        )),
        TriggerConfig::Recurring { schedule, retry } => {
            Arc::new(recurring::RecurringTrigger::new(kind, schedule, retry))
        }
        TriggerConfig::Sync {
            source,
            schedule,
            retry,
            cooldown,
            max_backfill_days,
        } => Arc::new(sync::SyncTrigger::new(
            kind,
            source,
            schedule,
            retry,
            cooldown,
            max_backfill_days,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;
    use valqeron_core::{MarketCalendar, Recurrence};

    #[test]
    fn descriptors_render_per_tier() {
        let interval = TriggerConfig::Interval {
            period: Duration::from_secs(3600),
            jitter: true,
            tracking: Tracking::Durable,
        };
        assert_eq!(interval.descriptor(), "interval:3600s±10%");
        assert_eq!(interval.trigger_kind(), TaskTrigger::Interval);
        assert_eq!(interval.tracking(), TaskTracking::Durable);
        assert_eq!(interval.source(), None);

        let plain = TriggerConfig::Interval {
            period: Duration::from_secs(300),
            jitter: false,
            tracking: Tracking::Ephemeral,
        };
        assert_eq!(plain.descriptor(), "interval:300s");
        assert_eq!(plain.tracking(), TaskTracking::Ephemeral);

        let schedule = Schedule::new(
            MarketCalendar::UTC,
            NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
            Recurrence::Daily,
        );
        let recurring = TriggerConfig::Recurring {
            schedule,
            retry: RetryPolicy::none(),
        };
        assert_eq!(recurring.descriptor(), "recurring:daily@03:00+00:00");
        assert_eq!(recurring.tracking(), TaskTracking::Durable);

        let sync = TriggerConfig::Sync {
            source: SyncSource::new("cvm").unwrap(),
            schedule: Schedule::new(
                MarketCalendar::B3,
                NaiveTime::from_hms_opt(7, 0, 0).unwrap(),
                Recurrence::Daily,
            ),
            retry: RetryPolicy::none(),
            cooldown: CooldownPolicy::new(300),
            max_backfill_days: 90,
        };
        assert_eq!(sync.descriptor(), "sync:daily@07:00-03:00");
        assert_eq!(sync.trigger_kind(), TaskTrigger::Sync);
        assert_eq!(
            sync.source().map(|s| s.as_str().to_owned()),
            Some("cvm".into())
        );
    }
}
