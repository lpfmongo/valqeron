//! The task vocabulary: what a task *is* ([`TaskDefinition`] + one typed
//! builder per trigger kind) and how it *runs* (the [`TaskHandler`]
//! contract).
//!
//! Builders carry the framework's defaults (an interval task is durable
//! with jitter; a sync source retries 3×300s with a 300s cooldown base and
//! a 90-day backfill cap) so a task states only what makes it different.
//! The `run` terminal binds a handler and yields the definition; the
//! definition's [`TaskSettings`] are the code defaults the boot reconcile
//! seeds into the registry — the DB values (operator-editable) are what
//! actually schedule the task.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use valqeron_core::{
    CooldownPolicy, LogPolicy, Schedule, SyncSource, TaskCategory, TaskKind, TaskSettings,
};

use crate::scheduler::trigger::{
    BoxFuture, RetryPolicy, TaskContext, TaskOutcome, Tracking, Trigger, TriggerConfig,
};

/// Sync defaults, shared by every source unless overridden: transient
/// faults get three attempts five minutes apart inside one run; terminal
/// failures start a 300s-base cooldown; unattended catch-up is capped at 90
/// business days.
const DEFAULT_SYNC_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    retry_delay_secs: 300,
};
const DEFAULT_SYNC_COOLDOWN_SECS: u32 = 300;
const DEFAULT_SYNC_MAX_BACKFILL_DAYS: u32 = 90;

// ================ HANDLER CONTRACT ================
/// The behavior contract: one run of one task. Object-safe so the runner
/// stores `Arc<dyn TaskHandler>`; stateless tasks stay closures (blanket
/// impl below), stateful ones (an ingestion client, a parser) implement it
/// on a struct.
pub(crate) trait TaskHandler: Send + Sync + 'static {
    fn run<'a>(&'a self, ctx: TaskContext) -> BoxFuture<'a, TaskOutcome>;
}

/// Every `Fn(TaskContext) -> Future<TaskOutcome>` closure is already a
/// handler — zero ceremony for trivial tasks.
impl<F, Fut> TaskHandler for F
where
    F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = TaskOutcome> + Send + 'static,
{
    fn run<'a>(&'a self, ctx: TaskContext) -> BoxFuture<'a, TaskOutcome> {
        Box::pin(self(ctx))
    }
}

// ================ DEFINITION ================
/// One fully specified task: everything the manager may know about it.
pub(crate) struct TaskDefinition {
    pub(crate) kind: &'static str,
    pub(crate) category: TaskCategory,
    pub(crate) trigger: TriggerConfig,
    pub(crate) log_policy: LogPolicy,
    /// Code-default settings seeded into the registry on first declare;
    /// `None` fields are code-owned forever (never captured in the DB).
    pub(crate) settings: TaskSettings,
    pub(crate) handler: Arc<dyn TaskHandler>,
}

impl TaskDefinition {
    /// Monotonic ticks since boot — liveness and housekeeping work.
    /// Defaults: durable, ±10% jitter, `LogPolicy::All`.
    pub fn interval(kind: &'static str, period: Duration) -> IntervalTaskBuilder {
        IntervalTaskBuilder {
            kind,
            category: TaskCategory::Other,
            period,
            jitter: true,
            tracking: Tracking::Durable,
            log_policy: None,
            pinned: false,
        }
    }

    /// Wall-clock business-day occurrences, always durable.
    /// Defaults: no retries (the next occurrence is the retry),
    /// `LogPolicy::All`.
    pub fn recurring(kind: &'static str, schedule: Schedule) -> RecurringTaskBuilder {
        RecurringTaskBuilder {
            kind,
            category: TaskCategory::Other,
            schedule,
            retry: RetryPolicy::none(),
            log_policy: LogPolicy::All,
        }
    }

    /// Cursor-driven recurrence with sequential catch-up — data ingestion.
    /// Defaults: retry 3×300s, 300s cooldown base, 90-day backfill cap,
    /// `LogPolicy::All`.
    pub fn sync(kind: &'static str, source: SyncSource, schedule: Schedule) -> SyncTaskBuilder {
        SyncTaskBuilder {
            kind,
            category: TaskCategory::FinanceDataSync,
            source,
            schedule,
            retry: DEFAULT_SYNC_RETRY,
            cooldown_secs: DEFAULT_SYNC_COOLDOWN_SECS,
            max_backfill_days: DEFAULT_SYNC_MAX_BACKFILL_DAYS,
            log_policy: LogPolicy::All,
        }
    }

    /// Validate the kind and split into the reconcile input. `None` when
    /// the kind fails validation — the caller logs and skips.
    pub(crate) fn validate(self) -> Option<PendingTask> {
        let kind = TaskKind::new(self.kind).ok()?;
        Some(PendingTask {
            kind,
            kind_str: self.kind,
            category: self.category,
            log_policy: self.log_policy,
            defaults: self.settings,
            config: self.trigger,
            enabled: true,
            handler: self.handler,
        })
    }
}

/// A validated definition on its way through the boot reconcile: the code
/// config plus the declaration facts. The reconcile folds the registry's
/// stored settings into `config` and captures the row's `enabled` flag
/// before the trigger is built.
#[derive(Clone)]
pub(crate) struct PendingTask {
    pub(crate) kind: TaskKind,
    pub(crate) kind_str: &'static str,
    pub(crate) category: TaskCategory,
    pub(crate) log_policy: LogPolicy,
    /// Code-default settings (the declaration payload).
    pub(crate) defaults: TaskSettings,
    /// The trigger config; code values until the reconcile merges the
    /// stored settings in.
    pub(crate) config: TriggerConfig,
    /// The row's committed `enabled` flag (true until the reconcile reads
    /// otherwise).
    pub(crate) enabled: bool,
    pub(crate) handler: Arc<dyn TaskHandler>,
}

/// The runtime form of a definition: what the runner holds per kind.
pub(crate) struct Registration {
    pub(crate) category: TaskCategory,
    pub(crate) log_policy: LogPolicy,
    /// In-memory image of the registry's committed `enabled` flag: read by
    /// every durable seed pass (no DB round trip), published to only after
    /// a successful registry write. Ephemeral runs ignore it (liveness).
    pub(crate) enabled: AtomicBool,
    pub(crate) handler: Arc<dyn TaskHandler>,
    pub(crate) trigger: Arc<dyn Trigger>,
}

// ================ INTERVAL BUILDER ================
pub(crate) struct IntervalTaskBuilder {
    kind: &'static str,
    category: TaskCategory,
    period: Duration,
    jitter: bool,
    tracking: Tracking,
    /// Resolved at the terminal: `All` for durable, `FailuresOnly` for
    /// ephemeral, unless set explicitly.
    log_policy: Option<LogPolicy>,
    /// When set, the period is code-owned: the registry never captures it
    /// and operators cannot tune it.
    pinned: bool,
}

impl IntervalTaskBuilder {
    pub fn category(mut self, category: TaskCategory) -> Self {
        self.category = category;
        self
    }

    /// Run inline without persisting rows (liveness work): no history, no
    /// retries, the `enabled` gate ignored — and precise cadence (jitter
    /// off) with quiet logging (`FailuresOnly`) by default.
    pub fn ephemeral(mut self) -> Self {
        self.tracking = Tracking::Ephemeral;
        self.jitter = false;
        self
    }

    /// Keep the period code-owned: it is computed at boot (e.g. from the
    /// systemd watchdog contract) and must not be operator-tunable, so the
    /// registry's `period_secs` stays NULL forever.
    pub fn pinned(mut self) -> Self {
        self.pinned = true;
        self
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "override surface; production interval tasks keep the jitter default"
        )
    )]
    pub fn no_jitter(mut self) -> Self {
        self.jitter = false;
        self
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "override surface; loud ephemeral tasks are a test-only need today"
        )
    )]
    pub fn log_all(mut self) -> Self {
        self.log_policy = Some(LogPolicy::All);
        self
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "override surface; quiet durable intervals have no producer yet"
        )
    )]
    pub fn log_failures_only(mut self) -> Self {
        self.log_policy = Some(LogPolicy::FailuresOnly);
        self
    }

    fn resolved_log_policy(&self) -> LogPolicy {
        self.log_policy.unwrap_or(match self.tracking {
            Tracking::Durable => LogPolicy::All,
            Tracking::Ephemeral => LogPolicy::FailuresOnly,
        })
    }

    fn config(&self) -> TriggerConfig {
        TriggerConfig::Interval {
            period: self.period,
            jitter: self.jitter,
            tracking: self.tracking,
        }
    }

    /// The code-default settings: the period, unless pinned. Periods that
    /// do not fit a positive whole-second column (sub-second test cadences,
    /// absurdly large values) stay code-owned.
    fn settings(&self) -> TaskSettings {
        TaskSettings {
            period_secs: (!self.pinned)
                .then(|| u32::try_from(self.period.as_secs()).ok())
                .flatten()
                .filter(|secs| *secs > 0),
            ..TaskSettings::default()
        }
    }

    pub fn run(self, handler: impl TaskHandler) -> TaskDefinition {
        TaskDefinition {
            kind: self.kind,
            category: self.category,
            log_policy: self.resolved_log_policy(),
            settings: self.settings(),
            trigger: self.config(),
            handler: Arc::new(handler),
        }
    }
}

// ================ RECURRING BUILDER ================
pub(crate) struct RecurringTaskBuilder {
    kind: &'static str,
    category: TaskCategory,
    schedule: Schedule,
    retry: RetryPolicy,
    log_policy: LogPolicy,
}

impl RecurringTaskBuilder {
    pub fn category(mut self, category: TaskCategory) -> Self {
        self.category = category;
        self
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "override surface; the built-in recurring task uses the no-retry default"
        )
    )]
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "override surface; failures of recurring work are always worth a line today"
        )
    )]
    pub fn log_failures_only(mut self) -> Self {
        self.log_policy = LogPolicy::FailuresOnly;
        self
    }

    fn config(&self) -> TriggerConfig {
        TriggerConfig::Recurring {
            schedule: self.schedule,
            retry: self.retry,
        }
    }

    pub fn run(self, handler: impl TaskHandler) -> TaskDefinition {
        TaskDefinition {
            kind: self.kind,
            category: self.category,
            log_policy: self.log_policy,
            settings: TaskSettings {
                at_local: Some(self.schedule.at()),
                recurrence: Some(self.schedule.recurrence()),
                ..TaskSettings::default()
            },
            trigger: self.config(),
            handler: Arc::new(handler),
        }
    }
}

// ================ SYNC BUILDER ================
pub(crate) struct SyncTaskBuilder {
    kind: &'static str,
    category: TaskCategory,
    source: SyncSource,
    schedule: Schedule,
    retry: RetryPolicy,
    cooldown_secs: u32,
    max_backfill_days: u32,
    log_policy: LogPolicy,
}

impl SyncTaskBuilder {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "override surface; sync sources default to FinanceDataSync"
        )
    )]
    pub fn category(mut self, category: TaskCategory) -> Self {
        self.category = category;
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn cooldown_secs(mut self, base_secs: u32) -> Self {
        self.cooldown_secs = base_secs;
        self
    }

    pub fn max_backfill_days(mut self, days: u32) -> Self {
        self.max_backfill_days = days;
        self
    }

    fn config(&self) -> TriggerConfig {
        TriggerConfig::Sync {
            source: self.source.clone(),
            schedule: self.schedule,
            retry: self.retry,
            cooldown: CooldownPolicy::new(self.cooldown_secs),
            max_backfill_days: self.max_backfill_days,
        }
    }

    pub fn run(self, handler: impl TaskHandler) -> TaskDefinition {
        TaskDefinition {
            kind: self.kind,
            category: self.category,
            log_policy: self.log_policy,
            settings: TaskSettings {
                at_local: Some(self.schedule.at()),
                recurrence: Some(self.schedule.recurrence()),
                cooldown_secs: Some(self.cooldown_secs),
                max_backfill_days: Some(self.max_backfill_days),
                ..TaskSettings::default()
            },
            trigger: self.config(),
            handler: Arc::new(handler),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;
    use valqeron_core::{MarketCalendar, Recurrence, TaskTracking, TaskTrigger};

    fn schedule() -> Schedule {
        Schedule::new(
            MarketCalendar::B3,
            NaiveTime::from_hms_opt(7, 0, 0).unwrap(),
            Recurrence::Daily,
        )
    }

    #[test]
    fn interval_defaults_are_durable_with_jitter_and_full_logging() {
        let definition = TaskDefinition::interval("t", Duration::from_secs(3600))
            .category(TaskCategory::EngineSystem)
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(definition.category, TaskCategory::EngineSystem);
        assert_eq!(definition.log_policy, LogPolicy::All);
        assert_eq!(definition.trigger.trigger_kind(), TaskTrigger::Interval);
        assert_eq!(definition.trigger.tracking(), TaskTracking::Durable);
        assert_eq!(definition.trigger.descriptor(), "interval:3600s±10%");
    }

    #[test]
    fn ephemeral_flips_jitter_off_and_quiets_the_log() {
        let definition = TaskDefinition::interval("t", Duration::from_secs(300))
            .ephemeral()
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(definition.log_policy, LogPolicy::FailuresOnly);
        assert_eq!(definition.trigger.tracking(), TaskTracking::Ephemeral);
        assert_eq!(definition.trigger.descriptor(), "interval:300s");

        // Explicit overrides still win, in both directions.
        let loud = TaskDefinition::interval("t", Duration::from_secs(300))
            .ephemeral()
            .log_all()
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(loud.log_policy, LogPolicy::All);

        let quiet = TaskDefinition::interval("t", Duration::from_secs(300))
            .no_jitter()
            .log_failures_only()
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(quiet.log_policy, LogPolicy::FailuresOnly);
        assert_eq!(quiet.trigger.descriptor(), "interval:300s");
        assert_eq!(quiet.trigger.tracking(), TaskTracking::Durable);
    }

    #[test]
    fn recurring_defaults_to_a_single_attempt_with_overrides_available() {
        let utc_daily = || {
            Schedule::new(
                MarketCalendar::UTC,
                NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
                Recurrence::Daily,
            )
        };
        let definition =
            TaskDefinition::recurring("t", utc_daily()).run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(definition.trigger.trigger_kind(), TaskTrigger::Recurring);
        assert_eq!(
            definition.trigger.descriptor(),
            "recurring:daily@03:00+00:00"
        );
        assert_eq!(
            definition.settings.at_local,
            NaiveTime::from_hms_opt(3, 0, 0),
            "recurring settings capture the schedule"
        );
        assert_eq!(definition.settings.recurrence, Some(Recurrence::Daily));
        assert_eq!(definition.settings.period_secs, None);

        let overridden = TaskDefinition::recurring("t", utc_daily())
            .retry(RetryPolicy {
                max_attempts: 2,
                retry_delay_secs: 600,
            })
            .log_failures_only()
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(overridden.log_policy, LogPolicy::FailuresOnly);
    }

    #[test]
    fn sync_definition_captures_defaults_as_settings() {
        let source = SyncSource::new("cvm").unwrap();
        let definition = TaskDefinition::sync("cvm_daily_sync", source, schedule())
            .category(TaskCategory::Other)
            .cooldown_secs(600)
            .max_backfill_days(30)
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(definition.trigger.descriptor(), "sync:daily@07:00-03:00");
        assert_eq!(definition.settings.cooldown_secs, Some(600));
        assert_eq!(definition.settings.max_backfill_days, Some(30));
        assert_eq!(definition.settings.recurrence, Some(Recurrence::Daily));

        let pending = definition.validate().expect("valid kind");
        assert_eq!(pending.kind.as_str(), "cvm_daily_sync");
        assert_eq!(pending.category, TaskCategory::Other, "category override");
        assert_eq!(
            pending.config.source().map(|s| s.as_str().to_owned()),
            Some("cvm".into())
        );
    }

    #[test]
    fn interval_settings_capture_the_period_unless_pinned() {
        let tunable = TaskDefinition::interval("t", Duration::from_secs(3600))
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(tunable.settings.period_secs, Some(3600));

        let pinned = TaskDefinition::interval("t", Duration::from_secs(3600))
            .ephemeral()
            .pinned()
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(
            pinned.settings.period_secs, None,
            "pinned periods stay code-owned"
        );
    }

    #[test]
    fn a_struct_implements_the_handler_contract() {
        struct Probe;
        impl TaskHandler for Probe {
            fn run<'a>(&'a self, _ctx: TaskContext) -> BoxFuture<'a, TaskOutcome> {
                Box::pin(async { TaskOutcome::Done })
            }
        }
        let definition = TaskDefinition::interval("t", Duration::from_secs(1)).run(Probe);
        assert!(definition.validate().is_some());
    }

    #[test]
    fn invalid_kinds_yield_no_pending_task() {
        let definition = TaskDefinition::interval("", Duration::from_secs(1))
            .run(|_ctx| async { TaskOutcome::Done });
        assert!(definition.validate().is_none());
    }
}
