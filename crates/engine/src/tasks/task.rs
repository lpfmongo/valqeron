//! The task vocabulary: what a task *is* ([`TaskDefinition`] + one typed
//! builder per trigger kind) and how it *runs* (the [`TaskHandler`]
//! contract).
//!
//! Builders carry the framework's defaults (an interval task is durable
//! with jitter; a sync source retries 3×300s with a 300s cooldown base and
//! a 90-day backfill cap) so a job states only what makes it different.
//! Every builder has two terminals: [`run`](IntervalTaskBuilder::run) binds
//! a handler and yields the definition, and
//! [`disabled`](IntervalTaskBuilder::disabled) yields the catalog
//! declaration for a task configured off this boot — same source of truth,
//! no hand-rolled declarations.

use std::sync::Arc;
use std::time::Duration;

use valqeron_core::{
    CooldownPolicy, LogPolicy, Schedule, SyncSource, TaskCategory, TaskDeclaration, TaskKind,
};

use crate::tasks::trigger::{
    self, BoxFuture, RetryPolicy, TaskContext, TaskOutcome, Tracking, Trigger, TriggerConfig,
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

    /// Split into the boot declaration and the runtime registration.
    /// `None` when the kind fails validation — the caller logs and skips.
    pub(crate) fn into_parts(self) -> Option<(TaskDeclaration, Registration)> {
        let kind = TaskKind::new(self.kind).ok()?;
        let declaration = TaskDeclaration {
            kind,
            category: self.category,
            trigger: self.trigger.trigger_kind(),
            tracking: self.trigger.tracking(),
            schedule: self.trigger.descriptor(),
            source: self.trigger.source(),
            log_policy: self.log_policy,
            config_enabled: true,
        };
        let registration = Registration {
            category: self.category,
            log_policy: self.log_policy,
            handler: self.handler,
            trigger: trigger::build(self.kind, self.trigger),
        };
        Some((declaration, registration))
    }
}

/// The runtime form of a definition: what the runner holds per kind.
pub(crate) struct Registration {
    pub(crate) category: TaskCategory,
    pub(crate) log_policy: LogPolicy,
    pub(crate) handler: Arc<dyn TaskHandler>,
    pub(crate) trigger: Arc<dyn Trigger>,
}

/// The declaration of a task configured off this boot: cataloged as
/// `disabled` instead of silently absent. `None` when the kind is invalid.
fn disabled_declaration(
    kind: &'static str,
    category: TaskCategory,
    log_policy: LogPolicy,
    config: &TriggerConfig,
) -> Option<TaskDeclaration> {
    let Ok(kind) = TaskKind::new(kind) else {
        tracing::error!(kind, "invalid task kind; cannot declare it disabled");
        return None;
    };
    Some(TaskDeclaration {
        kind,
        category,
        trigger: config.trigger_kind(),
        tracking: config.tracking(),
        schedule: config.descriptor(),
        source: config.source(),
        log_policy,
        config_enabled: false,
    })
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
}

impl IntervalTaskBuilder {
    pub fn category(mut self, category: TaskCategory) -> Self {
        self.category = category;
        self
    }

    /// Run inline without persisting rows (liveness work): no history, no
    /// retries, `paused` ignored — and precise cadence (jitter off) with
    /// quiet logging (`FailuresOnly`) by default.
    pub fn ephemeral(mut self) -> Self {
        self.tracking = Tracking::Ephemeral;
        self.jitter = false;
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

    pub fn run(self, handler: impl TaskHandler) -> TaskDefinition {
        TaskDefinition {
            kind: self.kind,
            category: self.category,
            log_policy: self.resolved_log_policy(),
            trigger: self.config(),
            handler: Arc::new(handler),
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "terminal parity across builders; interval tasks are never env-disabled today"
        )
    )]
    pub fn disabled(self) -> Option<TaskDeclaration> {
        disabled_declaration(
            self.kind,
            self.category,
            self.resolved_log_policy(),
            &self.config(),
        )
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
            trigger: self.config(),
            handler: Arc::new(handler),
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "terminal parity across builders; recurring tasks are never env-disabled today"
        )
    )]
    pub fn disabled(self) -> Option<TaskDeclaration> {
        disabled_declaration(self.kind, self.category, self.log_policy, &self.config())
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
            trigger: self.config(),
            handler: Arc::new(handler),
        }
    }

    pub fn disabled(self) -> Option<TaskDeclaration> {
        disabled_declaration(self.kind, self.category, self.log_policy, &self.config())
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

        let overridden = TaskDefinition::recurring("t", utc_daily())
            .retry(RetryPolicy {
                max_attempts: 2,
                retry_delay_secs: 600,
            })
            .log_failures_only()
            .run(|_ctx| async { TaskOutcome::Done });
        assert_eq!(overridden.log_policy, LogPolicy::FailuresOnly);

        let declaration = TaskDefinition::recurring("t", utc_daily())
            .disabled()
            .expect("valid kind");
        assert_eq!(declaration.trigger, TaskTrigger::Recurring);
        assert!(!declaration.config_enabled);
    }

    #[test]
    fn sync_defaults_and_disabled_declaration_share_one_source_of_truth() {
        let source = SyncSource::new("cvm").unwrap();
        let declaration = TaskDefinition::sync("cvm_daily_sync", source.clone(), schedule())
            .disabled()
            .expect("valid kind");
        assert_eq!(declaration.kind.as_str(), "cvm_daily_sync");
        assert_eq!(declaration.category, TaskCategory::FinanceDataSync);
        assert_eq!(declaration.trigger, TaskTrigger::Sync);
        assert_eq!(declaration.tracking, TaskTracking::Durable);
        assert_eq!(declaration.schedule, "sync:daily@07:00-03:00");
        assert_eq!(
            declaration.source.map(|s| s.as_str().to_owned()),
            Some("cvm".into())
        );
        assert!(!declaration.config_enabled);

        // The run terminal derives the identical shape, enabled.
        let definition = TaskDefinition::sync("cvm_daily_sync", source, schedule())
            .category(TaskCategory::Other)
            .cooldown_secs(600)
            .max_backfill_days(30)
            .run(|_ctx| async { TaskOutcome::Done });
        let (declared, registration) = definition.into_parts().expect("valid kind");
        assert!(declared.config_enabled);
        assert_eq!(declared.schedule, "sync:daily@07:00-03:00");
        assert_eq!(declared.category, TaskCategory::Other, "category override");
        assert_eq!(registration.category, TaskCategory::Other);
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
        assert!(definition.into_parts().is_some());
    }

    #[test]
    fn invalid_kinds_yield_no_parts_and_no_declaration() {
        let definition = TaskDefinition::interval("", Duration::from_secs(1))
            .run(|_ctx| async { TaskOutcome::Done });
        assert!(definition.into_parts().is_none());
        assert!(
            TaskDefinition::interval("", Duration::from_secs(1))
                .disabled()
                .is_none()
        );
    }
}
