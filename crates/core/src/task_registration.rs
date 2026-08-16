//! The task catalog: one persisted row per registered background task kind.
//!
//! Code is the only source of *executable* work (handlers cannot live in a
//! database); the catalog is the durable reflection of those registrations
//! plus the operational state layered on top:
//!
//! - **declaration** — kind, category, execution tier, schedule descriptor
//!   (upserted from code at every boot; a kind that disappears from code is
//!   retired, never deleted),
//! - **intent** — `paused` (operator, persisted) and `config_enabled`
//!   (environment verdict, rewritten each boot),
//! - **run summary** — last run, outcome, totals. Run *history* rows are
//!   pruned after days; the summary on the registration is the prune-proof
//!   memory.
//!
//! Status is never stored: [`derive_status`] computes it from the
//! registration, the newest queue row, and the sync cursor, so it cannot go
//! stale.

use chrono::{DateTime, Utc};
use std::str::FromStr;

use crate::sync::SyncCursor;
use crate::sync::SyncSource;
use crate::task::{BackgroundTask, TaskKind, TaskStatus};
use crate::task_registration::error::{
    LogPolicyError, RunOutcomeError, TaskCategoryError, TaskTierError, TaskTrackingError,
};

pub mod error;
pub mod repository;
pub mod service;

// ================ CATEGORY ================
/// Classification of a task for grouping, log filtering, and defaults —
/// deliberately a closed set so listings and filters stay reliable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskCategory {
    /// Engine housekeeping: database maintenance, liveness, pruning.
    EngineSystem,
    /// Financial-instrument data ingestion (CVM, ANBIMA, B3, …).
    FinanceDataSync,
    /// Everything else (reports, exports, custom jobs).
    Other,
}

impl TaskCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskCategory::EngineSystem => "ENGINE_SYSTEM",
            TaskCategory::FinanceDataSync => "FINANCE_DATA_SYNC",
            TaskCategory::Other => "OTHER",
        }
    }
}

impl FromStr for TaskCategory {
    type Err = TaskCategoryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ENGINE_SYSTEM" => Ok(TaskCategory::EngineSystem),
            "FINANCE_DATA_SYNC" => Ok(TaskCategory::FinanceDataSync),
            "OTHER" => Ok(TaskCategory::Other),
            _ => Err(TaskCategoryError::InvalidCategory),
        }
    }
}

impl From<TaskCategory> for String {
    fn from(val: TaskCategory) -> Self {
        val.as_str().into()
    }
}

// ================ TIER ================
/// Which execution plane schedules the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskTier {
    /// Monotonic interval since boot.
    Interval,
    /// Wall-clock business-day recurrence.
    Recurring,
    /// Cursor-driven recurrence with sequential catch-up.
    Sync,
}

impl TaskTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskTier::Interval => "INTERVAL",
            TaskTier::Recurring => "RECURRING",
            TaskTier::Sync => "SYNC",
        }
    }
}

impl FromStr for TaskTier {
    type Err = TaskTierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INTERVAL" => Ok(TaskTier::Interval),
            "RECURRING" => Ok(TaskTier::Recurring),
            "SYNC" => Ok(TaskTier::Sync),
            _ => Err(TaskTierError::InvalidTier),
        }
    }
}

impl From<TaskTier> for String {
    fn from(val: TaskTier) -> Self {
        val.as_str().into()
    }
}

// ================ TRACKING ================
/// Whether the task's runs persist as `background_task` rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskTracking {
    Durable,
    Ephemeral,
}

impl TaskTracking {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskTracking::Durable => "DURABLE",
            TaskTracking::Ephemeral => "EPHEMERAL",
        }
    }
}

impl FromStr for TaskTracking {
    type Err = TaskTrackingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "DURABLE" => Ok(TaskTracking::Durable),
            "EPHEMERAL" => Ok(TaskTracking::Ephemeral),
            _ => Err(TaskTrackingError::InvalidTracking),
        }
    }
}

impl From<TaskTracking> for String {
    fn from(val: TaskTracking) -> Self {
        val.as_str().into()
    }
}

// ================ LOG POLICY ================
/// How chatty the task manager is about this task's runs. Governs the
/// manager's per-run lines only — handler-internal logging is the task's
/// own business, and failures are always logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogPolicy {
    /// Log every run.
    All,
    /// Log failed runs only (chatty liveness work).
    FailuresOnly,
}

impl LogPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogPolicy::All => "ALL",
            LogPolicy::FailuresOnly => "FAILURES_ONLY",
        }
    }
}

impl FromStr for LogPolicy {
    type Err = LogPolicyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ALL" => Ok(LogPolicy::All),
            "FAILURES_ONLY" => Ok(LogPolicy::FailuresOnly),
            _ => Err(LogPolicyError::InvalidPolicy),
        }
    }
}

impl From<LogPolicy> for String {
    fn from(val: LogPolicy) -> Self {
        val.as_str().into()
    }
}

// ================ RUN OUTCOME ================
/// Task-level outcome recorded on the registration's run summary. Sync
/// detail (`NOT_READY`, cooldowns) lives on the sync cursor, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunOutcome {
    Succeeded,
    Failed,
}

impl RunOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunOutcome::Succeeded => "SUCCEEDED",
            RunOutcome::Failed => "FAILED",
        }
    }
}

impl FromStr for RunOutcome {
    type Err = RunOutcomeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SUCCEEDED" => Ok(RunOutcome::Succeeded),
            "FAILED" => Ok(RunOutcome::Failed),
            _ => Err(RunOutcomeError::InvalidOutcome),
        }
    }
}

impl From<RunOutcome> for String {
    fn from(val: RunOutcome) -> Self {
        val.as_str().into()
    }
}

// ================ DECLARATION ================
/// What code declares about a task — the upsert payload of the boot
/// reconcile. Everything else on the registration (intent, run summary) is
/// operational state the reconcile must preserve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDeclaration {
    pub kind: TaskKind,
    pub category: TaskCategory,
    pub tier: TaskTier,
    pub tracking: TaskTracking,
    /// Canonical schedule descriptor, display-only
    /// (e.g. `sync:daily@07:00-03:00`, `interval:3600s±10%`).
    pub schedule: String,
    /// Sync tier: the cursor key.
    pub source: Option<SyncSource>,
    pub log_policy: LogPolicy,
    /// Environment verdict for this boot; `false` = configured off.
    pub config_enabled: bool,
}

// ================ THE REGISTRATION ================
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRegistration {
    kind: TaskKind,
    category: TaskCategory,
    tier: TaskTier,
    tracking: TaskTracking,
    schedule: String,
    source: Option<SyncSource>,
    log_policy: LogPolicy,
    config_enabled: bool,
    paused: bool,
    registered: bool,
    last_run_at: Option<DateTime<Utc>>,
    last_outcome: Option<RunOutcome>,
    last_error: Option<String>,
    total_runs: u64,
    total_failures: u64,
    first_registered_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Plain-field mirror of [`TaskRegistration`] for persistence round-trips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRegistrationSnapshot {
    pub kind: TaskKind,
    pub category: TaskCategory,
    pub tier: TaskTier,
    pub tracking: TaskTracking,
    pub schedule: String,
    pub source: Option<SyncSource>,
    pub log_policy: LogPolicy,
    pub config_enabled: bool,
    pub paused: bool,
    pub registered: bool,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_outcome: Option<RunOutcome>,
    pub last_error: Option<String>,
    pub total_runs: u64,
    pub total_failures: u64,
    pub first_registered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TaskRegistration {
    /// A fresh registration as its first declaration creates it: clean
    /// intent and an empty run summary.
    pub fn declared(declaration: TaskDeclaration, now: DateTime<Utc>) -> Self {
        Self {
            kind: declaration.kind,
            category: declaration.category,
            tier: declaration.tier,
            tracking: declaration.tracking,
            schedule: declaration.schedule,
            source: declaration.source,
            log_policy: declaration.log_policy,
            config_enabled: declaration.config_enabled,
            paused: false,
            registered: true,
            last_run_at: None,
            last_outcome: None,
            last_error: None,
            total_runs: 0,
            total_failures: 0,
            first_registered_at: now,
            updated_at: now,
        }
    }

    pub fn kind(&self) -> &TaskKind {
        &self.kind
    }
    pub fn category(&self) -> TaskCategory {
        self.category
    }
    pub fn tier(&self) -> TaskTier {
        self.tier
    }
    pub fn tracking(&self) -> TaskTracking {
        self.tracking
    }
    pub fn schedule(&self) -> &str {
        &self.schedule
    }
    pub fn source(&self) -> Option<&SyncSource> {
        self.source.as_ref()
    }
    pub fn log_policy(&self) -> LogPolicy {
        self.log_policy
    }
    pub fn config_enabled(&self) -> bool {
        self.config_enabled
    }
    pub fn paused(&self) -> bool {
        self.paused
    }
    pub fn registered(&self) -> bool {
        self.registered
    }
    pub fn last_run_at(&self) -> Option<DateTime<Utc>> {
        self.last_run_at
    }
    pub fn last_outcome(&self) -> Option<RunOutcome> {
        self.last_outcome
    }
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
    pub fn total_runs(&self) -> u64 {
        self.total_runs
    }
    pub fn total_failures(&self) -> u64 {
        self.total_failures
    }
    pub fn first_registered_at(&self) -> DateTime<Utc> {
        self.first_registered_at
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn reconstitute(snapshot: TaskRegistrationSnapshot) -> Self {
        Self {
            kind: snapshot.kind,
            category: snapshot.category,
            tier: snapshot.tier,
            tracking: snapshot.tracking,
            schedule: snapshot.schedule,
            source: snapshot.source,
            log_policy: snapshot.log_policy,
            config_enabled: snapshot.config_enabled,
            paused: snapshot.paused,
            registered: snapshot.registered,
            last_run_at: snapshot.last_run_at,
            last_outcome: snapshot.last_outcome,
            last_error: snapshot.last_error,
            total_runs: snapshot.total_runs,
            total_failures: snapshot.total_failures,
            first_registered_at: snapshot.first_registered_at,
            updated_at: snapshot.updated_at,
        }
    }
}

// ================ DERIVED STATUS ================
/// The task's effective status — always computed, never stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DerivedTaskStatus {
    /// The kind is no longer registered in code.
    Retired,
    /// Configured off via the environment for this boot.
    Disabled,
    /// Paused by an operator; seeding is stopped.
    Paused,
    /// A run is executing right now.
    Running,
    /// Sync: consecutive terminal failures reached the halt threshold.
    Halted,
    /// Sync: waiting out a failure/not-ready cooldown.
    CoolingDown,
    /// Sync: a past-due run is queued — working through missed periods.
    CatchingUp,
    /// A future run is scheduled.
    Waiting,
    /// A run is due and waiting for the dispatcher.
    Due,
    /// Nothing queued (ephemeral tasks; the instant between completion and
    /// the next seed).
    Idle,
}

impl DerivedTaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            DerivedTaskStatus::Retired => "retired",
            DerivedTaskStatus::Disabled => "disabled",
            DerivedTaskStatus::Paused => "paused",
            DerivedTaskStatus::Running => "running",
            DerivedTaskStatus::Halted => "halted",
            DerivedTaskStatus::CoolingDown => "cooling_down",
            DerivedTaskStatus::CatchingUp => "catching_up",
            DerivedTaskStatus::Waiting => "waiting",
            DerivedTaskStatus::Due => "due",
            DerivedTaskStatus::Idle => "idle",
        }
    }
}

/// Derive the effective status of one task; first match wins.
///
/// `active` is the earliest non-terminal (`Pending`/`Running`) queue row of
/// the kind; `cursor` is the sync cursor when the task is a sync source;
/// `halted_after` is the consecutive-failure threshold that flips a failing
/// source from `CoolingDown` to `Halted`.
pub fn derive_status(
    registration: &TaskRegistration,
    active: Option<&BackgroundTask>,
    cursor: Option<&SyncCursor>,
    halted_after: u32,
    now: DateTime<Utc>,
) -> DerivedTaskStatus {
    if !registration.registered() {
        return DerivedTaskStatus::Retired;
    }
    if !registration.config_enabled() {
        return DerivedTaskStatus::Disabled;
    }
    if registration.paused() {
        return DerivedTaskStatus::Paused;
    }
    if active.is_some_and(|task| task.status() == TaskStatus::Running) {
        return DerivedTaskStatus::Running;
    }
    if let Some(cursor) = cursor {
        if halted_after > 0 && cursor.consecutive_failures() >= halted_after {
            return DerivedTaskStatus::Halted;
        }
        if !cursor.is_ready(now) {
            return DerivedTaskStatus::CoolingDown;
        }
        if active.is_some_and(|task| task.scheduled_at() <= now) {
            return DerivedTaskStatus::CatchingUp;
        }
    }
    match active {
        Some(task) if task.scheduled_at() > now => DerivedTaskStatus::Waiting,
        Some(_) => DerivedTaskStatus::Due,
        None => DerivedTaskStatus::Idle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::SyncOutcomeKind;
    use crate::task::BackgroundTaskSnapshot;
    use crate::task::TaskId;
    use chrono::TimeZone;

    fn utc(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0)
            .single()
            .unwrap_or_default()
    }

    fn kind(name: &str) -> Option<TaskKind> {
        TaskKind::new(name).ok()
    }

    fn declaration(name: &str) -> Option<TaskDeclaration> {
        Some(TaskDeclaration {
            kind: kind(name)?,
            category: TaskCategory::FinanceDataSync,
            tier: TaskTier::Sync,
            tracking: TaskTracking::Durable,
            schedule: "sync:daily@07:00-03:00".into(),
            source: crate::sync::SyncSource::new("cvm").ok(),
            log_policy: LogPolicy::All,
            config_enabled: true,
        })
    }

    fn registration(name: &str) -> Option<TaskRegistration> {
        Some(TaskRegistration::declared(
            declaration(name)?,
            utc(2026, 8, 17, 9),
        ))
    }

    fn row(status: TaskStatus, scheduled_at: DateTime<Utc>) -> Option<BackgroundTask> {
        let now = utc(2026, 8, 17, 9);
        Some(BackgroundTask::reconstitute(BackgroundTaskSnapshot {
            id: TaskId::new(),
            kind: kind("t")?,
            status,
            payload: None,
            scheduled_at,
            started_at: None,
            finished_at: None,
            attempts: 0,
            max_attempts: 1,
            retry_delay_secs: 0,
            last_error: None,
            created_at: now,
            updated_at: now,
        }))
    }

    fn cursor(failures: u32, cooldown_until: Option<DateTime<Utc>>) -> Option<SyncCursor> {
        let source = crate::sync::SyncSource::new("cvm").ok()?;
        let now = utc(2026, 8, 17, 9);
        let mut cursor = SyncCursor::seeded(
            source,
            utc(2026, 8, 14, 10),
            chrono::NaiveDate::from_ymd_opt(2026, 8, 13)?,
            now,
        );
        for i in 0..failures {
            let until = cooldown_until.unwrap_or(now);
            cursor = cursor.failed(format!("failure {i}"), until, now);
        }
        if failures == 0
            && let Some(until) = cooldown_until
        {
            cursor = cursor.held_not_ready(
                u32::try_from(until.signed_duration_since(now).num_seconds().max(0)).unwrap_or(0),
                now,
            );
        }
        Some(cursor)
    }

    #[test]
    fn enums_round_trip_through_strings() {
        for (text, category) in [
            ("ENGINE_SYSTEM", TaskCategory::EngineSystem),
            ("finance_data_sync", TaskCategory::FinanceDataSync),
            ("Other", TaskCategory::Other),
        ] {
            let parsed = TaskCategory::from_str(text);
            assert!(matches!(parsed, Ok(p) if p == category), "{text}");
        }
        assert!(TaskCategory::from_str("SYSTEM").is_err());
        assert!(matches!(TaskTier::from_str("sync"), Ok(TaskTier::Sync)));
        assert!(TaskTier::from_str("cron").is_err());
        assert!(matches!(
            TaskTracking::from_str("ephemeral"),
            Ok(TaskTracking::Ephemeral)
        ));
        assert!(matches!(
            LogPolicy::from_str("failures_only"),
            Ok(LogPolicy::FailuresOnly)
        ));
        assert!(matches!(
            RunOutcome::from_str("succeeded"),
            Ok(RunOutcome::Succeeded)
        ));
        let as_string: String = TaskCategory::FinanceDataSync.into();
        assert_eq!(as_string, "FINANCE_DATA_SYNC");
    }

    #[test]
    fn declared_registration_starts_clean() {
        let Some(registration) = registration("cvm_daily_sync") else {
            return;
        };
        assert!(registration.registered());
        assert!(!registration.paused());
        assert!(registration.config_enabled());
        assert_eq!(registration.total_runs(), 0);
        assert_eq!(registration.last_outcome(), None);
        assert_eq!(registration.category(), TaskCategory::FinanceDataSync);
    }

    #[test]
    fn status_precedence_retired_beats_everything() {
        let Some(registration) = registration("t") else {
            return;
        };
        let now = utc(2026, 8, 17, 12);
        let mut snapshot_source = TaskRegistrationSnapshot {
            kind: registration.kind().clone(),
            category: registration.category(),
            tier: registration.tier(),
            tracking: registration.tracking(),
            schedule: registration.schedule().to_owned(),
            source: registration.source().cloned(),
            log_policy: registration.log_policy(),
            config_enabled: false,
            paused: true,
            registered: false,
            last_run_at: None,
            last_outcome: None,
            last_error: None,
            total_runs: 0,
            total_failures: 0,
            first_registered_at: now,
            updated_at: now,
        };
        let retired = TaskRegistration::reconstitute(snapshot_source.clone());
        let running = row(TaskStatus::Running, now);
        assert_eq!(
            derive_status(&retired, running.as_ref(), None, 5, now),
            DerivedTaskStatus::Retired,
            "retired wins over disabled/paused/running"
        );

        snapshot_source.registered = true;
        let disabled = TaskRegistration::reconstitute(snapshot_source.clone());
        assert_eq!(
            derive_status(&disabled, running.as_ref(), None, 5, now),
            DerivedTaskStatus::Disabled,
            "disabled wins over paused/running"
        );

        snapshot_source.config_enabled = true;
        let paused = TaskRegistration::reconstitute(snapshot_source);
        assert_eq!(
            derive_status(&paused, running.as_ref(), None, 5, now),
            DerivedTaskStatus::Paused,
            "paused wins over running"
        );
    }

    #[test]
    fn status_running_beats_sync_states() {
        let Some(registration) = registration("t") else {
            return;
        };
        let now = utc(2026, 8, 17, 12);
        let running = row(TaskStatus::Running, now);
        let halted = cursor(5, Some(utc(2026, 8, 17, 13)));
        assert_eq!(
            derive_status(&registration, running.as_ref(), halted.as_ref(), 5, now),
            DerivedTaskStatus::Running
        );
    }

    #[test]
    fn status_halted_and_cooling_down() {
        let Some(registration) = registration("t") else {
            return;
        };
        let now = utc(2026, 8, 17, 12);
        let halted = cursor(5, Some(utc(2026, 8, 17, 13)));
        assert_eq!(
            derive_status(&registration, None, halted.as_ref(), 5, now),
            DerivedTaskStatus::Halted
        );
        let cooling = cursor(2, Some(utc(2026, 8, 17, 13)));
        assert_eq!(
            derive_status(&registration, None, cooling.as_ref(), 5, now),
            DerivedTaskStatus::CoolingDown,
            "below the halt threshold, an active cooldown is cooling_down"
        );
        let Some(c) = cooling.as_ref() else { return };
        assert_eq!(c.last_outcome(), Some(SyncOutcomeKind::Failed));
    }

    #[test]
    fn status_catching_up_waiting_due_idle() {
        let Some(registration) = registration("t") else {
            return;
        };
        let now = utc(2026, 8, 17, 12);
        let ready_cursor = cursor(0, None);

        // Sync source with a past-due pending row → catching up.
        let past_due = row(TaskStatus::Pending, utc(2026, 8, 17, 11));
        assert_eq!(
            derive_status(
                &registration,
                past_due.as_ref(),
                ready_cursor.as_ref(),
                5,
                now
            ),
            DerivedTaskStatus::CatchingUp
        );

        // Future pending row → waiting (with or without a cursor).
        let future = row(TaskStatus::Pending, utc(2026, 8, 18, 10));
        assert_eq!(
            derive_status(
                &registration,
                future.as_ref(),
                ready_cursor.as_ref(),
                5,
                now
            ),
            DerivedTaskStatus::Waiting
        );
        assert_eq!(
            derive_status(&registration, future.as_ref(), None, 5, now),
            DerivedTaskStatus::Waiting
        );

        // Past-due without a cursor → due (non-sync tiers).
        assert_eq!(
            derive_status(&registration, past_due.as_ref(), None, 5, now),
            DerivedTaskStatus::Due
        );

        // Nothing queued → idle.
        assert_eq!(
            derive_status(&registration, None, None, 5, now),
            DerivedTaskStatus::Idle
        );
    }

    #[test]
    fn not_ready_cooldown_is_cooling_down_without_failures() {
        let Some(registration) = registration("t") else {
            return;
        };
        let now = utc(2026, 8, 17, 12);
        let held = cursor(0, Some(utc(2026, 8, 17, 14)));
        assert_eq!(
            derive_status(&registration, None, held.as_ref(), 5, now),
            DerivedTaskStatus::CoolingDown,
            "a NotReady hold cools down without counting failures"
        );
    }
}
