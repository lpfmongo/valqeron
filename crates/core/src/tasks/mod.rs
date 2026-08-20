//! The background-tasks domain: everything a task *is* in one module.
//!
//! One feature, one vocabulary, four durable aggregates plus their pure
//! decision logic — the engine owns the clocks, the loops, and the I/O:
//!
//! - **queue** ([`BackgroundTask`]) — live work only; a `Pending` row is the
//!   durable alarm, a `Running` row is a claimed run, and a terminal run
//!   *moves* into the execution history;
//! - **history** ([`TaskExecution`]) — one write-once record per terminal
//!   run, pruned after a retention window;
//! - **stats** ([`TaskStats`]) — prune-proof per-kind aggregates, never
//!   deleted;
//! - **catalog** ([`TaskRegistration`]) — one row per registered kind:
//!   declaration (code-owned, rewritten each boot) plus operator intent;
//! - **sync progress** ([`SyncCursor`]) — per-source cursors with cooldown
//!   and halt state, the seam behind sequential catch-up.
//!
//! Status is never stored: [`derive_status`] computes it from the catalog,
//! the queue, and the cursors on every read.

use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use std::str::FromStr;
use uuid::Uuid;

use crate::common::RepositoryResult;
use crate::schedule::Recurrence;
use crate::tasks::error::{
    ExecutionOutcomeError, LogPolicyError, SyncOutcomeKindError, SyncSourceError, TaskBuilderError,
    TaskCategoryError, TaskKindError, TaskStatusError, TaskTrackingError, TaskTriggerError,
};
use crate::tasks::repository::{
    BackgroundTaskRepository, SyncCursorRepository, TaskRegistryRepository, TaskStatRepository,
};

pub mod error;
pub mod repository;

const TASK_KIND_MAX_LEN: usize = 100;

/// Backoff growth is capped so `retry_delay_secs * 2^(attempts-1)` cannot
/// overflow or schedule retries absurdly far out.
const MAX_BACKOFF: Duration = Duration::hours(1);

// ================ TASK ID ================
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn value(&self) -> String {
        self.0.to_string()
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

// ================ TASK KIND ================
/// Registered handler name a task row dispatches to (e.g. `db_maintenance`).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct TaskKind(String);

impl TaskKind {
    pub fn new(value: impl Into<String>) -> Result<Self, TaskKindError> {
        let value = value.into();
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(TaskKindError::Empty);
        }
        if trimmed.chars().count() > TASK_KIND_MAX_LEN {
            return Err(TaskKindError::TooLong {
                max: TASK_KIND_MAX_LEN,
            });
        }

        Ok(Self(trimmed.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ================ TASK STATUS ================
/// Queue rows are live work only — terminal runs leave the queue for the
/// execution history, so there is no terminal status here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TaskStatus {
    #[default]
    Pending,
    Running,
}

impl FromStr for TaskStatus {
    type Err = TaskStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PENDING" => Ok(TaskStatus::Pending),
            "RUNNING" => Ok(TaskStatus::Running),
            _ => Err(TaskStatusError::InvalidStatus),
        }
    }
}

impl From<TaskStatus> for String {
    fn from(val: TaskStatus) -> Self {
        match val {
            TaskStatus::Pending => "PENDING".into(),
            TaskStatus::Running => "RUNNING".into(),
        }
    }
}

// ================ COMPLETION ================
/// How a claimed (`Running`) task run ended, as recorded by
/// [`repository::BackgroundTaskRepository::complete`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskCompletion {
    /// The run ended for good: the queue row is deleted. The caller records
    /// the [`crate::tasks::TaskExecution`] history row and the
    /// stats fold in the same transaction.
    Terminal {
        outcome: ExecutionOutcome,
        error: Option<String>,
        finished_at: DateTime<Utc>,
    },
    /// The handler failed at `failed_at` with attempts left; the row goes
    /// back to `Pending`, due again at `retry_at`.
    Retry {
        error: String,
        failed_at: DateTime<Utc>,
        retry_at: DateTime<Utc>,
    },
}

// ================ THE ENTITY ================
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundTask {
    id: TaskId,
    kind: TaskKind,
    status: TaskStatus,
    payload: Option<String>,
    scheduled_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    attempts: u32,
    max_attempts: u32,
    retry_delay_secs: u32,
    last_error: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Plain-field mirror of [`BackgroundTask`] for persistence round-trips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundTaskSnapshot {
    pub id: TaskId,
    pub kind: TaskKind,
    pub status: TaskStatus,
    pub payload: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub retry_delay_secs: u32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl BackgroundTask {
    pub fn builder() -> BackgroundTaskBuilder {
        BackgroundTaskBuilder::new()
    }

    pub fn id(&self) -> &TaskId {
        &self.id
    }
    pub fn kind(&self) -> &TaskKind {
        &self.kind
    }
    pub fn status(&self) -> TaskStatus {
        self.status
    }
    pub fn payload(&self) -> Option<&str> {
        self.payload.as_deref()
    }
    pub fn scheduled_at(&self) -> DateTime<Utc> {
        self.scheduled_at
    }
    pub fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }
    pub fn attempts(&self) -> u32 {
        self.attempts
    }
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
    pub fn retry_delay_secs(&self) -> u32 {
        self.retry_delay_secs
    }
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Whether a failed run may be retried: claiming already counted the
    /// current attempt, so retries remain while `attempts < max_attempts`.
    pub fn can_retry(&self) -> bool {
        self.attempts < self.max_attempts
    }

    /// When the next retry becomes due: exponential backoff on the base delay
    /// (`retry_delay_secs * 2^(attempts-1)`), capped at one hour. A zero base
    /// delay retries immediately.
    pub fn next_retry_at(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let base = Duration::seconds(i64::from(self.retry_delay_secs));
        let exponent = self.attempts.saturating_sub(1).min(31);
        let factor = 2i32.checked_pow(exponent).unwrap_or(i32::MAX);
        let backoff = base
            .checked_mul(factor)
            .unwrap_or(MAX_BACKOFF)
            .min(MAX_BACKOFF);
        now.checked_add_signed(backoff).unwrap_or(now)
    }

    pub fn reconstitute(snapshot: BackgroundTaskSnapshot) -> Self {
        Self {
            id: snapshot.id,
            kind: snapshot.kind,
            status: snapshot.status,
            payload: snapshot.payload,
            scheduled_at: snapshot.scheduled_at,
            started_at: snapshot.started_at,
            attempts: snapshot.attempts,
            max_attempts: snapshot.max_attempts,
            retry_delay_secs: snapshot.retry_delay_secs,
            last_error: snapshot.last_error,
            created_at: snapshot.created_at,
            updated_at: snapshot.updated_at,
        }
    }
}

// ================ BUILDER ================
/// Builds a fresh `Pending` task with zero attempts; persisted history is
/// reconstituted through [`BackgroundTask::reconstitute`], never built.
#[derive(Default)]
pub struct BackgroundTaskBuilder {
    id: Option<TaskId>,
    kind: Option<TaskKind>,
    payload: Option<String>,
    scheduled_at: Option<DateTime<Utc>>,
    max_attempts: Option<u32>,
    retry_delay_secs: Option<u32>,
    created_at: Option<DateTime<Utc>>,
}

impl BackgroundTaskBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: TaskId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn kind(mut self, kind: TaskKind) -> Self {
        self.kind = Some(kind);
        self
    }

    pub fn payload(mut self, payload: impl Into<String>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    /// When the task becomes due. Defaults to `created_at` (due immediately).
    pub fn scheduled_at(mut self, scheduled_at: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(scheduled_at);
        self
    }

    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    pub fn retry_delay_secs(mut self, retry_delay_secs: u32) -> Self {
        self.retry_delay_secs = Some(retry_delay_secs);
        self
    }

    pub fn created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = Some(created_at);
        self
    }

    pub fn build(self) -> Result<BackgroundTask, TaskBuilderError> {
        let kind = self.kind.ok_or(TaskBuilderError::MissingKind)?;
        let max_attempts = self.max_attempts.unwrap_or(1);
        if max_attempts == 0 {
            return Err(TaskBuilderError::ZeroMaxAttempts);
        }

        let created_at = self.created_at.unwrap_or_else(Utc::now);
        Ok(BackgroundTask {
            id: self.id.unwrap_or_default(),
            kind,
            status: TaskStatus::Pending,
            payload: self.payload,
            scheduled_at: self.scheduled_at.unwrap_or(created_at),
            started_at: None,
            attempts: 0,
            max_attempts,
            retry_delay_secs: self.retry_delay_secs.unwrap_or(0),
            last_error: None,
            created_at,
            updated_at: created_at,
        })
    }
}

#[cfg(test)]
mod task_tests {
    use super::*;

    fn kind(name: &str) -> Option<TaskKind> {
        TaskKind::new(name).ok()
    }

    fn snapshot_with(
        attempts: u32,
        max_attempts: u32,
        retry_delay_secs: u32,
    ) -> Option<BackgroundTask> {
        let now = Utc::now();
        Some(BackgroundTask::reconstitute(BackgroundTaskSnapshot {
            id: TaskId::new(),
            kind: kind("test")?,
            status: TaskStatus::Running,
            payload: None,
            scheduled_at: now,
            started_at: Some(now),
            attempts,
            max_attempts,
            retry_delay_secs,
            last_error: None,
            created_at: now,
            updated_at: now,
        }))
    }

    /// Backoff of one hypothetical failed run (unlimited budget), at a fixed
    /// `now`.
    fn backoff_from(now: DateTime<Utc>, attempts: u32, retry_delay_secs: u32) -> Option<Duration> {
        let task = snapshot_with(attempts, u32::MAX, retry_delay_secs)?;
        Some(task.next_retry_at(now).signed_duration_since(now))
    }

    #[test]
    fn task_kind_trims_and_validates() {
        let trimmed = TaskKind::new(" db_maintenance ");
        assert!(matches!(&trimmed, Ok(k) if k.as_str() == "db_maintenance"));
        assert!(matches!(TaskKind::new("   "), Err(TaskKindError::Empty)));
        assert!(matches!(
            TaskKind::new("k".repeat(TASK_KIND_MAX_LEN + 1)),
            Err(TaskKindError::TooLong { max: 100 })
        ));
    }

    #[test]
    fn task_status_round_trips_queue_states_only() {
        for (text, status) in [
            ("PENDING", TaskStatus::Pending),
            ("running", TaskStatus::Running),
        ] {
            let parsed = TaskStatus::from_str(text);
            assert!(matches!(parsed, Ok(p) if p == status), "{text} parses");
        }
        // Terminal states left the queue for the execution history.
        for terminal in ["SUCCEEDED", "FAILED", "UNKNOWN"] {
            assert!(matches!(
                TaskStatus::from_str(terminal),
                Err(TaskStatusError::InvalidStatus)
            ));
        }
        let as_string: String = TaskStatus::Running.into();
        assert_eq!(as_string, "RUNNING");
    }

    #[test]
    fn builder_defaults_to_an_immediately_due_pending_task() {
        let Some(task_kind) = kind("t") else { return };
        let built = BackgroundTask::builder().kind(task_kind).build();
        assert!(built.is_ok());
        let Some(task) = built.ok() else { return };

        assert_eq!(task.status(), TaskStatus::Pending);
        assert_eq!(task.attempts(), 0);
        assert_eq!(task.max_attempts(), 1);
        assert_eq!(task.retry_delay_secs(), 0);
        assert_eq!(task.scheduled_at(), task.created_at());
        assert!(task.payload().is_none());
        assert!(task.started_at().is_none());
    }

    #[test]
    fn builder_requires_a_kind_and_nonzero_attempts() {
        assert!(matches!(
            BackgroundTask::builder().build(),
            Err(TaskBuilderError::MissingKind)
        ));
        let Some(task_kind) = kind("t") else { return };
        assert!(matches!(
            BackgroundTask::builder()
                .kind(task_kind)
                .max_attempts(0)
                .build(),
            Err(TaskBuilderError::ZeroMaxAttempts)
        ));
    }

    #[test]
    fn can_retry_tracks_the_attempt_budget() {
        for (attempts, max_attempts, expected) in
            [(1, 3, true), (2, 3, true), (3, 3, false), (1, 1, false)]
        {
            let Some(task) = snapshot_with(attempts, max_attempts, 0) else {
                return;
            };
            assert_eq!(task.can_retry(), expected, "{attempts}/{max_attempts}");
        }
    }

    #[test]
    fn backoff_doubles_per_attempt_and_caps_at_one_hour() {
        let now = Utc::now();
        // attempt 1 → base, attempt 2 → 2x, attempt 3 → 4x.
        assert_eq!(backoff_from(now, 1, 30), Some(Duration::seconds(30)));
        assert_eq!(backoff_from(now, 2, 30), Some(Duration::seconds(60)));
        assert_eq!(backoff_from(now, 3, 30), Some(Duration::seconds(120)));
        // Huge attempt counts saturate at the cap instead of overflowing.
        assert_eq!(backoff_from(now, u32::MAX, 3600), Some(MAX_BACKOFF));
    }

    #[test]
    fn zero_base_delay_retries_immediately() {
        assert_eq!(backoff_from(Utc::now(), 2, 0), Some(Duration::zero()));
    }
}

// ================ OUTCOME ================
/// How a terminal run ended. `NotReady` is recorded distinctly — "the
/// source has not published yet" is *waited*, not *worked*, and must never
/// read as a failure (it aligns with the sync cursor's outcome vocabulary).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionOutcome {
    Succeeded,
    NotReady,
    Failed,
}

impl ExecutionOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionOutcome::Succeeded => "SUCCEEDED",
            ExecutionOutcome::NotReady => "NOT_READY",
            ExecutionOutcome::Failed => "FAILED",
        }
    }
}

impl FromStr for ExecutionOutcome {
    type Err = ExecutionOutcomeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SUCCEEDED" => Ok(ExecutionOutcome::Succeeded),
            "NOT_READY" => Ok(ExecutionOutcome::NotReady),
            "FAILED" => Ok(ExecutionOutcome::Failed),
            _ => Err(ExecutionOutcomeError::InvalidOutcome),
        }
    }
}

impl From<ExecutionOutcome> for String {
    fn from(val: ExecutionOutcome) -> Self {
        val.as_str().into()
    }
}

// ================ EXECUTION ================
/// One terminal run: the write-once history record built from the queue row
/// it replaces. Plain fields on purpose — there is no state machine here,
/// only a fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskExecution {
    /// The run id, carried over from the queue row.
    pub id: TaskId,
    pub kind: TaskKind,
    pub outcome: ExecutionOutcome,
    pub payload: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: DateTime<Utc>,
    /// Attempts consumed by the run (retries included).
    pub attempts: u32,
    /// Wall time of the final attempt; `None` when no handler executed
    /// (cancelled or interrupted rows).
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
    /// When the queue row was created.
    pub created_at: DateTime<Utc>,
}

impl TaskExecution {
    /// The history record for a terminal completion of `task`.
    pub fn from_task(
        task: &BackgroundTask,
        outcome: ExecutionOutcome,
        error: Option<String>,
        duration_ms: Option<u64>,
        finished_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: *task.id(),
            kind: task.kind().clone(),
            outcome,
            payload: task.payload().map(str::to_owned),
            scheduled_at: task.scheduled_at(),
            started_at: task.started_at(),
            finished_at,
            attempts: task.attempts(),
            duration_ms,
            error,
            created_at: task.created_at(),
        }
    }
}

// ================ STATS ================
/// Per-kind aggregates, folded forward on every terminal run. History rows
/// are pruned; this row is not — it is the long-term answer to "has this
/// task ever run, and how did it go?".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStats {
    pub kind: TaskKind,
    /// Terminal runs of any outcome (`NotReady` counts as a run).
    pub total_runs: u64,
    /// Terminal `Failed` runs only.
    pub total_failures: u64,
    /// Sum of measured run durations (unmeasured runs add nothing).
    pub total_duration_ms: u64,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_outcome: Option<ExecutionOutcome>,
    pub last_error: Option<String>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_duration_ms: Option<u64>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod execution_tests {
    use super::*;
    use crate::tasks::TaskStatus;

    #[test]
    fn execution_outcome_round_trips_through_strings() {
        for (text, outcome) in [
            ("SUCCEEDED", ExecutionOutcome::Succeeded),
            ("not_ready", ExecutionOutcome::NotReady),
            ("Failed", ExecutionOutcome::Failed),
        ] {
            let parsed = ExecutionOutcome::from_str(text);
            assert!(matches!(parsed, Ok(p) if p == outcome), "{text}");
        }
        assert!(matches!(
            ExecutionOutcome::from_str("RETRIED"),
            Err(ExecutionOutcomeError::InvalidOutcome)
        ));
        let as_string: String = ExecutionOutcome::NotReady.into();
        assert_eq!(as_string, "NOT_READY");
    }

    #[test]
    fn from_task_carries_the_row_over() {
        let Ok(kind) = TaskKind::new("t") else { return };
        let Ok(task) = BackgroundTask::builder()
            .kind(kind)
            .payload("p")
            .max_attempts(3)
            .build()
        else {
            return;
        };
        assert_eq!(task.status(), TaskStatus::Pending);

        let now = Utc::now();
        let execution = TaskExecution::from_task(
            &task,
            ExecutionOutcome::Failed,
            Some("boom".into()),
            Some(42),
            now,
        );
        assert_eq!(execution.id, *task.id());
        assert_eq!(execution.kind, *task.kind());
        assert_eq!(execution.outcome, ExecutionOutcome::Failed);
        assert_eq!(execution.payload.as_deref(), Some("p"));
        assert_eq!(execution.scheduled_at, task.scheduled_at());
        assert_eq!(execution.finished_at, now);
        assert_eq!(execution.duration_ms, Some(42));
        assert_eq!(execution.error.as_deref(), Some("boom"));
        assert_eq!(execution.created_at, task.created_at());
    }
}

// ================ CATEGORY ================
/// Classification of a task for grouping, log filtering, and defaults —
/// deliberately a closed set so listings and filters stay reliable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskCategory {
    /// Engine housekeeping: database maintenance, liveness, pruning.
    EngineSystem,
    /// Financial-instrument data ingestion (CVM, ANBIMA, B3, …).
    FinanceDataSync,
    /// Everything else (reports, exports, custom tasks).
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

// ================ TRIGGER ================
/// The task's trigger kind: what its schedule *means* and what happens
/// on a missed occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskTrigger {
    /// Monotonic interval since boot.
    Interval,
    /// Wall-clock business-day recurrence.
    Recurring,
    /// Cursor-driven recurrence with sequential catch-up.
    Sync,
}

impl TaskTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskTrigger::Interval => "INTERVAL",
            TaskTrigger::Recurring => "RECURRING",
            TaskTrigger::Sync => "SYNC",
        }
    }
}

impl FromStr for TaskTrigger {
    type Err = TaskTriggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INTERVAL" => Ok(TaskTrigger::Interval),
            "RECURRING" => Ok(TaskTrigger::Recurring),
            "SYNC" => Ok(TaskTrigger::Sync),
            _ => Err(TaskTriggerError::InvalidTrigger),
        }
    }
}

impl From<TaskTrigger> for String {
    fn from(val: TaskTrigger) -> Self {
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

// ================ SETTINGS ================
/// The operator-tunable scheduling knobs stored on the catalog row.
///
/// `None` means the knob is **code-owned**: either the task never declared
/// it tunable (the boot reconcile leaves the column NULL forever and the
/// scheduler always computes the value in code), or an operator cleared it
/// back to NULL ("reset to default" — the next boot refills the code
/// default). `Some` values are operator/DB truth: the boot reconcile never
/// overwrites a non-NULL column, so they survive restarts and upgrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TaskSettings {
    /// Interval trigger: seconds between runs.
    pub period_secs: Option<u32>,
    /// Recurring/sync triggers: market-local time of day.
    pub at_local: Option<NaiveTime>,
    /// Recurring/sync triggers: the recurrence.
    pub recurrence: Option<Recurrence>,
    /// Sync trigger: failure-cooldown base seconds.
    pub cooldown_secs: Option<u32>,
    /// Sync trigger: catch-up bound in business days.
    pub max_backfill_days: Option<u32>,
}

// ================ DECLARATION ================
/// What code declares about a task — the upsert payload of the boot
/// reconcile. Identity columns are rewritten every boot; `settings` are
/// the code defaults that fill NULL columns only. Everything else on the
/// registration (operator intent, run summary) the reconcile must
/// preserve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDeclaration {
    pub kind: TaskKind,
    pub category: TaskCategory,
    pub trigger: TaskTrigger,
    pub tracking: TaskTracking,
    /// Canonical schedule descriptor, display-only
    /// (e.g. `sync:daily@07:00-03:00`, `interval:3600s±10%`).
    pub schedule: String,
    /// Sync trigger: the cursor key.
    pub source: Option<SyncSource>,
    pub log_policy: LogPolicy,
    /// Code-default settings; `None` fields are not tunable for this task.
    pub settings: TaskSettings,
}

// ================ THE REGISTRATION ================
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRegistration {
    kind: TaskKind,
    category: TaskCategory,
    trigger: TaskTrigger,
    tracking: TaskTracking,
    schedule: String,
    source: Option<SyncSource>,
    log_policy: LogPolicy,
    enabled: bool,
    settings: TaskSettings,
    registered: bool,
    first_registered_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Plain-field mirror of [`TaskRegistration`] for persistence round-trips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRegistrationSnapshot {
    pub kind: TaskKind,
    pub category: TaskCategory,
    pub trigger: TaskTrigger,
    pub tracking: TaskTracking,
    pub schedule: String,
    pub source: Option<SyncSource>,
    pub log_policy: LogPolicy,
    pub enabled: bool,
    pub settings: TaskSettings,
    pub registered: bool,
    pub first_registered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TaskRegistration {
    /// A fresh registration as its first declaration creates it: enabled,
    /// with the code-default settings.
    pub fn declared(declaration: TaskDeclaration, now: DateTime<Utc>) -> Self {
        Self {
            kind: declaration.kind,
            category: declaration.category,
            trigger: declaration.trigger,
            tracking: declaration.tracking,
            schedule: declaration.schedule,
            source: declaration.source,
            log_policy: declaration.log_policy,
            enabled: true,
            settings: declaration.settings,
            registered: true,
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
    pub fn trigger(&self) -> TaskTrigger {
        self.trigger
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
    pub fn enabled(&self) -> bool {
        self.enabled
    }
    pub fn settings(&self) -> &TaskSettings {
        &self.settings
    }
    pub fn registered(&self) -> bool {
        self.registered
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
            trigger: snapshot.trigger,
            tracking: snapshot.tracking,
            schedule: snapshot.schedule,
            source: snapshot.source,
            log_policy: snapshot.log_policy,
            enabled: snapshot.enabled,
            settings: snapshot.settings,
            registered: snapshot.registered,
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
    /// Turned off by an operator; nothing seeds and nothing dispatches.
    Disabled,
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
    if !registration.enabled() {
        return DerivedTaskStatus::Disabled;
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
mod registration_tests {
    use super::*;
    use crate::tasks::BackgroundTaskSnapshot;
    use crate::tasks::SyncOutcomeKind;
    use crate::tasks::TaskId;
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
            trigger: TaskTrigger::Sync,
            tracking: TaskTracking::Durable,
            schedule: "sync:daily@07:00-03:00".into(),
            source: crate::tasks::SyncSource::new("cvm").ok(),
            log_policy: LogPolicy::All,
            settings: TaskSettings {
                at_local: NaiveTime::from_hms_opt(7, 0, 0),
                recurrence: Some(Recurrence::Daily),
                cooldown_secs: Some(300),
                max_backfill_days: Some(90),
                ..TaskSettings::default()
            },
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
            attempts: 0,
            max_attempts: 1,
            retry_delay_secs: 0,
            last_error: None,
            created_at: now,
            updated_at: now,
        }))
    }

    fn cursor(failures: u32, cooldown_until: Option<DateTime<Utc>>) -> Option<SyncCursor> {
        let source = crate::tasks::SyncSource::new("cvm").ok()?;
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
        assert!(matches!(
            TaskTrigger::from_str("sync"),
            Ok(TaskTrigger::Sync)
        ));
        assert!(TaskTrigger::from_str("cron").is_err());
        assert!(matches!(
            TaskTracking::from_str("ephemeral"),
            Ok(TaskTracking::Ephemeral)
        ));
        assert!(matches!(
            LogPolicy::from_str("failures_only"),
            Ok(LogPolicy::FailuresOnly)
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
        assert!(registration.enabled());
        assert_eq!(registration.settings().cooldown_secs, Some(300));
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
            trigger: registration.trigger(),
            tracking: registration.tracking(),
            schedule: registration.schedule().to_owned(),
            source: registration.source().cloned(),
            log_policy: registration.log_policy(),
            enabled: false,
            settings: *registration.settings(),
            registered: false,
            first_registered_at: now,
            updated_at: now,
        };
        let retired = TaskRegistration::reconstitute(snapshot_source.clone());
        let running = row(TaskStatus::Running, now);
        assert_eq!(
            derive_status(&retired, running.as_ref(), None, 5, now),
            DerivedTaskStatus::Retired,
            "retired wins over disabled/running"
        );

        snapshot_source.registered = true;
        let disabled = TaskRegistration::reconstitute(snapshot_source);
        assert_eq!(
            derive_status(&disabled, running.as_ref(), None, 5, now),
            DerivedTaskStatus::Disabled,
            "disabled wins over running"
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

// ================ STATUS READ MODEL ================
/// One task's assembled view: the registration, its derived status, and
/// the live scheduling facts behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusEntry {
    pub registration: TaskRegistration,
    pub status: DerivedTaskStatus,
    /// The earliest queued run, when one exists.
    pub next_run_at: Option<DateTime<Utc>>,
    /// The prune-proof run aggregates, once the kind has ever run.
    pub stats: Option<TaskStats>,
    /// The sync cursor, for sync-trigger tasks.
    pub cursor: Option<SyncCursor>,
}

/// Assemble the status of every cataloged task (including retired ones).
pub fn list_task_statuses(
    registry: &impl TaskRegistryRepository,
    tasks: &impl BackgroundTaskRepository,
    stats: &impl TaskStatRepository,
    cursors: &impl SyncCursorRepository,
    halted_after: u32,
    now: DateTime<Utc>,
) -> RepositoryResult<Vec<TaskStatusEntry>> {
    let registrations = registry.list()?;
    let mut entries = Vec::with_capacity(registrations.len());
    for registration in registrations {
        let active = tasks
            .find_active(registration.kind())?
            .map(|versioned| versioned.data);
        let cursor = match registration.source() {
            Some(source) => cursors.get(source)?,
            None => None,
        };
        let status = derive_status(
            &registration,
            active.as_ref(),
            cursor.as_ref(),
            halted_after,
            now,
        );
        let next_run_at = active
            .as_ref()
            .filter(|task| task.status() == TaskStatus::Pending)
            .map(|task| task.scheduled_at());
        let stats = stats.get(registration.kind())?;
        entries.push(TaskStatusEntry {
            registration,
            status,
            next_run_at,
            stats,
            cursor,
        });
    }
    Ok(entries)
}

const SYNC_SOURCE_MAX_LEN: usize = 50;

// ================ SOURCE ================
/// Registered name of a sync source (e.g. `cvm`): the cursor's primary key
/// and the env-var namespace of its configuration.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct SyncSource(String);

impl SyncSource {
    pub fn new(value: impl Into<String>) -> Result<Self, SyncSourceError> {
        let value = value.into();
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(SyncSourceError::Empty);
        }
        if trimmed.chars().count() > SYNC_SOURCE_MAX_LEN {
            return Err(SyncSourceError::TooLong {
                max: SYNC_SOURCE_MAX_LEN,
            });
        }

        Ok(Self(trimmed.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ================ OUTCOMES ================
/// How one sync run ended, as reported by the source's handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The target period was ingested; the cursor advances past it.
    Synced,
    /// The source has not published the target period yet — not an error:
    /// the cursor holds, no failure is counted, and re-seeding waits
    /// `retry_after_secs`.
    NotReady { retry_after_secs: u32 },
    /// The run failed. Task-level retries absorb transient faults; a
    /// terminal failure holds the cursor and starts the failure cooldown.
    Failed { error: String },
}

impl SyncOutcome {
    pub fn kind(&self) -> SyncOutcomeKind {
        match self {
            SyncOutcome::Synced => SyncOutcomeKind::Synced,
            SyncOutcome::NotReady { .. } => SyncOutcomeKind::NotReady,
            SyncOutcome::Failed { .. } => SyncOutcomeKind::Failed,
        }
    }
}

/// The persisted marker of a run's outcome (`last_outcome` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncOutcomeKind {
    Synced,
    NotReady,
    Failed,
}

impl FromStr for SyncOutcomeKind {
    type Err = SyncOutcomeKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SYNCED" => Ok(SyncOutcomeKind::Synced),
            "NOT_READY" => Ok(SyncOutcomeKind::NotReady),
            "FAILED" => Ok(SyncOutcomeKind::Failed),
            _ => Err(SyncOutcomeKindError::InvalidKind),
        }
    }
}

impl From<SyncOutcomeKind> for String {
    fn from(val: SyncOutcomeKind) -> Self {
        match val {
            SyncOutcomeKind::Synced => "SYNCED".into(),
            SyncOutcomeKind::NotReady => "NOT_READY".into(),
            SyncOutcomeKind::Failed => "FAILED".into(),
        }
    }
}

// ================ THE CURSOR ================
/// Per-source sync progress: the single row that survives restarts, task
/// pruning, and crashes, and from which the reconciler derives the next
/// slot to seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncCursor {
    source: SyncSource,
    through_slot: DateTime<Utc>,
    through_target: NaiveDate,
    cooldown_until: Option<DateTime<Utc>>,
    consecutive_failures: u32,
    last_outcome: Option<SyncOutcomeKind>,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
}

/// Plain-field mirror of [`SyncCursor`] for persistence round-trips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncCursorSnapshot {
    pub source: SyncSource,
    pub through_slot: DateTime<Utc>,
    pub through_target: NaiveDate,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub consecutive_failures: u32,
    pub last_outcome: Option<SyncOutcomeKind>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl SyncCursor {
    /// A fresh cursor (cold start): everything through `through_slot` /
    /// `through_target` is declared covered, so the next occurrence after
    /// `through_slot` is the first run.
    pub fn seeded(
        source: SyncSource,
        through_slot: DateTime<Utc>,
        through_target: NaiveDate,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            source,
            through_slot,
            through_target,
            cooldown_until: None,
            consecutive_failures: 0,
            last_outcome: None,
            last_error: None,
            updated_at: now,
        }
    }

    pub fn source(&self) -> &SyncSource {
        &self.source
    }
    pub fn through_slot(&self) -> DateTime<Utc> {
        self.through_slot
    }
    pub fn through_target(&self) -> NaiveDate {
        self.through_target
    }
    pub fn cooldown_until(&self) -> Option<DateTime<Utc>> {
        self.cooldown_until
    }
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }
    pub fn last_outcome(&self) -> Option<SyncOutcomeKind> {
        self.last_outcome
    }
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Whether re-seeding may proceed at `now` (no active cooldown).
    pub fn is_ready(&self, now: DateTime<Utc>) -> bool {
        self.cooldown_until.is_none_or(|until| until <= now)
    }

    /// A successful run: advance past its slot and target, clear the
    /// failure state.
    pub fn advanced(self, slot: DateTime<Utc>, target_to: NaiveDate, now: DateTime<Utc>) -> Self {
        Self {
            through_slot: slot,
            through_target: target_to,
            cooldown_until: None,
            consecutive_failures: 0,
            last_outcome: Some(SyncOutcomeKind::Synced),
            last_error: None,
            updated_at: now,
            ..self
        }
    }

    /// The source has not published the target yet: hold position and wait.
    /// Deliberately not a failure — the count and last error are untouched
    /// by design, so a slow publisher never escalates.
    pub fn held_not_ready(self, retry_after_secs: u32, now: DateTime<Utc>) -> Self {
        let until = now
            .checked_add_signed(chrono::Duration::seconds(i64::from(retry_after_secs)))
            .unwrap_or(now);
        Self {
            cooldown_until: Some(until),
            last_outcome: Some(SyncOutcomeKind::NotReady),
            updated_at: now,
            ..self
        }
    }

    /// A terminally failed run: hold position, count the failure, and back
    /// off until `cooldown_until`.
    pub fn failed(self, error: String, cooldown_until: DateTime<Utc>, now: DateTime<Utc>) -> Self {
        Self {
            cooldown_until: Some(cooldown_until),
            consecutive_failures: self.consecutive_failures.saturating_add(1),
            last_outcome: Some(SyncOutcomeKind::Failed),
            last_error: Some(error),
            updated_at: now,
            ..self
        }
    }

    pub fn reconstitute(snapshot: SyncCursorSnapshot) -> Self {
        Self {
            source: snapshot.source,
            through_slot: snapshot.through_slot,
            through_target: snapshot.through_target,
            cooldown_until: snapshot.cooldown_until,
            consecutive_failures: snapshot.consecutive_failures,
            last_outcome: snapshot.last_outcome,
            last_error: snapshot.last_error,
            updated_at: snapshot.updated_at,
        }
    }
}

#[cfg(test)]
mod sync_tests {
    use super::*;
    use chrono::TimeZone;

    fn source() -> Option<SyncSource> {
        SyncSource::new("cvm").ok()
    }

    fn utc(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0)
            .single()
            .unwrap_or_default()
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap_or_default()
    }

    #[test]
    fn sync_source_trims_and_validates() {
        let trimmed = SyncSource::new(" cvm ");
        assert!(matches!(&trimmed, Ok(s) if s.as_str() == "cvm"));
        assert!(matches!(
            SyncSource::new("   "),
            Err(SyncSourceError::Empty)
        ));
        assert!(matches!(
            SyncSource::new("s".repeat(SYNC_SOURCE_MAX_LEN + 1)),
            Err(SyncSourceError::TooLong { max: 50 })
        ));
    }

    #[test]
    fn outcome_kind_round_trips() {
        for (text, kind) in [
            ("SYNCED", SyncOutcomeKind::Synced),
            ("not_ready", SyncOutcomeKind::NotReady),
            ("Failed", SyncOutcomeKind::Failed),
        ] {
            let parsed = SyncOutcomeKind::from_str(text);
            assert!(matches!(parsed, Ok(p) if p == kind), "{text} parses");
        }
        assert!(SyncOutcomeKind::from_str("UNKNOWN").is_err());
        let as_string: String = SyncOutcomeKind::NotReady.into();
        assert_eq!(as_string, "NOT_READY");
    }

    #[test]
    fn seeded_cursor_is_ready_with_clean_state() {
        let Some(source) = source() else { return };
        let now = utc(2026, 8, 12, 12);
        let cursor = SyncCursor::seeded(source, utc(2026, 8, 11, 10), date(2026, 8, 10), now);
        assert!(cursor.is_ready(now));
        assert_eq!(cursor.consecutive_failures(), 0);
        assert_eq!(cursor.last_outcome(), None);
        assert_eq!(cursor.through_target(), date(2026, 8, 10));
    }

    #[test]
    fn advanced_moves_both_positions_and_clears_failures() {
        let Some(source) = source() else { return };
        let now = utc(2026, 8, 12, 12);
        let cursor = SyncCursor::seeded(source, utc(2026, 8, 11, 10), date(2026, 8, 10), now)
            .failed("boom".into(), utc(2026, 8, 12, 13), now)
            .advanced(utc(2026, 8, 12, 10), date(2026, 8, 11), now);

        assert_eq!(cursor.through_slot(), utc(2026, 8, 12, 10));
        assert_eq!(cursor.through_target(), date(2026, 8, 11));
        assert_eq!(cursor.consecutive_failures(), 0, "success resets failures");
        assert_eq!(cursor.cooldown_until(), None, "success clears cooldown");
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::Synced));
        assert_eq!(cursor.last_error(), None);
    }

    #[test]
    fn not_ready_holds_position_without_counting_a_failure() {
        let Some(source) = source() else { return };
        let now = utc(2026, 8, 12, 12);
        let cursor = SyncCursor::seeded(source, utc(2026, 8, 11, 10), date(2026, 8, 10), now)
            .held_not_ready(600, now);

        assert_eq!(cursor.through_slot(), utc(2026, 8, 11, 10), "holds slot");
        assert_eq!(cursor.consecutive_failures(), 0, "not a failure");
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::NotReady));
        assert!(!cursor.is_ready(now), "cooldown active immediately");
        assert!(
            cursor.is_ready(utc(2026, 8, 12, 13)),
            "ready once retry_after has elapsed"
        );
    }

    #[test]
    fn failed_counts_up_and_cools_down() {
        let Some(source) = source() else { return };
        let now = utc(2026, 8, 12, 12);
        let cursor = SyncCursor::seeded(source, utc(2026, 8, 11, 10), date(2026, 8, 10), now)
            .failed("first".into(), utc(2026, 8, 12, 13), now)
            .failed("second".into(), utc(2026, 8, 12, 14), now);

        assert_eq!(cursor.consecutive_failures(), 2);
        assert_eq!(cursor.last_error(), Some("second"));
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::Failed));
        assert!(!cursor.is_ready(utc(2026, 8, 12, 13)));
        assert!(cursor.is_ready(utc(2026, 8, 12, 14)), "boundary: <= now");
    }

    #[test]
    fn reconstitute_round_trips() {
        let Some(source) = source() else { return };
        let snapshot = SyncCursorSnapshot {
            source,
            through_slot: utc(2026, 8, 11, 10),
            through_target: date(2026, 8, 10),
            cooldown_until: Some(utc(2026, 8, 12, 13)),
            consecutive_failures: 3,
            last_outcome: Some(SyncOutcomeKind::Failed),
            last_error: Some("boom".into()),
            updated_at: utc(2026, 8, 12, 12),
        };
        let cursor = SyncCursor::reconstitute(snapshot.clone());
        assert_eq!(cursor.source().as_str(), "cvm");
        assert_eq!(cursor.through_slot(), snapshot.through_slot);
        assert_eq!(cursor.through_target(), snapshot.through_target);
        assert_eq!(cursor.cooldown_until(), snapshot.cooldown_until);
        assert_eq!(cursor.consecutive_failures(), 3);
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::Failed));
        assert_eq!(cursor.last_error(), Some("boom"));
    }
}

// ================ COOLDOWN ================
/// Backoff growth is capped so `base_secs * 2^(failures-1)` cannot overflow
/// or schedule re-seeding absurdly far out.
const MAX_COOLDOWN: Duration = Duration::hours(1);

/// How a source's re-seeding backs off across consecutive terminal
/// failures. `NotReady` outcomes do not use this policy — they carry their
/// own `retry_after`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CooldownPolicy {
    base_secs: u32,
}

impl CooldownPolicy {
    pub const fn new(base_secs: u32) -> Self {
        Self { base_secs }
    }

    pub fn base_secs(&self) -> u32 {
        self.base_secs
    }

    /// When re-seeding becomes allowed again after the `failures`-th
    /// consecutive terminal failure: `now + min(base * 2^(failures-1), 1h)`.
    /// A zero base cools down for zero seconds (retry at the next
    /// reconcile pass).
    pub fn until(&self, failures: u32, now: DateTime<Utc>) -> DateTime<Utc> {
        let base = Duration::seconds(i64::from(self.base_secs));
        let exponent = failures.saturating_sub(1).min(31);
        let factor = 2i32.checked_pow(exponent).unwrap_or(i32::MAX);
        let cooldown = base
            .checked_mul(factor)
            .unwrap_or(MAX_COOLDOWN)
            .min(MAX_COOLDOWN);
        now.checked_add_signed(cooldown).unwrap_or(now)
    }
}

#[cfg(test)]
mod cooldown_tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 12, 12, 0, 0)
            .single()
            .unwrap_or_default()
    }

    fn cooldown_of(base_secs: u32, failures: u32) -> Duration {
        CooldownPolicy::new(base_secs)
            .until(failures, now())
            .signed_duration_since(now())
    }

    #[test]
    fn cooldown_doubles_per_failure_and_caps_at_one_hour() {
        assert_eq!(cooldown_of(300, 1), Duration::seconds(300));
        assert_eq!(cooldown_of(300, 2), Duration::seconds(600));
        assert_eq!(cooldown_of(300, 3), Duration::seconds(1200));
        assert_eq!(cooldown_of(300, 4), Duration::seconds(2400));
        // 300 * 2^4 = 4800 > 3600 → capped.
        assert_eq!(cooldown_of(300, 5), MAX_COOLDOWN);
        // Huge counts saturate instead of overflowing.
        assert_eq!(cooldown_of(3600, u32::MAX), MAX_COOLDOWN);
    }

    #[test]
    fn zero_base_retries_immediately() {
        assert_eq!(cooldown_of(0, 3), Duration::zero());
    }

    #[test]
    fn zero_failures_behaves_like_the_first() {
        assert_eq!(cooldown_of(300, 0), Duration::seconds(300));
    }
}
