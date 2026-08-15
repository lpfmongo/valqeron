//! Engine-internal background tasks: the persisted queue-and-history record
//! behind the engine's `BackgroundTasksManager`.
//!
//! A `BackgroundTask` row is simultaneously the *schedule* (a `Pending` row
//! becomes due at `scheduled_at`) and the *execution record* (attempts,
//! timings, `last_error`). Core owns the entity, its state machine, and the
//! retry arithmetic so they stay pure and unit-testable; the engine owns the
//! clocks, the dispatch loop, and the handlers.

use chrono::{DateTime, Duration, Utc};
use std::str::FromStr;
use uuid::Uuid;

use crate::task::error::{TaskBuilderError, TaskKindError, TaskStatusError};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TaskStatus {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl TaskStatus {
    /// Terminal statuses never transition again and are eligible for pruning.
    pub fn is_terminal(&self) -> bool {
        matches!(self, TaskStatus::Succeeded | TaskStatus::Failed)
    }
}

impl FromStr for TaskStatus {
    type Err = TaskStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PENDING" => Ok(TaskStatus::Pending),
            "RUNNING" => Ok(TaskStatus::Running),
            "SUCCEEDED" => Ok(TaskStatus::Succeeded),
            "FAILED" => Ok(TaskStatus::Failed),
            _ => Err(TaskStatusError::InvalidStatus),
        }
    }
}

impl From<TaskStatus> for String {
    fn from(val: TaskStatus) -> Self {
        match val {
            TaskStatus::Pending => "PENDING".into(),
            TaskStatus::Running => "RUNNING".into(),
            TaskStatus::Succeeded => "SUCCEEDED".into(),
            TaskStatus::Failed => "FAILED".into(),
        }
    }
}

// ================ COMPLETION ================
/// How a claimed (`Running`) task run ended, as recorded by
/// [`repository::BackgroundTaskRepository::complete`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskCompletion {
    /// The handler finished successfully; the row becomes terminal `Succeeded`.
    Succeeded { finished_at: DateTime<Utc> },
    /// The handler failed at `failed_at` with attempts left; the row goes
    /// back to `Pending`, due again at `retry_at`.
    Retry {
        error: String,
        failed_at: DateTime<Utc>,
        retry_at: DateTime<Utc>,
    },
    /// The handler failed on its final attempt (or cannot be dispatched at
    /// all); the row becomes terminal `Failed`.
    Failed {
        error: String,
        finished_at: DateTime<Utc>,
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
    finished_at: Option<DateTime<Utc>>,
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
    pub finished_at: Option<DateTime<Utc>>,
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
    pub fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
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
            finished_at: snapshot.finished_at,
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
            finished_at: None,
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
mod tests {
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
            finished_at: None,
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
    fn task_status_round_trips_and_flags_terminal() {
        for (text, status, terminal) in [
            ("PENDING", TaskStatus::Pending, false),
            ("running", TaskStatus::Running, false),
            ("Succeeded", TaskStatus::Succeeded, true),
            ("FAILED", TaskStatus::Failed, true),
        ] {
            let parsed = TaskStatus::from_str(text);
            assert!(matches!(parsed, Ok(p) if p == status), "{text} parses");
            assert_eq!(status.is_terminal(), terminal);
        }
        assert!(matches!(
            TaskStatus::from_str("UNKNOWN"),
            Err(TaskStatusError::InvalidStatus)
        ));
        let as_string: String = TaskStatus::Succeeded.into();
        assert_eq!(as_string, "SUCCEEDED");
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
        assert!(task.finished_at().is_none());
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
