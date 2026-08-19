//! Every error the tasks domain can produce, in one place.

use thiserror::Error;

// ================ QUEUE ================
#[derive(Error, Debug)]
pub enum TaskKindError {
    #[error("task kind cannot be empty")]
    Empty,

    #[error("task kind exceeds maximum length of {max} characters")]
    TooLong { max: usize },
}

#[derive(Error, Debug)]
pub enum TaskStatusError {
    #[error("Invalid status. Must be one of: {statuses:?}", statuses = vec!["PENDING", "RUNNING"])]
    InvalidStatus,
}

#[derive(Error, Debug)]
pub enum TaskBuilderError {
    #[error("a background task requires a kind")]
    MissingKind,

    #[error("max_attempts must be at least 1")]
    ZeroMaxAttempts,
}

// ================ HISTORY ================
#[derive(Error, Debug, PartialEq, Eq)]
pub enum ExecutionOutcomeError {
    #[error("invalid execution outcome: expected SUCCEEDED, NOT_READY, or FAILED")]
    InvalidOutcome,
}

// ================ CATALOG ================
#[derive(Error, Debug)]
pub enum TaskCategoryError {
    #[error("Invalid category. Must be one of: {categories:?}", categories = vec!["ENGINE_SYSTEM", "FINANCE_DATA_SYNC", "OTHER"])]
    InvalidCategory,
}

#[derive(Error, Debug)]
pub enum TaskTriggerError {
    #[error("Invalid trigger kind. Must be one of: {kinds:?}", kinds = vec!["INTERVAL", "RECURRING", "SYNC"])]
    InvalidTrigger,
}

#[derive(Error, Debug)]
pub enum TaskTrackingError {
    #[error("Invalid tracking. Must be one of: {kinds:?}", kinds = vec!["DURABLE", "EPHEMERAL"])]
    InvalidTracking,
}

#[derive(Error, Debug)]
pub enum LogPolicyError {
    #[error("Invalid log policy. Must be one of: {policies:?}", policies = vec!["ALL", "FAILURES_ONLY"])]
    InvalidPolicy,
}

// ================ SYNC ================
#[derive(Error, Debug)]
pub enum SyncSourceError {
    #[error("sync source cannot be empty")]
    Empty,

    #[error("sync source exceeds maximum length of {max} characters")]
    TooLong { max: usize },
}

#[derive(Error, Debug)]
pub enum SyncOutcomeKindError {
    #[error("Invalid outcome. Must be one of: {kinds:?}", kinds = vec!["SYNCED", "NOT_READY", "FAILED"])]
    InvalidKind,
}
