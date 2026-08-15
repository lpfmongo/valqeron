#[derive(thiserror::Error, Debug)]
pub enum TaskKindError {
    #[error("task kind cannot be empty")]
    Empty,

    #[error("task kind exceeds maximum length of {max} characters")]
    TooLong { max: usize },
}

#[derive(thiserror::Error, Debug)]
pub enum TaskStatusError {
    #[error("Invalid status. Must be one of: {statuses:?}", statuses = vec!["PENDING", "RUNNING", "SUCCEEDED", "FAILED"])]
    InvalidStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum TaskBuilderError {
    #[error("a background task requires a kind")]
    MissingKind,

    #[error("max_attempts must be at least 1")]
    ZeroMaxAttempts,
}
