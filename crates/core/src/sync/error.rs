#[derive(thiserror::Error, Debug)]
pub enum SyncSourceError {
    #[error("sync source cannot be empty")]
    Empty,

    #[error("sync source exceeds maximum length of {max} characters")]
    TooLong { max: usize },
}

#[derive(thiserror::Error, Debug)]
pub enum SyncOutcomeKindError {
    #[error("Invalid outcome. Must be one of: {kinds:?}", kinds = vec!["SYNCED", "NOT_READY", "FAILED"])]
    InvalidKind,
}
