use crate::storage::StorageFault;

#[derive(thiserror::Error, Debug)]
pub enum VenueNameError {
    #[error("venue name cannot be empty")]
    Empty,

    #[error("venue name exceeds maximum length of {max} characters")]
    TooLong { max: usize },
}

#[derive(thiserror::Error, Debug)]
pub enum VenueStatusError {
    #[error("Invalid status. Must be one of: {statuses:?}", statuses = vec!["ACTIVE", "RETIRED"])]
    InvalidStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterVenueError {
    #[error("a venue with this MIC already exists")]
    DuplicateMic,

    #[error(transparent)]
    Storage(#[from] StorageFault),
}
