#[derive(thiserror::Error, Debug)]
pub enum TaskCategoryError {
    #[error("Invalid category. Must be one of: {categories:?}", categories = vec!["ENGINE_SYSTEM", "FINANCE_DATA_SYNC", "OTHER"])]
    InvalidCategory,
}

#[derive(thiserror::Error, Debug)]
pub enum TaskTierError {
    #[error("Invalid tier. Must be one of: {tiers:?}", tiers = vec!["INTERVAL", "RECURRING", "SYNC"])]
    InvalidTier,
}

#[derive(thiserror::Error, Debug)]
pub enum TaskTrackingError {
    #[error("Invalid tracking. Must be one of: {kinds:?}", kinds = vec!["DURABLE", "EPHEMERAL"])]
    InvalidTracking,
}

#[derive(thiserror::Error, Debug)]
pub enum LogPolicyError {
    #[error("Invalid log policy. Must be one of: {policies:?}", policies = vec!["ALL", "FAILURES_ONLY"])]
    InvalidPolicy,
}

#[derive(thiserror::Error, Debug)]
pub enum RunOutcomeError {
    #[error("Invalid run outcome. Must be one of: {outcomes:?}", outcomes = vec!["SUCCEEDED", "FAILED"])]
    InvalidOutcome,
}
