use crate::storage::StorageFault;

#[derive(thiserror::Error, Debug)]
pub enum SecurityNameError {
    #[error("security name cannot be empty")]
    Empty,

    #[error("security name exceeds maximum length of {max} characters")]
    TooLong { max: usize },
}

#[derive(thiserror::Error, Debug)]
pub enum SecurityKindError {
    #[error(
        "Invalid kind. Must be one of: {kinds:?}",
        kinds = vec!["COMMON_SHARE", "PREFERRED_SHARE", "UNIT", "DEPOSITARY_RECEIPT"]
    )]
    InvalidKind,
}

#[derive(thiserror::Error, Debug)]
pub enum SecurityStatusError {
    #[error("Invalid status. Must be one of: {statuses:?}", statuses = vec!["ACTIVE", "RETIRED"])]
    InvalidStatus,
}

#[derive(thiserror::Error, Debug)]
pub enum DrRatioError {
    #[error("depositary receipt ratio requires a non-zero receipts side")]
    ZeroReceipts,

    #[error("depositary receipt ratio requires a non-zero underlying side")]
    ZeroUnderlying,
}

#[derive(Debug, thiserror::Error)]
pub enum SecurityBuilderError {
    #[error("An underlying security may only be set on a DEPOSITARY_RECEIPT security. Found: {0}")]
    UnderlyingRequiresDepositaryReceipt(String),

    #[error(
        "A depositary receipt ratio may only be set on a DEPOSITARY_RECEIPT security. Found: {0}"
    )]
    DrRatioRequiresDepositaryReceipt(String),

    #[error("a security cannot reference itself as its underlying")]
    SelfUnderlying,
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterSecurityError {
    #[error("the referenced issuer does not exist")]
    UnknownIssuer,

    #[error("a security with this ISIN already exists")]
    DuplicateIsin,

    #[error("the referenced underlying security does not exist")]
    UnknownUnderlyingSecurity,

    #[error(transparent)]
    Storage(#[from] StorageFault),
}
