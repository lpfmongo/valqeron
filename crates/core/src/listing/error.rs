use crate::storage::StorageFault;
use chrono::NaiveDate;

#[derive(thiserror::Error, Debug)]
pub enum TickerSymbolError {
    #[error("ticker symbol cannot be empty")]
    Empty,

    #[error("ticker symbol exceeds maximum length of {max} characters")]
    TooLong { max: usize },

    #[error("ticker symbol must contain only ASCII letters, digits, '.' or '-'")]
    InvalidCharacter,
}

#[derive(thiserror::Error, Debug)]
pub enum MarketSegmentError {
    #[error("market segment cannot be empty")]
    Empty,

    #[error("market segment exceeds maximum length of {max} characters")]
    TooLong { max: usize },
}

#[derive(thiserror::Error, Debug)]
pub enum ListingRoleError {
    #[error("Invalid role. Must be one of: {roles:?}", roles = vec!["PRIMARY", "SECONDARY"])]
    InvalidRole,
}

#[derive(thiserror::Error, Debug)]
pub enum ListingStatusError {
    #[error(
        "Invalid status. Must be one of: {statuses:?}",
        statuses = vec!["ACTIVE", "SUSPENDED", "DELISTED"]
    )]
    InvalidStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum ListingBuilderError {
    #[error("A delisting date requires the DELISTED status. Found: {0}")]
    DelistedDateRequiresDelistedStatus(String),

    #[error("delisting date {delisted_on} precedes listing date {listed_on}")]
    DelistedBeforeListed {
        listed_on: NaiveDate,
        delisted_on: NaiveDate,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterListingError {
    #[error("the referenced security does not exist")]
    UnknownSecurity,

    #[error("the referenced venue does not exist or is not active")]
    VenueNotActive,

    #[error("this ticker symbol is already actively listed on the venue")]
    TickerAlreadyListed,

    #[error("this security is already actively listed on the venue")]
    SecurityAlreadyListedOnVenue,

    #[error("this security already has an active primary listing")]
    PrimaryListingAlreadyExists,

    #[error(transparent)]
    Storage(#[from] StorageFault),
}
