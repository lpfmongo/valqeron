mod calendar;
mod common;
mod identifiers;
mod issuer;
mod listing;
mod schedule;
mod security;
mod storage;
mod sync;
mod task;
mod task_registration;
mod venue;

pub use common::{LoadMode, Loading, RepositoryResult, Versioned, WriteOutcome};

#[doc(hidden)]
pub use common::{Empty, NonEmpty};

pub use storage::{PersistenceManager, Repositories, StorageEngine, StorageError, StorageFault};

pub use valqeron_identifiers::{
    Cfi, CfiError, Cnpj, CnpjError, CountryCode, CountryCodeError, Isin, IsinError, Lei, LeiError,
    Mic, MicError,
};

// Interim home for CurrencyCode (ISO 4217) until valqeron-identifiers ships
// it; only this re-export changes when it moves.
pub use identifiers::{CurrencyCode, CurrencyCodeError};

pub use issuer::patch::IssuerPatch;

#[doc(hidden)]
pub use issuer::patch::IssuerPatchBuilder;

pub use issuer::repository::IssuerRepository;

pub use issuer::service::register_issuer;

pub use issuer::{
    Issuer, IssuerBuilder, IssuerId, IssuerName, IssuerSnapshot, IssuerStatus,
    error::{IssuerBuilderError, IssuerNameError, IssuerStatusError, RegisterIssuerError},
};

pub use venue::patch::VenuePatch;

#[doc(hidden)]
pub use venue::patch::VenuePatchBuilder;

pub use venue::repository::VenueRepository;

pub use venue::service::register_venue;

pub use venue::{
    Venue, VenueBuilder, VenueId, VenueName, VenueStatus,
    error::{RegisterVenueError, VenueNameError, VenueStatusError},
};

pub use security::patch::SecurityPatch;

#[doc(hidden)]
pub use security::patch::SecurityPatchBuilder;

pub use security::repository::SecurityRepository;

pub use security::service::register_security;

pub use security::{
    DepositaryReceiptRatio, Security, SecurityBuilder, SecurityId, SecurityKind, SecurityName,
    SecuritySnapshot, SecurityStatus,
    error::{
        DrRatioError, RegisterSecurityError, SecurityBuilderError, SecurityKindError,
        SecurityNameError, SecurityStatusError,
    },
};

pub use listing::patch::ListingPatch;

#[doc(hidden)]
pub use listing::patch::ListingPatchBuilder;

pub use listing::repository::ListingRepository;

pub use listing::service::register_listing;

pub use listing::{
    Listing, ListingBuilder, ListingId, ListingRole, ListingSnapshot, ListingStatus, MarketSegment,
    TickerSymbol,
    error::{
        ListingBuilderError, ListingRoleError, ListingStatusError, MarketSegmentError,
        RegisterListingError, TickerSymbolError,
    },
};

pub use task::repository::BackgroundTaskRepository;

pub use task::{
    BackgroundTask, BackgroundTaskBuilder, BackgroundTaskSnapshot, TaskCompletion, TaskId,
    TaskKind, TaskStatus,
    error::{TaskBuilderError, TaskKindError, TaskStatusError},
};

pub use calendar::MarketCalendar;

pub use schedule::{Recurrence, Schedule, TargetPeriod};

pub use sync::repository::SyncCursorRepository;

pub use sync::{
    SyncCursor, SyncCursorSnapshot, SyncOutcome, SyncOutcomeKind, SyncSource,
    cooldown::CooldownPolicy,
    error::{SyncOutcomeKindError, SyncSourceError},
};

pub use task_registration::repository::TaskRegistrationRepository;

pub use task_registration::service::{TaskStatusEntry, list_task_statuses};

pub use task_registration::{
    DerivedTaskStatus, LogPolicy, RunOutcome, TaskCategory, TaskDeclaration, TaskRegistration,
    TaskRegistrationSnapshot, TaskTier, TaskTracking, derive_status,
    error::{LogPolicyError, RunOutcomeError, TaskCategoryError, TaskTierError, TaskTrackingError},
};
