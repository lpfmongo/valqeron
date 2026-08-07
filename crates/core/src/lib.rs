mod common;
mod identifiers;
mod issuer;
mod listing;
mod security;
mod storage;
mod venue;

pub use common::{RepositoryResult, Versioned, WriteOutcome};

#[doc(hidden)]
pub use common::{Empty, NonEmpty};

pub use storage::{PersistenceManager, Repositories, StorageEngine, StorageError, StorageFault};

pub use ftracker_identifiers::{Cfi, Cnpj, CountryCode, Isin, Lei};

// Interim home for Mic / CurrencyCode until ftracker-identifiers ships them;
// only this re-export changes when they move.
pub use identifiers::{CurrencyCode, CurrencyCodeError, Mic, MicError};

pub use issuer::patch::IssuerPatch;

#[doc(hidden)]
pub use issuer::patch::IssuerPatchBuilder;

pub use issuer::repository::IssuerRepository;

pub use issuer::service::register_issuer;

pub use issuer::{
    Issuer, IssuerBuilder, IssuerId, IssuerName, IssuerStatus,
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
    DrRatio, Security, SecurityBuilder, SecurityId, SecurityKind, SecurityName, SecuritySnapshot,
    SecurityStatus,
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
