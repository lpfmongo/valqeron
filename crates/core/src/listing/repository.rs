use crate::common::{RepositoryResult, Versioned, WriteOutcome};
use crate::listing::patch::ListingPatch;
use crate::listing::{Listing, ListingId, TickerSymbol};
use crate::security::SecurityId;
use crate::venue::VenueId;
use std::rc::Rc;
use std::sync::Arc;

#[cfg_attr(test, mockall::automock)]
pub trait ListingRepository {
    fn find_by_id(&self, id: &ListingId) -> RepositoryResult<Option<Versioned<Listing>>>;

    /// Resolves an actively traded ticker on a venue, e.g. `VALE3` on B3.
    fn find_active_by_venue_and_symbol(
        &self,
        venue_id: &VenueId,
        symbol: &TickerSymbol,
    ) -> RepositoryResult<Option<Versioned<Listing>>>;

    fn list_all(&self) -> RepositoryResult<Vec<Versioned<Listing>>>;

    fn list_by_security(
        &self,
        security_id: &SecurityId,
    ) -> RepositoryResult<Vec<Versioned<Listing>>>;

    fn list_by_venue(&self, venue_id: &VenueId) -> RepositoryResult<Vec<Versioned<Listing>>>;

    fn list_paged(
        &self,
        after: Option<ListingId>,
        limit: u32,
    ) -> RepositoryResult<Vec<Versioned<Listing>>>;

    fn exists(&self, id: &ListingId) -> RepositoryResult<bool>;

    /// An active listing already occupies this ticker on the venue. Delisted
    /// tickers may be recycled, hence "active".
    fn exists_active_by_venue_and_symbol(
        &self,
        venue_id: &VenueId,
        symbol: &TickerSymbol,
    ) -> RepositoryResult<bool>;

    /// The security already has an active listing on the venue.
    fn exists_active_for_security_on_venue(
        &self,
        security_id: &SecurityId,
        venue_id: &VenueId,
    ) -> RepositoryResult<bool>;

    /// The security already has an active primary listing (on any venue).
    fn exists_active_primary_for_security(
        &self,
        security_id: &SecurityId,
    ) -> RepositoryResult<bool>;

    fn insert(&self, listing: &Listing) -> RepositoryResult<()>;

    fn apply_patch(
        &self,
        id: &ListingId,
        expected_version: u32,
        patch: ListingPatch,
    ) -> RepositoryResult<WriteOutcome>;

    fn update(&self, listing: &Listing, expected_version: u32) -> RepositoryResult<WriteOutcome>;

    fn delete(&self, id: &ListingId, expected_version: u32) -> RepositoryResult<WriteOutcome>;
}

macro_rules! delegate_listing_repository {
    ($ty:ty) => {
        impl<R: ListingRepository + ?Sized> ListingRepository for $ty {
            fn find_by_id(&self, id: &ListingId) -> RepositoryResult<Option<Versioned<Listing>>> {
                (**self).find_by_id(id)
            }
            fn find_active_by_venue_and_symbol(
                &self,
                venue_id: &VenueId,
                symbol: &TickerSymbol,
            ) -> RepositoryResult<Option<Versioned<Listing>>> {
                (**self).find_active_by_venue_and_symbol(venue_id, symbol)
            }
            fn list_all(&self) -> RepositoryResult<Vec<Versioned<Listing>>> {
                (**self).list_all()
            }
            fn list_by_security(
                &self,
                security_id: &SecurityId,
            ) -> RepositoryResult<Vec<Versioned<Listing>>> {
                (**self).list_by_security(security_id)
            }
            fn list_by_venue(
                &self,
                venue_id: &VenueId,
            ) -> RepositoryResult<Vec<Versioned<Listing>>> {
                (**self).list_by_venue(venue_id)
            }
            fn list_paged(
                &self,
                after: Option<ListingId>,
                limit: u32,
            ) -> RepositoryResult<Vec<Versioned<Listing>>> {
                (**self).list_paged(after, limit)
            }
            fn exists(&self, id: &ListingId) -> RepositoryResult<bool> {
                (**self).exists(id)
            }
            fn exists_active_by_venue_and_symbol(
                &self,
                venue_id: &VenueId,
                symbol: &TickerSymbol,
            ) -> RepositoryResult<bool> {
                (**self).exists_active_by_venue_and_symbol(venue_id, symbol)
            }
            fn exists_active_for_security_on_venue(
                &self,
                security_id: &SecurityId,
                venue_id: &VenueId,
            ) -> RepositoryResult<bool> {
                (**self).exists_active_for_security_on_venue(security_id, venue_id)
            }
            fn exists_active_primary_for_security(
                &self,
                security_id: &SecurityId,
            ) -> RepositoryResult<bool> {
                (**self).exists_active_primary_for_security(security_id)
            }
            fn insert(&self, listing: &Listing) -> RepositoryResult<()> {
                (**self).insert(listing)
            }
            fn apply_patch(
                &self,
                id: &ListingId,
                expected_version: u32,
                patch: ListingPatch,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).apply_patch(id, expected_version, patch)
            }
            fn update(
                &self,
                listing: &Listing,
                expected_version: u32,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).update(listing, expected_version)
            }
            fn delete(
                &self,
                id: &ListingId,
                expected_version: u32,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).delete(id, expected_version)
            }
        }
    };
}

delegate_listing_repository!(Box<R>);
delegate_listing_repository!(Rc<R>);
delegate_listing_repository!(Arc<R>);
