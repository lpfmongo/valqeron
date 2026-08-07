use crate::listing::Listing;
use crate::listing::error::RegisterListingError;
use crate::listing::repository::ListingRepository;
use crate::security::repository::SecurityRepository;
use crate::venue::repository::VenueRepository;

/// Registers a new listing after validating its cross-aggregate references
/// and market invariants:
///
/// 1. the security must exist;
/// 2. the venue must exist and be active;
/// 3. the ticker must not be actively listed on the venue (delisted tickers
///    may be recycled);
/// 4. the security must not already be actively listed on the venue;
/// 5. a primary listing requires the security to have no other active
///    primary listing.
pub fn register_listing<L, S, V>(
    listings: &L,
    securities: &S,
    venues: &V,
    listing: &Listing,
) -> Result<(), RegisterListingError>
where
    L: ListingRepository + ?Sized,
    S: SecurityRepository + ?Sized,
    V: VenueRepository + ?Sized,
{
    if !securities.exists(listing.security_id())? {
        return Err(RegisterListingError::UnknownSecurity);
    }

    let Some(venue) = venues.find_by_id(listing.venue_id())? else {
        return Err(RegisterListingError::VenueNotActive);
    };
    if !venue.data.status().is_active() {
        return Err(RegisterListingError::VenueNotActive);
    }

    if listings.exists_active_by_venue_and_symbol(listing.venue_id(), listing.symbol())? {
        return Err(RegisterListingError::TickerAlreadyListed);
    }

    if listings.exists_active_for_security_on_venue(listing.security_id(), listing.venue_id())? {
        return Err(RegisterListingError::SecurityAlreadyListedOnVenue);
    }

    if listing.role().is_primary()
        && listings.exists_active_primary_for_security(listing.security_id())?
    {
        return Err(RegisterListingError::PrimaryListingAlreadyExists);
    }

    listings.insert(listing)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Versioned;
    use crate::identifiers::Mic;
    use crate::listing::repository::MockListingRepository;
    use crate::listing::{ListingRole, TickerSymbol};
    use crate::security::SecurityId;
    use crate::security::repository::MockSecurityRepository;
    use crate::venue::repository::MockVenueRepository;
    use crate::venue::{Venue, VenueId, VenueStatus};

    fn vale3_listing(role: ListingRole) -> Option<Listing> {
        let symbol = TickerSymbol::new("VALE3").ok()?;
        Listing::builder(SecurityId::new(), VenueId::new(), symbol)
            .role(role)
            .build()
            .ok()
    }

    fn securities_with_existing_security() -> MockSecurityRepository {
        let mut securities = MockSecurityRepository::new();
        securities.expect_exists().returning(|_| Ok(true));
        securities
    }

    fn venues_returning_status(status: VenueStatus) -> MockVenueRepository {
        let mut venues = MockVenueRepository::new();
        venues.expect_find_by_id().returning(move |_| {
            let Some(mic) = Mic::parse("BVMF").ok() else {
                return Ok(None);
            };
            Ok(Some(Versioned {
                data: Venue::builder(mic).status(status).build(),
                version: 1,
            }))
        });
        venues
    }

    #[test]
    fn register_inserts_when_all_invariants_hold() {
        let securities = securities_with_existing_security();
        let venues = venues_returning_status(VenueStatus::Active);

        let mut listings = MockListingRepository::new();
        listings
            .expect_exists_active_by_venue_and_symbol()
            .returning(|_, _| Ok(false));
        listings
            .expect_exists_active_for_security_on_venue()
            .returning(|_, _| Ok(false));
        listings
            .expect_exists_active_primary_for_security()
            .returning(|_| Ok(false));
        listings.expect_insert().returning(|_| Ok(()));

        let Some(listing) = vale3_listing(ListingRole::Primary) else {
            return;
        };
        assert!(register_listing(&listings, &securities, &venues, &listing).is_ok());
    }

    #[test]
    fn register_rejects_unknown_security_before_insert() {
        let mut securities = MockSecurityRepository::new();
        securities.expect_exists().returning(|_| Ok(false));
        let venues = MockVenueRepository::new();

        let mut listings = MockListingRepository::new();
        // insert must never be called for an unknown security.
        listings.expect_insert().never();

        let Some(listing) = vale3_listing(ListingRole::Primary) else {
            return;
        };
        assert!(matches!(
            register_listing(&listings, &securities, &venues, &listing),
            Err(RegisterListingError::UnknownSecurity)
        ));
    }

    #[test]
    fn register_rejects_missing_venue_before_insert() {
        let securities = securities_with_existing_security();
        let mut venues = MockVenueRepository::new();
        venues.expect_find_by_id().returning(|_| Ok(None));

        let mut listings = MockListingRepository::new();
        listings.expect_insert().never();

        let Some(listing) = vale3_listing(ListingRole::Primary) else {
            return;
        };
        assert!(matches!(
            register_listing(&listings, &securities, &venues, &listing),
            Err(RegisterListingError::VenueNotActive)
        ));
    }

    #[test]
    fn register_rejects_retired_venue_before_insert() {
        let securities = securities_with_existing_security();
        let venues = venues_returning_status(VenueStatus::Retired);

        let mut listings = MockListingRepository::new();
        listings.expect_insert().never();

        let Some(listing) = vale3_listing(ListingRole::Primary) else {
            return;
        };
        assert!(matches!(
            register_listing(&listings, &securities, &venues, &listing),
            Err(RegisterListingError::VenueNotActive)
        ));
    }

    #[test]
    fn register_rejects_taken_ticker_before_insert() {
        let securities = securities_with_existing_security();
        let venues = venues_returning_status(VenueStatus::Active);

        let mut listings = MockListingRepository::new();
        listings
            .expect_exists_active_by_venue_and_symbol()
            .returning(|_, _| Ok(true));
        // insert must never be called when the ticker is taken.
        listings.expect_insert().never();

        let Some(listing) = vale3_listing(ListingRole::Primary) else {
            return;
        };
        assert!(matches!(
            register_listing(&listings, &securities, &venues, &listing),
            Err(RegisterListingError::TickerAlreadyListed)
        ));
    }

    #[test]
    fn register_rejects_security_already_listed_on_venue() {
        let securities = securities_with_existing_security();
        let venues = venues_returning_status(VenueStatus::Active);

        let mut listings = MockListingRepository::new();
        listings
            .expect_exists_active_by_venue_and_symbol()
            .returning(|_, _| Ok(false));
        listings
            .expect_exists_active_for_security_on_venue()
            .returning(|_, _| Ok(true));
        listings.expect_insert().never();

        let Some(listing) = vale3_listing(ListingRole::Primary) else {
            return;
        };
        assert!(matches!(
            register_listing(&listings, &securities, &venues, &listing),
            Err(RegisterListingError::SecurityAlreadyListedOnVenue)
        ));
    }

    #[test]
    fn register_rejects_second_active_primary_listing() {
        let securities = securities_with_existing_security();
        let venues = venues_returning_status(VenueStatus::Active);

        let mut listings = MockListingRepository::new();
        listings
            .expect_exists_active_by_venue_and_symbol()
            .returning(|_, _| Ok(false));
        listings
            .expect_exists_active_for_security_on_venue()
            .returning(|_, _| Ok(false));
        listings
            .expect_exists_active_primary_for_security()
            .returning(|_| Ok(true));
        listings.expect_insert().never();

        let Some(listing) = vale3_listing(ListingRole::Primary) else {
            return;
        };
        assert!(matches!(
            register_listing(&listings, &securities, &venues, &listing),
            Err(RegisterListingError::PrimaryListingAlreadyExists)
        ));
    }

    #[test]
    fn register_allows_secondary_listing_when_primary_exists() {
        let securities = securities_with_existing_security();
        let venues = venues_returning_status(VenueStatus::Active);

        let mut listings = MockListingRepository::new();
        listings
            .expect_exists_active_by_venue_and_symbol()
            .returning(|_, _| Ok(false));
        listings
            .expect_exists_active_for_security_on_venue()
            .returning(|_, _| Ok(false));
        // The primary-uniqueness check must not run for secondary listings.
        listings.expect_exists_active_primary_for_security().never();
        listings.expect_insert().returning(|_| Ok(()));

        let Some(listing) = vale3_listing(ListingRole::Secondary) else {
            return;
        };
        assert!(register_listing(&listings, &securities, &venues, &listing).is_ok());
    }
}
