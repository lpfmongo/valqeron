use crate::common::{RepositoryResult, Versioned, WriteOutcome};
use crate::identifiers::Mic;
use crate::venue::patch::VenuePatch;
use crate::venue::{Venue, VenueId};
use ftracker_identifiers::CountryCode;
use std::rc::Rc;
use std::sync::Arc;

#[cfg_attr(test, mockall::automock)]
pub trait VenueRepository {
    fn find_by_id(&self, id: &VenueId) -> RepositoryResult<Option<Versioned<Venue>>>;

    fn find_by_mic(&self, mic: &Mic) -> RepositoryResult<Option<Versioned<Venue>>>;

    fn list_all(&self) -> RepositoryResult<Vec<Versioned<Venue>>>;

    fn list_by_country(
        &self,
        country_code: &CountryCode,
    ) -> RepositoryResult<Vec<Versioned<Venue>>>;

    fn list_paged(
        &self,
        after: Option<VenueId>,
        limit: u32,
    ) -> RepositoryResult<Vec<Versioned<Venue>>>;

    fn exists(&self, id: &VenueId) -> RepositoryResult<bool>;

    fn exists_by_mic(&self, mic: &Mic) -> RepositoryResult<bool>;

    fn insert(&self, venue: &Venue) -> RepositoryResult<()>;

    fn apply_patch(
        &self,
        id: &VenueId,
        expected_version: u32,
        patch: VenuePatch,
    ) -> RepositoryResult<WriteOutcome>;

    fn update(&self, venue: &Venue, expected_version: u32) -> RepositoryResult<WriteOutcome>;

    fn delete(&self, id: &VenueId, expected_version: u32) -> RepositoryResult<WriteOutcome>;
}

macro_rules! delegate_venue_repository {
    ($ty:ty) => {
        impl<R: VenueRepository + ?Sized> VenueRepository for $ty {
            fn find_by_id(&self, id: &VenueId) -> RepositoryResult<Option<Versioned<Venue>>> {
                (**self).find_by_id(id)
            }
            fn find_by_mic(&self, mic: &Mic) -> RepositoryResult<Option<Versioned<Venue>>> {
                (**self).find_by_mic(mic)
            }
            fn list_all(&self) -> RepositoryResult<Vec<Versioned<Venue>>> {
                (**self).list_all()
            }
            fn list_by_country(
                &self,
                country_code: &CountryCode,
            ) -> RepositoryResult<Vec<Versioned<Venue>>> {
                (**self).list_by_country(country_code)
            }
            fn list_paged(
                &self,
                after: Option<VenueId>,
                limit: u32,
            ) -> RepositoryResult<Vec<Versioned<Venue>>> {
                (**self).list_paged(after, limit)
            }
            fn exists(&self, id: &VenueId) -> RepositoryResult<bool> {
                (**self).exists(id)
            }
            fn exists_by_mic(&self, mic: &Mic) -> RepositoryResult<bool> {
                (**self).exists_by_mic(mic)
            }
            fn insert(&self, venue: &Venue) -> RepositoryResult<()> {
                (**self).insert(venue)
            }
            fn apply_patch(
                &self,
                id: &VenueId,
                expected_version: u32,
                patch: VenuePatch,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).apply_patch(id, expected_version, patch)
            }
            fn update(
                &self,
                venue: &Venue,
                expected_version: u32,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).update(venue, expected_version)
            }
            fn delete(
                &self,
                id: &VenueId,
                expected_version: u32,
            ) -> RepositoryResult<WriteOutcome> {
                (**self).delete(id, expected_version)
            }
        }
    };
}

delegate_venue_repository!(Box<R>);
delegate_venue_repository!(Rc<R>);
delegate_venue_repository!(Arc<R>);
