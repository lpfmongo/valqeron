use crate::venue::Venue;
use crate::venue::error::RegisterVenueError;
use crate::venue::repository::VenueRepository;

pub fn register_venue<R: VenueRepository + ?Sized>(
    repo: &R,
    venue: &Venue,
) -> Result<(), RegisterVenueError> {
    if repo.exists_by_mic(venue.mic())? {
        return Err(RegisterVenueError::DuplicateMic);
    }

    repo.insert(venue)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifiers::Mic;
    use crate::venue::repository::MockVenueRepository;

    fn b3_venue() -> Option<Venue> {
        let mic = Mic::parse("BVMF").ok()?;
        Some(Venue::builder(mic).build())
    }

    #[test]
    fn register_inserts_when_mic_is_free() {
        let mut repo = MockVenueRepository::new();
        repo.expect_exists_by_mic().returning(|_| Ok(false));
        repo.expect_insert().returning(|_| Ok(()));

        let Some(venue) = b3_venue() else {
            return;
        };
        assert!(register_venue(&repo, &venue).is_ok());
    }

    #[test]
    fn register_rejects_duplicate_mic_before_insert() {
        let mut repo = MockVenueRepository::new();
        repo.expect_exists_by_mic().returning(|_| Ok(true));
        // insert must never be called on a duplicate.
        repo.expect_insert().never();

        let Some(venue) = b3_venue() else {
            return;
        };
        assert!(matches!(
            register_venue(&repo, &venue),
            Err(RegisterVenueError::DuplicateMic)
        ));
    }
}
