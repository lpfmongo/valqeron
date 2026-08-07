use crate::common::{Empty, NonEmpty};
use crate::identifiers::Mic;
use crate::venue::{VenueName, VenueStatus};
use ftracker_identifiers::CountryCode;
use std::marker::PhantomData;

/// Partial update for a venue. The MIC is the venue's identity and is not
/// patchable; retiring one MIC and registering another is a new venue.
#[derive(Debug, Clone)]
pub struct VenuePatch {
    pub(crate) name: Option<VenueName>,
    pub(crate) status: Option<VenueStatus>,
    pub(crate) country_code: Option<CountryCode>,
    pub(crate) operating_mic: Option<Mic>,
}

impl VenuePatch {
    #[must_use]
    pub const fn builder() -> VenuePatchBuilder<Empty> {
        VenuePatchBuilder::new()
    }

    const fn empty() -> Self {
        Self {
            name: None,
            status: None,
            country_code: None,
            operating_mic: None,
        }
    }

    #[must_use]
    pub const fn name(&self) -> Option<&VenueName> {
        self.name.as_ref()
    }

    #[must_use]
    pub const fn status(&self) -> Option<VenueStatus> {
        self.status
    }

    #[must_use]
    pub const fn country_code(&self) -> Option<&CountryCode> {
        self.country_code.as_ref()
    }

    #[must_use]
    pub const fn operating_mic(&self) -> Option<&Mic> {
        self.operating_mic.as_ref()
    }
}

pub struct VenuePatchBuilder<State> {
    inner: VenuePatch,
    _state: PhantomData<State>,
}

impl VenuePatchBuilder<Empty> {
    const fn new() -> Self {
        Self {
            inner: VenuePatch::empty(),
            _state: PhantomData,
        }
    }
}

impl<State> VenuePatchBuilder<State> {
    #[must_use]
    pub fn name(self, name: VenueName) -> VenuePatchBuilder<NonEmpty> {
        VenuePatchBuilder {
            inner: VenuePatch {
                name: Some(name),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn status(self, status: VenueStatus) -> VenuePatchBuilder<NonEmpty> {
        VenuePatchBuilder {
            inner: VenuePatch {
                status: Some(status),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn country_code(self, country_code: CountryCode) -> VenuePatchBuilder<NonEmpty> {
        VenuePatchBuilder {
            inner: VenuePatch {
                country_code: Some(country_code),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn operating_mic(self, operating_mic: Mic) -> VenuePatchBuilder<NonEmpty> {
        VenuePatchBuilder {
            inner: VenuePatch {
                operating_mic: Some(operating_mic),
                ..self.inner
            },
            _state: PhantomData,
        }
    }
}

impl VenuePatchBuilder<NonEmpty> {
    #[must_use]
    pub fn build(self) -> VenuePatch {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_field_patch_only_sets_that_field() {
        let status_patch = VenuePatch::builder().status(VenueStatus::Retired).build();

        assert!(matches!(status_patch.status(), Some(VenueStatus::Retired)));
        assert!(status_patch.name().is_none());
        assert!(status_patch.country_code().is_none());
        assert!(status_patch.operating_mic().is_none());
    }
}
