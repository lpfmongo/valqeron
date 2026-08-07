use crate::common::{Empty, NonEmpty};
use crate::issuer::{IssuerName, IssuerStatus};
use ftracker_identifiers::{Cnpj, CountryCode, Lei};
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct IssuerPatch {
    pub(crate) name: Option<IssuerName>,
    pub(crate) status: Option<IssuerStatus>,
    pub(crate) cnpj: Option<Cnpj>,
    pub(crate) lei: Option<Lei>,
    pub(crate) country_code: Option<CountryCode>,
}

impl IssuerPatch {
    #[must_use]
    pub const fn builder() -> IssuerPatchBuilder<Empty> {
        IssuerPatchBuilder::new()
    }

    const fn empty() -> Self {
        Self {
            name: None,
            status: None,
            cnpj: None,
            lei: None,
            country_code: None,
        }
    }

    #[must_use]
    pub const fn name(&self) -> Option<&IssuerName> {
        self.name.as_ref()
    }

    #[must_use]
    pub const fn status(&self) -> Option<IssuerStatus> {
        self.status
    }

    #[must_use]
    pub const fn cnpj(&self) -> Option<&Cnpj> {
        self.cnpj.as_ref()
    }

    #[must_use]
    pub const fn lei(&self) -> Option<&Lei> {
        self.lei.as_ref()
    }

    #[must_use]
    pub const fn country_code(&self) -> Option<&CountryCode> {
        self.country_code.as_ref()
    }
}

pub struct IssuerPatchBuilder<State> {
    inner: IssuerPatch,
    _state: PhantomData<State>,
}

impl IssuerPatchBuilder<Empty> {
    const fn new() -> Self {
        Self {
            inner: IssuerPatch::empty(),
            _state: PhantomData,
        }
    }
}

impl<State> IssuerPatchBuilder<State> {
    #[must_use]
    pub fn name(self, name: IssuerName) -> IssuerPatchBuilder<NonEmpty> {
        IssuerPatchBuilder {
            inner: IssuerPatch {
                name: Some(name),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn status(self, status: IssuerStatus) -> IssuerPatchBuilder<NonEmpty> {
        IssuerPatchBuilder {
            inner: IssuerPatch {
                status: Some(status),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn cnpj(self, cnpj: Cnpj) -> IssuerPatchBuilder<NonEmpty> {
        IssuerPatchBuilder {
            inner: IssuerPatch {
                cnpj: Some(cnpj),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn lei(self, lei: Lei) -> IssuerPatchBuilder<NonEmpty> {
        IssuerPatchBuilder {
            inner: IssuerPatch {
                lei: Some(lei),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn country_code(self, country_code: CountryCode) -> IssuerPatchBuilder<NonEmpty> {
        IssuerPatchBuilder {
            inner: IssuerPatch {
                country_code: Some(country_code),
                ..self.inner
            },
            _state: PhantomData,
        }
    }
}

impl IssuerPatchBuilder<NonEmpty> {
    #[must_use]
    pub fn build(self) -> IssuerPatch {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_field_patch_only_sets_that_field() {
        let name_result = IssuerName::new("Renamed Corp");
        assert!(name_result.is_ok());
        let Some(name) = name_result.ok() else {
            return;
        };
        let patch = IssuerPatch::builder().name(name.clone()).build();

        assert_eq!(patch.name, Some(name));
        assert!(patch.status.is_none());
        assert!(patch.cnpj.is_none());
        assert!(patch.lei.is_none());
        assert!(patch.country_code.is_none());
    }
}
