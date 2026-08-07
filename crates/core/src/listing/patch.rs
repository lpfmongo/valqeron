use crate::common::{Empty, NonEmpty};
use crate::identifiers::CurrencyCode;
use crate::listing::{ListingRole, ListingStatus, MarketSegment, TickerSymbol};
use chrono::NaiveDate;
use std::marker::PhantomData;

/// Partial update for a listing. `security_id` and `venue_id` are immutable
/// identity facts (moving venue = new listing); the ticker is patchable
/// because venue-local renames happen (FB → META). Like `IssuerPatch`,
/// cross-field rules are not re-run here; the storage schema backs them.
#[derive(Debug, Clone)]
pub struct ListingPatch {
    pub(crate) symbol: Option<TickerSymbol>,
    pub(crate) role: Option<ListingRole>,
    pub(crate) status: Option<ListingStatus>,
    pub(crate) currency: Option<CurrencyCode>,
    pub(crate) segment: Option<MarketSegment>,
    pub(crate) listed_on: Option<NaiveDate>,
    pub(crate) delisted_on: Option<NaiveDate>,
}

impl ListingPatch {
    #[must_use]
    pub const fn builder() -> ListingPatchBuilder<Empty> {
        ListingPatchBuilder::new()
    }

    const fn empty() -> Self {
        Self {
            symbol: None,
            role: None,
            status: None,
            currency: None,
            segment: None,
            listed_on: None,
            delisted_on: None,
        }
    }

    #[must_use]
    pub const fn symbol(&self) -> Option<&TickerSymbol> {
        self.symbol.as_ref()
    }

    #[must_use]
    pub const fn role(&self) -> Option<ListingRole> {
        self.role
    }

    #[must_use]
    pub const fn status(&self) -> Option<ListingStatus> {
        self.status
    }

    #[must_use]
    pub const fn currency(&self) -> Option<&CurrencyCode> {
        self.currency.as_ref()
    }

    #[must_use]
    pub const fn segment(&self) -> Option<&MarketSegment> {
        self.segment.as_ref()
    }

    #[must_use]
    pub const fn listed_on(&self) -> Option<NaiveDate> {
        self.listed_on
    }

    #[must_use]
    pub const fn delisted_on(&self) -> Option<NaiveDate> {
        self.delisted_on
    }
}

pub struct ListingPatchBuilder<State> {
    inner: ListingPatch,
    _state: PhantomData<State>,
}

impl ListingPatchBuilder<Empty> {
    const fn new() -> Self {
        Self {
            inner: ListingPatch::empty(),
            _state: PhantomData,
        }
    }
}

impl<State> ListingPatchBuilder<State> {
    #[must_use]
    pub fn symbol(self, symbol: TickerSymbol) -> ListingPatchBuilder<NonEmpty> {
        ListingPatchBuilder {
            inner: ListingPatch {
                symbol: Some(symbol),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn role(self, role: ListingRole) -> ListingPatchBuilder<NonEmpty> {
        ListingPatchBuilder {
            inner: ListingPatch {
                role: Some(role),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn status(self, status: ListingStatus) -> ListingPatchBuilder<NonEmpty> {
        ListingPatchBuilder {
            inner: ListingPatch {
                status: Some(status),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn currency(self, currency: CurrencyCode) -> ListingPatchBuilder<NonEmpty> {
        ListingPatchBuilder {
            inner: ListingPatch {
                currency: Some(currency),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn segment(self, segment: MarketSegment) -> ListingPatchBuilder<NonEmpty> {
        ListingPatchBuilder {
            inner: ListingPatch {
                segment: Some(segment),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn listed_on(self, listed_on: NaiveDate) -> ListingPatchBuilder<NonEmpty> {
        ListingPatchBuilder {
            inner: ListingPatch {
                listed_on: Some(listed_on),
                ..self.inner
            },
            _state: PhantomData,
        }
    }

    #[must_use]
    pub fn delisted_on(self, delisted_on: NaiveDate) -> ListingPatchBuilder<NonEmpty> {
        ListingPatchBuilder {
            inner: ListingPatch {
                delisted_on: Some(delisted_on),
                ..self.inner
            },
            _state: PhantomData,
        }
    }
}

impl ListingPatchBuilder<NonEmpty> {
    #[must_use]
    pub fn build(self) -> ListingPatch {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_field_patch_only_sets_that_field() {
        let symbol_result = TickerSymbol::new("META");
        assert!(symbol_result.is_ok());
        let Some(symbol) = symbol_result.ok() else {
            return;
        };
        let patch = ListingPatch::builder().symbol(symbol.clone()).build();

        assert_eq!(patch.symbol, Some(symbol));
        assert!(patch.role.is_none());
        assert!(patch.status.is_none());
        assert!(patch.currency.is_none());
        assert!(patch.segment.is_none());
        assert!(patch.listed_on.is_none());
        assert!(patch.delisted_on.is_none());
    }
}
