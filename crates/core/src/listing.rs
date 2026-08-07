//! Listing aggregate: the admission of a security to trading on a venue,
//! under a venue-local ticker symbol (B3 `VALE3`, NYSE `VALE`).
//!
//! A security may be listed on many venues; each listing carries the ticker,
//! trading currency, primary/secondary role, optional venue segment (e.g.
//! B3's "Novo Mercado" governance tier), lifecycle status, and listing dates.
//! Moving to another venue is a new listing — `security_id` and `venue_id`
//! are immutable identity facts.

use crate::identifiers::CurrencyCode;
use crate::listing::error::{
    ListingBuilderError, ListingRoleError, ListingStatusError, MarketSegmentError,
    TickerSymbolError,
};
use crate::security::SecurityId;
use crate::venue::VenueId;
use chrono::{DateTime, NaiveDate, Utc};
use std::str::FromStr;
use uuid::Uuid;

pub mod error;
pub mod patch;
pub mod repository;
pub mod service;

const TICKER_SYMBOL_MAX_LEN: usize = 12;
const MARKET_SEGMENT_MAX_LEN: usize = 100;

/// Venue-local ticker symbol, normalized to uppercase ASCII. Accepts letters,
/// digits, `.` and `-` (covers `VALE3`, `BRK.B`, and friends).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct TickerSymbol(String);

impl TickerSymbol {
    pub fn new(value: impl Into<String>) -> Result<Self, TickerSymbolError> {
        let value = value.into();
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(TickerSymbolError::Empty);
        }
        if trimmed.chars().count() > TICKER_SYMBOL_MAX_LEN {
            return Err(TickerSymbolError::TooLong {
                max: TICKER_SYMBOL_MAX_LEN,
            });
        }

        let normalized = trimmed.to_ascii_uppercase();
        if !normalized
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || matches!(c, '.' | '-'))
        {
            return Err(TickerSymbolError::InvalidCharacter);
        }

        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Venue-specific market segment, e.g. B3's "Novo Mercado" governance tier.
/// Free-form so any venue's segment taxonomy fits.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct MarketSegment(String);

impl MarketSegment {
    pub fn new(value: impl Into<String>) -> Result<Self, MarketSegmentError> {
        let value = value.into();
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(MarketSegmentError::Empty);
        }
        if trimmed.chars().count() > MARKET_SEGMENT_MAX_LEN {
            return Err(MarketSegmentError::TooLong {
                max: MARKET_SEGMENT_MAX_LEN,
            });
        }

        Ok(Self(trimmed.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ListingId(Uuid);

impl ListingId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn value(&self) -> String {
        self.0.to_string()
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Default for ListingId {
    fn default() -> Self {
        Self::new()
    }
}

/// Primary vs secondary listing. At most one active primary listing per
/// security is enforced by the `register_listing` domain service (and by the
/// storage schema).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ListingRole {
    #[default]
    Primary,
    Secondary,
}

impl ListingRole {
    pub fn is_primary(&self) -> bool {
        matches!(self, ListingRole::Primary)
    }

    pub fn is_secondary(&self) -> bool {
        matches!(self, ListingRole::Secondary)
    }
}

impl FromStr for ListingRole {
    type Err = ListingRoleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PRIMARY" => Ok(ListingRole::Primary),
            "SECONDARY" => Ok(ListingRole::Secondary),
            _ => Err(ListingRoleError::InvalidRole),
        }
    }
}

impl From<ListingRole> for String {
    fn from(val: ListingRole) -> Self {
        match val {
            ListingRole::Primary => "PRIMARY".into(),
            ListingRole::Secondary => "SECONDARY".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ListingStatus {
    #[default]
    Active,
    Suspended,
    Delisted,
}

impl ListingStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, ListingStatus::Active)
    }

    pub fn is_suspended(&self) -> bool {
        matches!(self, ListingStatus::Suspended)
    }

    pub fn is_delisted(&self) -> bool {
        matches!(self, ListingStatus::Delisted)
    }
}

impl FromStr for ListingStatus {
    type Err = ListingStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ACTIVE" => Ok(ListingStatus::Active),
            "SUSPENDED" => Ok(ListingStatus::Suspended),
            "DELISTED" => Ok(ListingStatus::Delisted),
            _ => Err(ListingStatusError::InvalidStatus),
        }
    }
}

impl From<ListingStatus> for String {
    fn from(val: ListingStatus) -> Self {
        match val {
            ListingStatus::Active => "ACTIVE".into(),
            ListingStatus::Suspended => "SUSPENDED".into(),
            ListingStatus::Delisted => "DELISTED".into(),
        }
    }
}

#[derive(Debug)]
pub struct Listing {
    id: ListingId,
    security_id: SecurityId,
    venue_id: VenueId,
    symbol: TickerSymbol,
    role: ListingRole,
    status: ListingStatus,
    created_at: DateTime<Utc>,

    currency: Option<CurrencyCode>,
    segment: Option<MarketSegment>,
    listed_on: Option<NaiveDate>,
    delisted_on: Option<NaiveDate>,
}

/// Raw state of a [`Listing`] as persisted. Used by storage adapters to
/// rehydrate the entity without re-running builder validation (grouped in a
/// struct because the field count outgrew a positional constructor).
#[derive(Debug)]
pub struct ListingSnapshot {
    pub id: ListingId,
    pub security_id: SecurityId,
    pub venue_id: VenueId,
    pub symbol: TickerSymbol,
    pub role: ListingRole,
    pub status: ListingStatus,
    pub created_at: DateTime<Utc>,
    pub currency: Option<CurrencyCode>,
    pub segment: Option<MarketSegment>,
    pub listed_on: Option<NaiveDate>,
    pub delisted_on: Option<NaiveDate>,
}

impl Listing {
    pub fn builder(
        security_id: SecurityId,
        venue_id: VenueId,
        symbol: TickerSymbol,
    ) -> ListingBuilder {
        ListingBuilder::new(security_id, venue_id, symbol)
    }

    pub fn id(&self) -> &ListingId {
        &self.id
    }
    pub fn security_id(&self) -> &SecurityId {
        &self.security_id
    }
    pub fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }
    pub fn symbol(&self) -> &TickerSymbol {
        &self.symbol
    }
    pub fn role(&self) -> ListingRole {
        self.role
    }
    pub fn status(&self) -> ListingStatus {
        self.status
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn currency(&self) -> Option<&CurrencyCode> {
        self.currency.as_ref()
    }
    pub fn segment(&self) -> Option<&MarketSegment> {
        self.segment.as_ref()
    }
    pub fn listed_on(&self) -> Option<NaiveDate> {
        self.listed_on
    }
    pub fn delisted_on(&self) -> Option<NaiveDate> {
        self.delisted_on
    }

    pub fn reconstitute(snapshot: ListingSnapshot) -> Self {
        Self {
            id: snapshot.id,
            security_id: snapshot.security_id,
            venue_id: snapshot.venue_id,
            symbol: snapshot.symbol,
            role: snapshot.role,
            status: snapshot.status,
            created_at: snapshot.created_at,
            currency: snapshot.currency,
            segment: snapshot.segment,
            listed_on: snapshot.listed_on,
            delisted_on: snapshot.delisted_on,
        }
    }
}

pub struct ListingBuilder {
    security_id: SecurityId,
    venue_id: VenueId,
    symbol: TickerSymbol,
    id: Option<ListingId>,
    role: Option<ListingRole>,
    status: Option<ListingStatus>,
    created_at: Option<DateTime<Utc>>,
    currency: Option<CurrencyCode>,
    segment: Option<MarketSegment>,
    listed_on: Option<NaiveDate>,
    delisted_on: Option<NaiveDate>,
}

impl ListingBuilder {
    pub fn new(security_id: SecurityId, venue_id: VenueId, symbol: TickerSymbol) -> Self {
        Self {
            security_id,
            venue_id,
            symbol,
            id: None,
            role: None,
            status: None,
            created_at: None,
            currency: None,
            segment: None,
            listed_on: None,
            delisted_on: None,
        }
    }

    pub fn id(mut self, id: ListingId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn role(mut self, role: ListingRole) -> Self {
        self.role = Some(role);
        self
    }

    pub fn status(mut self, status: ListingStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = Some(created_at);
        self
    }

    pub fn currency(mut self, currency: CurrencyCode) -> Self {
        self.currency = Some(currency);
        self
    }

    pub fn segment(mut self, segment: MarketSegment) -> Self {
        self.segment = Some(segment);
        self
    }

    pub fn listed_on(mut self, listed_on: NaiveDate) -> Self {
        self.listed_on = Some(listed_on);
        self
    }

    pub fn delisted_on(mut self, delisted_on: NaiveDate) -> Self {
        self.delisted_on = Some(delisted_on);
        self
    }

    pub fn build(self) -> Result<Listing, ListingBuilderError> {
        let id = self.id.unwrap_or_default();
        let role = self.role.unwrap_or_default();
        let status = self.status.unwrap_or_default();
        let created_at = self.created_at.unwrap_or_else(Utc::now);

        // Cross-field validation: a delisting date implies the listing is
        // delisted (the reverse is allowed - the date may be unknown).
        if self.delisted_on.is_some() && !status.is_delisted() {
            return Err(ListingBuilderError::DelistedDateRequiresDelistedStatus(
                status.into(),
            ));
        }

        // Cross-field validation: chronology.
        if let Some(listed_on) = self.listed_on
            && let Some(delisted_on) = self.delisted_on
            && delisted_on < listed_on
        {
            return Err(ListingBuilderError::DelistedBeforeListed {
                listed_on,
                delisted_on,
            });
        }

        Ok(Listing {
            id,
            security_id: self.security_id,
            venue_id: self.venue_id,
            symbol: self.symbol,
            role,
            status,
            created_at,
            currency: self.currency,
            segment: self.segment,
            listed_on: self.listed_on,
            delisted_on: self.delisted_on,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol(value: &str) -> Option<TickerSymbol> {
        TickerSymbol::new(value).ok()
    }

    fn date(year: i32, month: u32, day: u32) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(year, month, day)
    }

    #[test]
    fn ticker_symbol_trims_and_normalizes_to_uppercase() {
        let symbol_result = TickerSymbol::new(" vale3 ");
        assert!(symbol_result.is_ok());
        let Some(symbol) = symbol_result.ok() else {
            return;
        };
        assert_eq!(symbol.as_str(), "VALE3");
    }

    #[test]
    fn ticker_symbol_accepts_class_separators() {
        assert!(matches!(
            TickerSymbol::new("BRK.B"),
            Ok(s) if s.as_str() == "BRK.B"
        ));
        assert!(matches!(
            TickerSymbol::new("bf-b"),
            Ok(s) if s.as_str() == "BF-B"
        ));
    }

    #[test]
    fn ticker_symbol_rejects_empty() {
        assert!(matches!(
            TickerSymbol::new("  "),
            Err(TickerSymbolError::Empty)
        ));
    }

    #[test]
    fn ticker_symbol_rejects_too_long() {
        let long_symbol = "A".repeat(TICKER_SYMBOL_MAX_LEN.saturating_add(1));
        assert!(matches!(
            TickerSymbol::new(long_symbol),
            Err(TickerSymbolError::TooLong { max: 12 })
        ));
    }

    #[test]
    fn ticker_symbol_rejects_invalid_characters() {
        assert!(matches!(
            TickerSymbol::new("VALE 3"),
            Err(TickerSymbolError::InvalidCharacter)
        ));
        assert!(matches!(
            TickerSymbol::new("VALÉ3"),
            Err(TickerSymbolError::InvalidCharacter)
        ));
    }

    #[test]
    fn market_segment_trims_and_validates() {
        let segment_result = MarketSegment::new(" Novo Mercado ");
        assert!(segment_result.is_ok());
        let Some(segment) = segment_result.ok() else {
            return;
        };
        assert_eq!(segment.as_str(), "Novo Mercado");

        assert!(matches!(
            MarketSegment::new("  "),
            Err(MarketSegmentError::Empty)
        ));
        let long_segment = "A".repeat(MARKET_SEGMENT_MAX_LEN.saturating_add(1));
        assert!(matches!(
            MarketSegment::new(long_segment),
            Err(MarketSegmentError::TooLong { max: 100 })
        ));
    }

    #[test]
    fn listing_id_creation_and_conversions() {
        let original_uuid = Uuid::now_v7();
        let id = ListingId::from_uuid(original_uuid);

        assert_eq!(id.as_uuid(), &original_uuid);
        assert_eq!(id.value(), original_uuid.to_string());
        assert_eq!(id.as_bytes(), original_uuid.as_bytes());
        assert_ne!(ListingId::new(), ListingId::new());
    }

    #[test]
    fn listing_role_round_trips() {
        assert!(ListingRole::default().is_primary());

        let primary_str: String = ListingRole::Primary.into();
        assert_eq!(primary_str, "PRIMARY");
        let secondary_str: String = ListingRole::Secondary.into();
        assert_eq!(secondary_str, "SECONDARY");

        assert!(matches!(
            ListingRole::from_str("secondary"),
            Ok(ListingRole::Secondary)
        ));
        assert!(matches!(
            ListingRole::from_str("TERTIARY"),
            Err(ListingRoleError::InvalidRole)
        ));
    }

    #[test]
    fn listing_status_round_trips() {
        assert!(ListingStatus::default().is_active());

        for (status, canonical) in [
            (ListingStatus::Active, "ACTIVE"),
            (ListingStatus::Suspended, "SUSPENDED"),
            (ListingStatus::Delisted, "DELISTED"),
        ] {
            let as_string: String = status.into();
            assert_eq!(as_string, canonical);
            assert!(matches!(
                ListingStatus::from_str(canonical),
                Ok(parsed) if parsed == status
            ));
        }

        assert!(matches!(
            ListingStatus::from_str("HALTED"),
            Err(ListingStatusError::InvalidStatus)
        ));
    }

    #[test]
    fn builder_resolves_defaults() {
        let Some(vale3) = symbol("VALE3") else {
            return;
        };
        let listing_result = Listing::builder(SecurityId::new(), VenueId::new(), vale3).build();
        assert!(listing_result.is_ok());
        let Some(listing) = listing_result.ok() else {
            return;
        };

        assert_eq!(listing.symbol().as_str(), "VALE3");
        assert!(listing.role().is_primary());
        assert!(listing.status().is_active());
        assert!(listing.currency().is_none());
        assert!(listing.segment().is_none());
        assert!(listing.listed_on().is_none());
        assert!(listing.delisted_on().is_none());
        assert!(listing.created_at() <= Utc::now());
    }

    #[test]
    fn builder_accepts_full_brazilian_listing() {
        let Some(vale3) = symbol("VALE3") else {
            return;
        };
        let currency_result = CurrencyCode::parse("BRL");
        assert!(currency_result.is_ok());
        let Some(brl) = currency_result.ok() else {
            return;
        };
        let segment_result = MarketSegment::new("Novo Mercado");
        assert!(segment_result.is_ok());
        let Some(novo_mercado) = segment_result.ok() else {
            return;
        };
        let Some(listed_on) = date(2008, 5, 12) else {
            return;
        };

        let listing_result = Listing::builder(SecurityId::new(), VenueId::new(), vale3)
            .role(ListingRole::Primary)
            .currency(brl)
            .segment(novo_mercado)
            .listed_on(listed_on)
            .build();
        assert!(listing_result.is_ok());
        let Some(listing) = listing_result.ok() else {
            return;
        };

        assert!(matches!(listing.currency(), Some(c) if c.as_str() == "BRL"));
        assert!(matches!(listing.segment(), Some(s) if s.as_str() == "Novo Mercado"));
        assert_eq!(listing.listed_on(), Some(listed_on));
    }

    #[test]
    fn builder_rejects_delisted_date_without_delisted_status() {
        let Some(vale3) = symbol("VALE3") else {
            return;
        };
        let Some(delisted_on) = date(2020, 1, 2) else {
            return;
        };

        let result = Listing::builder(SecurityId::new(), VenueId::new(), vale3)
            .delisted_on(delisted_on)
            .build();

        assert!(
            matches!(
                result,
                Err(ListingBuilderError::DelistedDateRequiresDelistedStatus(status))
                    if status == "ACTIVE"
            ),
            "A delisting date requires the DELISTED status"
        );
    }

    #[test]
    fn builder_accepts_delisted_listing_with_dates_in_order() {
        let Some(vale3) = symbol("VALE3") else {
            return;
        };
        let Some(listed_on) = date(2008, 5, 12) else {
            return;
        };
        let Some(delisted_on) = date(2020, 1, 2) else {
            return;
        };

        let listing_result = Listing::builder(SecurityId::new(), VenueId::new(), vale3)
            .status(ListingStatus::Delisted)
            .listed_on(listed_on)
            .delisted_on(delisted_on)
            .build();

        assert!(listing_result.is_ok());
    }

    #[test]
    fn builder_rejects_delisting_before_listing() {
        let Some(vale3) = symbol("VALE3") else {
            return;
        };
        let Some(listed_on) = date(2020, 1, 2) else {
            return;
        };
        let Some(delisted_on) = date(2008, 5, 12) else {
            return;
        };

        let result = Listing::builder(SecurityId::new(), VenueId::new(), vale3)
            .status(ListingStatus::Delisted)
            .listed_on(listed_on)
            .delisted_on(delisted_on)
            .build();

        assert!(matches!(
            result,
            Err(ListingBuilderError::DelistedBeforeListed { .. })
        ));
    }
}
