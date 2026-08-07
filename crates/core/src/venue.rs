//! Trading venue aggregate: the place where securities are admitted to
//! trading, keyed by its ISO 10383 MIC (e.g. `BVMF` for B3, `XNYS` for NYSE).
//!
//! The ISO 10383 operating/segment hierarchy is captured by
//! [`Venue::operating_mic`]: a segment venue (e.g. `XNGS`, Nasdaq Global
//! Select) points at its market operator (`XNAS`), while operator rows may
//! reference themselves — both `None` and `operating_mic == mic` mean "this
//! venue is the operator", matching the ISO registry convention. Country-level
//! market grouping (US/BR) comes from [`Venue::country_code`].

use crate::identifiers::Mic;
use crate::venue::error::{VenueNameError, VenueStatusError};
use chrono::{DateTime, Utc};
use ftracker_identifiers::CountryCode;
use std::str::FromStr;
use uuid::Uuid;

pub mod error;
pub mod patch;
pub mod repository;
pub mod service;

const VENUE_NAME_MAX_LEN: usize = 200;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct VenueName(String);

impl VenueName {
    pub fn new(value: impl Into<String>) -> Result<Self, VenueNameError> {
        let value = value.into();
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(VenueNameError::Empty);
        }
        if trimmed.chars().count() > VENUE_NAME_MAX_LEN {
            return Err(VenueNameError::TooLong {
                max: VENUE_NAME_MAX_LEN,
            });
        }

        Ok(Self(trimmed.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct VenueId(Uuid);

impl VenueId {
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

impl Default for VenueId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VenueStatus {
    #[default]
    Active,
    Retired,
}

impl VenueStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, VenueStatus::Active)
    }

    pub fn is_retired(&self) -> bool {
        matches!(self, VenueStatus::Retired)
    }
}

impl FromStr for VenueStatus {
    type Err = VenueStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ACTIVE" => Ok(VenueStatus::Active),
            "RETIRED" => Ok(VenueStatus::Retired),
            _ => Err(VenueStatusError::InvalidStatus),
        }
    }
}

impl From<VenueStatus> for String {
    fn from(val: VenueStatus) -> Self {
        match val {
            VenueStatus::Active => "ACTIVE".into(),
            VenueStatus::Retired => "RETIRED".into(),
        }
    }
}

#[derive(Debug)]
pub struct Venue {
    id: VenueId,
    mic: Mic,
    status: VenueStatus,
    created_at: DateTime<Utc>,

    name: Option<VenueName>,
    country_code: Option<CountryCode>,
    operating_mic: Option<Mic>,
}

impl Venue {
    pub fn builder(mic: Mic) -> VenueBuilder {
        VenueBuilder::new(mic)
    }

    pub fn id(&self) -> &VenueId {
        &self.id
    }
    pub fn mic(&self) -> &Mic {
        &self.mic
    }
    pub fn status(&self) -> VenueStatus {
        self.status
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn name(&self) -> Option<&VenueName> {
        self.name.as_ref()
    }
    pub fn country_code(&self) -> Option<&CountryCode> {
        self.country_code.as_ref()
    }
    pub fn operating_mic(&self) -> Option<&Mic> {
        self.operating_mic.as_ref()
    }

    /// This venue operates its own market (no operator reference, or an ISO
    /// registry style self-reference).
    pub fn is_operating_venue(&self) -> bool {
        match &self.operating_mic {
            None => true,
            Some(operating) => operating == &self.mic,
        }
    }

    pub fn reconstitute(
        id: VenueId,
        mic: Mic,
        status: VenueStatus,
        created_at: DateTime<Utc>,
        name: Option<VenueName>,
        country_code: Option<CountryCode>,
        operating_mic: Option<Mic>,
    ) -> Self {
        Self {
            id,
            mic,
            status,
            created_at,
            name,
            country_code,
            operating_mic,
        }
    }
}

pub struct VenueBuilder {
    mic: Mic,
    id: Option<VenueId>,
    status: Option<VenueStatus>,
    created_at: Option<DateTime<Utc>>,
    name: Option<VenueName>,
    country_code: Option<CountryCode>,
    operating_mic: Option<Mic>,
}

impl VenueBuilder {
    pub fn new(mic: Mic) -> Self {
        Self {
            mic,
            id: None,
            status: None,
            created_at: None,
            name: None,
            country_code: None,
            operating_mic: None,
        }
    }

    pub fn id(mut self, id: VenueId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn status(mut self, status: VenueStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = Some(created_at);
        self
    }

    pub fn name(mut self, name: VenueName) -> Self {
        self.name = Some(name);
        self
    }

    pub fn country_code(mut self, country_code: CountryCode) -> Self {
        self.country_code = Some(country_code);
        self
    }

    pub fn operating_mic(mut self, operating_mic: Mic) -> Self {
        self.operating_mic = Some(operating_mic);
        self
    }

    /// Venue has no cross-field invariants today, so building is infallible.
    /// `operating_mic == mic` is deliberately allowed (ISO 10383 operator rows
    /// self-reference) and the operator's existence is not checked, keeping
    /// seeding order free.
    #[must_use]
    pub fn build(self) -> Venue {
        Venue {
            id: self.id.unwrap_or_default(),
            mic: self.mic,
            status: self.status.unwrap_or_default(),
            created_at: self.created_at.unwrap_or_else(Utc::now),
            name: self.name,
            country_code: self.country_code,
            operating_mic: self.operating_mic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mic(code: &str) -> Option<Mic> {
        Mic::parse(code).ok()
    }

    #[test]
    fn venue_name_trims_and_validates() {
        let name_result = VenueName::new(" B3 S.A. - Brasil, Bolsa, Balcao ");
        assert!(name_result.is_ok());
        let Some(name) = name_result.ok() else {
            return;
        };
        assert_eq!(name.as_str(), "B3 S.A. - Brasil, Bolsa, Balcao");
    }

    #[test]
    fn venue_name_empty_fails() {
        assert!(matches!(VenueName::new("  "), Err(VenueNameError::Empty)));
    }

    #[test]
    fn venue_name_too_long_fails() {
        let long_string = "A".repeat(VENUE_NAME_MAX_LEN.saturating_add(1));
        assert!(matches!(
            VenueName::new(long_string),
            Err(VenueNameError::TooLong { max: 200 })
        ));
    }

    #[test]
    fn venue_id_creation_and_conversions() {
        let original_uuid = Uuid::now_v7();
        let id = VenueId::from_uuid(original_uuid);

        assert_eq!(id.as_uuid(), &original_uuid);
        assert_eq!(id.value(), original_uuid.to_string());
        assert_eq!(id.as_bytes(), original_uuid.as_bytes());
        assert_ne!(VenueId::new(), VenueId::new());
    }

    #[test]
    fn venue_status_round_trips() {
        assert!(VenueStatus::default().is_active());

        let active_str: String = VenueStatus::Active.into();
        assert_eq!(active_str, "ACTIVE");
        let retired_str: String = VenueStatus::Retired.into();
        assert_eq!(retired_str, "RETIRED");

        assert!(matches!(
            VenueStatus::from_str("active"),
            Ok(VenueStatus::Active)
        ));
        assert!(matches!(
            VenueStatus::from_str("Retired"),
            Ok(VenueStatus::Retired)
        ));
        assert!(matches!(
            VenueStatus::from_str("UNKNOWN"),
            Err(VenueStatusError::InvalidStatus)
        ));
    }

    #[test]
    fn builder_resolves_defaults() {
        let Some(bvmf) = mic("BVMF") else {
            return;
        };
        let venue = Venue::builder(bvmf).build();

        assert_eq!(venue.mic().as_str(), "BVMF");
        assert!(venue.status().is_active());
        assert!(venue.name().is_none());
        assert!(venue.country_code().is_none());
        assert!(venue.operating_mic().is_none());
        assert!(venue.is_operating_venue());
        assert!(venue.created_at() <= Utc::now());
    }

    #[test]
    fn builder_accepts_self_referencing_operating_mic() {
        let Some(xnas) = mic("XNAS") else {
            return;
        };
        let venue = Venue::builder(xnas).operating_mic(xnas).build();

        assert!(
            venue.is_operating_venue(),
            "ISO registry operator rows self-reference their own MIC"
        );
    }

    #[test]
    fn segment_venue_points_at_its_operator() {
        let Some(xngs) = mic("XNGS") else {
            return;
        };
        let Some(xnas) = mic("XNAS") else {
            return;
        };
        let country_result = CountryCode::from_str("US");
        assert!(country_result.is_ok());
        let Some(country) = country_result.ok() else {
            return;
        };

        let venue = Venue::builder(xngs)
            .operating_mic(xnas)
            .country_code(country)
            .build();

        assert!(!venue.is_operating_venue());
        assert!(matches!(venue.operating_mic(), Some(op) if op.as_str() == "XNAS"));
        assert!(matches!(venue.country_code(), Some(c) if c.as_str() == "US"));
    }
}
