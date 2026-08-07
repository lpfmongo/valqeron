//! Security aggregate: the financial instrument an issuer brings to market,
//! identified globally by its ISIN (ISO 6166) when one is assigned.
//!
//! A security belongs to exactly one issuer and can be admitted to trading on
//! many venues (see the `listing` module). Depositary receipts (e.g. ADRs and
//! BDRs) are securities in their own right — they carry their own ISIN — and
//! may reference the security they wrap via
//! [`Security::underlying_security_id`] plus a [`DrRatio`]
//! (receipts : underlying shares).
//!
//! Vale example: the common share (`BRVALEACNOR0`, [`SecurityKind::CommonShare`])
//! and the NYSE ADR (`US91912E1055`, [`SecurityKind::DepositaryReceipt`],
//! underlying → the common share, ratio 1:1) are two distinct securities of
//! the same issuer.

use crate::issuer::IssuerId;
use crate::security::error::{
    DrRatioError, SecurityBuilderError, SecurityKindError, SecurityNameError, SecurityStatusError,
};
use chrono::{DateTime, Utc};
use ftracker_identifiers::{Cfi, Isin};
use std::num::NonZeroU32;
use std::str::FromStr;
use uuid::Uuid;

pub mod error;
pub mod patch;
pub mod repository;
pub mod service;

const SECURITY_NAME_MAX_LEN: usize = 200;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct SecurityName(String);

impl SecurityName {
    pub fn new(value: impl Into<String>) -> Result<Self, SecurityNameError> {
        let value = value.into();
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(SecurityNameError::Empty);
        }
        if trimmed.chars().count() > SECURITY_NAME_MAX_LEN {
            return Err(SecurityNameError::TooLong {
                max: SECURITY_NAME_MAX_LEN,
            });
        }

        Ok(Self(trimmed.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SecurityId(Uuid);

impl SecurityId {
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

impl Default for SecurityId {
    fn default() -> Self {
        Self::new()
    }
}

/// Kind of equity security. Covers US/BR stock markets today; future
/// instrument families (bonds, funds, ...) arrive as new variants, with the
/// optional CFI (ISO 10962) on [`Security`] carrying the standards-based
/// classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurityKind {
    /// Ordinary/common shares (B3 "ON", e.g. VALE3).
    CommonShare,
    /// Preference shares (B3 "PN"; US preferred stock).
    PreferredShare,
    /// Bundled certificates such as B3 units (e.g. SANB11).
    Unit,
    /// Depositary receipts (ADR/BDR/GDR) wrapping an underlying security.
    DepositaryReceipt,
}

impl SecurityKind {
    pub fn is_depositary_receipt(&self) -> bool {
        matches!(self, SecurityKind::DepositaryReceipt)
    }
}

impl FromStr for SecurityKind {
    type Err = SecurityKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "COMMON_SHARE" => Ok(SecurityKind::CommonShare),
            "PREFERRED_SHARE" => Ok(SecurityKind::PreferredShare),
            "UNIT" => Ok(SecurityKind::Unit),
            "DEPOSITARY_RECEIPT" => Ok(SecurityKind::DepositaryReceipt),
            _ => Err(SecurityKindError::InvalidKind),
        }
    }
}

impl From<SecurityKind> for String {
    fn from(val: SecurityKind) -> Self {
        match val {
            SecurityKind::CommonShare => "COMMON_SHARE".into(),
            SecurityKind::PreferredShare => "PREFERRED_SHARE".into(),
            SecurityKind::Unit => "UNIT".into(),
            SecurityKind::DepositaryReceipt => "DEPOSITARY_RECEIPT".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SecurityStatus {
    #[default]
    Active,
    Retired,
}

impl SecurityStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, SecurityStatus::Active)
    }

    pub fn is_retired(&self) -> bool {
        matches!(self, SecurityStatus::Retired)
    }
}

impl FromStr for SecurityStatus {
    type Err = SecurityStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ACTIVE" => Ok(SecurityStatus::Active),
            "RETIRED" => Ok(SecurityStatus::Retired),
            _ => Err(SecurityStatusError::InvalidStatus),
        }
    }
}

impl From<SecurityStatus> for String {
    fn from(val: SecurityStatus) -> Self {
        match val {
            SecurityStatus::Active => "ACTIVE".into(),
            SecurityStatus::Retired => "RETIRED".into(),
        }
    }
}

/// Depositary receipt ratio expressed as `receipts : underlying` shares.
///
/// `DrRatio::new(1, 1)` — one receipt represents one underlying share (Vale's
/// ADR). `DrRatio::new(1, 2)` — one receipt represents two underlying shares.
/// Zero is unrepresentable on either side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DrRatio {
    receipts: NonZeroU32,
    underlying: NonZeroU32,
}

impl DrRatio {
    pub fn new(receipts: u32, underlying: u32) -> Result<Self, DrRatioError> {
        let receipts = NonZeroU32::new(receipts).ok_or(DrRatioError::ZeroReceipts)?;
        let underlying = NonZeroU32::new(underlying).ok_or(DrRatioError::ZeroUnderlying)?;
        Ok(Self {
            receipts,
            underlying,
        })
    }

    #[must_use]
    pub const fn receipts(&self) -> NonZeroU32 {
        self.receipts
    }

    #[must_use]
    pub const fn underlying(&self) -> NonZeroU32 {
        self.underlying
    }
}

#[derive(Debug)]
pub struct Security {
    id: SecurityId,
    issuer_id: IssuerId,
    kind: SecurityKind,
    status: SecurityStatus,
    created_at: DateTime<Utc>,

    name: Option<SecurityName>,
    isin: Option<Isin>,
    cfi: Option<Cfi>,
    underlying_security_id: Option<SecurityId>,
    dr_ratio: Option<DrRatio>,
}

/// Raw state of a [`Security`] as persisted. Used by storage adapters to
/// rehydrate the entity without re-running builder validation (grouped in a
/// struct because the field count outgrew a positional constructor).
#[derive(Debug)]
pub struct SecuritySnapshot {
    pub id: SecurityId,
    pub issuer_id: IssuerId,
    pub kind: SecurityKind,
    pub status: SecurityStatus,
    pub created_at: DateTime<Utc>,
    pub name: Option<SecurityName>,
    pub isin: Option<Isin>,
    pub cfi: Option<Cfi>,
    pub underlying_security_id: Option<SecurityId>,
    pub dr_ratio: Option<DrRatio>,
}

impl Security {
    pub fn builder(issuer_id: IssuerId, kind: SecurityKind) -> SecurityBuilder {
        SecurityBuilder::new(issuer_id, kind)
    }

    pub fn id(&self) -> &SecurityId {
        &self.id
    }
    pub fn issuer_id(&self) -> &IssuerId {
        &self.issuer_id
    }
    pub fn kind(&self) -> SecurityKind {
        self.kind
    }
    pub fn status(&self) -> SecurityStatus {
        self.status
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn name(&self) -> Option<&SecurityName> {
        self.name.as_ref()
    }
    pub fn isin(&self) -> Option<&Isin> {
        self.isin.as_ref()
    }
    pub fn cfi(&self) -> Option<&Cfi> {
        self.cfi.as_ref()
    }
    pub fn underlying_security_id(&self) -> Option<&SecurityId> {
        self.underlying_security_id.as_ref()
    }
    pub fn dr_ratio(&self) -> Option<&DrRatio> {
        self.dr_ratio.as_ref()
    }

    pub fn reconstitute(snapshot: SecuritySnapshot) -> Self {
        Self {
            id: snapshot.id,
            issuer_id: snapshot.issuer_id,
            kind: snapshot.kind,
            status: snapshot.status,
            created_at: snapshot.created_at,
            name: snapshot.name,
            isin: snapshot.isin,
            cfi: snapshot.cfi,
            underlying_security_id: snapshot.underlying_security_id,
            dr_ratio: snapshot.dr_ratio,
        }
    }
}

pub struct SecurityBuilder {
    issuer_id: IssuerId,
    kind: SecurityKind,
    id: Option<SecurityId>,
    status: Option<SecurityStatus>,
    created_at: Option<DateTime<Utc>>,
    name: Option<SecurityName>,
    isin: Option<Isin>,
    cfi: Option<Cfi>,
    underlying_security_id: Option<SecurityId>,
    dr_ratio: Option<DrRatio>,
}

impl SecurityBuilder {
    pub fn new(issuer_id: IssuerId, kind: SecurityKind) -> Self {
        Self {
            issuer_id,
            kind,
            id: None,
            status: None,
            created_at: None,
            name: None,
            isin: None,
            cfi: None,
            underlying_security_id: None,
            dr_ratio: None,
        }
    }

    pub fn id(mut self, id: SecurityId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn status(mut self, status: SecurityStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = Some(created_at);
        self
    }

    pub fn name(mut self, name: SecurityName) -> Self {
        self.name = Some(name);
        self
    }

    pub fn isin(mut self, isin: Isin) -> Self {
        self.isin = Some(isin);
        self
    }

    pub fn cfi(mut self, cfi: Cfi) -> Self {
        self.cfi = Some(cfi);
        self
    }

    pub fn underlying_security_id(mut self, underlying_security_id: SecurityId) -> Self {
        self.underlying_security_id = Some(underlying_security_id);
        self
    }

    pub fn dr_ratio(mut self, dr_ratio: DrRatio) -> Self {
        self.dr_ratio = Some(dr_ratio);
        self
    }

    pub fn build(self) -> Result<Security, SecurityBuilderError> {
        let id = self.id.unwrap_or_default();
        let status = self.status.unwrap_or_default();
        let created_at = self.created_at.unwrap_or_else(Utc::now);

        // Cross-field validation: depositary receipt fields require the
        // DepositaryReceipt kind.
        if !self.kind.is_depositary_receipt() {
            if self.underlying_security_id.is_some() {
                return Err(SecurityBuilderError::UnderlyingRequiresDepositaryReceipt(
                    self.kind.into(),
                ));
            }
            if self.dr_ratio.is_some() {
                return Err(SecurityBuilderError::DrRatioRequiresDepositaryReceipt(
                    self.kind.into(),
                ));
            }
        }

        // Cross-field validation: a depositary receipt cannot wrap itself.
        if let Some(underlying) = &self.underlying_security_id
            && underlying == &id
        {
            return Err(SecurityBuilderError::SelfUnderlying);
        }

        Ok(Security {
            id,
            issuer_id: self.issuer_id,
            kind: self.kind,
            status,
            created_at,
            name: self.name,
            isin: self.isin,
            cfi: self.cfi,
            underlying_security_id: self.underlying_security_id,
            dr_ratio: self.dr_ratio,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Vale S.A. common shares (B3: VALE3).
    const VALE_ON_ISIN: &str = "BRVALEACNOR0";
    // Vale S.A. sponsored ADR (NYSE: VALE).
    const VALE_ADR_ISIN: &str = "US91912E1055";

    #[test]
    fn security_name_trims_and_validates() {
        let name_result = SecurityName::new(" Vale ON ");
        assert!(name_result.is_ok());
        let Some(name) = name_result.ok() else {
            return;
        };
        assert_eq!(name.as_str(), "Vale ON");
    }

    #[test]
    fn security_name_empty_fails() {
        assert!(matches!(
            SecurityName::new("   "),
            Err(SecurityNameError::Empty)
        ));
    }

    #[test]
    fn security_name_too_long_fails() {
        let long_string = "A".repeat(SECURITY_NAME_MAX_LEN.saturating_add(1));
        assert!(matches!(
            SecurityName::new(long_string),
            Err(SecurityNameError::TooLong { max: 200 })
        ));
    }

    #[test]
    fn security_id_creation_and_conversions() {
        let original_uuid = Uuid::now_v7();
        let id = SecurityId::from_uuid(original_uuid);

        assert_eq!(id.as_uuid(), &original_uuid);
        assert_eq!(id.value(), original_uuid.to_string());
        assert_eq!(id.as_bytes(), original_uuid.as_bytes());
        assert_ne!(SecurityId::new(), SecurityId::new());
    }

    #[test]
    fn security_kind_round_trips() {
        let kinds = [
            (SecurityKind::CommonShare, "COMMON_SHARE"),
            (SecurityKind::PreferredShare, "PREFERRED_SHARE"),
            (SecurityKind::Unit, "UNIT"),
            (SecurityKind::DepositaryReceipt, "DEPOSITARY_RECEIPT"),
        ];
        for (kind, canonical) in kinds {
            let as_string: String = kind.into();
            assert_eq!(as_string, canonical);
            assert!(matches!(
                SecurityKind::from_str(canonical),
                Ok(parsed) if parsed == kind
            ));
        }

        assert!(matches!(
            SecurityKind::from_str("common_share"),
            Ok(SecurityKind::CommonShare)
        ));
        assert!(matches!(
            SecurityKind::from_str("BOND"),
            Err(SecurityKindError::InvalidKind)
        ));
    }

    #[test]
    fn security_status_round_trips() {
        assert!(SecurityStatus::default().is_active());

        let active_str: String = SecurityStatus::Active.into();
        assert_eq!(active_str, "ACTIVE");

        assert!(matches!(
            SecurityStatus::from_str("retired"),
            Ok(SecurityStatus::Retired)
        ));
        assert!(matches!(
            SecurityStatus::from_str("UNKNOWN"),
            Err(SecurityStatusError::InvalidStatus)
        ));
    }

    #[test]
    fn dr_ratio_rejects_zero_on_either_side() {
        assert!(matches!(
            DrRatio::new(0, 1),
            Err(DrRatioError::ZeroReceipts)
        ));
        assert!(matches!(
            DrRatio::new(1, 0),
            Err(DrRatioError::ZeroUnderlying)
        ));

        let ratio_result = DrRatio::new(1, 2);
        assert!(ratio_result.is_ok());
        let Some(ratio) = ratio_result.ok() else {
            return;
        };
        assert_eq!(ratio.receipts().get(), 1);
        assert_eq!(ratio.underlying().get(), 2);
    }

    #[test]
    fn builder_resolves_defaults() {
        let security_result = Security::builder(IssuerId::new(), SecurityKind::CommonShare).build();
        assert!(security_result.is_ok());
        let Some(security) = security_result.ok() else {
            return;
        };

        assert!(security.status().is_active());
        assert!(matches!(security.kind(), SecurityKind::CommonShare));
        assert!(security.name().is_none());
        assert!(security.isin().is_none());
        assert!(security.cfi().is_none());
        assert!(security.underlying_security_id().is_none());
        assert!(security.dr_ratio().is_none());
        assert!(security.created_at() <= Utc::now());
    }

    #[test]
    fn builder_accepts_common_share_with_isin() {
        let isin_result = Isin::new(VALE_ON_ISIN);
        assert!(isin_result.is_ok());
        let Some(isin) = isin_result.ok() else {
            return;
        };

        let security_result = Security::builder(IssuerId::new(), SecurityKind::CommonShare)
            .isin(isin)
            .build();
        assert!(security_result.is_ok());
        let Some(security) = security_result.ok() else {
            return;
        };

        assert!(matches!(security.isin(), Some(i) if i.as_str() == VALE_ON_ISIN));
    }

    #[test]
    fn builder_accepts_depositary_receipt_with_underlying_and_ratio() {
        let underlying_id = SecurityId::new();
        let isin_result = Isin::new(VALE_ADR_ISIN);
        assert!(isin_result.is_ok());
        let Some(isin) = isin_result.ok() else {
            return;
        };
        let ratio_result = DrRatio::new(1, 1);
        assert!(ratio_result.is_ok());
        let Some(ratio) = ratio_result.ok() else {
            return;
        };

        let adr_result = Security::builder(IssuerId::new(), SecurityKind::DepositaryReceipt)
            .isin(isin)
            .underlying_security_id(underlying_id)
            .dr_ratio(ratio)
            .build();
        assert!(adr_result.is_ok());
        let Some(adr) = adr_result.ok() else {
            return;
        };

        assert!(adr.kind().is_depositary_receipt());
        assert!(matches!(
            adr.underlying_security_id(),
            Some(id) if id == &underlying_id
        ));
        assert!(matches!(adr.dr_ratio(), Some(r) if r.receipts().get() == 1));
    }

    #[test]
    fn builder_rejects_underlying_on_non_depositary_receipt() {
        let result = Security::builder(IssuerId::new(), SecurityKind::CommonShare)
            .underlying_security_id(SecurityId::new())
            .build();

        assert!(
            matches!(
                result,
                Err(SecurityBuilderError::UnderlyingRequiresDepositaryReceipt(kind))
                    if kind == "COMMON_SHARE"
            ),
            "Only depositary receipts may reference an underlying security"
        );
    }

    #[test]
    fn builder_rejects_dr_ratio_on_non_depositary_receipt() {
        let ratio_result = DrRatio::new(1, 1);
        assert!(ratio_result.is_ok());
        let Some(ratio) = ratio_result.ok() else {
            return;
        };

        let result = Security::builder(IssuerId::new(), SecurityKind::PreferredShare)
            .dr_ratio(ratio)
            .build();

        assert!(matches!(
            result,
            Err(SecurityBuilderError::DrRatioRequiresDepositaryReceipt(kind))
                if kind == "PREFERRED_SHARE"
        ));
    }

    #[test]
    fn builder_rejects_self_referencing_underlying() {
        let id = SecurityId::new();
        let result = Security::builder(IssuerId::new(), SecurityKind::DepositaryReceipt)
            .id(id)
            .underlying_security_id(id)
            .build();

        assert!(matches!(result, Err(SecurityBuilderError::SelfUnderlying)));
    }
}
