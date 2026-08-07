//! Interim market and currency identifier types.
//!
//! `Mic` (ISO 10383 Market Identifier Code) and `CurrencyCode` (ISO 4217) are
//! planned additions to the `ftracker-identifiers` crate. Until that release
//! lands, core hosts these format-validated implementations with the same API
//! shape as that crate's types (byte-backed, `Copy`, `new`/`parse`/`as_str`/
//! `as_bytes`/`FromStr`, one error enum per type) so the migration is a
//! re-export swap in `lib.rs`.
//!
//! Validation is format-only (length and character class, with whitespace
//! trimming and ASCII case normalization). Neither type checks the official
//! ISO registry, which keeps any future market usable without a code change.

use std::fmt;
use std::str::FromStr;

const MIC_LEN: usize = 4;
const CURRENCY_CODE_LEN: usize = 3;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MicError {
    #[error("MIC cannot be empty")]
    Empty,

    #[error("MIC must be exactly {expected} characters", expected = MIC_LEN)]
    InvalidLength,

    #[error("MIC must contain only ASCII letters and digits")]
    InvalidCharacter,
}

/// ISO 10383 Market Identifier Code, e.g. `BVMF` (B3) or `XNYS` (NYSE).
///
/// Stored as normalized uppercase ASCII. Both segment and operating MICs share
/// this format; the distinction is carried by the venue that owns the code.
#[derive(Copy, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Debug)]
pub struct Mic([u8; MIC_LEN]);

impl Mic {
    /// Parses a MIC, trimming surrounding whitespace and normalizing to
    /// uppercase ASCII.
    pub fn parse(input: &str) -> Result<Self, MicError> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return Err(MicError::Empty);
        }
        if trimmed.chars().count() != MIC_LEN {
            return Err(MicError::InvalidLength);
        }
        if !trimmed.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(MicError::InvalidCharacter);
        }

        let mut bytes = [0u8; MIC_LEN];
        for (slot, byte) in bytes.iter_mut().zip(trimmed.bytes()) {
            *slot = byte.to_ascii_uppercase();
        }
        Ok(Self(bytes))
    }

    /// Alias for [`Mic::parse`], mirroring the `ftracker-identifiers` API.
    pub fn new(input: &str) -> Result<Self, MicError> {
        Self::parse(input)
    }

    pub fn as_str(&self) -> &str {
        // The constructor guarantees uppercase ASCII, so this never falls back.
        std::str::from_utf8(&self.0).unwrap_or_default()
    }

    pub fn as_bytes(&self) -> &[u8; MIC_LEN] {
        &self.0
    }
}

impl FromStr for Mic {
    type Err = MicError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Display for Mic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CurrencyCodeError {
    #[error("currency code cannot be empty")]
    Empty,

    #[error("currency code must be exactly {expected} characters", expected = CURRENCY_CODE_LEN)]
    InvalidLength,

    #[error("currency code must contain only ASCII letters")]
    InvalidCharacter,
}

/// ISO 4217 alphabetic currency code, e.g. `BRL` or `USD`.
///
/// Stored as normalized uppercase ASCII.
#[derive(Copy, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Debug)]
pub struct CurrencyCode([u8; CURRENCY_CODE_LEN]);

impl CurrencyCode {
    /// Parses a currency code, trimming surrounding whitespace and normalizing
    /// to uppercase ASCII.
    pub fn parse(input: &str) -> Result<Self, CurrencyCodeError> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return Err(CurrencyCodeError::Empty);
        }
        if trimmed.chars().count() != CURRENCY_CODE_LEN {
            return Err(CurrencyCodeError::InvalidLength);
        }
        if !trimmed.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(CurrencyCodeError::InvalidCharacter);
        }

        let mut bytes = [0u8; CURRENCY_CODE_LEN];
        for (slot, byte) in bytes.iter_mut().zip(trimmed.bytes()) {
            *slot = byte.to_ascii_uppercase();
        }
        Ok(Self(bytes))
    }

    /// Alias for [`CurrencyCode::parse`], mirroring the `ftracker-identifiers`
    /// API.
    pub fn new(input: &str) -> Result<Self, CurrencyCodeError> {
        Self::parse(input)
    }

    pub fn as_str(&self) -> &str {
        // The constructor guarantees uppercase ASCII, so this never falls back.
        std::str::from_utf8(&self.0).unwrap_or_default()
    }

    pub fn as_bytes(&self) -> &[u8; CURRENCY_CODE_LEN] {
        &self.0
    }
}

impl FromStr for CurrencyCode {
    type Err = CurrencyCodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Display for CurrencyCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mic_parses_and_normalizes() {
        let mic_result = Mic::parse(" bvmf ");
        assert!(mic_result.is_ok());
        let Some(mic) = mic_result.ok() else {
            return;
        };
        assert_eq!(mic.as_str(), "BVMF");
        assert_eq!(mic.as_bytes(), b"BVMF");
        assert_eq!(mic.to_string(), "BVMF");
    }

    #[test]
    fn mic_accepts_digits() {
        assert!(Mic::parse("BVC3").is_ok());
    }

    #[test]
    fn mic_rejects_empty() {
        assert!(matches!(Mic::parse("   "), Err(MicError::Empty)));
    }

    #[test]
    fn mic_rejects_wrong_length() {
        assert!(matches!(Mic::parse("XNY"), Err(MicError::InvalidLength)));
        assert!(matches!(Mic::parse("XNYSE"), Err(MicError::InvalidLength)));
    }

    #[test]
    fn mic_rejects_invalid_characters() {
        assert!(matches!(
            Mic::parse("XN-S"),
            Err(MicError::InvalidCharacter)
        ));
        assert!(matches!(
            Mic::parse("XNÝS"),
            Err(MicError::InvalidCharacter)
        ));
    }

    #[test]
    fn mic_from_str_round_trips() {
        let mic_result = "XNYS".parse::<Mic>();
        assert!(matches!(mic_result, Ok(mic) if mic.as_str() == "XNYS"));
    }

    #[test]
    fn currency_code_parses_and_normalizes() {
        let currency_result = CurrencyCode::parse(" brl ");
        assert!(currency_result.is_ok());
        let Some(currency) = currency_result.ok() else {
            return;
        };
        assert_eq!(currency.as_str(), "BRL");
        assert_eq!(currency.as_bytes(), b"BRL");
        assert_eq!(currency.to_string(), "BRL");
    }

    #[test]
    fn currency_code_rejects_empty() {
        assert!(matches!(
            CurrencyCode::parse(""),
            Err(CurrencyCodeError::Empty)
        ));
    }

    #[test]
    fn currency_code_rejects_wrong_length() {
        assert!(matches!(
            CurrencyCode::parse("US"),
            Err(CurrencyCodeError::InvalidLength)
        ));
        assert!(matches!(
            CurrencyCode::parse("USDT"),
            Err(CurrencyCodeError::InvalidLength)
        ));
    }

    #[test]
    fn currency_code_rejects_non_letters() {
        assert!(matches!(
            CurrencyCode::parse("US1"),
            Err(CurrencyCodeError::InvalidCharacter)
        ));
    }

    #[test]
    fn currency_code_from_str_round_trips() {
        let currency_result = "usd".parse::<CurrencyCode>();
        assert!(matches!(currency_result, Ok(c) if c.as_str() == "USD"));
    }
}
