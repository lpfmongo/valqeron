//! Sync sources: durable progress cursors for scheduled, sequentially
//! backfilled data ingestion (CVM today; other sources reuse everything).
//!
//! A [`SyncCursor`] row records, per source, the last slot whose run
//! completed (`through_slot`), the last civil date whose data is covered
//! (`through_target`), and the failure/cooldown state that throttles
//! re-seeding. Core owns the entity and its state transitions so they stay
//! pure; the engine owns the clocks, the reconciler, and the handlers.
//!
//! The cursor advances only on [`SyncOutcome::Synced`], which is what makes
//! catch-up sequential: after an outage every missed slot is seeded, run,
//! and recorded one at a time, in chronological order.

use chrono::{DateTime, NaiveDate, Utc};
use std::str::FromStr;

use crate::sync::error::{SyncOutcomeKindError, SyncSourceError};

pub mod cooldown;
pub mod error;
pub mod repository;

const SYNC_SOURCE_MAX_LEN: usize = 50;

// ================ SOURCE ================
/// Registered name of a sync source (e.g. `cvm`): the cursor's primary key
/// and the env-var namespace of its configuration.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct SyncSource(String);

impl SyncSource {
    pub fn new(value: impl Into<String>) -> Result<Self, SyncSourceError> {
        let value = value.into();
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(SyncSourceError::Empty);
        }
        if trimmed.chars().count() > SYNC_SOURCE_MAX_LEN {
            return Err(SyncSourceError::TooLong {
                max: SYNC_SOURCE_MAX_LEN,
            });
        }

        Ok(Self(trimmed.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ================ OUTCOMES ================
/// How one sync run ended, as reported by the source's handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The target period was ingested; the cursor advances past it.
    Synced,
    /// The source has not published the target period yet — not an error:
    /// the cursor holds, no failure is counted, and re-seeding waits
    /// `retry_after_secs`.
    NotReady { retry_after_secs: u32 },
    /// The run failed. Task-level retries absorb transient faults; a
    /// terminal failure holds the cursor and starts the failure cooldown.
    Failed { error: String },
}

impl SyncOutcome {
    pub fn kind(&self) -> SyncOutcomeKind {
        match self {
            SyncOutcome::Synced => SyncOutcomeKind::Synced,
            SyncOutcome::NotReady { .. } => SyncOutcomeKind::NotReady,
            SyncOutcome::Failed { .. } => SyncOutcomeKind::Failed,
        }
    }
}

/// The persisted marker of a run's outcome (`last_outcome` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncOutcomeKind {
    Synced,
    NotReady,
    Failed,
}

impl FromStr for SyncOutcomeKind {
    type Err = SyncOutcomeKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SYNCED" => Ok(SyncOutcomeKind::Synced),
            "NOT_READY" => Ok(SyncOutcomeKind::NotReady),
            "FAILED" => Ok(SyncOutcomeKind::Failed),
            _ => Err(SyncOutcomeKindError::InvalidKind),
        }
    }
}

impl From<SyncOutcomeKind> for String {
    fn from(val: SyncOutcomeKind) -> Self {
        match val {
            SyncOutcomeKind::Synced => "SYNCED".into(),
            SyncOutcomeKind::NotReady => "NOT_READY".into(),
            SyncOutcomeKind::Failed => "FAILED".into(),
        }
    }
}

// ================ THE CURSOR ================
/// Per-source sync progress: the single row that survives restarts, task
/// pruning, and crashes, and from which the reconciler derives the next
/// slot to seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncCursor {
    source: SyncSource,
    through_slot: DateTime<Utc>,
    through_target: NaiveDate,
    cooldown_until: Option<DateTime<Utc>>,
    consecutive_failures: u32,
    last_outcome: Option<SyncOutcomeKind>,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
}

/// Plain-field mirror of [`SyncCursor`] for persistence round-trips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncCursorSnapshot {
    pub source: SyncSource,
    pub through_slot: DateTime<Utc>,
    pub through_target: NaiveDate,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub consecutive_failures: u32,
    pub last_outcome: Option<SyncOutcomeKind>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl SyncCursor {
    /// A fresh cursor (cold start): everything through `through_slot` /
    /// `through_target` is declared covered, so the next occurrence after
    /// `through_slot` is the first run.
    pub fn seeded(
        source: SyncSource,
        through_slot: DateTime<Utc>,
        through_target: NaiveDate,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            source,
            through_slot,
            through_target,
            cooldown_until: None,
            consecutive_failures: 0,
            last_outcome: None,
            last_error: None,
            updated_at: now,
        }
    }

    pub fn source(&self) -> &SyncSource {
        &self.source
    }
    pub fn through_slot(&self) -> DateTime<Utc> {
        self.through_slot
    }
    pub fn through_target(&self) -> NaiveDate {
        self.through_target
    }
    pub fn cooldown_until(&self) -> Option<DateTime<Utc>> {
        self.cooldown_until
    }
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }
    pub fn last_outcome(&self) -> Option<SyncOutcomeKind> {
        self.last_outcome
    }
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Whether re-seeding may proceed at `now` (no active cooldown).
    pub fn is_ready(&self, now: DateTime<Utc>) -> bool {
        self.cooldown_until.is_none_or(|until| until <= now)
    }

    /// A successful run: advance past its slot and target, clear the
    /// failure state.
    pub fn advanced(self, slot: DateTime<Utc>, target_to: NaiveDate, now: DateTime<Utc>) -> Self {
        Self {
            through_slot: slot,
            through_target: target_to,
            cooldown_until: None,
            consecutive_failures: 0,
            last_outcome: Some(SyncOutcomeKind::Synced),
            last_error: None,
            updated_at: now,
            ..self
        }
    }

    /// The source has not published the target yet: hold position and wait.
    /// Deliberately not a failure — the count and last error are untouched
    /// by design, so a slow publisher never escalates.
    pub fn held_not_ready(self, retry_after_secs: u32, now: DateTime<Utc>) -> Self {
        let until = now
            .checked_add_signed(chrono::Duration::seconds(i64::from(retry_after_secs)))
            .unwrap_or(now);
        Self {
            cooldown_until: Some(until),
            last_outcome: Some(SyncOutcomeKind::NotReady),
            updated_at: now,
            ..self
        }
    }

    /// A terminally failed run: hold position, count the failure, and back
    /// off until `cooldown_until`.
    pub fn failed(self, error: String, cooldown_until: DateTime<Utc>, now: DateTime<Utc>) -> Self {
        Self {
            cooldown_until: Some(cooldown_until),
            consecutive_failures: self.consecutive_failures.saturating_add(1),
            last_outcome: Some(SyncOutcomeKind::Failed),
            last_error: Some(error),
            updated_at: now,
            ..self
        }
    }

    pub fn reconstitute(snapshot: SyncCursorSnapshot) -> Self {
        Self {
            source: snapshot.source,
            through_slot: snapshot.through_slot,
            through_target: snapshot.through_target,
            cooldown_until: snapshot.cooldown_until,
            consecutive_failures: snapshot.consecutive_failures,
            last_outcome: snapshot.last_outcome,
            last_error: snapshot.last_error,
            updated_at: snapshot.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn source() -> Option<SyncSource> {
        SyncSource::new("cvm").ok()
    }

    fn utc(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0)
            .single()
            .unwrap_or_default()
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap_or_default()
    }

    #[test]
    fn sync_source_trims_and_validates() {
        let trimmed = SyncSource::new(" cvm ");
        assert!(matches!(&trimmed, Ok(s) if s.as_str() == "cvm"));
        assert!(matches!(
            SyncSource::new("   "),
            Err(SyncSourceError::Empty)
        ));
        assert!(matches!(
            SyncSource::new("s".repeat(SYNC_SOURCE_MAX_LEN + 1)),
            Err(SyncSourceError::TooLong { max: 50 })
        ));
    }

    #[test]
    fn outcome_kind_round_trips() {
        for (text, kind) in [
            ("SYNCED", SyncOutcomeKind::Synced),
            ("not_ready", SyncOutcomeKind::NotReady),
            ("Failed", SyncOutcomeKind::Failed),
        ] {
            let parsed = SyncOutcomeKind::from_str(text);
            assert!(matches!(parsed, Ok(p) if p == kind), "{text} parses");
        }
        assert!(SyncOutcomeKind::from_str("UNKNOWN").is_err());
        let as_string: String = SyncOutcomeKind::NotReady.into();
        assert_eq!(as_string, "NOT_READY");
    }

    #[test]
    fn seeded_cursor_is_ready_with_clean_state() {
        let Some(source) = source() else { return };
        let now = utc(2026, 8, 12, 12);
        let cursor = SyncCursor::seeded(source, utc(2026, 8, 11, 10), date(2026, 8, 10), now);
        assert!(cursor.is_ready(now));
        assert_eq!(cursor.consecutive_failures(), 0);
        assert_eq!(cursor.last_outcome(), None);
        assert_eq!(cursor.through_target(), date(2026, 8, 10));
    }

    #[test]
    fn advanced_moves_both_positions_and_clears_failures() {
        let Some(source) = source() else { return };
        let now = utc(2026, 8, 12, 12);
        let cursor = SyncCursor::seeded(source, utc(2026, 8, 11, 10), date(2026, 8, 10), now)
            .failed("boom".into(), utc(2026, 8, 12, 13), now)
            .advanced(utc(2026, 8, 12, 10), date(2026, 8, 11), now);

        assert_eq!(cursor.through_slot(), utc(2026, 8, 12, 10));
        assert_eq!(cursor.through_target(), date(2026, 8, 11));
        assert_eq!(cursor.consecutive_failures(), 0, "success resets failures");
        assert_eq!(cursor.cooldown_until(), None, "success clears cooldown");
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::Synced));
        assert_eq!(cursor.last_error(), None);
    }

    #[test]
    fn not_ready_holds_position_without_counting_a_failure() {
        let Some(source) = source() else { return };
        let now = utc(2026, 8, 12, 12);
        let cursor = SyncCursor::seeded(source, utc(2026, 8, 11, 10), date(2026, 8, 10), now)
            .held_not_ready(600, now);

        assert_eq!(cursor.through_slot(), utc(2026, 8, 11, 10), "holds slot");
        assert_eq!(cursor.consecutive_failures(), 0, "not a failure");
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::NotReady));
        assert!(!cursor.is_ready(now), "cooldown active immediately");
        assert!(
            cursor.is_ready(utc(2026, 8, 12, 13)),
            "ready once retry_after has elapsed"
        );
    }

    #[test]
    fn failed_counts_up_and_cools_down() {
        let Some(source) = source() else { return };
        let now = utc(2026, 8, 12, 12);
        let cursor = SyncCursor::seeded(source, utc(2026, 8, 11, 10), date(2026, 8, 10), now)
            .failed("first".into(), utc(2026, 8, 12, 13), now)
            .failed("second".into(), utc(2026, 8, 12, 14), now);

        assert_eq!(cursor.consecutive_failures(), 2);
        assert_eq!(cursor.last_error(), Some("second"));
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::Failed));
        assert!(!cursor.is_ready(utc(2026, 8, 12, 13)));
        assert!(cursor.is_ready(utc(2026, 8, 12, 14)), "boundary: <= now");
    }

    #[test]
    fn reconstitute_round_trips() {
        let Some(source) = source() else { return };
        let snapshot = SyncCursorSnapshot {
            source,
            through_slot: utc(2026, 8, 11, 10),
            through_target: date(2026, 8, 10),
            cooldown_until: Some(utc(2026, 8, 12, 13)),
            consecutive_failures: 3,
            last_outcome: Some(SyncOutcomeKind::Failed),
            last_error: Some("boom".into()),
            updated_at: utc(2026, 8, 12, 12),
        };
        let cursor = SyncCursor::reconstitute(snapshot.clone());
        assert_eq!(cursor.source().as_str(), "cvm");
        assert_eq!(cursor.through_slot(), snapshot.through_slot);
        assert_eq!(cursor.through_target(), snapshot.through_target);
        assert_eq!(cursor.cooldown_until(), snapshot.cooldown_until);
        assert_eq!(cursor.consecutive_failures(), 3);
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::Failed));
        assert_eq!(cursor.last_error(), Some("boom"));
    }
}
