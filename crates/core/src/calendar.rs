//! Market calendars: which dates are business days for a data source, and
//! in which local time the source's publication clock runs.
//!
//! Core owns this arithmetic so it stays pure and unit-testable; the engine
//! owns the clocks that feed it. All scans are bounded so a dense future
//! holiday table can never turn a lookup into an unbounded loop.

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Utc, Weekday};

/// Upper bound on consecutive non-business days any scan will cross before
/// giving up. Generous: no real market closes for a year straight.
const MAX_SCAN_DAYS: u32 = 366;

/// A market's civil-calendar rules: its UTC offset and its holiday table.
///
/// The offset is fixed (no DST arithmetic): Brazil abolished DST in 2019,
/// so `B3` is exact. For a daily schedule a one-hour DST error in some
/// future market still lands on the same calendar date, which is what the
/// business-day math depends on.
///
/// Holidays are `(year, month, day)` triples so calendars can be `const`;
/// [`MarketCalendar::is_business_day`] is the seam where a real holiday
/// table (B3 national holidays, Carnival, etc.) plugs in later without any
/// caller changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketCalendar {
    utc_offset_secs: i32,
    holidays: &'static [(i32, u32, u32)],
}

impl MarketCalendar {
    /// B3 / Brazilian sources: America/Sao_Paulo, fixed −03:00, no DST
    /// since 2019. Holiday table intentionally empty for now.
    pub const B3: Self = Self {
        utc_offset_secs: -10_800,
        holidays: &[],
    };

    /// A custom calendar for sources not covered by the built-in constants.
    /// `utc_offset_secs` is seconds east of UTC; holidays are
    /// `(year, month, day)` triples.
    pub const fn new(utc_offset_secs: i32, holidays: &'static [(i32, u32, u32)]) -> Self {
        Self {
            utc_offset_secs,
            holidays,
        }
    }

    /// Timezone-neutral calendar for internal schedules (maintenance work)
    /// that still want business-day recurrence semantics.
    pub const UTC: Self = Self {
        utc_offset_secs: 0,
        holidays: &[],
    };

    /// The market's fixed UTC offset. `None` only for an out-of-range
    /// offset, which no built-in calendar has.
    pub fn offset(&self) -> Option<FixedOffset> {
        FixedOffset::east_opt(self.utc_offset_secs)
    }

    /// The civil date in market-local time at instant `at`.
    pub fn local_date(&self, at: DateTime<Utc>) -> Option<NaiveDate> {
        Some(at.with_timezone(&self.offset()?).date_naive())
    }

    /// Whether `date` is a business day: not a weekend, not a holiday.
    pub fn is_business_day(&self, date: NaiveDate) -> bool {
        !is_weekend(date) && !self.is_holiday(date)
    }

    fn is_holiday(&self, date: NaiveDate) -> bool {
        self.holidays
            .iter()
            .any(|&(y, m, d)| NaiveDate::from_ymd_opt(y, m, d) == Some(date))
    }

    /// The closest business day strictly before `date`.
    pub fn previous_business_day(&self, date: NaiveDate) -> Option<NaiveDate> {
        let mut candidate = date.pred_opt()?;
        for _ in 0..MAX_SCAN_DAYS {
            if self.is_business_day(candidate) {
                return Some(candidate);
            }
            candidate = candidate.pred_opt()?;
        }
        None
    }

    /// The closest business day strictly after `date`.
    pub fn next_business_day(&self, date: NaiveDate) -> Option<NaiveDate> {
        let mut candidate = date.succ_opt()?;
        for _ in 0..MAX_SCAN_DAYS {
            if self.is_business_day(candidate) {
                return Some(candidate);
            }
            candidate = candidate.succ_opt()?;
        }
        None
    }

    /// How many business days lie strictly after `from` and up to (and
    /// including) `to`. Zero when `to <= from`. Saturates at `u32::MAX`.
    pub fn business_days_between(&self, from: NaiveDate, to: NaiveDate) -> u32 {
        if to <= from {
            return 0;
        }
        let mut count = 0u32;
        let mut candidate = from;
        while candidate < to {
            candidate = match candidate.succ_opt() {
                Some(next) => next,
                None => return count,
            };
            if self.is_business_day(candidate) {
                count = count.saturating_add(1);
            }
        }
        count
    }
}

fn is_weekend(date: NaiveDate) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// Test-only calendar with a holiday table, to exercise the seam.
    const WITH_HOLIDAYS: MarketCalendar = MarketCalendar {
        utc_offset_secs: -10_800,
        // Tiradentes 2026-04-21 (Tue) and a fake Mon–Tue block.
        holidays: &[(2026, 4, 21), (2026, 6, 1), (2026, 6, 2)],
    };

    /// Invalid components collapse to 1970-01-01, which makes the assertion
    /// that uses them fail loudly instead of panicking.
    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap_or_default()
    }

    #[test]
    fn b3_offset_is_minus_three_hours() {
        let offset = MarketCalendar::B3.offset();
        assert!(
            matches!(offset, Some(o) if o.local_minus_utc() == -10_800),
            "B3 offset must resolve to −03:00: {offset:?}"
        );
    }

    #[test]
    fn local_date_crosses_midnight_correctly() {
        // 01:30 UTC is still the previous day at −03:00. An invalid
        // instant would make the assertions below fail on `None`.
        let at = Utc
            .with_ymd_and_hms(2026, 8, 12, 1, 30, 0)
            .single()
            .unwrap_or_default();
        assert_eq!(
            MarketCalendar::B3.local_date(at),
            Some(date(2026, 8, 11)),
            "01:30Z on the 12th is 22:30 on the 11th in São Paulo"
        );
        assert_eq!(MarketCalendar::UTC.local_date(at), Some(date(2026, 8, 12)));
    }

    #[test]
    fn weekday_truth_table() {
        let cal = MarketCalendar::B3;
        // 2026-08-10 is a Monday.
        for (day, business) in [
            (10, true),  // Mon
            (11, true),  // Tue
            (12, true),  // Wed
            (13, true),  // Thu
            (14, true),  // Fri
            (15, false), // Sat
            (16, false), // Sun
        ] {
            assert_eq!(
                cal.is_business_day(date(2026, 8, day)),
                business,
                "2026-08-{day}"
            );
        }
    }

    #[test]
    fn holidays_are_not_business_days() {
        assert!(!WITH_HOLIDAYS.is_business_day(date(2026, 4, 21)));
        assert!(WITH_HOLIDAYS.is_business_day(date(2026, 4, 22)));
        // The same date is a business day on a calendar without the table.
        assert!(MarketCalendar::B3.is_business_day(date(2026, 4, 21)));
    }

    #[test]
    fn previous_business_day_skips_weekends() {
        let cal = MarketCalendar::B3;
        // Monday → Friday.
        assert_eq!(
            cal.previous_business_day(date(2026, 8, 10)),
            Some(date(2026, 8, 7))
        );
        // Tuesday → Monday.
        assert_eq!(
            cal.previous_business_day(date(2026, 8, 11)),
            Some(date(2026, 8, 10))
        );
        // Sunday → Friday.
        assert_eq!(
            cal.previous_business_day(date(2026, 8, 16)),
            Some(date(2026, 8, 14))
        );
    }

    #[test]
    fn previous_business_day_skips_holiday_blocks() {
        // Wed 2026-06-03: Mon 1st + Tue 2nd are holidays → previous is
        // Friday 2026-05-29.
        assert_eq!(
            WITH_HOLIDAYS.previous_business_day(date(2026, 6, 3)),
            Some(date(2026, 5, 29))
        );
    }

    #[test]
    fn next_business_day_skips_weekends_and_holidays() {
        let cal = MarketCalendar::B3;
        // Friday → Monday.
        assert_eq!(
            cal.next_business_day(date(2026, 8, 14)),
            Some(date(2026, 8, 17))
        );
        // Friday 2026-05-29 → Wed 2026-06-03 (weekend + Mon/Tue holidays).
        assert_eq!(
            WITH_HOLIDAYS.next_business_day(date(2026, 5, 29)),
            Some(date(2026, 6, 3))
        );
    }

    #[test]
    fn month_and_year_rollovers() {
        let cal = MarketCalendar::B3;
        // 2026-01-01 (Thu) ← previous business day is 2025-12-31 (Wed).
        assert_eq!(
            cal.previous_business_day(date(2026, 1, 1)),
            Some(date(2025, 12, 31))
        );
        // Friday 2026-07-31 → Monday 2026-08-03.
        assert_eq!(
            cal.next_business_day(date(2026, 7, 31)),
            Some(date(2026, 8, 3))
        );
    }

    #[test]
    fn business_days_between_counts_the_half_open_range() {
        let cal = MarketCalendar::B3;
        // (Mon 10th, Fri 14th] = Tue, Wed, Thu, Fri.
        assert_eq!(
            cal.business_days_between(date(2026, 8, 10), date(2026, 8, 14)),
            4
        );
        // Across a weekend: (Fri 14th, Mon 17th] = Mon only.
        assert_eq!(
            cal.business_days_between(date(2026, 8, 14), date(2026, 8, 17)),
            1
        );
        // Empty and inverted ranges are zero.
        assert_eq!(
            cal.business_days_between(date(2026, 8, 14), date(2026, 8, 14)),
            0
        );
        assert_eq!(
            cal.business_days_between(date(2026, 8, 17), date(2026, 8, 10)),
            0
        );
    }

    #[test]
    fn business_days_between_excludes_holidays() {
        // (Fri 2026-05-29, Wed 2026-06-03] with Mon+Tue holidays = Wed only.
        assert_eq!(
            WITH_HOLIDAYS.business_days_between(date(2026, 5, 29), date(2026, 6, 3)),
            1
        );
    }
}
