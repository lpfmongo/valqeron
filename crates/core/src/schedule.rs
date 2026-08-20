//! Business-day recurrence schedules: when a wall-clock task runs and which
//! span of data a run is responsible for.
//!
//! A [`Schedule`] combines a [`MarketCalendar`], a market-local time of day,
//! and a [`Recurrence`]. Occurrence arithmetic is pure — the engine owns the
//! clocks that feed `after`/`before` — and every scan is bounded by the
//! calendar's own scan limits, so a degenerate holiday table cannot spin.

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Utc, Weekday};

use crate::calendar::MarketCalendar;

/// Upper bound on the day-by-day occurrence scans. Covers more than a year
/// of consecutive non-occurrences, which no real schedule produces.
const MAX_SCAN_DAYS: u32 = 400;

// ================ RECURRENCE ================
/// How often a scheduled task recurs, in business-day terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recurrence {
    /// Every business day.
    Daily,
    /// Once a week, anchored on `on`; when that weekday is not a business
    /// day the run rolls forward to the next business day.
    Weekly { on: Weekday },
}

impl std::fmt::Display for Recurrence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Recurrence::Daily => f.write_str("daily"),
            Recurrence::Weekly { on } => {
                let day = match on {
                    Weekday::Mon => "mon",
                    Weekday::Tue => "tue",
                    Weekday::Wed => "wed",
                    Weekday::Thu => "thu",
                    Weekday::Fri => "fri",
                    Weekday::Sat => "sat",
                    Weekday::Sun => "sun",
                };
                write!(f, "weekly:{day}")
            }
        }
    }
}

/// A recurrence text (`daily`, `weekly:mon`) did not parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid recurrence; expected `daily` or `weekly:<mon..sun>`")]
pub struct RecurrenceParseError;

impl std::str::FromStr for Recurrence {
    type Err = RecurrenceParseError;

    /// Parses the exact vocabulary [`Display`](Self) renders: `daily` or
    /// `weekly:<mon..sun>` (case-insensitive), so stored values round-trip.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lowered = s.trim().to_lowercase();
        if lowered == "daily" {
            return Ok(Recurrence::Daily);
        }
        let day = lowered
            .strip_prefix("weekly:")
            .ok_or(RecurrenceParseError)?;
        let on = match day {
            "mon" => Weekday::Mon,
            "tue" => Weekday::Tue,
            "wed" => Weekday::Wed,
            "thu" => Weekday::Thu,
            "fri" => Weekday::Fri,
            "sat" => Weekday::Sat,
            "sun" => Weekday::Sun,
            _ => return Err(RecurrenceParseError),
        };
        Ok(Recurrence::Weekly { on })
    }
}

// ================ TARGET PERIOD ================
/// The span of civil dates one run is responsible for, inclusive on both
/// ends. Under [`Recurrence::Daily`] this collapses to a single date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetPeriod {
    pub from: NaiveDate,
    pub to: NaiveDate,
}

// ================ SCHEDULE ================
/// A recurrence bound to a market calendar and a market-local time of day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    calendar: MarketCalendar,
    at: NaiveTime,
    recurrence: Recurrence,
}

impl std::fmt::Display for Schedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use chrono::Timelike;
        write!(
            f,
            "{}@{:02}:{:02}",
            self.recurrence,
            self.at.hour(),
            self.at.minute()
        )?;
        match self.calendar.offset() {
            Some(offset) => write!(f, "{offset}"),
            None => f.write_str("+??:??"),
        }
    }
}

impl Schedule {
    pub fn new(calendar: MarketCalendar, at: NaiveTime, recurrence: Recurrence) -> Self {
        Self {
            calendar,
            at,
            recurrence,
        }
    }

    pub fn calendar(&self) -> &MarketCalendar {
        &self.calendar
    }

    pub fn at(&self) -> NaiveTime {
        self.at
    }

    pub fn recurrence(&self) -> Recurrence {
        self.recurrence
    }

    /// The UTC instant of this schedule's slot on market-local `date`.
    fn slot_on(&self, date: NaiveDate) -> Option<DateTime<Utc>> {
        let offset = self.calendar.offset()?;
        date.and_time(self.at)
            .and_local_timezone(offset)
            .single()
            .map(|dt| dt.with_timezone(&Utc))
    }

    /// The run date produced by anchor date `anchor`: the anchor itself when
    /// it is a business day, otherwise rolled forward to the next one.
    fn run_date(&self, anchor: NaiveDate) -> Option<NaiveDate> {
        if self.calendar.is_business_day(anchor) {
            Some(anchor)
        } else {
            self.calendar.next_business_day(anchor)
        }
    }

    /// Whether `date` is an anchor of this recurrence (before business-day
    /// rolling).
    fn is_anchor(&self, date: NaiveDate) -> bool {
        match self.recurrence {
            Recurrence::Daily => self.calendar.is_business_day(date),
            Recurrence::Weekly { on } => date.weekday() == on,
        }
    }

    /// The first occurrence strictly after `after`.
    ///
    /// Strictness is what keeps a just-completed slot from being seeded
    /// again: `next_occurrence_after(slot) > slot` always.
    pub fn next_occurrence_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Start a week back so a weekly anchor whose rolled-forward run
        // lands after `after` is not missed.
        let mut candidate = self
            .calendar
            .local_date(after)?
            .checked_sub_days(chrono::Days::new(7))?;
        for _ in 0..MAX_SCAN_DAYS {
            if self.is_anchor(candidate)
                && let Some(run) = self.run_date(candidate)
                && let Some(slot) = self.slot_on(run)
                && slot > after
            {
                return Some(slot);
            }
            candidate = candidate.succ_opt()?;
        }
        None
    }

    /// The last occurrence strictly before `before` — the cold-start seed.
    pub fn latest_occurrence_before(&self, before: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Rolling only moves runs later, so no anchor after `before`'s date
        // can produce a run before it; descending from that date is enough.
        let mut candidate = self.calendar.local_date(before)?;
        for _ in 0..MAX_SCAN_DAYS {
            if self.is_anchor(candidate)
                && let Some(run) = self.run_date(candidate)
                && let Some(slot) = self.slot_on(run)
                && slot < before
            {
                return Some(slot);
            }
            candidate = candidate.pred_opt()?;
        }
        None
    }

    /// Canonical human/CLI descriptor: `daily@07:00-03:00`,
    /// `weekly:mon@08:00+00:00`. Display-only — never parsed back.
    pub fn descriptor(&self) -> String {
        self.to_string()
    }

    /// The span of dates a run at `slot` is responsible for, given that
    /// everything through `through_target` is already covered:
    /// `(through_target, previous business day before the slot]`.
    ///
    /// Under `Daily` with contiguous slots this collapses to exactly the
    /// previous business day (a Friday slot targets Thursday; a Monday slot
    /// targets Friday). Under `Weekly` it spans the whole preceding week of
    /// business days. `None` when the period would be empty — everything up
    /// to the slot is already covered.
    pub fn target_period(
        &self,
        through_target: NaiveDate,
        slot: DateTime<Utc>,
    ) -> Option<TargetPeriod> {
        let slot_date = self.calendar.local_date(slot)?;
        let to = self.calendar.previous_business_day(slot_date)?;
        let from = self.calendar.next_business_day(through_target)?;
        if from > to {
            return None;
        }
        Some(TargetPeriod { from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// Calendar with a Mon–Tue holiday block (2026-06-01, 2026-06-02).
    const WITH_HOLIDAYS: MarketCalendar =
        MarketCalendar::new(-10_800, &[(2026, 6, 1), (2026, 6, 2)]);

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap_or_default()
    }

    fn time(h: u32, m: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(h, m, 0).unwrap_or_default()
    }

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, 0)
            .single()
            .unwrap_or_default()
    }

    /// Weekdays 07:00 São Paulo — the CVM shape. 07:00−03:00 = 10:00Z.
    fn daily_b3() -> Schedule {
        Schedule::new(MarketCalendar::B3, time(7, 0), Recurrence::Daily)
    }

    fn weekly_b3(on: Weekday) -> Schedule {
        Schedule::new(MarketCalendar::B3, time(7, 0), Recurrence::Weekly { on })
    }

    // 2026-08-10 is a Monday; 2026-08-12 a Wednesday; 2026-08-14 a Friday.

    #[test]
    fn daily_next_occurrence_lands_same_day_before_the_slot_time() {
        // 05:00 local on Wednesday → Wednesday's own 07:00 slot.
        let next = daily_b3().next_occurrence_after(utc(2026, 8, 12, 8, 0));
        assert_eq!(next, Some(utc(2026, 8, 12, 10, 0)));
    }

    #[test]
    fn next_occurrence_is_strictly_after() {
        // Exactly at Wednesday's slot → Thursday's slot, never the same one.
        let next = daily_b3().next_occurrence_after(utc(2026, 8, 12, 10, 0));
        assert_eq!(next, Some(utc(2026, 8, 13, 10, 0)));
    }

    #[test]
    fn daily_skips_the_weekend() {
        // Friday after the slot → Monday's slot.
        let next = daily_b3().next_occurrence_after(utc(2026, 8, 14, 11, 0));
        assert_eq!(next, Some(utc(2026, 8, 17, 10, 0)));
    }

    #[test]
    fn daily_skips_holidays() {
        // Friday 2026-05-29 after the slot; Mon+Tue are holidays → Wed 3rd.
        let sched = Schedule::new(WITH_HOLIDAYS, time(7, 0), Recurrence::Daily);
        let next = sched.next_occurrence_after(utc(2026, 5, 29, 11, 0));
        assert_eq!(next, Some(utc(2026, 6, 3, 10, 0)));
    }

    #[test]
    fn weekly_next_occurrence_finds_the_anchor_weekday() {
        // Tuesday → next Monday's slot.
        let next = weekly_b3(Weekday::Mon).next_occurrence_after(utc(2026, 8, 11, 12, 0));
        assert_eq!(next, Some(utc(2026, 8, 17, 10, 0)));
    }

    #[test]
    fn weekly_same_day_before_slot_time_is_todays_slot() {
        // Monday 05:00 local → Monday's own slot.
        let next = weekly_b3(Weekday::Mon).next_occurrence_after(utc(2026, 8, 10, 8, 0));
        assert_eq!(next, Some(utc(2026, 8, 10, 10, 0)));
    }

    #[test]
    fn weekly_anchor_on_holiday_rolls_forward() {
        // Monday 2026-06-01 and Tuesday 2nd are holidays → runs Wednesday.
        let sched = Schedule::new(
            WITH_HOLIDAYS,
            time(7, 0),
            Recurrence::Weekly { on: Weekday::Mon },
        );
        let next = sched.next_occurrence_after(utc(2026, 5, 29, 12, 0));
        assert_eq!(next, Some(utc(2026, 6, 3, 10, 0)));

        // And from *inside* the rolled window (Tuesday) the rolled run is
        // still found — this is why the scan starts a week back.
        let next = sched.next_occurrence_after(utc(2026, 6, 2, 12, 0));
        assert_eq!(next, Some(utc(2026, 6, 3, 10, 0)));
    }

    #[test]
    fn latest_occurrence_before_is_strictly_before() {
        // Exactly at Wednesday's slot → Tuesday's slot.
        let latest = daily_b3().latest_occurrence_before(utc(2026, 8, 12, 10, 0));
        assert_eq!(latest, Some(utc(2026, 8, 11, 10, 0)));

        // Just after Wednesday's slot → Wednesday's slot.
        let latest = daily_b3().latest_occurrence_before(utc(2026, 8, 12, 10, 1));
        assert_eq!(latest, Some(utc(2026, 8, 12, 10, 0)));
    }

    #[test]
    fn latest_occurrence_before_crosses_the_weekend() {
        // Monday before the slot → Friday's slot.
        let latest = daily_b3().latest_occurrence_before(utc(2026, 8, 17, 8, 0));
        assert_eq!(latest, Some(utc(2026, 8, 14, 10, 0)));
    }

    #[test]
    fn weekly_latest_occurrence_before() {
        let latest = weekly_b3(Weekday::Mon).latest_occurrence_before(utc(2026, 8, 12, 12, 0));
        assert_eq!(latest, Some(utc(2026, 8, 10, 10, 0)));
    }

    #[test]
    fn friday_slot_targets_thursday() {
        // Covered through Wed 5th; Friday 7th's slot → covers Thu 6th.
        let period = daily_b3().target_period(date(2026, 8, 5), utc(2026, 8, 7, 10, 0));
        assert_eq!(
            period,
            Some(TargetPeriod {
                from: date(2026, 8, 6),
                to: date(2026, 8, 6),
            })
        );
    }

    #[test]
    fn daily_catchup_covers_one_day_per_run() {
        // Covered through Mon 3rd; next slot Wed 5th (catch-up, Tue's slot
        // was missed) → the run covers exactly Tuesday.
        let period = daily_b3().target_period(date(2026, 8, 3), utc(2026, 8, 5, 10, 0));
        assert_eq!(
            period,
            Some(TargetPeriod {
                from: date(2026, 8, 4),
                to: date(2026, 8, 4),
            })
        );
    }

    #[test]
    fn monday_slot_targets_friday() {
        // Covered through Thu 6th; Monday 10th's slot → covers Fri 7th.
        let period = daily_b3().target_period(date(2026, 8, 6), utc(2026, 8, 10, 10, 0));
        assert_eq!(
            period,
            Some(TargetPeriod {
                from: date(2026, 8, 7),
                to: date(2026, 8, 7),
            })
        );
    }

    #[test]
    fn weekly_target_spans_the_whole_preceding_week() {
        // Covered through Fri Jul 31 (last week's run); Monday Aug 10 slot
        // → covers Mon 3rd through Fri 7th, no gap.
        let period =
            weekly_b3(Weekday::Mon).target_period(date(2026, 7, 31), utc(2026, 8, 10, 10, 0));
        assert_eq!(
            period,
            Some(TargetPeriod {
                from: date(2026, 8, 3),
                to: date(2026, 8, 7),
            })
        );
    }

    #[test]
    fn descriptors_render_canonically() {
        assert_eq!(daily_b3().descriptor(), "daily@07:00-03:00");
        assert_eq!(
            weekly_b3(Weekday::Mon).descriptor(),
            "weekly:mon@07:00-03:00"
        );
        let utc_daily = Schedule::new(MarketCalendar::UTC, time(3, 0), Recurrence::Daily);
        assert_eq!(utc_daily.descriptor(), "daily@03:00+00:00");
    }

    #[test]
    fn already_covered_period_is_none() {
        // Covered through Fri 7th; Monday 10th's slot targets Fri 7th →
        // nothing left to sync.
        let period = daily_b3().target_period(date(2026, 8, 7), utc(2026, 8, 10, 10, 0));
        assert_eq!(period, None);
    }
}
