//! Failure-cooldown arithmetic for sync sources, mirroring the shape of
//! [`crate::BackgroundTask::next_retry_at`]: exponential backoff on a base
//! delay, capped so a halted source retries at most hourly.

use chrono::{DateTime, Duration, Utc};

/// Backoff growth is capped so `base_secs * 2^(failures-1)` cannot overflow
/// or schedule re-seeding absurdly far out.
const MAX_COOLDOWN: Duration = Duration::hours(1);

/// How a source's re-seeding backs off across consecutive terminal
/// failures. `NotReady` outcomes do not use this policy — they carry their
/// own `retry_after`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CooldownPolicy {
    base_secs: u32,
}

impl CooldownPolicy {
    pub const fn new(base_secs: u32) -> Self {
        Self { base_secs }
    }

    pub fn base_secs(&self) -> u32 {
        self.base_secs
    }

    /// When re-seeding becomes allowed again after the `failures`-th
    /// consecutive terminal failure: `now + min(base * 2^(failures-1), 1h)`.
    /// A zero base cools down for zero seconds (retry at the next
    /// reconcile pass).
    pub fn until(&self, failures: u32, now: DateTime<Utc>) -> DateTime<Utc> {
        let base = Duration::seconds(i64::from(self.base_secs));
        let exponent = failures.saturating_sub(1).min(31);
        let factor = 2i32.checked_pow(exponent).unwrap_or(i32::MAX);
        let cooldown = base
            .checked_mul(factor)
            .unwrap_or(MAX_COOLDOWN)
            .min(MAX_COOLDOWN);
        now.checked_add_signed(cooldown).unwrap_or(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 12, 12, 0, 0)
            .single()
            .unwrap_or_default()
    }

    fn cooldown_of(base_secs: u32, failures: u32) -> Duration {
        CooldownPolicy::new(base_secs)
            .until(failures, now())
            .signed_duration_since(now())
    }

    #[test]
    fn cooldown_doubles_per_failure_and_caps_at_one_hour() {
        assert_eq!(cooldown_of(300, 1), Duration::seconds(300));
        assert_eq!(cooldown_of(300, 2), Duration::seconds(600));
        assert_eq!(cooldown_of(300, 3), Duration::seconds(1200));
        assert_eq!(cooldown_of(300, 4), Duration::seconds(2400));
        // 300 * 2^4 = 4800 > 3600 → capped.
        assert_eq!(cooldown_of(300, 5), MAX_COOLDOWN);
        // Huge counts saturate instead of overflowing.
        assert_eq!(cooldown_of(3600, u32::MAX), MAX_COOLDOWN);
    }

    #[test]
    fn zero_base_retries_immediately() {
        assert_eq!(cooldown_of(0, 3), Duration::zero());
    }

    #[test]
    fn zero_failures_behaves_like_the_first() {
        assert_eq!(cooldown_of(300, 0), Duration::seconds(300));
    }
}
