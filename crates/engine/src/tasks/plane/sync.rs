//! The sync plane: cursor-driven recurrence with sequential catch-up.
//!
//! Each sync source owns a durable [`SyncCursor`] row. The seeding pass
//! keeps **at most one** `background_task` row alive per source —
//! `exists_active` is the gate — with `scheduled_at` set to the next
//! occurrence after the cursor. A stale cursor therefore yields a past-due
//! row that runs immediately, and a current cursor yields a future row that
//! waits: catch-up after an outage and steady state are the same code path,
//! one slot at a time, in chronological order.
//!
//! The cursor is owned by the plane, not by handlers: it advances on
//! [`TaskOutcome::Done`], holds with a cooldown on
//! [`TaskOutcome::NotReady`], and holds with an escalating failure cooldown
//! when a run fails terminally. The consequence is at-least-once execution —
//! a crash between a handler's success and the cursor write re-runs that
//! period — so sync handlers must stay idempotent (upsert-by-date).

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use tokio::sync::Notify;
use valqeron_core::{
    BackgroundTask, BackgroundTaskRepository, CooldownPolicy, Repositories, Schedule, StorageError,
    StorageFault, SyncCursor, SyncCursorRepository, SyncSource, TargetPeriod, TaskKind,
};
use valqeron_infrastructure::SqliteStorageEngine;

use crate::storage::AsyncStorage;
use crate::tasks::plane::{
    BoxFuture, Plane, RECONCILE_INTERVAL, RetryPolicy, RunWindow, SeedPass, TaskFailure,
    TaskOutcome, TickMode,
};

/// Consecutive terminal failures after which the halt log escalates from
/// `warn` to `error` — also the status read model's `halted` threshold.
pub(crate) const ESCALATE_AFTER_FAILURES: u32 = 5;

pub(crate) struct SyncPlane {
    kind: &'static str,
    source: SyncSource,
    schedule: Schedule,
    retry: RetryPolicy,
    cooldown: CooldownPolicy,
    max_backfill_days: u32,
    /// Run completions wake the seeder, so catch-up is paced by handler
    /// speed rather than the fallback tick.
    wake: Notify,
}

impl SyncPlane {
    pub(crate) fn new(
        kind: &'static str,
        source: SyncSource,
        schedule: Schedule,
        retry: RetryPolicy,
        cooldown: CooldownPolicy,
        max_backfill_days: u32,
    ) -> Self {
        Self {
            kind,
            source,
            schedule,
            retry,
            cooldown,
            max_backfill_days,
            wake: Notify::new(),
        }
    }

    /// The cold-start cursor: positioned one occurrence back from the most
    /// recent one, so the most recent occurrence runs immediately (one run
    /// of the latest period, no history).
    fn cold_start_cursor(&self, now: DateTime<Utc>) -> Option<SyncCursor> {
        let latest = self.schedule.latest_occurrence_before(now)?;
        let previous = self.schedule.latest_occurrence_before(latest)?;
        let through_target = self
            .schedule
            .calendar()
            .previous_business_day(self.schedule.calendar().local_date(previous)?)?;
        Some(SyncCursor::seeded(
            self.source.clone(),
            previous,
            through_target,
            now,
        ))
    }
}

fn fault(message: String) -> StorageError {
    StorageError::Fault(StorageFault::new(message))
}

impl Plane for SyncPlane {
    fn cadence(&self) -> (tokio::time::Instant, Duration) {
        // First pass immediately: a catch-up must not wait for the tick.
        (tokio::time::Instant::now(), RECONCILE_INTERVAL)
    }

    fn mode(&self) -> TickMode {
        TickMode::Seed
    }

    /// One seeding pass: gate, cursor resolution (cold start / staleness
    /// clamp), cooldown check, and the seed — all inside the manager's
    /// single write-lane transaction.
    fn reconcile(
        &self,
        repos: &Repositories<SqliteStorageEngine>,
        now: DateTime<Utc>,
    ) -> Result<SeedPass, StorageError> {
        let source = self.source.as_str();
        let kind = TaskKind::new(self.kind).map_err(|e| fault(e.to_string()))?;
        if repos.tasks.exists_active(&kind)? {
            tracing::debug!(source, "sync run already active; nothing to seed");
            return Ok(SeedPass::Idle);
        }

        let calendar = self.schedule.calendar();
        let Some(today) = calendar.local_date(now) else {
            tracing::error!(source, "calendar produced no local date; misconfigured");
            return Ok(SeedPass::Idle);
        };

        // Resolve the cursor: existing, clamped when too stale, or cold
        // start.
        let (cursor, is_reseed, skipped_days) = match repos.cursors.get(&self.source)? {
            Some(existing) => {
                let pending = calendar.business_days_between(existing.through_target(), today);
                if pending > self.max_backfill_days {
                    let Some(reseeded) = self.cold_start_cursor(now) else {
                        tracing::error!(source, "schedule produced no occurrence; misconfigured");
                        return Ok(SeedPass::Idle);
                    };
                    let skipped = calendar.business_days_between(
                        existing.through_target(),
                        reseeded.through_target(),
                    );
                    (reseeded, true, Some(skipped))
                } else {
                    (existing, false, None)
                }
            }
            None => {
                let Some(seeded) = self.cold_start_cursor(now) else {
                    tracing::error!(source, "schedule produced no occurrence; misconfigured");
                    return Ok(SeedPass::Idle);
                };
                (seeded, true, None)
            }
        };

        if !cursor.is_ready(now) {
            tracing::debug!(
                source,
                until = ?cursor.cooldown_until(),
                consecutive_failures = cursor.consecutive_failures(),
                "sync source cooling down; nothing seeded"
            );
            return Ok(SeedPass::Idle);
        }

        let Some(slot) = self.schedule.next_occurrence_after(cursor.through_slot()) else {
            tracing::error!(source, "schedule produced no occurrence; misconfigured");
            return Ok(SeedPass::Idle);
        };
        let Some(target) = self.schedule.target_period(cursor.through_target(), slot) else {
            // Everything up to `slot` is already covered — advance the slot
            // position without running and let the next pass try the one
            // after.
            let advanced =
                SyncCursor::seeded(self.source.clone(), slot, cursor.through_target(), now);
            repos.cursors.upsert(&advanced)?;
            tracing::warn!(
                source,
                slot = %slot.to_rfc3339_opts(SecondsFormat::Millis, true),
                "slot had an empty target period; advanced past it without a run"
            );
            return Ok(SeedPass::Idle);
        };

        if is_reseed {
            repos.cursors.upsert(&cursor)?;
        }
        if let Some(skipped) = skipped_days {
            tracing::warn!(
                target: "valqeron::audit",
                operation = "sync_skip",
                source,
                skipped_days = skipped,
                max_backfill_days = self.max_backfill_days,
                "sync cursor was too far behind; skipped ahead to the most \
                 recent occurrence (bulk history needs an explicit backfill)"
            );
        }

        let task = BackgroundTask::builder()
            .kind(kind)
            .payload(encode_payload(slot, target))
            .scheduled_at(slot)
            .max_attempts(self.retry.max_attempts)
            .retry_delay_secs(self.retry.retry_delay_secs)
            .build()
            .map_err(|e| fault(e.to_string()))?;
        let task_id = *task.id();
        repos.tasks.insert(&task)?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "sync_seed",
            source,
            kind = self.kind,
            task_id = %task_id.value(),
            slot = %slot.to_rfc3339_opts(SecondsFormat::Millis, true),
            target_from = %target.from,
            target_to = %target.to,
            pending_days = calendar.business_days_between(cursor.through_target(), today),
            "seeded the next sync run"
        );
        Ok(SeedPass::Seeded)
    }

    fn window_for(&self, payload: Option<&str>) -> Result<RunWindow, String> {
        let Some(payload) = payload else {
            return Err("sync run has no payload".to_owned());
        };
        let Some((slot, target)) = decode_payload(payload) else {
            return Err(format!("corrupt sync payload: {payload:?}"));
        };
        Ok(RunWindow::Period { slot, target })
    }

    /// Apply the outcome to the cursor: advance on `Done`, hold with the
    /// handler's cooldown on `NotReady`. Cursor write failures fail the run
    /// — re-running a synced period is safe (handlers are idempotent),
    /// silently losing the advance is not. `Failed` is handed back to the
    /// dispatcher so task-level retries apply; the cursor is only touched
    /// on *terminal* failure, in [`Plane::on_terminal`].
    fn interpret<'a>(
        &'a self,
        storage: &'a AsyncStorage,
        window: RunWindow,
        outcome: TaskOutcome,
    ) -> BoxFuture<'a, Result<(), TaskFailure>> {
        Box::pin(async move {
            let RunWindow::Period { slot, target } = window else {
                return Err(TaskFailure::new("sync run without a period window"));
            };
            let source = self.source.clone();
            match outcome {
                TaskOutcome::Done => {
                    let advanced = storage
                        .write("sync_cursor_advance", false, move |repos| {
                            let now = Utc::now();
                            let cursor = repos
                                .cursors
                                .get(&source)?
                                .unwrap_or_else(|| {
                                    SyncCursor::seeded(source.clone(), slot, target.to, now)
                                })
                                .advanced(slot, target.to, now);
                            repos.cursors.upsert(&cursor).map_err(StorageError::from)
                        })
                        .await;
                    match advanced {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(TaskFailure::new(format!("cursor advance failed: {e}"))),
                        Err(e) => Err(TaskFailure::new(format!(
                            "cursor advance not executed: {e}"
                        ))),
                    }
                }
                TaskOutcome::NotReady { retry_after_secs } => {
                    let held = storage
                        .write("sync_cursor_hold", false, move |repos| {
                            let now = Utc::now();
                            let cursor = repos
                                .cursors
                                .get(&source)?
                                .unwrap_or_else(|| {
                                    // A vanished cursor still holds *before*
                                    // the slot.
                                    SyncCursor::seeded(source.clone(), slot, target.to, now)
                                })
                                .held_not_ready(retry_after_secs, now);
                            repos.cursors.upsert(&cursor).map_err(StorageError::from)
                        })
                        .await;
                    match held {
                        Ok(Ok(())) => {
                            tracing::info!(
                                target: "valqeron::audit",
                                operation = "sync_not_ready",
                                source = self.source.as_str(),
                                slot = %slot.to_rfc3339_opts(SecondsFormat::Millis, true),
                                retry_after_secs,
                                "source has not published the target period yet; holding"
                            );
                            Ok(())
                        }
                        Ok(Err(e)) => Err(TaskFailure::new(format!("cursor hold failed: {e}"))),
                        Err(e) => Err(TaskFailure::new(format!("cursor hold not executed: {e}"))),
                    }
                }
                TaskOutcome::Failed(error) => Err(TaskFailure::new(error)),
            }
        })
    }

    /// A run's *final* attempt failed: count the failure on the cursor and
    /// start the escalating cooldown. The cursor holds its position — the
    /// same period is retried after the cooldown, and nothing after it runs
    /// until it succeeds (halt semantics: no silent gaps).
    fn on_terminal<'a>(&'a self, storage: &'a AsyncStorage, error: String) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let source = self.source.clone();
            let cooldown = self.cooldown;
            let error_text = error.clone();
            let recorded = storage
                .write("sync_cursor_failure", false, move |repos| {
                    let now = Utc::now();
                    let Some(cursor) = repos.cursors.get(&source)? else {
                        // No cursor yet (first-ever run failed before any
                        // advance): nothing to hold; the seeder cold-starts
                        // again.
                        return Ok::<Option<(u32, DateTime<Utc>)>, StorageError>(None);
                    };
                    let failures = cursor.consecutive_failures().saturating_add(1);
                    let until = cooldown.until(failures, now);
                    let cursor = cursor.failed(error_text, until, now);
                    repos.cursors.upsert(&cursor)?;
                    Ok(Some((failures, until)))
                })
                .await;

            let source = self.source.as_str();
            match recorded {
                Ok(Ok(Some((failures, until)))) => {
                    let until = until.to_rfc3339_opts(SecondsFormat::Millis, true);
                    if failures >= ESCALATE_AFTER_FAILURES {
                        tracing::error!(
                            target: "valqeron::audit",
                            operation = "sync_halted",
                            source,
                            consecutive_failures = failures,
                            cooldown_until = %until,
                            error = %error,
                            "sync source keeps failing terminally; halted on \
                             the same period until it succeeds"
                        );
                    } else {
                        tracing::warn!(
                            target: "valqeron::audit",
                            operation = "sync_halted",
                            source,
                            consecutive_failures = failures,
                            cooldown_until = %until,
                            error = %error,
                            "sync run failed terminally; will retry the same \
                             period after the cooldown"
                        );
                    }
                }
                Ok(Ok(None)) => tracing::warn!(
                    source,
                    error = %error,
                    "sync run failed terminally before any cursor existed"
                ),
                Ok(Err(e)) => tracing::warn!(
                    source,
                    error = %e,
                    "recording sync failure on the cursor failed"
                ),
                Err(e) => tracing::warn!(
                    source,
                    error = %e,
                    "recording sync failure on the cursor not executed"
                ),
            }
        })
    }

    fn wake(&self) {
        self.wake.notify_one();
    }

    fn wake_notified<'a>(&'a self) -> BoxFuture<'a, ()> {
        Box::pin(self.wake.notified())
    }
}

// ================ PAYLOAD ================
/// Task payload for sync runs: `slot|from|to` (RFC 3339 slot, ISO dates).
/// Compact, dependency-free, and parsed only in this module.
pub(crate) fn encode_payload(slot: DateTime<Utc>, target: TargetPeriod) -> String {
    format!(
        "{}|{}|{}",
        slot.to_rfc3339_opts(SecondsFormat::Millis, true),
        target.from,
        target.to
    )
}

pub(crate) fn decode_payload(payload: &str) -> Option<(DateTime<Utc>, TargetPeriod)> {
    let mut parts = payload.splitn(3, '|');
    let slot = DateTime::parse_from_rfc3339(parts.next()?)
        .ok()?
        .with_timezone(&Utc);
    let from = parts.next()?.parse().ok()?;
    let to = parts.next()?.parse().ok()?;
    Some((slot, TargetPeriod { from, to }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone};

    fn utc(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0).single().unwrap()
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn payload_round_trips() {
        let slot = utc(2026, 8, 12, 10);
        let target = TargetPeriod {
            from: date(2026, 8, 11),
            to: date(2026, 8, 11),
        };
        let encoded = encode_payload(slot, target);
        assert_eq!(encoded, "2026-08-12T10:00:00.000Z|2026-08-11|2026-08-11");
        assert_eq!(decode_payload(&encoded), Some((slot, target)));
    }

    #[test]
    fn corrupt_payloads_decode_to_none() {
        for bad in [
            "",
            "not-a-date|2026-08-11|2026-08-11",
            "2026-08-12T10:00:00Z|not-a-date|2026-08-11",
            "2026-08-12T10:00:00Z|2026-08-11",
            "2026-08-12T10:00:00Z",
        ] {
            assert_eq!(decode_payload(bad), None, "{bad:?} must not decode");
        }
    }
}

#[cfg(test)]
mod manager_tests {
    use super::*;
    use crate::tasks::plane::PlaneConfig;
    use crate::tasks::{BackgroundTasksManager, TaskSpec};
    use chrono::NaiveTime;
    use std::sync::{Arc, Mutex};
    use valqeron_core::{
        LogPolicy, MarketCalendar, Recurrence, SyncOutcomeKind, TaskCategory,
        TaskRegistrationRepository, TaskStatus, TaskTracking, Versioned,
    };
    use valqeron_infrastructure::DatabaseConfig;

    fn storage() -> (tempfile::TempDir, AsyncStorage) {
        let dir = tempfile::tempdir().expect("create temp dir for test database");
        let storage = AsyncStorage::open(
            dir.path().join("sync.db"),
            DatabaseConfig {
                reader_pool_size: 2,
                ..DatabaseConfig::default()
            },
        )
        .expect("open temp storage");
        (dir, storage)
    }

    async fn wait_until(deadline_secs: u64, mut probe: impl AsyncFnMut() -> bool) {
        let wait = async {
            while !probe().await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let met = tokio::time::timeout(Duration::from_secs(deadline_secs), wait).await;
        assert!(met.is_ok(), "condition not reached within {deadline_secs}s");
    }

    fn daily_b3() -> Schedule {
        Schedule::new(
            MarketCalendar::B3,
            NaiveTime::from_hms_opt(7, 0, 0).unwrap(),
            Recurrence::Daily,
        )
    }

    fn source(name: &str) -> SyncSource {
        SyncSource::new(name).unwrap()
    }

    fn spec(name: &str, kind: &'static str, cooldown_secs: u32) -> TaskSpec {
        TaskSpec {
            kind,
            category: TaskCategory::FinanceDataSync,
            plane: PlaneConfig::Sync {
                source: source(name),
                schedule: daily_b3(),
                retry: RetryPolicy::none(),
                cooldown: CooldownPolicy::new(cooldown_secs),
                max_backfill_days: 90,
            },
            log_policy: LogPolicy::All,
        }
    }

    /// The occurrence `steps` back from the latest one before `now`.
    fn occurrence_back(schedule: &Schedule, now: DateTime<Utc>, steps: u32) -> DateTime<Utc> {
        let mut slot = schedule.latest_occurrence_before(now).unwrap();
        for _ in 0..steps {
            slot = schedule.latest_occurrence_before(slot).unwrap();
        }
        slot
    }

    /// Seed a cursor positioned at `slot` (its target = the previous
    /// business day of the slot's date), as a completed run would leave it.
    async fn seed_cursor(
        storage: &AsyncStorage,
        name: &str,
        schedule: &Schedule,
        slot: DateTime<Utc>,
    ) {
        let through_target = schedule
            .calendar()
            .previous_business_day(schedule.calendar().local_date(slot).unwrap())
            .unwrap();
        let cursor = SyncCursor::seeded(source(name), slot, through_target, Utc::now());
        storage
            .write("test.seed_cursor", false, move |repos| {
                repos.cursors.upsert(&cursor).map_err(StorageError::from)
            })
            .await
            .expect("no backpressure")
            .expect("seed cursor");
    }

    async fn get_cursor(storage: &AsyncStorage, name: &str) -> Option<SyncCursor> {
        let src = source(name);
        storage
            .read("test.get_cursor", move |repos| repos.cursors.get(&src))
            .await
            .expect("no backpressure")
            .expect("get cursor")
    }

    async fn all_rows(storage: &AsyncStorage) -> Vec<Versioned<BackgroundTask>> {
        storage
            .read("test.rows", |repos| repos.tasks.list_recent(100))
            .await
            .expect("no backpressure")
            .expect("list rows")
    }

    type Recorded = Arc<Mutex<Vec<(DateTime<Utc>, TargetPeriod)>>>;

    fn recording_handler(
        outcome: TaskOutcome,
    ) -> (
        Recorded,
        impl Fn(
            crate::tasks::plane::TaskContext,
        ) -> std::pin::Pin<Box<dyn Future<Output = TaskOutcome> + Send>>
        + Clone,
    ) {
        let runs: Recorded = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&runs);
        let handler = move |ctx: crate::tasks::plane::TaskContext| -> std::pin::Pin<Box<dyn Future<Output = TaskOutcome> + Send>> {
            let sink = Arc::clone(&sink);
            let outcome = outcome.clone();
            Box::pin(async move {
                if let RunWindow::Period { slot, target } = ctx.window {
                    sink.lock().expect("runs lock").push((slot, target));
                }
                outcome
            })
        };
        (runs, handler)
    }

    // ================ THE HEADLINE: SEQUENTIAL BACKFILL ================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ten_missed_business_days_backfill_sequentially_in_order() {
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let now = Utc::now();

        // Cursor as a run 10 occurrences ago left it: 10 slots are missed.
        let seed_slot = occurrence_back(&schedule, now, 10);
        let latest = occurrence_back(&schedule, now, 0);
        seed_cursor(&storage, "cvm", &schedule, seed_slot).await;

        let (runs, handler) = recording_handler(TaskOutcome::Done);
        let manager = BackgroundTasksManager::builder()
            .register(spec("cvm", "test_cvm_sync", 300), handler)
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(20, async || probe.lock().expect("lock").len() >= 10).await;
        let probe_storage = storage.clone();
        wait_until(20, async || {
            get_cursor(&probe_storage, "cvm")
                .await
                .is_some_and(|c| c.through_slot() == latest)
        })
        .await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        // Exactly ten runs, strictly ascending, each covering exactly one
        // business day, contiguous with the previous.
        let runs = runs.lock().expect("lock").clone();
        assert_eq!(runs.len(), 10, "one run per missed business day");
        let calendar = schedule.calendar();
        let mut expected_from = calendar
            .next_business_day(
                calendar
                    .previous_business_day(calendar.local_date(seed_slot).unwrap())
                    .unwrap(),
            )
            .unwrap();
        for (i, (slot, target)) in runs.iter().enumerate() {
            if i > 0 {
                assert!(
                    *slot > runs[i - 1].0,
                    "slots must be strictly ascending: {runs:?}"
                );
            }
            assert_eq!(target.from, target.to, "daily runs cover one day");
            assert_eq!(
                target.from, expected_from,
                "run {i} covers the next uncovered business day"
            );
            expected_from = calendar.next_business_day(target.to).unwrap();
        }

        let cursor = get_cursor(&storage, "cvm").await.expect("cursor exists");
        assert_eq!(cursor.through_slot(), latest);
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::Synced));
        assert_eq!(cursor.consecutive_failures(), 0);

        // And the next occurrence is already seeded, in the future.
        let rows = all_rows(&storage).await;
        let pending: Vec<_> = rows
            .iter()
            .filter(|t| t.data.status() == TaskStatus::Pending)
            .collect();
        assert_eq!(pending.len(), 1, "exactly one future row: {rows:?}");
        assert!(
            pending.iter().all(|t| t.data.scheduled_at() > now),
            "the seeded row waits for a future slot"
        );
        let succeeded = rows
            .iter()
            .filter(|t| t.data.status() == TaskStatus::Succeeded)
            .count();
        assert_eq!(succeeded, 10);
    }

    // ================ OUTCOMES ================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn not_ready_holds_the_cursor_without_burning_failures() {
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let seed_slot = occurrence_back(&schedule, Utc::now(), 2);
        seed_cursor(&storage, "cvm", &schedule, seed_slot).await;

        let (runs, handler) = recording_handler(TaskOutcome::NotReady {
            retry_after_secs: 3600,
        });
        let manager = BackgroundTasksManager::builder()
            .register(spec("cvm", "test_cvm_sync", 300), handler)
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(10, async || !probe.lock().expect("lock").is_empty()).await;
        // Give the post-completion reconcile a chance to (wrongly) seed.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        assert_eq!(
            runs.lock().expect("lock").len(),
            1,
            "no run during cooldown"
        );
        let cursor = get_cursor(&storage, "cvm").await.expect("cursor exists");
        assert_eq!(cursor.through_slot(), seed_slot, "position held");
        assert_eq!(cursor.consecutive_failures(), 0, "not a failure");
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::NotReady));
        assert!(cursor.cooldown_until().is_some());

        let rows = all_rows(&storage).await;
        assert_eq!(rows.len(), 1, "one task row total: {rows:?}");
        assert_eq!(
            rows[0].data.status(),
            TaskStatus::Succeeded,
            "not-ready is not an error"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_failure_halts_on_the_same_slot_with_cooldown() {
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let seed_slot = occurrence_back(&schedule, Utc::now(), 2);
        let first_missed = schedule.next_occurrence_after(seed_slot).unwrap();
        seed_cursor(&storage, "cvm", &schedule, seed_slot).await;

        let (runs, handler) = recording_handler(TaskOutcome::Failed("cvm exploded".into()));
        // Cooldown base 3600s: after the first terminal failure nothing
        // more may be seeded within this test's lifetime.
        let manager = BackgroundTasksManager::builder()
            .register(spec("cvm", "test_cvm_sync", 3600), handler)
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(10, async || {
            get_cursor(&probe_storage, "cvm")
                .await
                .is_some_and(|c| c.consecutive_failures() == 1)
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        assert_eq!(runs.lock().expect("lock").len(), 1);
        let cursor = get_cursor(&storage, "cvm").await.expect("cursor exists");
        assert_eq!(cursor.through_slot(), seed_slot, "cursor never advanced");
        assert_eq!(cursor.last_outcome(), Some(SyncOutcomeKind::Failed));
        assert_eq!(cursor.last_error(), Some("cvm exploded"));
        assert!(cursor.cooldown_until().is_some_and(|u| u > Utc::now()));

        let rows = all_rows(&storage).await;
        assert_eq!(rows.len(), 1, "halted: no further seeds: {rows:?}");
        assert_eq!(rows[0].data.status(), TaskStatus::Failed);
        assert_eq!(rows[0].data.scheduled_at(), first_missed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_cooldown_retries_the_same_slot_and_counts_failures() {
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let seed_slot = occurrence_back(&schedule, Utc::now(), 2);
        let first_missed = schedule.next_occurrence_after(seed_slot).unwrap();
        seed_cursor(&storage, "cvm", &schedule, seed_slot).await;

        let (runs, handler) = recording_handler(TaskOutcome::Failed("still broken".into()));
        let manager = BackgroundTasksManager::builder()
            .register(spec("cvm", "test_cvm_sync", 0), handler)
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(15, async || {
            get_cursor(&probe_storage, "cvm")
                .await
                .is_some_and(|c| c.consecutive_failures() >= 3)
        })
        .await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        // Halt semantics: every retry targets the same first missed slot.
        let runs = runs.lock().expect("lock").clone();
        assert!(runs.len() >= 3);
        assert!(
            runs.iter().all(|(slot, _)| *slot == first_missed),
            "all retries target the first missed slot: {runs:?}"
        );
        let cursor = get_cursor(&storage, "cvm").await.expect("cursor exists");
        assert_eq!(cursor.through_slot(), seed_slot, "never advanced");
        assert!(cursor.consecutive_failures() >= 3);
    }

    // ================ COLD START & CLAMP ================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_start_runs_only_the_most_recent_period() {
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let now = Utc::now();
        let latest = occurrence_back(&schedule, now, 0);

        let (runs, handler) = recording_handler(TaskOutcome::Done);
        let manager = BackgroundTasksManager::builder()
            .register(spec("cvm", "test_cvm_sync", 300), handler)
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(10, async || {
            get_cursor(&probe_storage, "cvm")
                .await
                .is_some_and(|c| c.through_slot() == latest)
        })
        .await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        let runs = runs.lock().expect("lock").clone();
        assert_eq!(runs.len(), 1, "no history on a fresh install");
        let (slot, target) = runs[0];
        assert_eq!(slot, latest);
        let expected_target = schedule
            .calendar()
            .previous_business_day(schedule.calendar().local_date(latest).unwrap())
            .unwrap();
        assert_eq!(target.from, expected_target);
        assert_eq!(target.to, expected_target);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_cursor_beyond_the_cap_skips_ahead_to_the_latest() {
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let now = Utc::now();
        let seed_slot = occurrence_back(&schedule, now, 10);
        let latest = occurrence_back(&schedule, now, 0);
        seed_cursor(&storage, "cvm", &schedule, seed_slot).await;

        let (runs, handler) = recording_handler(TaskOutcome::Done);
        let mut clamped = spec("cvm", "test_cvm_sync", 300);
        if let PlaneConfig::Sync {
            max_backfill_days, ..
        } = &mut clamped.plane
        {
            *max_backfill_days = 3; // 10 pending days > 3 → skip ahead.
        }
        let manager = BackgroundTasksManager::builder()
            .register(clamped, handler)
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(10, async || {
            get_cursor(&probe_storage, "cvm")
                .await
                .is_some_and(|c| c.through_slot() == latest)
        })
        .await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        let runs = runs.lock().expect("lock").clone();
        assert_eq!(runs.len(), 1, "the gap is skipped, not backfilled");
        assert_eq!(runs[0].0, latest, "only the most recent slot ran");
    }

    // ================ REGISTRY & PAUSE ================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_sources_advance_independently() {
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let now = Utc::now();
        let a_seed = occurrence_back(&schedule, now, 3);
        let latest = occurrence_back(&schedule, now, 0);
        seed_cursor(&storage, "alpha", &schedule, a_seed).await;
        // "beta" cold-starts.

        let (a_runs, a_handler) = recording_handler(TaskOutcome::Done);
        let (b_runs, b_handler) = recording_handler(TaskOutcome::Done);
        let manager = BackgroundTasksManager::builder()
            .register(spec("alpha", "test_alpha_sync", 300), a_handler)
            .register(spec("beta", "test_beta_sync", 300), b_handler)
            .start(storage.clone())
            .await;

        let probe_storage = storage.clone();
        wait_until(15, async || {
            let a = get_cursor(&probe_storage, "alpha").await;
            let b = get_cursor(&probe_storage, "beta").await;
            a.is_some_and(|c| c.through_slot() == latest)
                && b.is_some_and(|c| c.through_slot() == latest)
        })
        .await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        assert_eq!(a_runs.lock().expect("lock").len(), 3, "alpha caught up");
        assert_eq!(b_runs.lock().expect("lock").len(), 1, "beta cold-started");

        let rows = all_rows(&storage).await;
        let alpha_rows = rows
            .iter()
            .filter(|t| t.data.kind().as_str() == "test_alpha_sync")
            .count();
        let beta_rows = rows
            .iter()
            .filter(|t| t.data.kind().as_str() == "test_beta_sync")
            .count();
        assert_eq!(alpha_rows, 4, "3 succeeded + 1 future");
        assert_eq!(beta_rows, 2, "1 succeeded + 1 future");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn duplicate_sync_kind_is_skipped_with_no_cursor() {
        let (_dir, storage) = storage();
        let (runs, handler) = recording_handler(TaskOutcome::Done);

        let manager = BackgroundTasksManager::builder()
            .register(
                TaskSpec {
                    kind: "test_dup_kind",
                    category: TaskCategory::EngineSystem,
                    plane: PlaneConfig::Interval {
                        period: Duration::from_secs(600),
                        jitter: false,
                        tracking: crate::tasks::plane::Tracking::Durable,
                    },
                    log_policy: LogPolicy::All,
                },
                |_ctx| async { TaskOutcome::Done },
            )
            .register(spec("cvm", "test_dup_kind", 300), handler)
            .start(storage.clone())
            .await;

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        assert!(runs.lock().expect("lock").is_empty(), "source never ran");
        assert!(
            get_cursor(&storage, "cvm").await.is_none(),
            "no sync seeder, no cursor"
        );
        assert!(all_rows(&storage).await.is_empty(), "nothing seeded");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn paused_source_resumes_with_sequential_catchup() {
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let now = Utc::now();
        let seed_slot = occurrence_back(&schedule, now, 3);
        let latest = occurrence_back(&schedule, now, 0);
        seed_cursor(&storage, "cvm", &schedule, seed_slot).await;

        // Pause before boot: catalog the kind so the flag has a row, then
        // flip it — the boot reconcile must preserve it.
        storage
            .write("test.pause", false, move |repos| {
                let now = Utc::now();
                let declaration = valqeron_core::TaskDeclaration {
                    kind: TaskKind::new("test_cvm_sync").unwrap(),
                    category: TaskCategory::FinanceDataSync,
                    tier: valqeron_core::TaskTier::Sync,
                    tracking: TaskTracking::Durable,
                    schedule: "sync:daily@07:00-03:00".into(),
                    source: Some(SyncSource::new("cvm").unwrap()),
                    log_policy: LogPolicy::All,
                    config_enabled: true,
                };
                repos.registry.declare(&declaration, now)?;
                let kind = TaskKind::new("test_cvm_sync").unwrap();
                repos.registry.set_paused(&kind, true, now)?;
                Ok::<_, StorageError>(())
            })
            .await
            .unwrap()
            .unwrap();

        let (runs, handler) = recording_handler(TaskOutcome::Done);
        let manager = BackgroundTasksManager::builder()
            .register(spec("cvm", "test_cvm_sync", 300), handler)
            .start(storage.clone())
            .await;

        // Paused across boot: the stale cursor must NOT trigger catch-up.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(runs.lock().expect("lock").is_empty(), "paused: no runs");
        assert!(all_rows(&storage).await.is_empty(), "paused: no seeds");

        // Unpause and kick the seeder (production waits for the 60s tick).
        storage
            .write("test.unpause", false, move |repos| {
                let kind = TaskKind::new("test_cvm_sync").unwrap();
                repos.registry.set_paused(&kind, false, Utc::now())?;
                Ok::<_, StorageError>(())
            })
            .await
            .unwrap()
            .unwrap();
        manager.kick("test_cvm_sync");

        // Sequential catch-up of the three missed periods, then steady state.
        let probe_storage = storage.clone();
        wait_until(15, async || {
            get_cursor(&probe_storage, "cvm")
                .await
                .is_some_and(|c| c.through_slot() == latest)
        })
        .await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        let runs = runs.lock().expect("lock").clone();
        assert_eq!(runs.len(), 3, "one run per missed period after resume");
        assert!(
            runs.windows(2).all(|w| w[0].0 < w[1].0),
            "strictly chronological: {runs:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_between_handler_success_and_completion_reruns_idempotently() {
        // Simulates the at-least-once seam: a row left RUNNING with the
        // cursor already advanced (crash after the handler + cursor write,
        // before completion). Recovery must requeue and re-run it, and the
        // second run must leave the cursor exactly where it was.
        let (_dir, storage) = storage();
        let schedule = daily_b3();
        let now = Utc::now();
        let seed_slot = occurrence_back(&schedule, now, 1);
        let missed = schedule.next_occurrence_after(seed_slot).unwrap();
        let target = schedule
            .target_period(
                schedule
                    .calendar()
                    .previous_business_day(schedule.calendar().local_date(seed_slot).unwrap())
                    .unwrap(),
                missed,
            )
            .unwrap();

        // Cursor already advanced past `missed`…
        seed_cursor(&storage, "cvm", &schedule, missed).await;
        // …but the row for `missed` is still RUNNING (claimed, never
        // completed).
        let payload = encode_payload(missed, target);
        storage
            .write("test.seed_running", false, move |repos| {
                let task = BackgroundTask::builder()
                    .kind(TaskKind::new("test_cvm_sync").expect("valid kind"))
                    .payload(payload)
                    .scheduled_at(missed)
                    // Attempt budget left, so recovery requeues instead of
                    // terminally failing the interrupted run.
                    .max_attempts(2)
                    .build()
                    .expect("valid task");
                repos.tasks.insert(&task).map_err(StorageError::from)?;
                let claimed = repos
                    .tasks
                    .claim_due(Utc::now(), 8)
                    .map_err(StorageError::from)?;
                assert_eq!(claimed.len(), 1);
                Ok::<_, StorageError>(())
            })
            .await
            .expect("no backpressure")
            .expect("seed running row");

        let (runs, handler) = recording_handler(TaskOutcome::Done);
        let manager = BackgroundTasksManager::builder()
            .register(spec("cvm", "test_cvm_sync", 300), handler)
            .start(storage.clone())
            .await;

        let probe = Arc::clone(&runs);
        wait_until(10, async || !probe.lock().expect("lock").is_empty()).await;
        let probe_storage = storage.clone();
        wait_until(10, async || {
            all_rows(&probe_storage)
                .await
                .iter()
                .any(|t| t.data.status() == TaskStatus::Succeeded)
        })
        .await;
        assert!(manager.drain(Duration::from_secs(3)).await);

        let runs = runs.lock().expect("lock").clone();
        assert_eq!(runs[0].0, missed, "the recovered run targets its own slot");
        let cursor = get_cursor(&storage, "cvm").await.expect("cursor exists");
        assert_eq!(
            cursor.through_slot(),
            missed,
            "re-running an already-recorded slot is a no-op on the cursor"
        );
    }
}
