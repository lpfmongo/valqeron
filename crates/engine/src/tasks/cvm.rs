//! FINANCE_DATA_SYNC: the CVM daily sync source.
//!
//! Runs every business day at the configured America/Sao_Paulo time and
//! targets the previous business day ("Friday syncs Thursday, Monday syncs
//! Friday"), with full sequential catch-up after downtime — all provided by
//! the sync trigger. The schedule, cooldown, and backfill knobs below are
//! code *defaults*: the registry row (operator-editable, boot-preserved)
//! is what actually schedules the source, and the `enabled` flag there
//! turns it off. The handler is a placeholder: it logs the period it
//! would ingest and reports `Done`, which advances the cursor. Real
//! ingestion (and this module's own tables + repositories) lands later and
//! changes only this file.

use chrono::{NaiveTime, SecondsFormat};
use valqeron_core::{MarketCalendar, Recurrence, Schedule, SyncSource};

use crate::scheduler::{
    RetryPolicy, RunWindow, SchedulerBuilder, TaskContext, TaskDefinition, TaskOutcome,
};

pub(crate) const CVM_DAILY_SYNC_TASK: &str = "cvm_daily_sync";

/// Cursor key and log identity of the CVM source.
pub(crate) const CVM_SYNC_SOURCE: &str = "cvm";

/// Default market-local time of day the sync runs at (07:00 São Paulo).
const DEFAULT_SYNC_AT: (u32, u32) = (7, 0);

/// Default failure-cooldown base and unattended catch-up bound.
const DEFAULT_SYNC_COOLDOWN_SECS: u32 = 300;
const DEFAULT_SYNC_MAX_BACKFILL_DAYS: u32 = 90;

/// Task-level retries of one CVM run: transient faults get three attempts
/// five minutes apart before the failure is terminal (and the cursor-level
/// cooldown takes over). Identical to the framework default, restated here
/// because CVM's cadence was tuned deliberately.
const CVM_SYNC_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    retry_delay_secs: 300,
};

fn default_at() -> NaiveTime {
    let (h, m) = DEFAULT_SYNC_AT;
    NaiveTime::from_hms_opt(h, m, 0).unwrap_or_default()
}

/// Register the CVM sync source with its code defaults. Enablement and
/// tuning live on the registry row.
pub(crate) fn register(builder: SchedulerBuilder) -> SchedulerBuilder {
    let Ok(source) = SyncSource::new(CVM_SYNC_SOURCE) else {
        // Unreachable with a valid constant; refuse loudly rather than boot
        // a half-registered source.
        tracing::error!(source = CVM_SYNC_SOURCE, "invalid CVM sync source name");
        return builder;
    };

    builder.task(
        TaskDefinition::sync(
            CVM_DAILY_SYNC_TASK,
            source,
            Schedule::new(MarketCalendar::B3, default_at(), Recurrence::Daily),
        )
        .retry(CVM_SYNC_RETRY)
        .cooldown_secs(DEFAULT_SYNC_COOLDOWN_SECS)
        .max_backfill_days(DEFAULT_SYNC_MAX_BACKFILL_DAYS)
        .run(|ctx: TaskContext| async move {
            let RunWindow::Period { slot, target } = ctx.window else {
                return TaskOutcome::Failed("cvm sync run without a period window".into());
            };
            tracing::info!(
                target: "valqeron::audit",
                operation = "cvm_daily_sync",
                source = CVM_SYNC_SOURCE,
                slot = %slot.to_rfc3339_opts(SecondsFormat::Millis, true),
                target_from = %target.from,
                target_to = %target.to,
                "CVM daily sync placeholder — no ingestion implemented yet"
            );
            TaskOutcome::Done
        }),
    )
}
