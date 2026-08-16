//! FINANCE_DATA_SYNC: the CVM daily sync source.
//!
//! Runs every business day at the configured America/Sao_Paulo time and
//! targets the previous business day ("Friday syncs Thursday, Monday syncs
//! Friday"), with full sequential catch-up after downtime — all provided by
//! the sync plane. The handler is a placeholder: it logs the period it
//! would ingest and reports `Done`, which advances the cursor. Real
//! ingestion (and this module's own tables + repositories) lands later and
//! changes only this file.

use chrono::{NaiveTime, SecondsFormat};
use valqeron_core::{
    CooldownPolicy, LogPolicy, MarketCalendar, Recurrence, Schedule, SyncSource, TaskCategory,
    TaskDeclaration, TaskKind, TaskTier, TaskTracking,
};

use crate::engine::{DEFAULT_SYNC_AT, EngineConfig};
use crate::tasks::plane::{PlaneConfig, RetryPolicy, RunWindow, TaskOutcome};
use crate::tasks::{BackgroundTasksBuilder, TaskSpec};

pub(crate) const CVM_DAILY_SYNC_TASK: &str = "cvm_daily_sync";

/// Cursor key and log identity of the CVM source.
pub(crate) const CVM_SYNC_SOURCE: &str = "cvm";

/// Task-level retries of one CVM run: transient faults get three attempts
/// five minutes apart before the failure is terminal (and the cursor-level
/// cooldown takes over).
const CVM_SYNC_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    retry_delay_secs: 300,
};

fn default_at() -> NaiveTime {
    let (h, m) = DEFAULT_SYNC_AT;
    NaiveTime::from_hms_opt(h, m, 0).unwrap_or_default()
}

/// Register the CVM sync source, or — when disabled via the environment —
/// declare it so the catalog shows `disabled` instead of a silent absence.
pub(crate) fn register(
    builder: BackgroundTasksBuilder,
    config: &EngineConfig,
) -> BackgroundTasksBuilder {
    let Ok(source) = SyncSource::new(CVM_SYNC_SOURCE) else {
        // Unreachable with a valid constant; refuse loudly rather than boot
        // a half-registered source.
        tracing::error!(source = CVM_SYNC_SOURCE, "invalid CVM sync source name");
        return builder;
    };

    let Some(settings) = config.cvm_sync() else {
        tracing::info!(
            source = CVM_SYNC_SOURCE,
            "CVM sync disabled via environment"
        );
        let Ok(kind) = TaskKind::new(CVM_DAILY_SYNC_TASK) else {
            return builder;
        };
        let default_schedule = Schedule::new(MarketCalendar::B3, default_at(), Recurrence::Daily);
        return builder.declare_disabled(TaskDeclaration {
            kind,
            category: TaskCategory::FinanceDataSync,
            tier: TaskTier::Sync,
            tracking: TaskTracking::Durable,
            schedule: format!("sync:{}", default_schedule.descriptor()),
            source: Some(source),
            log_policy: LogPolicy::All,
            config_enabled: false,
        });
    };

    builder.register(
        TaskSpec {
            kind: CVM_DAILY_SYNC_TASK,
            category: TaskCategory::FinanceDataSync,
            plane: PlaneConfig::Sync {
                source,
                schedule: Schedule::new(MarketCalendar::B3, settings.at, settings.recurrence),
                retry: CVM_SYNC_RETRY,
                cooldown: CooldownPolicy::new(settings.cooldown_secs),
                max_backfill_days: settings.max_backfill_days,
            },
            log_policy: LogPolicy::All,
        },
        |ctx| async move {
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
        },
    )
}
