use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::signal::unix::{SignalKind, signal};
use tokio::time::MissedTickBehavior;
use valqeron_infrastructure::{DatabaseConfig, SqliteStorageEngine};

use crate::config::{EngineConfig, READER_POOL_SIZE};
use crate::error::{EngineError, EngineResult};
use crate::lockfile::EngineLock;

/// How long the graceful shutdown waits for an in-flight maintenance job.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound on draining the blocking pool when the runtime shuts down. A stuck
/// job must not hold the process open forever — the service manager's
/// SIGKILL (after `TimeoutStopSec` / launchd's exit timeout) is the final
/// backstop.
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

/// Foreground engine entry point: lock → open (migrations) → scheduled jobs
/// → ordered shutdown.
///
/// Shutdown order matters: all background work stops **before** the storage
/// engine drops, because `Drop` runs `PRAGMA optimize` plus a
/// `wal_checkpoint(TRUNCATE)` that must not race in-flight writes. The lock
/// releases last, after the checkpoint proves the database is quiesced.
pub fn run(config: &EngineConfig) -> EngineResult<()> {
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        db_path = %config.db_path().display(),
        lock_path = %config.lock_path().display(),
        maintenance_interval_secs = config.maintenance_interval().as_secs(),
        heartbeat_interval_secs = config.heartbeat_interval().as_secs(),
        durability = config.durability_label(),
        reader_pool_size = READER_POOL_SIZE,
        "valqeron-engine starting"
    );

    config.ensure_db_parent()?;
    let lock = EngineLock::acquire(config.db_path(), config.lock_path())?;
    tracing::info!(
        target: "valqeron::audit",
        operation = "engine_start",
        lock_path = %lock.path().display(),
        "single-instance lock acquired"
    );

    let db_config = DatabaseConfig {
        reader_pool_size: READER_POOL_SIZE,
        synchronous: config.synchronous(),
        ..DatabaseConfig::default()
    };
    let engine = Arc::new(SqliteStorageEngine::open(config.db_path(), db_config)?);
    tracing::info!("database open; migrations applied");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| EngineError::Io(format!("building tokio runtime: {e}")))?;

    let loop_result = runtime.block_on(run_loop(Arc::clone(&engine), config));

    // Wait (bounded) for any still-running blocking task before the final
    // checkpoint; queued-but-unstarted tasks are dropped.
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);

    match Arc::try_unwrap(engine) {
        Ok(engine) => {
            // Drop = PRAGMA optimize + wal_checkpoint(TRUNCATE).
            drop(engine);
            tracing::info!("final WAL checkpoint complete");
        }
        Err(_still_shared) => {
            tracing::warn!(
                "a blocking task still holds the storage engine; skipping the final checkpoint"
            );
        }
    }

    drop(lock);
    tracing::info!(
        target: "valqeron::audit",
        operation = "engine_stop",
        "engine stopped cleanly"
    );
    loop_result
}

async fn run_loop(engine: Arc<SqliteStorageEngine>, config: &EngineConfig) -> EngineResult<()> {
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| EngineError::Io(format!("installing SIGTERM handler: {e}")))?;
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| EngineError::Io(format!("installing SIGINT handler: {e}")))?;

    let started = Instant::now();

    // First ticks are one full period out (the startup banner already covers
    // "the engine is alive"); the maintenance period carries jitter so
    // periodic jobs do not synchronise with other periodic load.
    let maintenance_period = jittered(config.maintenance_interval());
    let mut maintenance = tokio::time::interval_at(
        tokio::time::Instant::now()
            .checked_add(maintenance_period)
            .unwrap_or_else(tokio::time::Instant::now),
        maintenance_period,
    );
    maintenance.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let heartbeat_period = config.heartbeat_interval();
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now()
            .checked_add(heartbeat_period)
            .unwrap_or_else(tokio::time::Instant::now),
        heartbeat_period,
    );
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut in_flight: Option<tokio::task::JoinHandle<()>> = None;

    let reason = loop {
        tokio::select! {
            _ = sigterm.recv() => break "SIGTERM",
            _ = sigint.recv() => break "SIGINT",
            _ = maintenance.tick() => {
                let busy = in_flight.as_ref().is_some_and(|h| !h.is_finished());
                if busy {
                    tracing::warn!(
                        job = "db_maintenance",
                        "previous run still in flight; skipping this tick"
                    );
                } else {
                    let engine = Arc::clone(&engine);
                    in_flight = Some(tokio::task::spawn_blocking(move || {
                        run_maintenance_job(&engine);
                    }));
                }
            }
            _ = heartbeat.tick() => {
                tracing::info!(
                    job = "heartbeat",
                    uptime_secs = started.elapsed().as_secs(),
                    "engine alive"
                );
            }
        }
    };

    tracing::info!(
        signal = reason,
        "shutdown requested; draining background work"
    );

    if let Some(handle) = in_flight.take()
        && !handle.is_finished()
    {
        // A second signal during the drain forces an immediate (non-zero)
        // exit instead of waiting out the deadline.
        tokio::select! {
            _ = sigterm.recv() => return Err(EngineError::ForcedShutdown),
            _ = sigint.recv() => return Err(EngineError::ForcedShutdown),
            joined = tokio::time::timeout(DRAIN_TIMEOUT, handle) => match joined {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "maintenance task failed during drain");
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        drain_timeout_secs = DRAIN_TIMEOUT.as_secs(),
                        "maintenance task did not finish within the drain deadline"
                    );
                }
            },
        }
    }

    Ok(())
}

/// One maintenance run, executed on the blocking pool — never on the
/// runtime thread. Failures are logged and retried on the next tick; they
/// must not take the daemon down.
fn run_maintenance_job(engine: &SqliteStorageEngine) {
    let started = Instant::now();
    match engine.run_maintenance() {
        Ok(Some(stats)) => tracing::info!(
            target: "valqeron::audit",
            operation = "db_maintenance",
            busy = stats.busy,
            wal_frames = stats.log_frames,
            checkpointed_frames = stats.checkpointed_frames,
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "maintenance completed"
        ),
        Ok(None) => tracing::info!(
            target: "valqeron::audit",
            operation = "db_maintenance",
            "maintenance completed (no WAL to checkpoint)"
        ),
        Err(e) => tracing::warn!(
            target: "valqeron::audit",
            operation = "db_maintenance",
            error = %e,
            "maintenance failed; retrying at the next interval"
        ),
    }
}

/// Scale a period into 90%..=110% using clock sub-second noise — enough to
/// de-synchronise periodic jobs without pulling in an RNG dependency.
fn jittered(base: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let percent = u64::from(90u32.saturating_add(nanos.checked_rem(21).unwrap_or(0)));
    let millis = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
    let scaled = millis
        .saturating_mul(percent)
        .checked_div(100)
        .unwrap_or(millis);
    if scaled == 0 {
        base
    } else {
        Duration::from_millis(scaled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_ten_percent() {
        let base = Duration::from_secs(3600);
        for _ in 0..50 {
            let j = jittered(base);
            assert!(
                j >= Duration::from_secs(3240) && j <= Duration::from_secs(3960),
                "jittered value out of ±10% envelope: {j:?}"
            );
        }
    }

    #[test]
    fn jitter_never_zeroes_a_tiny_period() {
        assert!(jittered(Duration::from_millis(1)) > Duration::ZERO);
    }
}
