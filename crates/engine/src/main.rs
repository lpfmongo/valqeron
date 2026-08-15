#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_used
    )
)]

//! `valqeron-engine` — the daemon that owns the Valqeron database.
//!
//! The engine holds the SQLite file exclusively behind a single-instance
//! lock, runs migrations at startup, serves the gRPC API over a Unix domain
//! socket (`IssuerService` + `AdminService`), performs periodic maintenance
//! (`PRAGMA optimize` + passive WAL checkpoints), and shuts down gracefully.
//! Clients (`valqeron` CLI, future desktop app) reach the database only
//! through `valqeron-client` — no other process opens the file.
//!
//! The binary takes **no arguments**: it is a service-manager payload,
//! configured entirely through `VALQERON_*` environment variables set in the
//! service definition (see `scripts/install/*.example`). Starting is the
//! service manager's (or the operator's) job; stopping is signal-driven.
//! Installing the engine as a login service is a *separate lifecycle* handled
//! outside this binary (`just engine-install` / `just engine-uninstall`).
//!
//! Async containment: the gRPC edge runs on a `multi_thread` tokio runtime,
//! but every storage call crosses to tokio's blocking pool through the
//! bounded [`storage::AsyncStorage`] facade. `valqeron-core` and
//! `valqeron-infrastructure` stay fully synchronous and async-free.

mod engine;
mod grpc;
mod lifecycle;
mod notify;
mod storage;
mod tasks;

use std::io::IsTerminal;
use std::path::Path;
use std::process::ExitCode;

use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use crate::engine::{ENGINE_LOG_LEVEL_ENV, EngineConfig, EngineResult, ValqeronEngine, exit_code};

fn main() -> ExitCode {
    // Configuration comes exclusively from VALQERON_* environment variables
    // (set in the service definition); argv carries nothing. Rejecting every
    // argument makes a stale service definition (e.g. one still passing the
    // old `run` subcommand) fail fast and visibly in the manager's log
    // instead of silently diverging from the expected contract.
    if std::env::args_os().len() > 1 {
        eprintln!(
            "error: valqeron-engine takes no arguments; configure it via VALQERON_* \
             environment variables in the service definition (see scripts/install/)"
        );
        return ExitCode::from(u8::try_from(exit_code::CONFIG).unwrap_or(1));
    }

    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(u8::try_from(err.exit_code()).unwrap_or(1))
        }
    }
}

fn run() -> EngineResult<()> {
    let config = EngineConfig::resolve()?;
    let _log_guard = init_logging(Level::INFO, config.log_file());
    ValqeronEngine::run(&config)
}

/// Dual-layer tracing, mirroring the CLI's setup: compact human output on
/// stderr (the service manager's log stream) plus an optional JSON file layer.
///
/// Unlike the CLI, the `valqeron::audit` target stays **enabled** on stderr:
/// for a daemon, stderr *is* the operator surface (journald / launchd log
/// files), and audit events belong there.
fn init_logging(
    level: Level,
    log_file: Option<&Path>,
) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let stderr_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.to_string()));

    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .compact()
        .with_filter(stderr_filter);

    let file = log_file.and_then(|path| match open_log_file(path) {
        Ok(handle) => Some(handle),
        Err(e) => {
            eprintln!("warning: file logging disabled: {e}");
            None
        }
    });

    match file {
        Some((non_blocking, guard)) => {
            let file_filter = EnvFilter::new(file_log_level());

            let file_layer = fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .json()
                .with_filter(file_filter);

            tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer)
                .init();
            Some(guard)
        }
        None => {
            tracing_subscriber::registry().with(stderr_layer).init();
            None
        }
    }
}

/// Level for the JSON file layer: `VALQERON_ENGINE_LOG_LEVEL`, default `info`.
fn file_log_level() -> String {
    std::env::var(ENGINE_LOG_LEVEL_ENV).unwrap_or_else(|_| "info".to_string())
}

fn open_log_file(
    path: &Path,
) -> std::io::Result<(
    tracing_appender::non_blocking::NonBlocking,
    tracing_appender::non_blocking::WorkerGuard,
)> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    Ok(tracing_appender::non_blocking(file))
}
