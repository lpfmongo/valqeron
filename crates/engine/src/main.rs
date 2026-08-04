#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_used
    )
)]

//! `valqeron-engine` — a user-bounded background daemon for Valqeron.
//!
//! v1 scope: own long-lived database duties (migrations at startup, periodic
//! `PRAGMA optimize` + passive WAL checkpoints) behind a single-instance
//! lock, with graceful shutdown and launchd/systemd service management.
//!
//! The engine deliberately does **not** lock the CLI out of the database yet
//! (phase 1 of the ownership plan): SQLite's WAL mode plus busy timeouts make
//! cross-process coexistence safe. Exclusive ownership arrives with the gRPC
//! surface and the client library.
//!
//! tokio note: this is the only crate in the workspace allowed to use tokio
//! (`current_thread` today, `multi_thread` when the gRPC edge lands).
//! `valqeron-core` and `valqeron-infrastructure` stay async-free.

mod cli;
mod config;
mod error;
mod lockfile;
mod logging;
mod runtime;
mod service;

use clap::Parser;

use crate::cli::{Cli, Command};
use crate::config::EngineConfig;
use crate::error::EngineResult;

fn main() {
    let cli = Cli::parse();
    if let Err(err) = dispatch(&cli) {
        eprintln!("error: {err}");
        std::process::exit(err.exit_code());
    }
}

fn dispatch(cli: &Cli) -> EngineResult<()> {
    match &cli.command {
        Command::Run(args) => {
            let config = EngineConfig::resolve(cli, args)?;
            let _log_guard = logging::init(cli.log_level(), config.log_file());
            runtime::run(&config)
        }
        Command::Install(args) => service::install(cli, args),
        Command::Uninstall => service::uninstall(),
        Command::Status => service::status(cli),
    }
}
