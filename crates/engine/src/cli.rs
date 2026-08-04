use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use tracing::Level;

#[derive(Parser, Debug)]
#[command(
    name = "valqeron-engine",
    bin_name = "valqeron-engine",
    author,
    version,
    about = "Valqeron engine daemon",
    long_about = "Valqeron engine — a user-bounded background process that owns long-lived \
                  database duties: migrations at startup and periodic maintenance \
                  (PRAGMA optimize + WAL checkpoints).\n\n\
                  `run` executes in the foreground and is what the service manager invokes; \
                  `install` registers it as a launchd agent (macOS) or systemd user unit \
                  (Linux) started at login.",
    propagate_version = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    #[arg(
        short,
        long,
        global = true,
        action = clap::ArgAction::Count,
        long_help = "Increase stderr log verbosity:\n\
                     -v     debug\n\
                     -vv    trace\n\
                     (default: info; RUST_LOG overrides)\n\n\
                     This affects stderr only. The log file (on by default) always\n\
                     records operations at info level (VALQERON_ENGINE_LOG_LEVEL overrides)."
    )]
    pub verbose: u8,

    #[arg(
        long,
        global = true,
        value_name = "PATH",
        env = "VALQERON_DB",
        help = "Database file path.",
        long_help = "Path to the SQLite database file. Overrides VALQERON_DB and the default. \
                     Must resolve to the same file the CLI uses."
    )]
    pub db_path: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the engine in the foreground (what the service manager executes).
    Run(RunArgs),
    /// Install and start the engine as a login service (launchd/systemd user unit).
    Install(InstallArgs),
    /// Stop the login service and remove its definition.
    Uninstall,
    /// Report engine state: lock holder, process liveness, service registration.
    Status,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    #[arg(
        long,
        value_name = "FILE",
        help = "Log file location.",
        long_help = "Path to the JSON log file. Overrides VALQERON_ENGINE_LOG_FILE and the default.",
        default_missing_value = "",
        num_args = 0..=1
    )]
    pub log_file: Option<PathBuf>,

    #[arg(
        long,
        conflicts_with = "log_file",
        help = "Disable logging to a file.",
        long_help = "Disable logging to a file. Also settable via VALQERON_ENGINE_LOG_FILE=off."
    )]
    pub no_log_file: bool,

    #[arg(
        long,
        help = "Use strict durability for writes (slower).",
        long_help = "Use strict durability for writes (slower). The default relaxed durability \
                     is faster but the last transactions may be lost if the power/OS crashes."
    )]
    pub durable: bool,

    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = 3600,
        help = "Seconds between database maintenance runs (PRAGMA optimize + WAL checkpoint)."
    )]
    pub maintenance_interval: u64,

    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = 300,
        help = "Seconds between heartbeat log lines."
    )]
    pub heartbeat_interval: u64,
}

#[derive(Args, Debug)]
pub struct InstallArgs {
    #[arg(long, help = "Overwrite an existing service definition.")]
    pub force: bool,
}

impl Cli {
    /// Default stderr level. Unlike the one-shot CLI (which defaults to
    /// `warn`), a daemon's stderr is the service manager's log stream, so the
    /// startup banner and job outcomes must be visible by default.
    pub fn log_level(&self) -> Level {
        match self.verbose {
            0 => Level::INFO,
            1 => Level::DEBUG,
            _ => Level::TRACE,
        }
    }
}

impl RunArgs {
    /// Tri-state `--log-file` flag, mirroring the CLI's semantics:
    /// absent → `None`; bare `--log-file` → `Some(None)` (default location);
    /// `--log-file PATH` → `Some(Some(path))`.
    pub fn log_file_arg(&self) -> Option<Option<PathBuf>> {
        match &self.log_file {
            None => None,
            Some(p) if p.as_os_str().is_empty() => Some(None),
            Some(p) => Some(Some(p.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_run_with_defaults() {
        let cli = Cli::try_parse_from(["valqeron-engine", "run"]).unwrap();
        match cli.command {
            Command::Run(args) => {
                assert_eq!(args.maintenance_interval, 3600);
                assert_eq!(args.heartbeat_interval, 300);
                assert!(!args.durable);
            }
            other => panic!("expected run, got {other:?}"),
        }
    }

    #[test]
    fn verbosity_scales_from_info() {
        let cli = Cli::try_parse_from(["valqeron-engine", "run"]).unwrap();
        assert_eq!(cli.log_level(), Level::INFO);
        let cli = Cli::try_parse_from(["valqeron-engine", "-v", "run"]).unwrap();
        assert_eq!(cli.log_level(), Level::DEBUG);
    }

    #[test]
    fn no_log_file_conflicts_with_log_file() {
        let result = Cli::try_parse_from([
            "valqeron-engine",
            "run",
            "--log-file",
            "/x",
            "--no-log-file",
        ]);
        assert!(result.is_err());
    }
}
