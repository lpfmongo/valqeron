use std::path::{Path, PathBuf};
use std::time::Duration;

use valqeron_config::ENGINE_APP;
use valqeron_infrastructure::Synchronous;

use crate::cli::{Cli, RunArgs};
use crate::error::{EngineError, EngineResult};

/// The engine serves no read traffic yet, so it keeps the smallest legal
/// reader pool. Grows when the gRPC surface lands.
pub const READER_POOL_SIZE: usize = 1;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    db_path: PathBuf,
    log_file: Option<PathBuf>,
    durable: bool,
    maintenance_interval: Duration,
    heartbeat_interval: Duration,
}

impl EngineConfig {
    pub fn resolve(cli: &Cli, run: &RunArgs) -> EngineResult<Self> {
        let db_path = resolve_db_path(cli)?;
        let log_file =
            valqeron_config::resolve_log_file(&ENGINE_APP, run.log_file_arg(), run.no_log_file)
                .map_err(config_err)?;
        Ok(Self {
            db_path,
            log_file,
            durable: run.durable,
            maintenance_interval: Duration::from_secs(run.maintenance_interval.max(1)),
            heartbeat_interval: Duration::from_secs(run.heartbeat_interval.max(1)),
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn lock_path(&self) -> PathBuf {
        lock_path_for(&self.db_path)
    }

    pub fn log_file(&self) -> Option<&Path> {
        self.log_file.as_deref()
    }

    pub fn synchronous(&self) -> Synchronous {
        if self.durable {
            Synchronous::Full
        } else {
            Synchronous::Normal
        }
    }

    pub fn durability_label(&self) -> &'static str {
        if self.durable { "strict" } else { "relaxed" }
    }

    pub fn maintenance_interval(&self) -> Duration {
        self.maintenance_interval
    }

    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    pub fn ensure_db_parent(&self) -> EngineResult<()> {
        if let Some(parent) = self.db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| EngineError::Io(format!("creating {}: {e}", parent.display())))?;
        }
        Ok(())
    }
}

/// Resolve the database path exactly like the CLI does (shared logic in
/// `valqeron-config`) — both binaries must always agree on the file.
pub fn resolve_db_path(cli: &Cli) -> EngineResult<PathBuf> {
    valqeron_config::resolve_db_path(cli.db_path.clone()).map_err(config_err)
}

/// Single-instance lock file location: `<db>.lock` next to the database.
pub fn lock_path_for(db_path: &Path) -> PathBuf {
    let mut os = db_path.as_os_str().to_os_string();
    os.push(".lock");
    PathBuf::from(os)
}

fn config_err(err: valqeron_config::ConfigError) -> EngineError {
    EngineError::Config(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_file_sits_next_to_the_db() {
        assert_eq!(
            lock_path_for(Path::new("/data/valqeron.db")),
            PathBuf::from("/data/valqeron.db.lock")
        );
    }

    #[test]
    fn zero_intervals_are_clamped_to_one_second() {
        use clap::Parser;
        let cli = Cli::try_parse_from([
            "valqeron-engine",
            "--db-path",
            "/tmp/x.db",
            "run",
            "--maintenance-interval",
            "0",
            "--heartbeat-interval",
            "0",
            "--no-log-file",
        ])
        .unwrap();
        let crate::cli::Command::Run(run) = &cli.command else {
            panic!("expected run");
        };
        let cfg = EngineConfig::resolve(&cli, run).unwrap();
        assert_eq!(cfg.maintenance_interval(), Duration::from_secs(1));
        assert_eq!(cfg.heartbeat_interval(), Duration::from_secs(1));
    }
}
