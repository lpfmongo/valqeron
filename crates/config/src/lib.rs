//! Shared configuration and path resolution for Valqeron binaries.
//!
//! The CLI (`valqeron`) and the engine daemon (`valqeron-engine`) must agree on
//! which database file they mean — otherwise the engine's single-instance
//! guarantees are meaningless. This crate is the single source of truth for
//! that resolution so the two binaries cannot drift.
//!
//! Precedence everywhere: explicit flag > environment variable > platform
//! default (via `directories::ProjectDirs`).

#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

use std::ffi::OsStr;
use std::path::PathBuf;

use directories::ProjectDirs;

/// `ProjectDirs` qualifier shared by every Valqeron application.
pub const QUALIFIER: &str = "io";
/// `ProjectDirs` organization shared by every Valqeron application.
pub const ORGANIZATION: &str = "valqeron";
/// Application name for state shared by all binaries (the database).
pub const SHARED_APP: &str = "valqeron";

/// Default database file name inside the shared data directory.
pub const DB_FILE_NAME: &str = "valqeron.db";
/// Environment variable overriding the database path for every binary.
pub const DB_PATH_ENV: &str = "VALQERON_DB";

/// Default level for the JSON file log layer when the env override is absent.
pub const DEFAULT_FILE_LOG_LEVEL: &str = "info";

/// Per-binary identity: where its private files (logs) live and which
/// environment variables configure them.
///
/// The database is deliberately *not* part of this — it is shared state
/// resolved identically for every binary via [`resolve_db_path`].
#[derive(Debug, Clone, Copy)]
pub struct AppIdentity {
    /// `ProjectDirs` application name for per-binary files.
    pub app_name: &'static str,
    /// Environment variable naming the log file (or `off` to disable).
    pub log_file_env: &'static str,
    /// Environment variable overriding the file log level.
    pub log_level_env: &'static str,
    /// Default log file name.
    pub log_file_name: &'static str,
}

/// Identity of the one-shot CLI binary (`valqeron` / `vq`).
pub const CLI_APP: AppIdentity = AppIdentity {
    app_name: "valqeron-cli",
    log_file_env: "VALQERON_LOG_FILE",
    log_level_env: "VALQERON_LOG_LEVEL",
    log_file_name: "valqeron-cli.log",
};

/// Identity of the engine daemon binary (`valqeron-engine`).
pub const ENGINE_APP: AppIdentity = AppIdentity {
    app_name: "valqeron-engine",
    log_file_env: "VALQERON_ENGINE_LOG_FILE",
    log_level_env: "VALQERON_ENGINE_LOG_LEVEL",
    log_file_name: "valqeron-engine.log",
};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("could not determine a home directory for {purpose}")]
    NoHomeDirectory { purpose: &'static str },
}

/// Project directories for state shared by all Valqeron binaries (the DB).
pub fn shared_dirs() -> Result<ProjectDirs, ConfigError> {
    ProjectDirs::from(QUALIFIER, ORGANIZATION, SHARED_APP).ok_or(ConfigError::NoHomeDirectory {
        purpose: "the database",
    })
}

/// Project directories private to one binary (logs, etc.).
pub fn app_dirs(app: &AppIdentity) -> Result<ProjectDirs, ConfigError> {
    ProjectDirs::from(QUALIFIER, ORGANIZATION, app.app_name).ok_or(ConfigError::NoHomeDirectory {
        purpose: "application files",
    })
}

/// Resolve the SQLite database path: flag > `VALQERON_DB` > shared data dir.
///
/// Every binary must go through this function so they always agree on the
/// database they operate on.
pub fn resolve_db_path(flag: Option<PathBuf>) -> Result<PathBuf, ConfigError> {
    if let Some(path) = flag {
        return Ok(path);
    }
    if let Some(env) = std::env::var_os(DB_PATH_ENV) {
        return Ok(PathBuf::from(env));
    }
    Ok(shared_dirs()?.data_dir().join(DB_FILE_NAME))
}

/// Resolve the log file location for a binary.
///
/// Semantics (mirroring the CLI's historical behaviour):
/// - `no_log_file` (`--no-log-file`) always wins and disables file logging.
/// - `flag = Some(Some(path))` (`--log-file PATH`) pins an explicit path.
/// - `flag = Some(None)` (`--log-file` with no value) selects the default
///   location.
/// - `flag = None` consults the binary's env var; the values `off`, `false`,
///   `0` and `none` (case-insensitive) disable file logging; any other value
///   is used as the path. Absent env means file logging is on by default.
pub fn resolve_log_file(
    app: &AppIdentity,
    flag: Option<Option<PathBuf>>,
    no_log_file: bool,
) -> Result<Option<PathBuf>, ConfigError> {
    if no_log_file {
        return Ok(None);
    }

    match flag {
        Some(Some(path)) => Ok(Some(path)),
        Some(None) => Ok(Some(default_log_file(app)?)),
        None => match std::env::var_os(app.log_file_env) {
            Some(env) if is_off(&env) => Ok(None),
            Some(env) => Ok(Some(PathBuf::from(env))),
            None => Ok(Some(default_log_file(app)?)),
        },
    }
}

/// Default log file for a binary: the platform state dir when it exists,
/// otherwise `<data dir>/logs`.
pub fn default_log_file(app: &AppIdentity) -> Result<PathBuf, ConfigError> {
    let dirs = app_dirs(app)?;
    let dir = dirs
        .state_dir()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| dirs.data_dir().join("logs"));
    Ok(dir.join(app.log_file_name))
}

/// File log level for a binary: its env var, or [`DEFAULT_FILE_LOG_LEVEL`].
pub fn file_log_level(app: &AppIdentity) -> String {
    std::env::var(app.log_level_env).unwrap_or_else(|_| DEFAULT_FILE_LOG_LEVEL.to_string())
}

fn is_off(value: &OsStr) -> bool {
    value
        .to_str()
        .map(|s| {
            matches!(
                s.trim().to_ascii_lowercase().as_str(),
                "off" | "false" | "0" | "none"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn explicit_db_flag_wins_over_everything() {
        let path = resolve_db_path(Some(PathBuf::from("/tmp/explicit.db"))).unwrap();
        assert_eq!(path, Path::new("/tmp/explicit.db"));
    }

    #[test]
    fn default_db_path_lives_in_shared_data_dir() {
        // No flag; the env var may leak from the calling shell, so only assert
        // the default shape when it is absent.
        if std::env::var_os(DB_PATH_ENV).is_none() {
            let path = resolve_db_path(None).unwrap();
            assert!(path.ends_with(DB_FILE_NAME), "unexpected default: {path:?}");
        }
    }

    #[test]
    fn cli_and_engine_agree_on_the_db_path() {
        // Parity by construction: both binaries call the same function. This
        // asserts the resolution is deterministic for identical inputs.
        let a = resolve_db_path(Some(PathBuf::from("/tmp/shared.db"))).unwrap();
        let b = resolve_db_path(Some(PathBuf::from("/tmp/shared.db"))).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn explicit_log_file_flag_wins() {
        let log =
            resolve_log_file(&CLI_APP, Some(Some(PathBuf::from("/tmp/out.log"))), false).unwrap();
        assert_eq!(log, Some(PathBuf::from("/tmp/out.log")));
    }

    #[test]
    fn log_file_flag_without_path_uses_default_location() {
        let log = resolve_log_file(&CLI_APP, Some(None), false)
            .unwrap()
            .expect("default log path");
        assert!(log.ends_with(CLI_APP.log_file_name));
    }

    #[test]
    fn no_log_file_beats_explicit_path() {
        let log =
            resolve_log_file(&CLI_APP, Some(Some(PathBuf::from("/tmp/out.log"))), true).unwrap();
        assert!(log.is_none());
    }

    #[test]
    fn engine_defaults_use_engine_identity() {
        let log = default_log_file(&ENGINE_APP).unwrap();
        assert!(log.ends_with(ENGINE_APP.log_file_name));
        let cli = default_log_file(&CLI_APP).unwrap();
        assert_ne!(log, cli, "engine and CLI must not share a log file");
    }

    #[test]
    fn is_off_recognizes_disable_values() {
        for v in ["off", "OFF", "Off", "false", "0", "none", "  off  "] {
            assert!(is_off(OsStr::new(v)), "{v:?} should disable");
        }
        for v in ["on", "1", "true", "/tmp/logs/x.log"] {
            assert!(!is_off(OsStr::new(v)), "{v:?} should not disable");
        }
    }
}
