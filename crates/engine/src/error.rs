use std::path::PathBuf;

pub type EngineResult<T> = Result<T, EngineError>;

/// Exit codes for the engine binary.
///
/// Distinct codes let service managers and scripts distinguish "another
/// instance already runs" (a benign race that must only be throttled, not
/// treated as a crash loop bug) from real failures.
pub mod exit_code {
    /// Generic runtime failure (also: `status` reporting a stopped engine).
    pub const RUNTIME: i32 = 1;
    /// Configuration could not be resolved.
    pub const CONFIG: i32 = 2;
    /// Another engine instance already holds the database lock.
    pub const ALREADY_RUNNING: i32 = 3;
    /// A service-manager operation (launchctl/systemctl) failed.
    pub const SERVICE: i32 = 4;
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error(
        "another engine instance already owns {} (pid {pid}); \
         stop it first or check `valqeron-engine status`",
        db_path.display()
    )]
    AlreadyRunning { db_path: PathBuf, pid: String },

    #[error("storage error: {0}")]
    Storage(#[from] valqeron_core::StorageError),

    #[error("i/o error: {0}")]
    Io(String),

    #[error("service manager operation failed: {0}")]
    Service(String),

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    #[error("engine services are only supported on macOS (launchd) and Linux (systemd)")]
    UnsupportedPlatform,

    #[error("engine is not running")]
    NotRunning,

    #[error("forced shutdown: second signal received before graceful shutdown finished")]
    ForcedShutdown,
}

impl EngineError {
    pub fn exit_code(&self) -> i32 {
        match self {
            EngineError::Config(_) => exit_code::CONFIG,
            EngineError::AlreadyRunning { .. } => exit_code::ALREADY_RUNNING,
            EngineError::Service(_) => exit_code::SERVICE,
            #[cfg(not(any(target_os = "macos", target_os = "linux")))]
            EngineError::UnsupportedPlatform => exit_code::SERVICE,
            EngineError::Storage(_)
            | EngineError::Io(_)
            | EngineError::NotRunning
            | EngineError::ForcedShutdown => exit_code::RUNTIME,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn already_running_maps_to_its_dedicated_exit_code() {
        let err = EngineError::AlreadyRunning {
            db_path: PathBuf::from("/tmp/x.db"),
            pid: "123".to_string(),
        };
        assert_eq!(err.exit_code(), exit_code::ALREADY_RUNNING);
        let msg = err.to_string();
        assert!(msg.contains("/tmp/x.db"), "message names the db: {msg}");
        assert!(msg.contains("123"), "message names the pid: {msg}");
    }

    #[test]
    fn config_errors_map_to_config_exit_code() {
        assert_eq!(
            EngineError::Config("bad".into()).exit_code(),
            exit_code::CONFIG
        );
    }
}
