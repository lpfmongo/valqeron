//! Service management: install/uninstall/status for the login service.
//!
//! Platform dispatch: launchd LaunchAgent on macOS, systemd user unit on
//! Linux. Both are **user-bounded** services (the database is user-scoped),
//! started at login — see the platform modules for the restart policies.

mod render;

#[cfg(target_os = "macos")]
mod launchd;
#[cfg(target_os = "linux")]
mod systemd;

#[cfg(target_os = "macos")]
use launchd as platform;
#[cfg(target_os = "linux")]
use systemd as platform;

use std::path::PathBuf;

use crate::cli::{Cli, InstallArgs};
use crate::error::{EngineError, EngineResult};

/// launchd label / reverse-DNS identity of the engine service.
pub const LABEL: &str = "io.valqeron.engine";

/// Both templates are embedded on every platform (same pattern as the SQL
/// migrations) so template tests run everywhere, not only on the target OS.
/// On the non-native platform each getter is only reached from tests.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub fn launchd_template() -> &'static str {
    include_str!("templates/io.valqeron.engine.plist")
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn systemd_template() -> &'static str {
    include_str!("templates/valqeron-engine.service")
}

pub fn install(cli: &Cli, args: &InstallArgs) -> EngineResult<()> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        platform::install(cli, args)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (cli, args);
        Err(EngineError::UnsupportedPlatform)
    }
}

pub fn uninstall() -> EngineResult<()> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        platform::uninstall()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Err(EngineError::UnsupportedPlatform)
    }
}

/// Report engine state and exit non-zero when the engine is not running,
/// so scripts can use `valqeron-engine status` as a liveness probe.
pub fn status(cli: &Cli) -> EngineResult<()> {
    let db_path = crate::config::resolve_db_path(cli)?;
    let lock_path = crate::config::lock_path_for(&db_path);
    let pid = crate::lockfile::read_lock_pid(&lock_path);
    let alive = pid.as_deref().is_some_and(process_alive);

    let service_state = {
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            platform::service_state().unwrap_or_else(|e| format!("unknown ({e})"))
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            "unavailable on this platform".to_string()
        }
    };

    println!("database:   {}", db_path.display());
    println!("lock file:  {}", lock_path.display());
    println!("service:    {service_state}");
    match (&pid, alive) {
        (Some(pid), true) => {
            println!("engine:     running (pid {pid})");
            Ok(())
        }
        (Some(pid), false) => {
            println!("engine:     not running (stale lock file, last pid {pid})");
            Err(EngineError::NotRunning)
        }
        (None, _) => {
            println!("engine:     not running");
            Err(EngineError::NotRunning)
        }
    }
}

/// Best-effort process liveness via `kill -0` (signal 0 = permission/existence
/// probe only). Diagnostic: PIDs can be recycled; the kernel flock held by
/// the engine remains the real authority.
fn process_alive(pid: &str) -> bool {
    if pid.is_empty() || !pid.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    run_tool("kill", &["-0", pid])
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Run an external service-manager tool, capturing its output.
pub(crate) fn run_tool(program: &str, args: &[&str]) -> EngineResult<std::process::Output> {
    std::process::Command::new(program)
        .args(args)
        .output()
        .map_err(|e| EngineError::Service(format!("running {program} {}: {e}", args.join(" "))))
}

/// Directory receiving the service manager's captured stdout/stderr; derived
/// from the engine's default log file so everything lands in one place.
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(crate) fn engine_log_dir() -> EngineResult<PathBuf> {
    let log_file = valqeron_config::default_log_file(&valqeron_config::ENGINE_APP)
        .map_err(|e| EngineError::Config(e.to_string()))?;
    Ok(log_file
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(".")))
}

/// Absolute path of the running binary, recorded into the service
/// definition. Moving or rebuilding the binary elsewhere requires
/// `install --force` to re-render.
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(crate) fn current_exe_path() -> EngineResult<PathBuf> {
    std::env::current_exe()
        .map_err(|e| EngineError::Io(format!("resolving the engine binary path: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn liveness_probe_rejects_garbage_pids() {
        assert!(!process_alive(""));
        assert!(!process_alive("abc"));
        assert!(!process_alive("12x"));
    }

    #[test]
    fn our_own_pid_is_alive() {
        assert!(process_alive(&std::process::id().to_string()));
    }
}
