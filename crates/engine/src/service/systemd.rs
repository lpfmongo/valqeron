//! systemd user unit management (Linux).
//!
//! The engine installs as a **user unit** (`~/.config/systemd/user`), not a
//! system service: the database is user-scoped, so the service is
//! user-bounded and starts at login.
//!
//! Note: user units stop at logout and only start at login. For boot-time
//! start without a session, enable lingering: `loginctl enable-linger $USER`.
//!
//! Restart policy: `Restart=on-failure` restarts crashes but leaves clean
//! exits stopped; the start-limit settings prevent restart storms when
//! startup keeps failing (e.g. the lock is held).

use std::path::PathBuf;

use crate::cli::{Cli, InstallArgs};
use crate::error::{EngineError, EngineResult};
use crate::service::render::render;
use crate::service::{current_exe_path, engine_log_dir, run_tool};

const UNIT_NAME: &str = "valqeron-engine.service";

fn unit_path() -> EngineResult<PathBuf> {
    let config_home = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .ok_or_else(|| EngineError::Config("neither XDG_CONFIG_HOME nor HOME is set".into()))?;
    Ok(config_home.join("systemd").join("user").join(UNIT_NAME))
}

fn systemctl_ok(args: &[&str]) -> EngineResult<()> {
    let output = run_tool("systemctl", args)?;
    if !output.status.success() {
        return Err(EngineError::Service(format!(
            "systemctl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

pub fn install(cli: &Cli, args: &InstallArgs) -> EngineResult<()> {
    let exe = current_exe_path()?;
    let log_dir = engine_log_dir()?;
    std::fs::create_dir_all(&log_dir)
        .map_err(|e| EngineError::Io(format!("creating {}: {e}", log_dir.display())))?;

    // The sandbox (ProtectSystem=strict / ProtectHome=read-only) must be
    // punched through for exactly what the engine writes: the database
    // directory (db + WAL + lock file) and its log directory.
    let db_path = valqeron_config::resolve_db_path(cli.db_path.clone())
        .map_err(|e| EngineError::Config(e.to_string()))?;
    let db_dir = db_path
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let rw_paths = format!("\"{}\" \"{}\"", db_dir.display(), log_dir.display());

    // Propagate an explicit database override into the unit's environment.
    let env_block = match &cli.db_path {
        Some(path) => format!("Environment=\"VALQERON_DB={}\"\n", path.display()),
        None => String::new(),
    };

    let rendered = render(
        crate::service::systemd_template(),
        &[
            ("EXE", &exe.display().to_string()),
            ("RW_PATHS", &rw_paths),
            ("ENV_BLOCK", &env_block),
        ],
    )?;

    let unit = unit_path()?;
    if unit.exists() && !args.force {
        return Err(EngineError::Service(format!(
            "{} already exists; rerun with --force to overwrite",
            unit.display()
        )));
    }
    if let Some(parent) = unit.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| EngineError::Io(format!("creating {}: {e}", parent.display())))?;
    }
    std::fs::write(&unit, rendered)
        .map_err(|e| EngineError::Io(format!("writing {}: {e}", unit.display())))?;

    systemctl_ok(&["--user", "daemon-reload"])?;
    systemctl_ok(&["--user", "enable", "--now", UNIT_NAME])?;

    println!("installed systemd user unit: {}", unit.display());
    println!("the engine starts now and at every login");
    println!("for boot start without a session: loginctl enable-linger $USER");
    println!("inspect with:  systemctl --user status {UNIT_NAME}");
    println!("logs:          journalctl --user -u {UNIT_NAME}");
    Ok(())
}

pub fn uninstall() -> EngineResult<()> {
    // Best-effort: the unit may not be enabled or even installed.
    let _ = run_tool("systemctl", &["--user", "disable", "--now", UNIT_NAME]);

    let unit = unit_path()?;
    match std::fs::remove_file(&unit) {
        Ok(()) => println!("removed {}", unit.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("nothing to remove: {} does not exist", unit.display());
        }
        Err(e) => {
            return Err(EngineError::Io(format!("removing {}: {e}", unit.display())));
        }
    }
    let _ = run_tool("systemctl", &["--user", "daemon-reload"]);
    println!("engine service uninstalled");
    Ok(())
}

/// Human-readable service registration state for `status`.
pub fn service_state() -> EngineResult<String> {
    let output = run_tool("systemctl", &["--user", "is-active", UNIT_NAME])?;
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if state.is_empty() {
        Ok("unknown".to_string())
    } else {
        Ok(format!("{state} ({UNIT_NAME})"))
    }
}
