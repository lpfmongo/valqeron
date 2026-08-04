//! launchd user agent management (macOS).
//!
//! The engine installs as a **LaunchAgent** (`~/Library/LaunchAgents`), not a
//! system daemon: the database is user-scoped, so the service is
//! user-bounded and starts at login.
//!
//! Restart policy: `KeepAlive = { SuccessfulExit = false }` restarts crashes
//! but leaves clean exits stopped; `ThrottleInterval` prevents restart
//! storms when startup keeps failing (e.g. the lock is held).

use std::path::PathBuf;

use crate::cli::{Cli, InstallArgs};
use crate::error::{EngineError, EngineResult};
use crate::service::render::{render, xml_escape};
use crate::service::{LABEL, current_exe_path, engine_log_dir, run_tool};

fn plist_path() -> EngineResult<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| EngineError::Config("HOME is not set".to_string()))?;
    Ok(home
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{LABEL}.plist")))
}

fn gui_domain() -> EngineResult<String> {
    let output = run_tool("id", &["-u"])?;
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if uid.is_empty() || !uid.bytes().all(|b| b.is_ascii_digit()) {
        return Err(EngineError::Service(format!(
            "could not determine the current uid (got {uid:?})"
        )));
    }
    Ok(format!("gui/{uid}"))
}

pub fn install(cli: &Cli, args: &InstallArgs) -> EngineResult<()> {
    let exe = current_exe_path()?;
    let log_dir = engine_log_dir()?;
    std::fs::create_dir_all(&log_dir)
        .map_err(|e| EngineError::Io(format!("creating {}: {e}", log_dir.display())))?;

    let stdout_path = log_dir.join("launchd.stdout.log");
    let stderr_path = log_dir.join("launchd.stderr.log");

    // Propagate an explicit database override (flag or VALQERON_DB at
    // install time) into the agent's environment; the default path needs
    // nothing since the engine resolves it the same way the CLI does.
    let env_block = match &cli.db_path {
        Some(path) => format!(
            "  <key>EnvironmentVariables</key>\n  <dict>\n    <key>VALQERON_DB</key>\n    <string>{}</string>\n  </dict>\n",
            xml_escape(&path.display().to_string())
        ),
        None => String::new(),
    };

    let rendered = render(
        crate::service::launchd_template(),
        &[
            ("LABEL", LABEL),
            ("EXE", &xml_escape(&exe.display().to_string())),
            (
                "STDOUT_PATH",
                &xml_escape(&stdout_path.display().to_string()),
            ),
            (
                "STDERR_PATH",
                &xml_escape(&stderr_path.display().to_string()),
            ),
            ("ENV_BLOCK", &env_block),
        ],
    )?;

    let plist = plist_path()?;
    if plist.exists() && !args.force {
        return Err(EngineError::Service(format!(
            "{} already exists; rerun with --force to overwrite",
            plist.display()
        )));
    }
    if let Some(parent) = plist.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| EngineError::Io(format!("creating {}: {e}", parent.display())))?;
    }
    std::fs::write(&plist, rendered)
        .map_err(|e| EngineError::Io(format!("writing {}: {e}", plist.display())))?;

    let domain = gui_domain()?;
    // Best-effort unload of a previous registration, then load the new one.
    let _ = run_tool("launchctl", &["bootout", &format!("{domain}/{LABEL}")]);
    let plist_str = plist.display().to_string();
    let output = run_tool("launchctl", &["bootstrap", &domain, &plist_str])?;
    if !output.status.success() {
        return Err(EngineError::Service(format!(
            "launchctl bootstrap failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    println!("installed launchd agent: {}", plist.display());
    println!("the engine starts now and at every login");
    println!("inspect with:  launchctl print {domain}/{LABEL}");
    println!("logs:          {}", stderr_path.display());
    Ok(())
}

pub fn uninstall() -> EngineResult<()> {
    let domain = gui_domain()?;
    // Best-effort: the agent may not be loaded (e.g. after a manual bootout).
    let _ = run_tool("launchctl", &["bootout", &format!("{domain}/{LABEL}")]);

    let plist = plist_path()?;
    match std::fs::remove_file(&plist) {
        Ok(()) => println!("removed {}", plist.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("nothing to remove: {} does not exist", plist.display());
        }
        Err(e) => {
            return Err(EngineError::Io(format!(
                "removing {}: {e}",
                plist.display()
            )));
        }
    }
    println!("engine service uninstalled");
    Ok(())
}

/// Human-readable service registration state for `status`.
pub fn service_state() -> EngineResult<String> {
    let domain = gui_domain()?;
    let output = run_tool("launchctl", &["print", &format!("{domain}/{LABEL}")])?;
    if output.status.success() {
        Ok(format!("loaded ({domain}/{LABEL})"))
    } else {
        Ok(format!("not loaded ({domain}/{LABEL})"))
    }
}
