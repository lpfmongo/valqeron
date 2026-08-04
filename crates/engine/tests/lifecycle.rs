//! End-to-end lifecycle tests for the `valqeron-engine` binary.
//!
//! These observe *process behaviour* (signals, exit codes, lock files, WAL
//! state) rather than internals, so they stay valid however the runtime is
//! implemented. Service-manager behaviour (launchd/systemd) cannot run in CI
//! and is covered by the manual checklist in the backlog items.

// Integration tests are a separate crate: the workspace's test lint
// allowances at the binary's crate root do not reach this file.
#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_valqeron-engine");
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const EXIT_TIMEOUT: Duration = Duration::from_secs(15);

/// Kills the child on drop so failed assertions never leak daemons.
struct Engine {
    child: Child,
    stderr: Receiver<String>,
    seen: Vec<String>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Engine {
    fn pid(&self) -> String {
        self.child.id().to_string()
    }

    /// Blocks until a stderr line containing `needle` arrives.
    fn wait_for_line(&mut self, needle: &str) {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        while Instant::now() < deadline {
            match self.stderr.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => {
                    let hit = line.contains(needle);
                    self.seen.push(line);
                    if hit {
                        return;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        panic!(
            "did not observe {needle:?} within {STARTUP_TIMEOUT:?}; stderr so far:\n{}",
            self.seen.join("\n")
        );
    }

    fn signal(&self, sig: &str) {
        let status = Command::new("kill")
            .args([sig, &self.pid()])
            .status()
            .expect("kill must be runnable");
        assert!(status.success(), "kill {sig} {} failed", self.pid());
    }

    /// Waits for exit, returning the exit code (None when signal-killed).
    fn wait_exit(&mut self) -> Option<i32> {
        let deadline = Instant::now() + EXIT_TIMEOUT;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return status.code();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("engine did not exit within {EXIT_TIMEOUT:?}");
    }

    fn all_stderr(&mut self) -> String {
        while let Ok(line) = self.stderr.try_recv() {
            self.seen.push(line);
        }
        self.seen.join("\n")
    }
}

fn engine_command(db: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env_remove("RUST_LOG")
        .env_remove("VALQERON_DB")
        .env_remove("VALQERON_ENGINE_LOG_FILE")
        .env_remove("VALQERON_ENGINE_LOG_LEVEL")
        .arg("--db-path")
        .arg(db);
    cmd
}

fn spawn_engine(db: &Path, maintenance_secs: &str, heartbeat_secs: &str) -> Engine {
    let mut child = engine_command(db)
        .args([
            "run",
            "--no-log-file",
            "--maintenance-interval",
            maintenance_secs,
            "--heartbeat-interval",
            heartbeat_secs,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("engine binary must spawn");

    let stderr = child.stderr.take().expect("stderr piped");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    Engine {
        child,
        stderr: rx,
        seen: Vec::new(),
    }
}

fn temp_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("it.db");
    (dir, db)
}

fn lock_path(db: &Path) -> PathBuf {
    let mut os = db.as_os_str().to_os_string();
    os.push(".lock");
    PathBuf::from(os)
}

#[test]
fn help_and_version_exit_zero() {
    for flag in ["--help", "--version"] {
        let output = Command::new(BIN).arg(flag).output().expect("run");
        assert!(output.status.success(), "{flag} must succeed");
    }
}

#[test]
fn starts_heartbeats_and_shuts_down_cleanly_on_sigterm() {
    let (_dir, db) = temp_db();
    let mut engine = spawn_engine(&db, "3600", "1");

    // Heartbeat proves the run loop (and thus the signal handlers) is live.
    engine.wait_for_line("engine alive");

    let lock = lock_path(&db);
    assert!(lock.exists(), "lock file must exist while running");
    let recorded_pid = std::fs::read_to_string(&lock).expect("lock readable");
    assert_eq!(recorded_pid.trim(), engine.pid(), "lock records the pid");

    // `status` agrees the engine is running (exit 0).
    let status = engine_command(&db).arg("status").output().expect("status");
    assert_eq!(status.status.code(), Some(0), "status must report running");

    engine.signal("-TERM");
    assert_eq!(engine.wait_exit(), Some(0), "SIGTERM means clean exit 0");

    assert!(!lock.exists(), "lock file removed on clean exit");
    let stderr = engine.all_stderr();
    assert!(
        stderr.contains("valqeron-engine starting"),
        "startup banner missing:\n{stderr}"
    );
    assert!(
        stderr.contains("engine stopped cleanly"),
        "shutdown audit line missing:\n{stderr}"
    );

    // Drop's wal_checkpoint(TRUNCATE) leaves the WAL empty (or removed).
    let wal = std::fs::metadata(db.with_extension("db-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert_eq!(wal, 0, "WAL must be truncated after a clean shutdown");
}

#[test]
fn maintenance_job_runs_on_its_interval() {
    let (_dir, db) = temp_db();
    let mut engine = spawn_engine(&db, "1", "3600");

    engine.wait_for_line("maintenance completed");

    engine.signal("-TERM");
    assert_eq!(engine.wait_exit(), Some(0));
}

#[test]
fn second_instance_fails_fast_naming_holder() {
    let (_dir, db) = temp_db();
    let mut first = spawn_engine(&db, "3600", "1");
    first.wait_for_line("engine alive");

    let second = engine_command(&db)
        .args(["run", "--no-log-file"])
        .output()
        .expect("second instance runs to completion");

    assert_eq!(
        second.status.code(),
        Some(3),
        "already-running must exit with its dedicated code"
    );
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains(&first.pid()),
        "error names the holder pid:\n{stderr}"
    );
    assert!(
        stderr.contains("it.db"),
        "error names the database:\n{stderr}"
    );

    first.signal("-TERM");
    assert_eq!(first.wait_exit(), Some(0));
}

#[test]
fn sigkill_leaves_stale_lock_file_that_never_blocks_the_next_start() {
    let (_dir, db) = temp_db();
    let lock = lock_path(&db);

    let mut first = spawn_engine(&db, "3600", "1");
    first.wait_for_line("engine alive");
    let first_pid = first.pid();

    first.signal("-KILL");
    assert_eq!(first.wait_exit(), None, "SIGKILL exits via signal");
    assert!(
        lock.exists(),
        "SIGKILL leaves the lock file behind (kernel lock is released)"
    );

    let mut second = spawn_engine(&db, "3600", "1");
    second.wait_for_line("engine alive");
    let recorded = std::fs::read_to_string(&lock).expect("lock readable");
    assert_eq!(
        recorded.trim(),
        second.pid(),
        "new pid overwrites stale one"
    );
    assert_ne!(recorded.trim(), first_pid);

    second.signal("-TERM");
    assert_eq!(second.wait_exit(), Some(0));
    assert!(!lock.exists());
}

#[test]
fn status_reports_not_running_with_nonzero_exit() {
    let (_dir, db) = temp_db();
    let output = engine_command(&db).arg("status").output().expect("status");
    assert_eq!(output.status.code(), Some(1), "stopped engine → exit 1");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("not running"),
        "status must say not running:\n{stdout}"
    );
}
