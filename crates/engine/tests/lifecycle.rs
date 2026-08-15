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

/// Base invocation: the binary takes no arguments, so all configuration is
/// injected through `VALQERON_*` env vars, with inherited ones scrubbed first.
fn engine_command(db: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env_remove("RUST_LOG")
        .env_remove("VALQERON_ENGINE_LOG_LEVEL")
        .env_remove("VALQERON_ENGINE_DURABLE")
        .env_remove("VALQERON_ENGINE_MAINTENANCE_INTERVAL")
        .env_remove("VALQERON_ENGINE_HEARTBEAT_INTERVAL")
        .env_remove("NOTIFY_SOCKET")
        .env_remove("WATCHDOG_USEC")
        .env_remove("WATCHDOG_PID")
        .env("VALQERON_DB", db)
        // Isolate the gRPC socket per test: parallel tests must never
        // contend on (or clean up) the shared default socket path.
        .env("VALQERON_SOCKET", socket_path(db))
        .env("VALQERON_ENGINE_LOG_FILE", "off");
    cmd
}

/// Test socket next to the test database, mirroring `<db>.lock`.
fn socket_path(db: &Path) -> PathBuf {
    db.with_extension("sock")
}

fn spawn_engine(db: &Path, maintenance_secs: &str, heartbeat_secs: &str) -> Engine {
    spawn_engine_with(db, maintenance_secs, heartbeat_secs, &[])
}

/// Like [`spawn_engine`] with debug-level stderr (`RUST_LOG=debug`), for
/// tests that observe debug-only lines such as the heartbeat.
fn spawn_engine_verbose(db: &Path, maintenance_secs: &str, heartbeat_secs: &str) -> Engine {
    spawn_engine_with(
        db,
        maintenance_secs,
        heartbeat_secs,
        &[("RUST_LOG", "debug")],
    )
}

fn spawn_engine_with(
    db: &Path,
    maintenance_secs: &str,
    heartbeat_secs: &str,
    extra_env: &[(&str, &str)],
) -> Engine {
    let mut child = engine_command(db)
        .env("VALQERON_ENGINE_MAINTENANCE_INTERVAL", maintenance_secs)
        .env("VALQERON_ENGINE_HEARTBEAT_INTERVAL", heartbeat_secs)
        .envs(extra_env.iter().copied())
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

/// The binary is a service-manager payload configured purely through
/// `VALQERON_*` env vars: every argument — including the retired `run`
/// subcommand and clap's old `--help`/`--version` — must be rejected with the
/// CONFIG exit code and a pointer at the environment contract, so a stale
/// service definition fails fast and visibly in the manager's log.
#[test]
fn any_argument_is_rejected() {
    for arg in [
        "run",
        "install",
        "uninstall",
        "status",
        "--help",
        "--version",
    ] {
        let output = Command::new(BIN).arg(arg).output().expect("run");
        assert_eq!(
            output.status.code(),
            Some(2),
            "`{arg}` must be rejected with the CONFIG exit code"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("takes no arguments") && stderr.contains("VALQERON_"),
            "`{arg}` rejection must explain the env-only contract:\n{stderr}"
        );
    }
}

#[test]
fn starts_heartbeats_and_shuts_down_cleanly_on_sigterm() {
    let (_dir, db) = temp_db();
    // The heartbeat logs at debug level; spawn with `-v` so it reaches stderr.
    let mut engine = spawn_engine_verbose(&db, "3600", "1");

    // Heartbeat proves the run loop (and thus the signal handlers) is live.
    engine.wait_for_line("engine alive");

    let lock = lock_path(&db);
    assert!(lock.exists(), "lock file must exist while running");
    let recorded_pid = std::fs::read_to_string(&lock).expect("lock readable");
    assert_eq!(recorded_pid.trim(), engine.pid(), "lock records the pid");

    engine.signal("-TERM");
    assert_eq!(engine.wait_exit(), Some(0), "SIGTERM means clean exit 0");

    assert!(!lock.exists(), "lock file removed on clean exit");
    assert!(
        !socket_path(&db).exists(),
        "socket file removed on clean exit"
    );
    let stderr = engine.all_stderr();
    assert!(
        stderr.contains("valqeron-engine starting"),
        "startup banner missing:\n{stderr}"
    );
    assert!(
        stderr.contains("engine ready"),
        "readiness audit line missing:\n{stderr}"
    );
    assert!(
        stderr.find("engine ready") < stderr.find("engine alive"),
        "readiness must precede the first heartbeat:\n{stderr}"
    );
    assert!(
        stderr.contains("engine stopped cleanly"),
        "shutdown audit line missing:\n{stderr}"
    );

    // The lifecycle FSM walks its clean path, in order.
    let transitions = [
        r#"from="starting" to="ready""#,
        r#"from="ready" to="stopping""#,
        r#"from="stopping" to="stopped""#,
    ];
    let mut search_from = 0;
    for transition in transitions {
        let found = stderr
            .get(search_from..)
            .and_then(|tail| tail.find(transition))
            .unwrap_or_else(|| {
                panic!("lifecycle transition {transition:?} missing or out of order:\n{stderr}")
            });
        search_from += found + transition.len();
    }

    // Drop's wal_checkpoint(TRUNCATE) leaves the WAL empty (or removed).
    let wal = std::fs::metadata(db.with_extension("db-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert_eq!(wal, 0, "WAL must be truncated after a clean shutdown");
}

/// Receive datagrams until `want` arrives, skipping others (watchdog pings
/// interleave freely with state announcements).
fn recv_until(receiver: &std::os::unix::net::UnixDatagram, want: &str) {
    let mut seen = Vec::new();
    loop {
        let mut buf = [0u8; 64];
        let received = receiver
            .recv(&mut buf)
            .unwrap_or_else(|e| panic!("waiting for {want:?}, got {seen:?}, then: {e}"));
        let datagram = String::from_utf8_lossy(buf.get(..received).unwrap_or_default()).to_string();
        if datagram == want {
            return;
        }
        seen.push(datagram);
    }
}

/// End-to-end sd_notify protocol against a fake NOTIFY_SOCKET: readiness,
/// watchdog pings at half the advertised interval, and the stopping
/// announcement — no systemd required.
#[test]
fn sd_notify_reports_ready_watchdog_pings_and_stopping() {
    let (_dir, db) = temp_db();
    let notify_dir = tempfile::tempdir().expect("tempdir");
    let notify_path = notify_dir.path().join("notify.sock");
    let receiver =
        std::os::unix::net::UnixDatagram::bind(&notify_path).expect("bind notify socket");
    receiver
        .set_read_timeout(Some(STARTUP_TIMEOUT))
        .expect("read timeout");

    let notify_socket = notify_path.to_str().expect("utf-8 path").to_string();
    let mut engine = spawn_engine_with(
        &db,
        "3600",
        "3600",
        &[
            ("NOTIFY_SOCKET", notify_socket.as_str()),
            // 1s watchdog → WATCHDOG=1 every 500ms.
            ("WATCHDOG_USEC", "1000000"),
        ],
    );

    recv_until(&receiver, "READY=1");
    recv_until(&receiver, "WATCHDOG=1");

    engine.signal("-TERM");
    recv_until(&receiver, "STOPPING=1");
    assert_eq!(engine.wait_exit(), Some(0), "clean exit after the protocol");
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
    let mut first = spawn_engine(&db, "3600", "3600");
    first.wait_for_line("engine ready");

    let second = engine_command(&db)
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
    assert!(
        stderr.contains(r#"from="starting" to="failed""#),
        "a boot failure must transition Starting -> Failed:\n{stderr}"
    );

    first.signal("-TERM");
    assert_eq!(first.wait_exit(), Some(0));
}

#[test]
fn sigkill_leaves_stale_lock_file_that_never_blocks_the_next_start() {
    let (_dir, db) = temp_db();
    let lock = lock_path(&db);

    let mut first = spawn_engine(&db, "3600", "3600");
    first.wait_for_line("engine ready");
    let first_pid = first.pid();

    first.signal("-KILL");
    assert_eq!(first.wait_exit(), None, "SIGKILL exits via signal");
    assert!(
        lock.exists(),
        "SIGKILL leaves the lock file behind (kernel lock is released)"
    );

    let mut second = spawn_engine(&db, "3600", "3600");
    second.wait_for_line("engine ready");
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
