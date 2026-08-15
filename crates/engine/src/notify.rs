//! Hand-rolled `sd_notify` signaling (no dependency).
//!
//! When systemd starts the engine as a `Type=notify` unit it exports
//! `NOTIFY_SOCKET`; the engine reports `READY=1` once it is serving,
//! `STOPPING=1` when shutdown begins, and — when the unit configures
//! `WatchdogSec=` (exported as `WATCHDOG_USEC`/`WATCHDOG_PID`) — periodic
//! `WATCHDOG=1` pings so systemd can detect a *hung* engine, not just a dead
//! one. Everywhere else (foreground runs, launchd, tests) the variables are
//! absent and every call is a silent no-op.
//!
//! Sends are **non-blocking** fire-and-forget: they run on runtime worker
//! threads (lifecycle transitions, the watchdog ticker) which must never
//! block, and notification is advisory — the engine itself is the source of
//! truth. Failures are logged at debug and never affect the engine.

use std::ffi::OsStr;
use std::os::unix::net::UnixDatagram;
use std::time::Duration;

/// Report "started and serving" (`READY=1`).
pub(crate) fn notify_ready() {
    notify("READY=1");
}

/// Report "shutdown began" (`STOPPING=1`).
pub(crate) fn notify_stopping() {
    notify("STOPPING=1");
}

/// Ping the service-manager watchdog (`WATCHDOG=1`).
pub(crate) fn notify_watchdog() {
    notify("WATCHDOG=1");
}

/// The interval within which systemd expects watchdog pings, when a watchdog
/// is armed **for this process**: requires `NOTIFY_SOCKET` + a positive
/// `WATCHDOG_USEC`, and `WATCHDOG_PID` (when set) must name us. Callers
/// should ping at half this interval.
pub(crate) fn watchdog_interval() -> Option<Duration> {
    std::env::var_os("NOTIFY_SOCKET")?;
    watchdog_interval_from(
        std::env::var_os("WATCHDOG_USEC").as_deref(),
        std::env::var_os("WATCHDOG_PID").as_deref(),
        std::process::id(),
    )
}

/// Pure core of [`watchdog_interval`]: env values are injected so tests
/// never touch process-global environment state.
fn watchdog_interval_from(
    usec: Option<&OsStr>,
    pid: Option<&OsStr>,
    my_pid: u32,
) -> Option<Duration> {
    let usec = usec?;
    if let Some(pid) = pid {
        // systemd names the process expected to ping; a mismatch means the
        // variable was inherited (e.g. by a child) and must be ignored.
        if pid.to_str().and_then(|s| s.trim().parse::<u32>().ok()) != Some(my_pid) {
            return None;
        }
    }
    match usec.to_str().and_then(|s| s.trim().parse::<u64>().ok()) {
        Some(micros) if micros > 0 => Some(Duration::from_micros(micros)),
        _ => {
            tracing::warn!(?usec, "invalid WATCHDOG_USEC; watchdog disabled");
            None
        }
    }
}

fn notify(state: &str) {
    let Some(socket) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    match send_state(socket.as_os_str(), state) {
        Ok(()) => tracing::debug!(state, "sd_notify state sent"),
        Err(e) => tracing::debug!(state, error = %e, "sd_notify send failed"),
    }
}

/// Send one datagram to the notification socket, without ever blocking the
/// calling thread (a full receiver queue drops the advisory message instead).
/// A leading `@` means a Linux abstract-namespace address (systemd uses
/// these in containers).
fn send_state(socket: &OsStr, state: &str) -> std::io::Result<()> {
    let sender = UnixDatagram::unbound()?;
    sender.set_nonblocking(true)?;
    if socket.as_encoded_bytes().first() == Some(&b'@') {
        return send_abstract(&sender, socket, state);
    }
    sender.send_to(state.as_bytes(), std::path::Path::new(socket))?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn send_abstract(sender: &UnixDatagram, socket: &OsStr, state: &str) -> std::io::Result<()> {
    use std::os::linux::net::SocketAddrExt;
    let name = socket.as_encoded_bytes().get(1..).unwrap_or_default();
    let addr = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
    sender.send_to_addr(state.as_bytes(), &addr)?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn send_abstract(_sender: &UnixDatagram, _socket: &OsStr, _state: &str) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "abstract NOTIFY_SOCKET addresses are only supported on Linux",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_state_delivers_every_protocol_datagram_to_a_path_socket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notify.sock");
        let receiver = UnixDatagram::bind(&path).unwrap();

        for state in ["READY=1", "WATCHDOG=1", "STOPPING=1"] {
            send_state(path.as_os_str(), state).unwrap();

            let mut buf = [0u8; 64];
            let received = receiver.recv(&mut buf).unwrap();
            assert_eq!(buf.get(..received), Some(state.as_bytes()));
        }
    }

    #[test]
    fn send_state_to_a_missing_socket_reports_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gone.sock");
        assert!(send_state(path.as_os_str(), "READY=1").is_err());
    }

    #[test]
    fn watchdog_requires_a_positive_interval() {
        assert_eq!(watchdog_interval_from(None, None, 42), None);
        assert_eq!(
            watchdog_interval_from(Some(OsStr::new("1000000")), None, 42),
            Some(Duration::from_secs(1))
        );
        for bad in ["0", "-1", "soon", ""] {
            assert_eq!(
                watchdog_interval_from(Some(OsStr::new(bad)), None, 42),
                None,
                "{bad:?} must disable the watchdog"
            );
        }
    }

    #[test]
    fn watchdog_pid_must_name_this_process_when_set() {
        let usec = OsStr::new("500000");
        assert_eq!(
            watchdog_interval_from(Some(usec), Some(OsStr::new("42")), 42),
            Some(Duration::from_millis(500)),
            "matching pid arms the watchdog"
        );
        assert_eq!(
            watchdog_interval_from(Some(usec), Some(OsStr::new("41")), 42),
            None,
            "inherited (foreign-pid) watchdog env must be ignored"
        );
        assert_eq!(
            watchdog_interval_from(Some(usec), Some(OsStr::new("not-a-pid")), 42),
            None
        );
    }
}
