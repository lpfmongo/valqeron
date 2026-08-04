use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{EngineError, EngineResult};

/// Advisory single-instance lock next to the database file.
///
/// The authority is a kernel advisory lock (`std::fs::File::try_lock`,
/// flock-style), so it is released automatically when the process dies — a
/// stale lock *file* left behind by `SIGKILL` never prevents the next start.
/// The holder's PID is stored in the file purely as a diagnostic for
/// `status` and error messages.
///
/// Scope (phase 1): this guards **engine vs engine** only. The CLI keeps
/// opening the database directly; cross-process safety comes from SQLite's
/// WAL mode and busy timeouts. Exclusive database ownership arrives with the
/// gRPC surface.
#[derive(Debug)]
pub struct EngineLock {
    file: File,
    path: PathBuf,
}

impl EngineLock {
    pub fn acquire(db_path: &Path, lock_path: PathBuf) -> EngineResult<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                EngineError::Io(format!("opening lock file {}: {e}", lock_path.display()))
            })?;

        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let pid = read_lock_pid(&lock_path).unwrap_or_else(|| "unknown".to_string());
                return Err(EngineError::AlreadyRunning {
                    db_path: db_path.to_path_buf(),
                    pid,
                });
            }
            Err(TryLockError::Error(e)) => {
                return Err(EngineError::Io(format!(
                    "locking {}: {e}",
                    lock_path.display()
                )));
            }
        }

        // We own the lock: record our PID for diagnostics.
        write_pid(&mut file)
            .map_err(|e| EngineError::Io(format!("writing pid to {}: {e}", lock_path.display())))?;

        Ok(Self {
            file,
            path: lock_path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for EngineLock {
    fn drop(&mut self) {
        // Remove the file first, then let the kernel lock release when the
        // handle closes: a starter racing the removal still cannot acquire
        // the flock until this handle is gone, and fresh starts simply
        // create a new inode.
        let _ = std::fs::remove_file(&self.path);
        let _ = self.file.unlock();
    }
}

/// PID recorded in a lock file, if any. Diagnostic only — the kernel lock is
/// the authority, never this value.
pub fn read_lock_pid(lock_path: &Path) -> Option<String> {
    let contents = std::fs::read_to_string(lock_path).ok()?;
    let pid = contents.trim();
    if pid.is_empty() {
        None
    } else {
        Some(pid.to_string())
    }
}

fn write_pid(file: &mut File) -> std::io::Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(std::process::id().to_string().as_bytes())?;
    file.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_paths() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let lock = dir.path().join("test.db.lock");
        (dir, db, lock)
    }

    #[test]
    fn acquiring_writes_our_pid() {
        let (_dir, db, lock_path) = temp_paths();
        let lock = EngineLock::acquire(&db, lock_path.clone()).unwrap();
        let pid = read_lock_pid(&lock_path).expect("pid recorded");
        assert_eq!(pid, std::process::id().to_string());
        drop(lock);
    }

    #[test]
    fn second_acquire_fails_with_already_running() {
        let (_dir, db, lock_path) = temp_paths();
        let _held = EngineLock::acquire(&db, lock_path.clone()).unwrap();

        let err = EngineLock::acquire(&db, lock_path).unwrap_err();
        match err {
            EngineError::AlreadyRunning { db_path, pid } => {
                assert_eq!(db_path, db);
                assert_eq!(pid, std::process::id().to_string());
            }
            other => panic!("expected AlreadyRunning, got {other:?}"),
        }
    }

    #[test]
    fn dropping_removes_the_file_and_releases_the_lock() {
        let (_dir, db, lock_path) = temp_paths();
        let lock = EngineLock::acquire(&db, lock_path.clone()).unwrap();
        drop(lock);
        assert!(!lock_path.exists(), "lock file removed on clean release");

        // Reacquire works immediately.
        let again = EngineLock::acquire(&db, lock_path.clone());
        assert!(again.is_ok(), "lock must be reacquirable after release");
    }

    #[test]
    fn stale_lock_file_with_dead_content_does_not_block_acquisition() {
        let (_dir, db, lock_path) = temp_paths();
        // Simulate SIGKILL residue: a file exists but nobody holds the flock.
        std::fs::write(&lock_path, "99999999").unwrap();

        let lock = EngineLock::acquire(&db, lock_path.clone()).unwrap();
        let pid = read_lock_pid(&lock_path).expect("pid overwritten");
        assert_eq!(pid, std::process::id().to_string());
        drop(lock);
    }
}
