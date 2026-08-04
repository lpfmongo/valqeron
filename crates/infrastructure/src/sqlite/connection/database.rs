use std::path::Path;
use valqeron_core::{StorageError, StorageFault};

use crate::sqlite::connection::config::DatabaseConfig;
use crate::sqlite::connection::dry_run::{is_dry_run_active, with_dry_run_conn};
use crate::sqlite::connection::handle::DbHandle;
use crate::sqlite::connection::pool::{ReaderPool, ReaderSource, lock_writer};
use crate::sqlite::connection::pragmas::{self, ConnectionRole, DbPath, SharedConnection};
use crate::sqlite::connection::sync::{Arc, Mutex};
use crate::sqlite::error::SqliteError;
use crate::sqlite::migrations;

/// Result of a `PRAGMA wal_checkpoint` invocation.
#[derive(Debug, Clone, Copy)]
pub struct WalCheckpointStats {
    /// `1` when the checkpoint could not run to completion because another
    /// connection kept the WAL busy, `0` otherwise.
    pub busy: i64,
    /// Total number of frames in the WAL file.
    pub log_frames: i64,
    /// Number of frames successfully transferred into the database file.
    pub checkpointed_frames: i64,
}

pub(crate) struct Database {
    writer: SharedConnection,
    readers: ReaderSource,
    path: DbPath,
}

impl Database {
    #[cfg(test)]
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        Self::open_with_config(path, DatabaseConfig::default())
    }

    pub(crate) fn open_with_config(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
    ) -> Result<Self, SqliteError> {
        let path = DbPath::File(path.as_ref().to_path_buf());
        Self::open_inner(path, config)
    }

    pub(crate) fn open_in_memory() -> Result<Self, SqliteError> {
        Self::open_in_memory_with_config(DatabaseConfig::default())
    }

    pub(crate) fn open_in_memory_with_config(config: DatabaseConfig) -> Result<Self, SqliteError> {
        Self::open_inner(DbPath::Memory, config)
    }

    fn open_inner(path: DbPath, config: DatabaseConfig) -> Result<Self, SqliteError> {
        if config.reader_pool_size < 1 {
            return Err(SqliteError::InvalidPoolSize);
        }

        let is_memory = path.is_memory();

        let mut writer = pragmas::open_connection(&path, ConnectionRole::Writer)?;
        pragmas::configure(
            &writer,
            ConnectionRole::Writer,
            is_memory,
            config.synchronous,
            config.busy_timeout,
        )?;
        migrations::run(&mut writer)?;

        let readers = if is_memory {
            ReaderSource::SharedWithWriter
        } else {
            let mut pool = Vec::with_capacity(config.reader_pool_size);
            for _ in 0..config.reader_pool_size {
                let conn = pragmas::open_connection(&path, ConnectionRole::Reader)?;
                pragmas::configure(
                    &conn,
                    ConnectionRole::Reader,
                    is_memory,
                    config.synchronous,
                    config.busy_timeout,
                )?;
                pool.push(conn);
            }
            ReaderSource::Pool(Arc::new(ReaderPool::new(pool)))
        };

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers,
            path,
        })
    }

    pub(crate) fn handle(&self) -> DbHandle {
        DbHandle::Live {
            writer: Arc::clone(&self.writer),
            readers: self.readers.clone(),
        }
    }

    /// Periodic maintenance for long-lived processes: `PRAGMA optimize` plus
    /// a **passive** WAL checkpoint for file-backed databases.
    ///
    /// `PASSIVE` is deliberate — it never blocks readers or writers in other
    /// processes (the CLI may be using the database concurrently). The
    /// aggressive `TRUNCATE` checkpoint remains reserved for [`Drop`], when
    /// all in-process work has stopped.
    ///
    /// Returns `None` for in-memory databases (no WAL to checkpoint).
    pub(crate) fn run_maintenance(&self) -> Result<Option<WalCheckpointStats>, SqliteError> {
        fn maintenance_err(source: rusqlite::Error) -> SqliteError {
            SqliteError::Maintenance { source }
        }

        let conn = lock_writer(&self.writer);

        conn.execute_batch("PRAGMA optimize;")
            .map_err(maintenance_err)?;

        if !matches!(self.path, DbPath::File(_)) {
            return Ok(None);
        }

        let stats = conn
            .query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |row| {
                Ok(WalCheckpointStats {
                    busy: row.get(0)?,
                    log_frames: row.get(1)?,
                    checkpointed_frames: row.get(2)?,
                })
            })
            .map_err(maintenance_err)?;

        Ok(Some(stats))
    }

    pub(crate) fn dry_run<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&DbHandle) -> T,
    {
        debug_assert!(
            !is_dry_run_active(),
            "nested dry_run would deadlock on the writer mutex"
        );

        let guard = lock_writer(&self.writer);

        guard
            .execute_batch("SAVEPOINT valqeron_dry_run")
            .map_err(|e| StorageError::Fault(StorageFault::new(e)))?;

        let handle = DbHandle::DryRun;
        let result = with_dry_run_conn(&guard, || f(&handle));

        if let Err(e) =
            guard.execute_batch("ROLLBACK TO valqeron_dry_run; RELEASE valqeron_dry_run")
        {
            tracing::error!(
                error = %e,
                "dry-run savepoint rollback/release failed; attempting a plain RELEASE"
            );
            if let Err(e) = guard.execute_batch("RELEASE valqeron_dry_run") {
                tracing::error!(error = %e, "dry-run savepoint RELEASE also failed");
            }
        }

        Ok(result)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let conn = lock_writer(&self.writer);
        if let Err(e) = conn.execute_batch("PRAGMA optimize;") {
            tracing::warn!(error = %e, "PRAGMA optimize failed on close");
        }

        if matches!(self.path, DbPath::File(_))
            && let Err(e) = conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
        {
            tracing::warn!(error = %e, "WAL checkpoint(TRUNCATE) failed on close");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::connection::handle::Db;
    use crate::sqlite::connection::pragmas::{ConnectionRole, Synchronous, configure};
    use crate::sqlite::migrations::{self, MIGRATIONS};
    use rusqlite::Connection;
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    fn insert_dummy(conn: &Connection) {
        insert_dummy_result(conn).unwrap();
    }

    fn insert_dummy_result(conn: &Connection) -> rusqlite::Result<()> {
        conn.execute_batch(
            "INSERT INTO issuer (id, status, created_at)
             VALUES (randomblob(16), 'ACTIVE', '2026-01-01T00:00:00Z')",
        )
    }

    fn count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM issuer", [], |row| row.get(0))
            .unwrap()
    }

    fn temp_file_db(config: DatabaseConfig) -> (tempfile::TempDir, Database) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stress.db");
        let db = Database::open_with_config(path, config).unwrap();
        (dir, db)
    }

    #[test]
    fn fresh_database_ends_up_at_latest_version() {
        let db = Database::open_in_memory().unwrap();
        let handle = db.handle();
        let conn = handle.read();
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, MIGRATIONS.len() as i64);
    }

    #[test]
    fn opening_twice_is_a_harmless_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("twice.db");
        let _db1 = Database::open(&path).unwrap();
        drop(_db1);
        // Reopening runs migrations again over an already-migrated file.
        let _db2 = Database::open(&path).unwrap();
    }

    #[test]
    fn schema_from_the_future_is_rejected_rather_than_silently_skipped() {
        let mut conn = Connection::open_in_memory().unwrap();
        configure(
            &conn,
            ConnectionRole::Writer,
            true,
            Synchronous::Normal,
            Duration::from_secs(5),
        )
        .unwrap();

        conn.pragma_update(None, "user_version", (MIGRATIONS.len() as i64) + 5)
            .unwrap();

        let result = migrations::run(&mut conn);
        assert!(matches!(
            result,
            Err(SqliteError::UnknownSchemaVersion { .. })
        ));
    }

    #[test]
    fn invalid_pool_size_is_rejected() {
        let result = Database::open_in_memory_with_config(DatabaseConfig {
            reader_pool_size: 0,
            ..Default::default()
        });
        assert!(matches!(result, Err(SqliteError::InvalidPoolSize)));
    }

    #[test]
    fn dry_run_rolls_back_writes() {
        let db = Database::open_in_memory().unwrap();

        db.dry_run(|h| {
            insert_dummy(&h.write());
            assert_eq!(count(&h.read()), 1, "write visible inside the dry-run");
        })
        .unwrap();

        // After the dry-run, nothing persisted.
        assert_eq!(count(&db.handle().read()), 0);
    }

    #[test]
    fn dry_run_returns_closure_value() {
        let db = Database::open_in_memory().unwrap();
        let n = db
            .dry_run(|h| {
                insert_dummy(&h.write());
                count(&h.read())
            })
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn dry_run_does_not_roll_back_committed_writes_on_other_connections() {
        let db = Database::open_in_memory().unwrap();

        insert_dummy(&db.handle().write());
        assert_eq!(count(&db.handle().read()), 1);

        db.dry_run(|h| {
            insert_dummy(&h.write());
            assert_eq!(count(&h.read()), 2, "sees its own + the committed row");
        })
        .unwrap();

        assert_eq!(count(&db.handle().read()), 1);
    }

    #[test]
    fn without_a_dry_run_writes_persist_normally() {
        let db = Database::open_in_memory().unwrap();
        insert_dummy(&db.handle().write());
        assert_eq!(count(&db.handle().read()), 1);
    }

    #[test]
    fn concurrent_reads_are_served_while_a_write_lock_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_config(
            dir.path().join("test.db"),
            DatabaseConfig {
                reader_pool_size: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let handle = db.handle();
        let write_guard = handle.write();
        let h2 = handle.clone();
        let done = thread::spawn(move || {
            let r = h2.read();
            count(&r)
        });
        let n = done.join().unwrap();
        assert_eq!(n, 0);
        drop(write_guard);
    }

    #[test]
    fn in_memory_reads_share_the_writer_connection_and_are_not_query_only() {
        let db = Database::open_in_memory().unwrap();
        let handle = db.handle();
        assert_eq!(
            query_only(&handle.read()),
            0,
            "in-memory reads share the writer connection and are never query_only"
        );
    }

    #[test]
    fn reader_pool_blocks_then_resumes_when_exhausted() {
        let db = Database::open_in_memory_with_config(DatabaseConfig {
            reader_pool_size: 1,
            ..Default::default()
        })
        .unwrap();
        let handle = db.handle();

        let counter = StdArc::new(AtomicUsize::new(0));

        let held = handle.read();

        let h2 = handle.clone();
        let c2 = StdArc::clone(&counter);
        let waiter = thread::spawn(move || {
            let _r = h2.read(); // blocks until `held` is returned
            c2.fetch_add(1, Ordering::SeqCst);
        });

        thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "waiter should be blocked"
        );

        drop(held);
        waiter.join().unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn two_handles_on_one_file_see_each_others_committed_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.db");

        let db_a = Database::open(&path).unwrap();
        let db_b = Database::open(&path).unwrap();

        insert_dummy(&db_a.handle().write());

        assert_eq!(count(&db_b.handle().read()), 1);
    }

    #[test]
    fn shared_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Database>();
        assert_send_sync::<DbHandle>();
    }

    #[test]
    fn poisoned_writer_with_open_transaction_is_healed_on_next_write() {
        let db = Database::open_in_memory().unwrap();
        let handle = db.handle();

        let h2 = handle.clone();
        let poisoner = thread::spawn(move || {
            let conn = h2.write();
            conn.execute_batch(
                "BEGIN; INSERT INTO issuer (id, status, created_at) \
                                VALUES (randomblob(16), 'ACTIVE', '2026-01-01T00:00:00Z')",
            )
            .unwrap();
            panic!("boom mid-write");
        });
        assert!(
            poisoner.join().is_err(),
            "poisoner thread should have panicked"
        );

        {
            let conn = handle.write();
            assert!(
                conn.is_autocommit(),
                "recovered writer must have had its stranded transaction rolled back"
            );
            assert_eq!(
                count(&conn),
                0,
                "stranded transaction's write must be discarded"
            );
        }

        insert_dummy(&db.handle().write());
        assert_eq!(count(&db.handle().read()), 1);
    }

    #[test]
    fn dry_run_serializes_against_a_concurrent_writer() {
        use std::sync::mpsc;

        let db = StdArc::new(Database::open_in_memory().unwrap());
        let handle = db.handle();

        let (inside_tx, inside_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let db_dry = StdArc::clone(&db);
        let dry = thread::spawn(move || {
            db_dry
                .dry_run(|h| {
                    insert_dummy(&h.write());
                    inside_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    assert_eq!(count(&h.read()), 1, "dry-run sees only its own row");
                })
                .unwrap();
        });

        inside_rx.recv().unwrap();

        let h2 = handle.clone();
        let writer = thread::spawn(move || {
            insert_dummy(&h2.write());
        });

        thread::sleep(std::time::Duration::from_millis(50));

        release_tx.send(()).unwrap();
        dry.join().unwrap();
        writer.join().unwrap();

        assert_eq!(
            count(&db.handle().read()),
            1,
            "concurrent real write survives; dry-run write rolled back"
        );
    }

    #[test]
    fn file_reader_connections_are_read_only_at_the_sqlite_level() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readonly.db");
        let db = Database::open(&path).unwrap();
        let handle = db.handle();

        let reader = handle.read();
        let err = insert_dummy_result(&reader).expect_err("write on a reader must fail");
        match err {
            rusqlite::Error::SqliteFailure(e, _) => {
                assert_eq!(
                    e.code,
                    rusqlite::ErrorCode::ReadOnly,
                    "expected SQLITE_READONLY, got {e:?}"
                );
            }
            other => panic!("expected a SqliteFailure(ReadOnly), got {other:?}"),
        }
    }

    fn journal_mode(conn: &Connection) -> String {
        conn.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .unwrap()
    }

    fn query_only(conn: &Connection) -> i64 {
        conn.pragma_query_value(None, "query_only", |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn journal_mode_is_wal_on_a_file_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.db");
        let db = Database::open(&path).unwrap();

        assert_eq!(
            journal_mode(&db.handle().write()).to_lowercase(),
            "wal",
            "file-backed writer should be in WAL mode"
        );
    }

    #[test]
    fn in_memory_database_is_not_wal() {
        let db = Database::open_in_memory().unwrap();
        let mode = journal_mode(&db.handle().write()).to_lowercase();
        assert_ne!(
            mode, "wal",
            "in-memory database must not use WAL, got {mode:?}"
        );
    }

    #[test]
    fn synchronous_full_is_applied_to_the_writer_when_configured() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable.db");
        let db = Database::open_with_config(
            path,
            DatabaseConfig {
                synchronous: Synchronous::Full,
                ..Default::default()
            },
        )
        .unwrap();

        let level: i64 = db
            .handle()
            .write()
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        assert_eq!(level, 2, "synchronous should be FULL (2) when configured");
    }

    #[test]
    fn synchronous_defaults_to_normal_on_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("relaxed.db");
        let db = Database::open(&path).unwrap();

        let level: i64 = db
            .handle()
            .write()
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        assert_eq!(level, 1, "synchronous should default to NORMAL (1)");
    }

    #[test]
    fn reader_connections_are_query_only_and_writer_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path().join("test.db")).unwrap();
        let handle = db.handle();
        {
            let reader = handle.read();
            assert_eq!(
                query_only(&reader),
                1,
                "reader pragma query_only must be ON"
            );
        }
        assert_eq!(
            query_only(&handle.write()),
            0,
            "writer pragma query_only must be OFF"
        );
    }

    #[test]
    fn two_in_memory_databases_are_isolated() {
        let db_a = Database::open_in_memory().unwrap();
        let db_b = Database::open_in_memory().unwrap();

        insert_dummy(&db_a.handle().write());

        assert_eq!(count(&db_a.handle().read()), 1, "db_a sees its own write");
        assert_eq!(
            count(&db_b.handle().read()),
            0,
            "db_b must not see db_a's rows (independent in-memory databases)"
        );
    }

    #[test]
    fn wal_file_stays_bounded_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bounded.db");
        let wal_path = dir.path().join("bounded.db-wal");

        {
            let db = Database::open(&path).unwrap();
            let handle = db.handle();
            for _ in 0..5_000 {
                insert_dummy(&handle.write());
            }
        }

        if let Ok(meta) = std::fs::metadata(&wal_path) {
            let size = meta.len();
            assert!(
                size < 64 * 1024,
                "WAL should be truncated on close; found {size} bytes"
            );
        }

        let db = Database::open(&path).unwrap();
        assert_eq!(count(&db.handle().read()), 5_000);
    }

    #[test]
    fn maintenance_checkpoints_the_wal_and_keeps_it_bounded_across_cycles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("maint.db");
        let wal_path = dir.path().join("maint.db-wal");

        let db = Database::open(&path).unwrap();
        let handle = db.handle();

        // Cycle 1: write, then maintain.
        for _ in 0..2_000 {
            insert_dummy(&handle.write());
        }
        let stats = db
            .run_maintenance()
            .unwrap()
            .expect("file-backed db reports checkpoint stats");
        assert_eq!(stats.busy, 0, "no other connection should block PASSIVE");
        assert_eq!(
            stats.log_frames, stats.checkpointed_frames,
            "with idle readers the full WAL must be checkpointed"
        );
        let wal_after_first = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);

        // Cycle 2: same again. A full passive checkpoint lets SQLite rewind
        // the WAL, so its size must not keep growing cycle over cycle.
        for _ in 0..2_000 {
            insert_dummy(&handle.write());
        }
        db.run_maintenance().unwrap();
        let wal_after_second = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        assert!(
            wal_after_second <= wal_after_first.saturating_mul(2),
            "WAL must stay bounded across maintenance cycles \
             (first: {wal_after_first} bytes, second: {wal_after_second} bytes)"
        );

        // The database stays fully usable afterwards.
        assert_eq!(count(&db.handle().read()), 4_000);
        insert_dummy(&handle.write());
        assert_eq!(count(&db.handle().read()), 4_001);
    }

    #[test]
    fn maintenance_on_in_memory_database_skips_the_checkpoint() {
        let db = Database::open_in_memory().unwrap();
        let stats = db.run_maintenance().unwrap();
        assert!(stats.is_none(), "in-memory databases have no WAL");
    }

    #[test]
    #[ignore = "stress test; run with --ignored"]
    fn dry_run_does_not_race_concurrent_writes() {
        use std::sync::Barrier;

        const ITERS: usize = 200;

        let (_dir, db) = temp_file_db(DatabaseConfig::default());
        let db = StdArc::new(db);
        let barrier = StdArc::new(Barrier::new(2));

        let (db1, b1) = (StdArc::clone(&db), StdArc::clone(&barrier));
        let writer = thread::spawn(move || {
            b1.wait();
            for _ in 0..ITERS {
                insert_dummy_result(&db1.handle().write())
                    .expect("concurrent real write must not surface SQLITE_BUSY");
            }
        });

        let (db2, b2) = (StdArc::clone(&db), StdArc::clone(&barrier));
        let dry_runner = thread::spawn(move || {
            b2.wait();
            for _ in 0..ITERS {
                db2.dry_run(|h| {
                    insert_dummy_result(&h.write())
                        .expect("dry-run write must not surface SQLITE_BUSY");
                })
                .expect("dry_run itself must not fail");
            }
        });

        writer.join().unwrap();
        dry_runner.join().unwrap();

        assert_eq!(
            count(&db.handle().read()),
            ITERS as i64,
            "exactly the committed writes survive; all dry-runs rolled back"
        );
    }

    #[test]
    #[ignore = "soak test; run with --ignored"]
    fn mixed_read_write_soak() {
        use std::sync::Barrier;

        const THREADS: usize = 8;
        const OPS_PER_THREAD: usize = 500;

        let (_dir, db) = temp_file_db(DatabaseConfig {
            reader_pool_size: THREADS,
            ..Default::default()
        });
        let db = StdArc::new(db);
        let barrier = StdArc::new(Barrier::new(THREADS));
        let inserted = StdArc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let db = StdArc::clone(&db);
                let barrier = StdArc::clone(&barrier);
                let inserted = StdArc::clone(&inserted);
                thread::spawn(move || {
                    let handle = db.handle();
                    let mut rng: u64 = 0x9E3779B97F4A7C15 ^ (t as u64).wrapping_mul(0x1234_5678);
                    let mut next = || {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        rng
                    };

                    barrier.wait();

                    let mut my_ids: Vec<[u8; 16]> = Vec::new();

                    for _ in 0..OPS_PER_THREAD {
                        match next() % 4 {
                            0 => {
                                // INSERT with a fresh random id.
                                let mut id = [0u8; 16];
                                let r = next();
                                id[..8].copy_from_slice(&r.to_le_bytes());
                                id[8..].copy_from_slice(&next().to_le_bytes());
                                let conn = handle.write();
                                let affected = conn
                                    .execute(
                                        "INSERT INTO issuer (id, status, created_at, version) \
                                         VALUES (?1, 'ACTIVE', '2026-01-01T00:00:00Z', 1)",
                                        rusqlite::params![&id[..]],
                                    )
                                    .expect("insert must not surface SQLITE_BUSY");
                                if affected == 1 {
                                    my_ids.push(id);
                                    inserted.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            1 => {
                                if let Some(id) =
                                    my_ids.get((next() as usize) % my_ids.len().max(1))
                                {
                                    let conn = handle.write();
                                    // Read current version, then bump with the guard.
                                    let ver: Option<i64> = conn
                                        .query_row(
                                            "SELECT version FROM issuer WHERE id = ?1",
                                            rusqlite::params![&id[..]],
                                            |r| r.get(0),
                                        )
                                        .ok();
                                    if let Some(ver) = ver {
                                        conn.execute(
                                            "UPDATE issuer SET status = 'RETIRED', \
                                             version = version + 1 WHERE id = ?1 AND version = ?2",
                                            rusqlite::params![&id[..], ver],
                                        )
                                        .expect("patch must not surface SQLITE_BUSY");
                                    }
                                }
                            }
                            2 => {
                                if let Some(id) =
                                    my_ids.get((next() as usize) % my_ids.len().max(1))
                                {
                                    let conn = handle.read();
                                    let _found: Option<i64> = conn
                                        .query_row(
                                            "SELECT version FROM issuer WHERE id = ?1",
                                            rusqlite::params![&id[..]],
                                            |r| r.get(0),
                                        )
                                        .ok();
                                }
                            }
                            _ => {
                                let conn = handle.read();
                                let _total = count(&conn);
                            }
                        }
                    }

                    my_ids
                })
            })
            .collect();

        let mut all_ids: Vec<[u8; 16]> = Vec::new();
        for h in handles {
            all_ids.extend(h.join().expect("no thread should panic"));
        }

        let handle = db.handle();
        let conn = handle.read();

        let final_count = count(&conn);
        assert_eq!(
            final_count,
            inserted.load(Ordering::Relaxed) as i64,
            "final row count must equal successful inserts (no lost inserts, no phantom rows)"
        );
        assert_eq!(final_count as usize, all_ids.len());

        for id in &all_ids {
            let ver: i64 = conn
                .query_row(
                    "SELECT version FROM issuer WHERE id = ?1",
                    rusqlite::params![&id[..]],
                    |r| r.get(0),
                )
                .expect("every inserted id must still exist");
            assert!(ver >= 1, "version must be monotonic (>= 1), got {ver}");
        }
    }
}
