use crate::sqlite::error::SqliteError;
use crate::sqlite::migrations;
#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Condvar, Mutex, MutexGuard};
use rusqlite::{Connection, OpenFlags};
use std::cell::Cell;
use std::cmp::min;
use std::path::Path;
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use valqeron_core::{StorageError, StorageFault};

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub reader_pool_size: usize,
    pub synchronous: Synchronous,
    pub busy_timeout: Duration,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            reader_pool_size: default_reader_pool_size(),
            synchronous: Synchronous::default(),
            busy_timeout: Duration::from_secs(5),
        }
    }
}

fn default_reader_pool_size() -> usize {
    match thread::available_parallelism() {
        Ok(available_parallelism) => min(available_parallelism.into(), 6),
        Err(_) => 2,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WalCheckpointStats {
    /// `1` when the checkpoint could not run to completion because another connection kept the WAL
    /// busy, `0` otherwise.
    pub busy: i64,
    /// Total number of frames in the WAL file.
    pub log_frames: i64,
    /// Number of frames successfully transferred into the database file.
    pub checkpointed_frames: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Synchronous {
    #[default]
    Normal,
    Full,
}

impl Synchronous {
    fn as_pragma(self) -> &'static str {
        match self {
            Synchronous::Normal => "NORMAL",
            Synchronous::Full => "FULL",
        }
    }
}

#[derive(Clone, Copy)]
enum ConnectionRole {
    Writer,
    Reader,
}

fn open_connection(path: &Path, role: ConnectionRole) -> Result<Connection, SqliteError> {
    let flags = match role {
        ConnectionRole::Writer => OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        ConnectionRole::Reader => OpenFlags::SQLITE_OPEN_READ_ONLY,
    };
    Connection::open_with_flags(path, flags).map_err(|source| SqliteError::Connection { source })
}

#[cfg(not(loom))]
const READER_CHECKOUT_WARN_AFTER: Duration = Duration::from_secs(5);

pub(crate) struct WaitPool<T> {
    idle: Mutex<Vec<T>>,
    available: Condvar,
}

impl<T> WaitPool<T> {
    fn new(items: Vec<T>) -> Self {
        Self {
            idle: Mutex::new(items),
            available: Condvar::new(),
        }
    }

    fn take(&self) -> T {
        let mut idle = lock(&self.idle);
        loop {
            if let Some(item) = idle.pop() {
                return item;
            }
            idle = self.wait_for_item(idle);
        }
    }

    #[cfg(loom)]
    fn wait_for_item<'a>(&self, idle: MutexGuard<'a, Vec<T>>) -> MutexGuard<'a, Vec<T>> {
        self.available
            .wait(idle)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(not(loom))]
    fn wait_for_item<'a>(&self, idle: MutexGuard<'a, Vec<T>>) -> MutexGuard<'a, Vec<T>> {
        let (guard, timeout) = self
            .available
            .wait_timeout_while(idle, READER_CHECKOUT_WARN_AFTER, |idle| idle.is_empty())
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if timeout.timed_out() {
            tracing::warn!(
                waited_secs = READER_CHECKOUT_WARN_AFTER.as_secs(),
                "reader pool checkout has been blocked for a while; a ReadGuard may be leaked or long-held"
            );
        }
        guard
    }

    fn put(&self, item: T) {
        lock(&self.idle).push(item);
        self.available.notify_one();
    }
}

pub(crate) type ReaderPool = WaitPool<Connection>;

impl ReaderPool {
    pub(crate) fn checkout(pool: &Arc<Self>) -> PooledReader {
        PooledReader {
            pool: Arc::clone(pool),
            conn: Some(pool.take()),
        }
    }

    pub(crate) fn checkin(&self, conn: Connection) {
        self.put(conn);
    }
}

pub(crate) struct PooledReader {
    pool: Arc<ReaderPool>,
    conn: Option<Connection>,
}

impl Drop for PooledReader {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.checkin(conn);
        }
    }
}

impl std::ops::Deref for PooledReader {
    type Target = Connection;
    #[allow(clippy::expect_used)]
    fn deref(&self) -> &Connection {
        self.conn.as_ref().expect("connection present until drop")
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_writer(m: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    m.lock().unwrap_or_else(|poisoned| {
        let guard = poisoned.into_inner();
        if !guard.is_autocommit() {
            tracing::warn!(
                "recovered a poisoned writer mutex with an open transaction; forcing ROLLBACK"
            );
            if let Err(e) = guard.execute_batch("ROLLBACK") {
                tracing::error!(
                    error = %e,
                    "failed to ROLLBACK a stranded transaction after writer poison recovery"
                );
            }
        } else {
            tracing::warn!("recovered a poisoned writer mutex (connection was in autocommit)");
        }
        guard
    })
}

pub(crate) enum ReadGuard<'a> {
    Pooled(PooledReader),
    Borrowed(&'a Connection),
}

impl ReadGuard<'_> {
    fn start_operation(&self) {
        install_sqlite_progress_handler(self);
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        clear_sqlite_progress_handler(self);
    }
}

impl std::ops::Deref for ReadGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        match self {
            ReadGuard::Pooled(p) => p,
            ReadGuard::Borrowed(c) => c,
        }
    }
}

pub(crate) enum WriteGuard<'a> {
    Locked(MutexGuard<'a, Connection>),
    Borrowed(&'a Connection),
}

impl WriteGuard<'_> {
    fn start_operation(&self) {
        install_sqlite_progress_handler(self);
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        clear_sqlite_progress_handler(self);
    }
}

impl std::ops::Deref for WriteGuard<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        match self {
            WriteGuard::Locked(g) => g,
            WriteGuard::Borrowed(c) => c,
        }
    }
}

fn clear_sqlite_progress_handler(conn: &Connection) {
    let _ = conn.progress_handler(0, None::<fn() -> bool>);
}

const MAX_OPERATION_EXECUTION_TIME: Duration = Duration::from_secs(15);

fn install_sqlite_progress_handler(conn: &Connection) {
    install_sqlite_progress_handler_with_timeout(conn, MAX_OPERATION_EXECUTION_TIME);
}

fn install_sqlite_progress_handler_with_timeout(conn: &Connection, timeout: Duration) {
    let operation_start = Instant::now();

    let _ = conn.progress_handler(
        5_000,
        Some(move || {
            if Instant::now().duration_since(operation_start) > timeout {
                tracing::error!(
                    "SQLite operation interrupted: execution exceeded time limit of {:?}",
                    timeout
                );
                true
            } else {
                false
            }
        }),
    );
}

thread_local! {
    static DRY_RUN_CONN: Cell<Option<*const Connection>> = const { Cell::new(None) };
}

fn with_dry_run_conn<T>(conn: &Connection, f: impl FnOnce() -> T) -> T {
    struct Restore(Option<*const Connection>);
    impl Drop for Restore {
        fn drop(&mut self) {
            DRY_RUN_CONN.set(self.0);
        }
    }

    let _restore = Restore(DRY_RUN_CONN.replace(Some(std::ptr::from_ref(conn))));
    f()
}

fn is_dry_run_active() -> bool {
    DRY_RUN_CONN.get().is_some()
}

fn current_dry_run_conn() -> &'static Connection {
    #[allow(clippy::expect_used)]
    let ptr = DRY_RUN_CONN
        .get()
        .expect("DbHandle::DryRun used outside an active dry-run");
    // SAFETY: the pointer was published by `with_dry_run_conn` from a live, locked `&Connection`
    // that outlives every access on this thread; the slot is cleared before that connection is
    // released, so a dangling pointer can never be observed here.
    unsafe { &*ptr }
}

pub(crate) trait Db {
    fn write(&self) -> WriteGuard<'_>;

    fn read(&self) -> ReadGuard<'_>;
}

#[derive(Clone)]
pub(crate) enum DbHandle {
    Live {
        writer: Arc<Mutex<Connection>>,
        readers: Arc<ReaderPool>,
    },
    DryRun,
}

impl Db for DbHandle {
    fn write(&self) -> WriteGuard<'_> {
        let guard = match self {
            DbHandle::Live { writer, .. } => WriteGuard::Locked(lock_writer(writer)),
            DbHandle::DryRun => WriteGuard::Borrowed(current_dry_run_conn()),
        };
        guard.start_operation();
        guard
    }

    fn read(&self) -> ReadGuard<'_> {
        let guard = match self {
            DbHandle::Live { readers, .. } => ReadGuard::Pooled(ReaderPool::checkout(readers)),
            DbHandle::DryRun => ReadGuard::Borrowed(current_dry_run_conn()),
        };
        guard.start_operation();
        guard
    }
}

/// A file-backed SQLite database in WAL mode.
///
/// Connection topology: one shared read/write connection (the writer, behind
/// a mutex) plus a pool of `reader_pool_size` read-only connections that
/// serve concurrent reads. Tests that need a database create a temporary
/// file-backed one via [`Database::open_temp`].
pub(crate) struct Database {
    writer: Arc<Mutex<Connection>>,
    readers: Arc<ReaderPool>,
    /// Number of reader connections the pool was built with; callers size
    /// their admission control against this so it never drifts from the pool.
    reader_pool_size: usize,
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
        let path = path.as_ref();

        if config.reader_pool_size < 1 {
            return Err(SqliteError::InvalidPoolSize);
        }

        let mut writer = open_connection(path, ConnectionRole::Writer)?;
        configure(
            &writer,
            ConnectionRole::Writer,
            config.synchronous,
            config.busy_timeout,
        )?;
        migrations::run(&mut writer)?;

        let mut pool = Vec::with_capacity(config.reader_pool_size);
        for _ in 0..config.reader_pool_size {
            let conn = open_connection(path, ConnectionRole::Reader)?;
            configure(
                &conn,
                ConnectionRole::Reader,
                config.synchronous,
                config.busy_timeout,
            )?;
            pool.push(conn);
        }

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: Arc::new(ReaderPool::new(pool)),
            reader_pool_size: config.reader_pool_size,
        })
    }

    /// Number of reader connections in the pool, as configured at open time.
    pub(crate) fn reader_pool_size(&self) -> usize {
        self.reader_pool_size
    }

    /// Test helper: a fresh database in its own temporary directory, bundled
    /// with that directory so it lives exactly as long as the database.
    #[cfg(test)]
    pub(crate) fn open_temp() -> TempDatabase {
        Self::open_temp_with_config(DatabaseConfig::default())
    }

    #[cfg(test)]
    pub(crate) fn open_temp_with_config(config: DatabaseConfig) -> TempDatabase {
        let dir = tempfile::tempdir().expect("create temp dir for test database");
        let db = Self::open_with_config(dir.path().join("test.db"), config)
            .expect("open temp test database");
        TempDatabase { db, _dir: dir }
    }

    pub(crate) fn handle(&self) -> DbHandle {
        DbHandle::Live {
            writer: Arc::clone(&self.writer),
            readers: Arc::clone(&self.readers),
        }
    }

    /// Periodic maintenance for long-lived processes: `PRAGMA optimize` plus
    /// a **passive** WAL checkpoint.
    ///
    /// `PASSIVE` is deliberate — it never blocks readers or writers in other
    /// processes (the database may be in use concurrently). The aggressive
    /// `TRUNCATE` checkpoint remains reserved for [`Drop`], when all
    /// in-process work has stopped.
    pub(crate) fn run_maintenance(&self) -> Result<WalCheckpointStats, SqliteError> {
        fn maintenance_err(source: rusqlite::Error) -> SqliteError {
            SqliteError::Maintenance { source }
        }

        let conn = lock_writer(&self.writer);

        conn.execute_batch("PRAGMA optimize;")
            .map_err(maintenance_err)?;

        conn.query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |row| {
            Ok(WalCheckpointStats {
                busy: row.get(0)?,
                log_frames: row.get(1)?,
                checkpointed_frames: row.get(2)?,
            })
        })
        .map_err(maintenance_err)
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

        if let Err(e) = conn.pragma_update(None, "wal_checkpoint", "TRUNCATE") {
            tracing::warn!(error = %e, "WAL checkpoint(TRUNCATE) failed on close");
        }
    }
}

fn configure(
    conn: &Connection,
    role: ConnectionRole,
    synchronous: Synchronous,
    busy_timeout: Duration,
) -> Result<(), SqliteError> {
    let pragma_err = |source| SqliteError::Pragma { source };

    match role {
        ConnectionRole::Writer => conn
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(pragma_err)?,
        ConnectionRole::Reader => {
            conn.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .map_err(pragma_err)?;
        }
    }

    conn.pragma_update(None, "synchronous", synchronous.as_pragma())
        .map_err(pragma_err)?;

    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(pragma_err)?;

    conn.busy_timeout(busy_timeout).map_err(pragma_err)?;

    conn.pragma_update(None, "cache_size", -64_000i64)
        .map_err(pragma_err)?;

    conn.pragma_update(None, "temp_store", "MEMORY")
        .map_err(pragma_err)?;

    conn.pragma_update(None, "mmap_size", 256i64 * 1024 * 1024)
        .map_err(pragma_err)?;

    conn.pragma_update(None, "wal_autocheckpoint", 1000i64)
        .map_err(pragma_err)?;

    conn.set_prepared_statement_cache_capacity(64);

    if let ConnectionRole::Reader = role {
        conn.pragma_update(None, "query_only", "ON")
            .map_err(pragma_err)?;
    }

    Ok(())
}

#[cfg(test)]
pub(crate) struct TempDatabase {
    db: Database,
    _dir: tempfile::TempDir,
}

#[cfg(test)]
impl std::ops::Deref for TempDatabase {
    type Target = Database;

    fn deref(&self) -> &Database {
        &self.db
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::sqlite::migrations::{self, MIGRATIONS};
    use rusqlite::Connection;
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

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

    #[test]
    fn fresh_database_ends_up_at_latest_version() {
        let db = Database::open_temp();
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
    fn v3_database_upgrades_to_latest_with_the_new_engine_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upgrade.db");

        // Build a genuine v3 database: apply the first three migrations by
        // hand and stamp the version, exactly as an engine at v3 left it.
        {
            let conn = Connection::open(&path).unwrap();
            for sql in MIGRATIONS.iter().take(3) {
                conn.execute_batch(sql).unwrap();
            }
            conn.pragma_update(None, "user_version", 3).unwrap();
        }

        let mut conn = Connection::open(&path).unwrap();
        migrations::run(&mut conn).unwrap();

        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, MIGRATIONS.len() as i64);
        for table in ["sync_cursor", "task_registration"] {
            let rows: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(rows, 0, "{table} exists and is empty");
        }
    }

    #[test]
    fn schema_from_the_future_is_rejected_rather_than_silently_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = Connection::open(dir.path().join("future.db")).unwrap();

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
        let dir = tempfile::tempdir().unwrap();
        let result = Database::open_with_config(
            dir.path().join("invalid.db"),
            DatabaseConfig {
                reader_pool_size: 0,
                ..Default::default()
            },
        );
        assert!(matches!(result, Err(SqliteError::InvalidPoolSize)));
    }

    #[test]
    fn dry_run_rolls_back_writes() {
        let db = Database::open_temp();

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
        let db = Database::open_temp();
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
        let db = Database::open_temp();

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
        let db = Database::open_temp();
        insert_dummy(&db.handle().write());
        assert_eq!(count(&db.handle().read()), 1);
    }

    #[test]
    fn concurrent_reads_are_served_while_a_write_lock_is_held() {
        let db = Database::open_temp_with_config(DatabaseConfig {
            reader_pool_size: 2,
            ..Default::default()
        });
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
    fn reader_pool_blocks_then_resumes_when_exhausted() {
        let db = Database::open_temp_with_config(DatabaseConfig {
            reader_pool_size: 1,
            ..Default::default()
        });
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
        let db = Database::open_temp();
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

        let db = Database::open_temp();
        let db = StdArc::new(db);
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
    fn reader_connections_are_read_only_at_the_sqlite_level() {
        let db = Database::open_temp();
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
    fn journal_mode_is_wal() {
        let db = Database::open_temp();

        assert_eq!(
            journal_mode(&db.handle().write()).to_lowercase(),
            "wal",
            "writer should be in WAL mode"
        );
    }

    #[test]
    fn synchronous_full_is_applied_to_the_writer_when_configured() {
        let db = Database::open_temp_with_config(DatabaseConfig {
            synchronous: Synchronous::Full,
            ..Default::default()
        });

        let level: i64 = db
            .handle()
            .write()
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        assert_eq!(level, 2, "synchronous should be FULL (2) when configured");
    }

    #[test]
    fn synchronous_defaults_to_normal_on_the_writer() {
        let db = Database::open_temp();

        let level: i64 = db
            .handle()
            .write()
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        assert_eq!(level, 1, "synchronous should default to NORMAL (1)");
    }

    #[test]
    fn reader_connections_are_query_only_and_writer_is_not() {
        let db = Database::open_temp();
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
        let stats = db.run_maintenance().unwrap();
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
    #[ignore = "stress test; run with --ignored"]
    fn dry_run_does_not_race_concurrent_writes() {
        use std::sync::Barrier;

        const ITERS: usize = 200;

        let db = Database::open_temp();
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

        let db = Database::open_temp_with_config(DatabaseConfig {
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

#[cfg(test)]
mod guard_tests {
    use super::*;
    use rusqlite::Connection;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn dropping_a_guard_clears_its_expired_progress_handler() {
        let conn = Connection::open_in_memory().unwrap();

        {
            let guard = ReadGuard::Borrowed(&conn);
            install_sqlite_progress_handler_with_timeout(&guard, Duration::from_millis(1));
            thread::sleep(Duration::from_millis(5));
        }

        let value: i64 = conn
            .query_row(
                "WITH RECURSIVE counter(x) AS (
                     SELECT 1
                     UNION ALL
                     SELECT x + 1 FROM counter LIMIT 100000
                 )
                 SELECT sum(x) FROM counter",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, 5_000_050_000);
    }
}

#[cfg(all(loom, test))]
mod loom_tests {
    use super::WaitPool;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn single_item_is_never_lost_or_duplicated_under_contention() {
        loom::model(|| {
            let pool = Arc::new(WaitPool::new(vec![0usize]));

            let in_use = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let pool = Arc::clone(&pool);
                    let in_use = Arc::clone(&in_use);
                    loom::thread::spawn(move || {
                        let item = pool.take();
                        let concurrent = in_use.fetch_add(1, Ordering::Acquire);
                        assert_eq!(
                            concurrent, 0,
                            "two threads held the single pooled item at once"
                        );
                        in_use.fetch_sub(1, Ordering::Release);
                        pool.put(item);
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            let item = pool.take();
            assert_eq!(item, 0);
            pool.put(item);
        });
    }

    #[test]
    fn blocked_take_is_woken_by_a_later_put() {
        loom::model(|| {
            let pool: Arc<WaitPool<usize>> = Arc::new(WaitPool::new(vec![]));

            let taker = {
                let pool = Arc::clone(&pool);
                loom::thread::spawn(move || {
                    let item = pool.take();
                    assert_eq!(item, 7);
                })
            };

            pool.put(7);

            taker.join().unwrap();
        });
    }
}
