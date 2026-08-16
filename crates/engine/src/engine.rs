use crate::grpc::{AdminGrpc, IssuerGrpc};
use crate::lifecycle::{Lifecycle, LifecycleState};
use crate::storage::AsyncStorage;
use crate::tasks::{BackgroundTasksBuilder, BackgroundTasksManager};
use chrono::{NaiveTime, Weekday};
use directories::ProjectDirs;
use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use valqeron_core::Recurrence;
use valqeron_infrastructure::{DatabaseConfig, Synchronous};
use valqeron_proto::v1::rpc_admin_service_server::RpcAdminServiceServer;
use valqeron_proto::v1::rpc_issuer_service_server::RpcIssuerServiceServer;

// ================ TYPES ================
pub type EngineResult<T> = Result<T, EngineError>;

// ================ ERRORS & EXIT CODES ================
pub mod exit_code {
    /// Generic runtime failure (including a forced shutdown).
    pub const RUNTIME: i32 = 1;
    /// Configuration could not be resolved.
    pub const CONFIG: i32 = 2;
    /// Another engine instance already holds the database lock.
    pub const ALREADY_RUNNING: i32 = 3;
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error(
        "could not determine a data directory for {app}; set {env_var} or pass an explicit path"
    )]
    InvalidDataDirectory {
        app: &'static str,
        env_var: &'static str,
    },

    #[error("another engine instance already owns {} (pid {pid}); stop it first or check \
    `valqeron engine status`", db_path.display())]
    AlreadyRunning { db_path: PathBuf, pid: String },

    #[error("storage error: {0}")]
    Storage(#[from] valqeron_core::StorageError),

    #[error("i/o error: {0}")]
    Io(String),

    #[error("forced shutdown: second signal received before graceful shutdown finished")]
    ForcedShutdown,
}

impl EngineError {
    pub fn exit_code(&self) -> i32 {
        match self {
            EngineError::Config(_) | EngineError::InvalidDataDirectory { .. } => exit_code::CONFIG,
            EngineError::AlreadyRunning { .. } => exit_code::ALREADY_RUNNING,
            EngineError::Storage(_) | EngineError::Io(_) | EngineError::ForcedShutdown => {
                exit_code::RUNTIME
            }
        }
    }
}

// ================ ENVIRONMENT VARIABLES ================

pub const DB_PATH_ENV: &str = "VALQERON_DB";
pub const ENGINE_LOG_FILE_ENV: &str = "VALQERON_ENGINE_LOG_FILE";
pub const ENGINE_LOG_LEVEL_ENV: &str = "VALQERON_ENGINE_LOG_LEVEL";
pub const ENGINE_DURABLE_ENV: &str = "VALQERON_ENGINE_DURABLE";
pub const ENGINE_MAINTENANCE_INTERVAL_ENV: &str = "VALQERON_ENGINE_MAINTENANCE_INTERVAL";
pub const ENGINE_HEARTBEAT_INTERVAL_ENV: &str = "VALQERON_ENGINE_HEARTBEAT_INTERVAL";
pub const ENGINE_SYNC_CVM_ENV: &str = "VALQERON_ENGINE_SYNC_CVM";
pub const ENGINE_SYNC_CVM_AT_ENV: &str = "VALQERON_ENGINE_SYNC_CVM_AT";
pub const ENGINE_SYNC_CVM_SCHEDULE_ENV: &str = "VALQERON_ENGINE_SYNC_CVM_SCHEDULE";
pub const ENGINE_SYNC_CVM_COOLDOWN_ENV: &str = "VALQERON_ENGINE_SYNC_CVM_COOLDOWN";
pub const ENGINE_SYNC_MAX_BACKFILL_DAYS_ENV: &str = "VALQERON_ENGINE_SYNC_MAX_BACKFILL_DAYS";

// ================ DEFAULT VALUES ================
pub const DEFAULT_VALQERON_QUALIFIER: &str = "io";
pub const DEFAULT_VALQERON_ORGANIZATION: &str = "valqeron";
pub const DEFAULT_VALQERON_APP: &str = "valqeron";
pub const DEFAULT_ENGINE_DB_NAME: &str = "valqeron.db";
pub const DEFAULT_ENGINE_LOG_FILE_NAME: &str = "engine.log";
pub const DEFAULT_MAINTENANCE_INTERVAL_SECS: u64 = 3600;
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 300;
/// Default market-local time of day sync sources run at (`HH:MM`).
pub const DEFAULT_SYNC_AT: (u32, u32) = (7, 0);
/// Default base of the failure cooldown (doubles per consecutive terminal
/// failure, capped at one hour by core).
pub const DEFAULT_SYNC_COOLDOWN_SECS: u32 = 300;
/// Default bound on unattended sequential catch-up, in business days.
pub const DEFAULT_SYNC_MAX_BACKFILL_DAYS: u32 = 90;

// ================ SIZING & TIMEOUTS ================

/// How long the graceful shutdown waits for in-flight RPCs / jobs / storage.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on draining the blocking pool when the runtime shuts down. A stuck
/// job must not hold the process open forever — the service manager's SIGKILL
/// (after `TimeoutStopSec` / launchd's exit timeout) is the final backstop.
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

// ================ DATABASE PATH ================
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct DatabasePath(PathBuf);

impl DatabasePath {
    /// `VALQERON_DB` > `<platform data dir>/valqeron.db`.
    pub fn resolve() -> EngineResult<Self> {
        let project_data_dir =
            resolve_default_project_dir().map(|dirs| dirs.data_dir().to_path_buf());

        Self::resolve_with(std::env::var_os(DB_PATH_ENV), project_data_dir)
    }

    /// Pure resolution core: env value and platform dir are injected so tests
    /// never touch process-global environment state.
    fn resolve_with(
        env_value: Option<OsString>,
        project_data_dir: Option<PathBuf>,
    ) -> EngineResult<Self> {
        if let Some(path) = env_value {
            return Ok(Self(PathBuf::from(path)));
        }

        project_data_dir
            .map(|dir| Self(dir.join(DEFAULT_ENGINE_DB_NAME)))
            .ok_or(EngineError::InvalidDataDirectory {
                app: DEFAULT_VALQERON_APP,
                env_var: DB_PATH_ENV,
            })
    }
}

// ================ SOCKET PATH ================
/// The socket path is shared knowledge: engine and clients must resolve the
/// same file, so resolution delegates to `valqeron-proto` (flag >
/// `VALQERON_SOCKET` > platform runtime dir + `valqeron.sock`).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct SocketPath(PathBuf);

impl SocketPath {
    /// `VALQERON_SOCKET` > platform runtime dir + `valqeron.sock` (resolved
    /// by `valqeron-proto`, which reads the env var itself).
    pub fn resolve() -> EngineResult<Self> {
        valqeron_proto::resolve_socket_path(None)
            .map(Self)
            .map_err(|e| EngineError::Config(e.to_string()))
    }
}

impl Display for SocketPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

// ================ ENGINE LOG FILE PATH ================
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct LogFilePath(PathBuf);

impl LogFilePath {
    /// `VALQERON_ENGINE_LOG_FILE` (`off`/`false`/`0`/`none` disables) >
    /// `<platform data dir>/engine.log`.
    pub fn resolve() -> EngineResult<Option<Self>> {
        let project_data_dir =
            resolve_default_project_dir().map(|dirs| dirs.data_dir().to_path_buf());

        Self::resolve_with(std::env::var_os(ENGINE_LOG_FILE_ENV), project_data_dir)
    }

    /// Pure resolution core: env value and platform dir are injected so tests
    /// never touch process-global environment state.
    fn resolve_with(
        env_value: Option<OsString>,
        project_data_dir: Option<PathBuf>,
    ) -> EngineResult<Option<Self>> {
        if let Some(value) = env_value {
            if valqeron_common::os_str_is_off(&value) {
                return Ok(None);
            }
            if !value.is_empty() {
                return Ok(Some(Self(PathBuf::from(value))));
            }
            // Set-but-empty falls through to the default, like unset.
        }

        project_data_dir
            .map(|dir| Some(Self(dir.join(DEFAULT_ENGINE_LOG_FILE_NAME))))
            .ok_or(EngineError::InvalidDataDirectory {
                app: DEFAULT_VALQERON_APP,
                env_var: ENGINE_LOG_FILE_ENV,
            })
    }
}

// ================ SYNC SETTINGS ================
/// Resolved configuration of one sync source (env-namespaced per source).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncSettings {
    /// Market-local time of day the source runs at.
    pub at: NaiveTime,
    pub recurrence: Recurrence,
    /// Base of the terminal-failure cooldown, in seconds.
    pub cooldown_secs: u32,
    /// Bound on unattended sequential catch-up, in business days.
    pub max_backfill_days: u32,
}

// ================ ENGINE CONFIGURATION ================
#[derive(Clone, Debug)]
pub struct EngineConfig {
    db_path: DatabasePath,
    socket_path: SocketPath,
    /// `None` = file logging disabled.
    log_file: Option<LogFilePath>,
    durable: bool,
    maintenance_interval: Duration,
    heartbeat_interval: Duration,
    /// `None` = CVM sync disabled.
    cvm_sync: Option<SyncSettings>,
}

impl EngineConfig {
    /// Resolve the full run configuration from the environment: `VALQERON_*`
    /// variables (set in the service definition) win over platform defaults.
    pub fn resolve() -> EngineResult<Self> {
        Ok(Self {
            db_path: DatabasePath::resolve()?,
            socket_path: SocketPath::resolve()?,
            log_file: LogFilePath::resolve()?,
            durable: durable_from(std::env::var_os(ENGINE_DURABLE_ENV)),
            maintenance_interval: Duration::from_secs(interval_secs_from(
                ENGINE_MAINTENANCE_INTERVAL_ENV,
                std::env::var_os(ENGINE_MAINTENANCE_INTERVAL_ENV),
                DEFAULT_MAINTENANCE_INTERVAL_SECS,
            )?),
            heartbeat_interval: Duration::from_secs(interval_secs_from(
                ENGINE_HEARTBEAT_INTERVAL_ENV,
                std::env::var_os(ENGINE_HEARTBEAT_INTERVAL_ENV),
                DEFAULT_HEARTBEAT_INTERVAL_SECS,
            )?),
            cvm_sync: cvm_sync_from(
                std::env::var_os(ENGINE_SYNC_CVM_ENV),
                std::env::var_os(ENGINE_SYNC_CVM_AT_ENV),
                std::env::var_os(ENGINE_SYNC_CVM_SCHEDULE_ENV),
                std::env::var_os(ENGINE_SYNC_CVM_COOLDOWN_ENV),
                std::env::var_os(ENGINE_SYNC_MAX_BACKFILL_DAYS_ENV),
            )?,
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path.0
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path.0
    }

    /// Single-instance lock file: `.lock` appended to the full database file
    /// name (`valqeron.db` → `valqeron.db.lock`).
    pub fn lock_path(&self) -> PathBuf {
        let mut os = self.db_path.0.as_os_str().to_os_string();
        os.push(".lock");
        PathBuf::from(os)
    }

    pub fn log_file(&self) -> Option<&Path> {
        self.log_file.as_ref().map(|p| p.0.as_path())
    }

    pub fn synchronous(&self) -> Synchronous {
        if self.durable {
            Synchronous::Full
        } else {
            Synchronous::Normal
        }
    }

    pub fn durability_label(&self) -> &'static str {
        if self.durable { "full" } else { "normal" }
    }

    pub fn maintenance_interval(&self) -> Duration {
        self.maintenance_interval
    }

    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// CVM sync configuration; `None` when disabled via env.
    pub fn cvm_sync(&self) -> Option<SyncSettings> {
        self.cvm_sync
    }
}

// ================ ENGINE LOCK ================
#[derive(Debug)]
struct EngineLock {
    file: File,
    path: PathBuf,
}

impl EngineLock {
    pub fn acquire(db_path: PathBuf, lock_path: PathBuf) -> EngineResult<Self> {
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
                let pid = Self::read_lock_pid(&lock_path).unwrap_or_else(|| "unknown".to_string());
                tracing::warn!(
                    db_path = %db_path.display(),
                    lock_path = %lock_path.display(),
                    holder_pid = %pid,
                    "engine lock contended: another instance appears to be running"
                );
                return Err(EngineError::AlreadyRunning { db_path, pid });
            }
            Err(TryLockError::Error(e)) => {
                return Err(EngineError::Io(format!(
                    "locking {}: {e}",
                    lock_path.display()
                )));
            }
        }

        // We own the lock: record our PID for diagnostics.
        Self::write_pid(&mut file)
            .map_err(|e| EngineError::Io(format!("writing pid to {}: {e}", lock_path.display())))?;

        tracing::debug!(lock_path = %lock_path.display(), pid = std::process::id(), "pid written to lock file");

        Ok(Self {
            file,
            path: lock_path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn write_pid(file: &mut File) -> std::io::Result<()> {
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(std::process::id().to_string().as_bytes())?;
        file.flush()
    }

    fn read_lock_pid(lock_path: &Path) -> Option<String> {
        let contents = std::fs::read_to_string(lock_path).ok()?;
        let pid = contents.trim();
        if pid.is_empty() {
            None
        } else {
            Some(pid.to_string())
        }
    }
}

impl Drop for EngineLock {
    fn drop(&mut self) {
        // Remove the file first, then let the kernel lock release when the handle closes: a starter
        // racing the removal still cannot acquire the flock until this handle is gone, and fresh
        // starts simply create a new inode.
        let _ = std::fs::remove_file(&self.path);
        let _ = self.file.unlock();

        tracing::debug!(
            lock_path = %self.path.display(),
            "single-instance lock released"
        );
    }
}

// ================ PHASED BOOT SEQUENCE ================
/// Phase 0: nothing acquired yet.
struct Bootstrap<'c> {
    config: &'c EngineConfig,
}

impl<'c> Bootstrap<'c> {
    /// Start a boot sequence; logs the startup banner.
    fn new(config: &'c EngineConfig) -> Self {
        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            db_path = %config.db_path().display(),
            socket_path = %config.socket_path().display(),
            lock_path = %config.lock_path().display(),
            maintenance_interval_secs = config.maintenance_interval().as_secs(),
            heartbeat_interval_secs = config.heartbeat_interval().as_secs(),
            durability = config.durability_label(),
            "valqeron-engine starting"
        );
        Self { config }
    }

    /// Acquire the exclusive single-instance lock (`<db>.lock`).
    fn acquire_lock(self) -> EngineResult<Locked<'c>> {
        // Ensure the database parent directory exists.
        if let Some(parent) = self.config.db_path().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| EngineError::Io(format!("creating {}: {e}", parent.display())))?;
        }

        let lock =
            EngineLock::acquire(self.config.db_path().to_path_buf(), self.config.lock_path())?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "engine_start",
            lock_path = %lock.path().display(),
            "single-instance lock acquired"
        );

        Ok(Locked {
            config: self.config,
            lock,
        })
    }
}

/// Phase 1: single-instance lock held.
struct Locked<'c> {
    config: &'c EngineConfig,
    lock: EngineLock,
}

impl<'c> Locked<'c> {
    /// Create the 0700 socket directory and unlink any leftover socket file.
    /// Safe because we hold the exclusive lock: no live engine can be serving on it.
    fn prepare_socket(self) -> EngineResult<SocketReady<'c>> {
        Self::ensure_socket_dir(self.config.socket_path())?;
        Self::remove_stale_socket(self.config.socket_path())?;

        tracing::info!(
            socket_dir = ?self.config.socket_path().parent(),
            "socket directory prepared and restricted to 0700"
        );

        Ok(SocketReady {
            config: self.config,
            lock: self.lock,
        })
    }

    /// Create the socket's parent directory and restrict it to `0700`.
    /// Directory permissions are the primary local access control for the engine socket.
    fn ensure_socket_dir(socket: &Path) -> EngineResult<()> {
        use std::os::unix::fs::PermissionsExt;
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| EngineError::Io(format!("creating {}: {e}", parent.display())))?;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                .map_err(|e| EngineError::Io(format!("restricting {}: {e}", parent.display())))?;
        }
        Ok(())
    }

    /// Unlink a leftover socket file. Safe because the caller holds the
    /// exclusive engine lock: no live engine can be serving on it.
    fn remove_stale_socket(path: &Path) -> EngineResult<()> {
        match std::fs::remove_file(path) {
            Ok(()) => {
                tracing::info!(socket = %path.display(), "removed stale socket file");
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(EngineError::Io(format!(
                "removing stale socket {}: {e}",
                path.display()
            ))),
        }
    }
}

/// Phase 2: socket directory prepared, stale socket removed (under the lock).
struct SocketReady<'c> {
    config: &'c EngineConfig,
    lock: EngineLock,
}

impl<'c> SocketReady<'c> {
    /// Open the database (migrations run here, before any reader exists)
    /// already wrapped in the lane-bounded storage facade —
    /// [`AsyncStorage::open`] is the engine's single construction path, so
    /// an ungoverned engine handle never exists in this module. Lane sizing
    /// is derived from the actual reader pool: permits ≡ connections by
    /// construction.
    fn open_database(self) -> EngineResult<DatabaseOpen<'c>> {
        let db_config = DatabaseConfig {
            synchronous: self.config.synchronous(),
            ..DatabaseConfig::default()
        };

        let storage = AsyncStorage::open(self.config.db_path(), db_config)?;

        tracing::info!(
            db_path = %self.config.db_path().display(),
            reader_pool_size = storage.reader_pool_size(),
            "database open; migrations applied"
        );

        Ok(DatabaseOpen {
            config: self.config,
            lock: self.lock,
            storage,
        })
    }
}

/// Phase 3: database open, migrations applied, storage facade constructed.
struct DatabaseOpen<'c> {
    config: &'c EngineConfig,
    lock: EngineLock,
    storage: AsyncStorage,
}

impl<'c> DatabaseOpen<'c> {
    /// Bind the Unix listener synchronously and restrict the socket file.
    /// From this point connections queue in the backlog until serving starts;
    /// the socket's existence proves the database is open and migrated.
    fn bind_socket(self) -> EngineResult<Bound<'c>> {
        let socket_path = self.config.socket_path();
        let listener = StdUnixListener::bind(socket_path).map_err(|e| {
            EngineError::Io(format!("binding socket {}: {e}", socket_path.display()))
        })?;
        listener.set_nonblocking(true).map_err(|e| {
            EngineError::Io(format!(
                "setting {} nonblocking: {e}",
                socket_path.display()
            ))
        })?;
        Self::restrict_socket_file(socket_path)?;

        tracing::info!(
            socket_path = %socket_path.display(),
            "unix socket bound, set non-blocking, and restricted to 0600"
        );

        Ok(Bound {
            config: self.config,
            lock: self.lock,
            storage: self.storage,
            listener,
        })
    }

    /// Tighten the bound socket file itself (the 0700 parent directory is the primary control; this is
    /// a belt-and-braces).
    fn restrict_socket_file(path: &Path) -> EngineResult<()> {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| EngineError::Io(format!("restricting {}: {e}", path.display())))
    }
}

/// Phase 4: listener bound and restricted — the readiness point of the boot sequence. The listener
/// is nonblocking, ready for tokio registration.
struct Bound<'c> {
    config: &'c EngineConfig,
    lock: EngineLock,
    storage: AsyncStorage,
    listener: StdUnixListener,
}

impl<'c> Bound<'c> {
    /// Build the multi-thread tokio runtime: named threads and a bounded
    /// blocking pool sized to the opened storage lanes.
    fn build_runtime(self) -> EngineResult<ValqeronEngine<'c>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("valqeron-worker")
            .build()
            .map_err(|e| EngineError::Io(format!("building tokio runtime: {e}")))?;

        tracing::info!(thread_name = "valqeron-worker", "tokio runtime initialized");

        Ok(ValqeronEngine {
            config: self.config,
            lock: self.lock,
            storage: self.storage,
            listener: self.listener,
            runtime,
        })
    }
}

// ================ THE ENGINE ================
/// Phase 5: everything acquired; ready to serve.
pub struct ValqeronEngine<'c> {
    config: &'c EngineConfig,
    lock: EngineLock,
    storage: AsyncStorage,
    listener: StdUnixListener,
    runtime: tokio::runtime::Runtime,
}

impl ValqeronEngine<'_> {
    /// The one public entry point: drive the full boot sequence, serve until
    /// a shutdown signal, then tear down in checkpoint-safe order. The
    /// runtime lifecycle FSM (`lifecycle.rs`) tracks the coarse state:
    /// `Starting` here, `Ready`/`Stopping` inside [`run_loop`], and the
    /// terminal `Stopped`/`Failed` at the end of [`ValqeronEngine::serve`].
    pub fn run(config: &EngineConfig) -> EngineResult<()> {
        let (lifecycle, state) = Lifecycle::new();
        match Self::boot(config) {
            Ok(engine) => engine.serve(&lifecycle, state),
            Err(e) => {
                let _ = lifecycle.transition(LifecycleState::Failed);
                Err(e)
            }
        }
    }

    /// Drive the phased boot chain: lock → socket prep → open (migrations) →
    /// bind → runtime.
    fn boot(config: &EngineConfig) -> EngineResult<ValqeronEngine<'_>> {
        Bootstrap::new(config)
            .acquire_lock()?
            .prepare_socket()?
            .open_database()?
            .bind_socket()?
            .build_runtime()
    }

    /// Serve until shutdown, then tear down in reverse-dependency order:
    /// drain the runtime → unlink the socket → reclaim and drop the engine
    /// (final `wal_checkpoint(TRUNCATE)`) → release the lock last.
    fn serve(
        self,
        lifecycle: &Lifecycle,
        state: watch::Receiver<LifecycleState>,
    ) -> EngineResult<()> {
        let Self {
            config,
            lock,
            storage,
            listener,
            runtime,
        } = self;

        let loop_result = runtime.block_on(run_loop(
            storage.clone(),
            listener,
            config,
            lifecycle,
            state,
        ));

        // Wait (bounded) for any still-running blocking task before the final
        // checkpoint; queued-but-unstarted tasks are dropped.
        runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);

        let _ = std::fs::remove_file(config.socket_path());

        match storage.into_engine() {
            Ok(engine) => {
                // Drop = PRAGMA optimize + wal_checkpoint(TRUNCATE).
                drop(engine);
                tracing::info!("final WAL checkpoint complete");
            }
            Err(_still_shared) => {
                tracing::warn!(
                    "a storage task still holds the engine; skipping the final checkpoint"
                );
            }
        }

        drop(lock);

        // The terminal transition lands only now, after the checkpoint and
        // lock release: "stopped"/"failed" factually means everything is
        // drained and released. Legal from Stopping (clean/forced/server
        // death) and from Starting (run_loop failed before readiness).
        let terminal = match &loop_result {
            Ok(()) => LifecycleState::Stopped,
            Err(_) => LifecycleState::Failed,
        };
        let _ = lifecycle.transition(terminal);

        match &loop_result {
            Ok(()) => tracing::info!(
                target: "valqeron::audit",
                operation = "engine_stop",
                "engine stopped cleanly"
            ),
            Err(e) => tracing::warn!(
                target: "valqeron::audit",
                operation = "engine_stop",
                error = %e,
                "engine stopped with an error"
            ),
        }
        loop_result
    }
}

// ================ SIGNALS ================
/// The two shutdown signals, installed once and polled across both the serve
/// and the drain phases (a second delivery during the drain forces exit).
struct Signals {
    sigterm: tokio::signal::unix::Signal,
    sigint: tokio::signal::unix::Signal,
}

impl Signals {
    fn install() -> EngineResult<Self> {
        Ok(Self {
            sigterm: signal(SignalKind::terminate())
                .map_err(|e| EngineError::Io(format!("installing SIGTERM handler: {e}")))?,
            sigint: signal(SignalKind::interrupt())
                .map_err(|e| EngineError::Io(format!("installing SIGINT handler: {e}")))?,
        })
    }

    /// Resolves on the next delivery of either signal, naming it.
    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.sigterm.recv() => "SIGTERM",
            _ = self.sigint.recv() => "SIGINT",
        }
    }
}

// ================ GRPC SERVER ================
/// The spawned tonic server plus its graceful-shutdown trigger.
struct GrpcServer {
    join: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl GrpcServer {
    /// Register the bootstrap-bound listener with this runtime's reactor and
    /// spawn the tonic server on it. Connections that queued in the backlog
    /// while the runtime was being built are served as soon as tonic starts.
    fn spawn(
        listener: StdUnixListener,
        storage: &AsyncStorage,
        config: &EngineConfig,
        started: Instant,
    ) -> EngineResult<Self> {
        let listener = UnixListener::from_std(listener).map_err(|e| {
            EngineError::Io(format!(
                "registering socket {} with the runtime: {e}",
                config.socket_path().display()
            ))
        })?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let issuer_service = RpcIssuerServiceServer::new(IssuerGrpc::new(storage.clone()));
        let admin_service = RpcAdminServiceServer::new(AdminGrpc::new(
            config.db_path().display().to_string(),
            started,
        ));

        let join = tokio::spawn(
            Server::builder()
                .add_service(issuer_service)
                .add_service(admin_service)
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                    let _ = shutdown_rx.await;
                }),
        );

        tracing::info!(
            target: "valqeron::audit",
            operation = "grpc_listen",
            socket = %config.socket_path().display(),
            "gRPC server listening"
        );

        Ok(Self {
            join,
            shutdown: Some(shutdown_tx),
        })
    }

    /// Stop accepting connections and begin draining in-flight RPCs.
    fn begin_shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

// ================ BACKGROUND TASK REGISTRATIONS ================
/// Compose the engine's background work from the task implementations in
/// `crate::jobs` — the manager itself knows nothing about them.
fn background_tasks(
    config: &EngineConfig,
    started: Instant,
    state: watch::Receiver<LifecycleState>,
) -> BackgroundTasksBuilder {
    let builder = BackgroundTasksManager::builder();
    let builder = crate::jobs::system::register(builder, config, started, state);
    crate::jobs::cvm::register(builder, config)
}

// ================ SERVE LOOP ================
/// Serve until a shutdown signal arrives or the server dies on its own, then
/// hand off to the ordered drain. The mechanics live in [`Signals`],
/// [`GrpcServer`], [`background_tasks`], and [`graceful_shutdown`]; this
/// function is only the orchestration order.
async fn run_loop(
    storage: AsyncStorage,
    listener: StdUnixListener,
    config: &EngineConfig,
    lifecycle: &Lifecycle,
    state: watch::Receiver<LifecycleState>,
) -> EngineResult<()> {
    let mut signals = Signals::install()?;
    let started = Instant::now();
    let mut server = GrpcServer::spawn(listener, &storage, config, started)?;

    // The deterministic readiness point: lock held, migrations applied,
    // socket bound, server task serving. The Starting → Ready transition
    // fires sd_notify READY=1 — under a systemd Type=notify unit this
    // completes startup; everywhere else it is a no-op.
    tracing::info!(
        target: "valqeron::audit",
        operation = "engine_ready",
        socket = %config.socket_path().display(),
        "engine ready"
    );
    let _ = lifecycle.transition(LifecycleState::Ready);

    // Background work (crash recovery, periodic schedules, the dispatcher)
    // runs on its own tasks; the select below stays a pure watcher.
    let tasks = background_tasks(config, started, state)
        .start(storage.clone())
        .await;

    let outcome: EngineResult<&'static str> = tokio::select! {
        reason = signals.recv() => Ok(reason),
        joined = &mut server.join => Err(server_exit_error(joined)),
    };

    // Every continuation from here is a shutdown, clean or not; the
    // Ready → Stopping transition fires sd_notify STOPPING=1.
    let _ = lifecycle.transition(LifecycleState::Stopping);

    match outcome {
        Ok(reason) => {
            tracing::info!(
                signal = reason,
                "shutdown requested; draining in-flight RPCs and background work"
            );
            graceful_shutdown(signals, server, tasks, &storage).await
        }
        Err(e) => {
            // The server died on its own; stop background work and bail.
            let _ = tasks.drain(Duration::from_secs(1)).await;
            storage.close();
            Err(e)
        }
    }
}

/// The ordered drain: stop accepting RPCs and drain in-flight ones (a second
/// signal forces an immediate non-zero exit), stop the background tasks
/// (waiting for bodies still in flight), then reject new storage work and
/// wait for the remaining closures — the exact reverse-dependency order the
/// final WAL checkpoint in [`ValqeronEngine::serve`] requires.
async fn graceful_shutdown(
    mut signals: Signals,
    mut server: GrpcServer,
    tasks: BackgroundTasksManager,
    storage: &AsyncStorage,
) -> EngineResult<()> {
    server.begin_shutdown();
    tokio::select! {
        _ = signals.recv() => {
            storage.close();
            return Err(EngineError::ForcedShutdown);
        }
        joined = &mut server.join => {
            if let Err(e) = server_join_outcome(joined) {
                tracing::warn!(error = %e, "gRPC server ended with an error during drain");
            }
        }
        _ = tokio::time::sleep(DRAIN_TIMEOUT) => {
            tracing::warn!(
                drain_timeout_secs = DRAIN_TIMEOUT.as_secs(),
                "gRPC server did not drain within the deadline; aborting it"
            );
            server.join.abort();
        }
    }

    if !tasks.drain(DRAIN_TIMEOUT).await {
        tracing::warn!(
            drain_timeout_secs = DRAIN_TIMEOUT.as_secs(),
            "background tasks did not finish within the drain deadline"
        );
    }
    storage.close();
    if !storage.wait_idle(DRAIN_TIMEOUT).await {
        tracing::warn!(
            drain_timeout_secs = DRAIN_TIMEOUT.as_secs(),
            "storage closures still in flight after the drain deadline"
        );
    }

    Ok(())
}

fn resolve_default_project_dir() -> Option<ProjectDirs> {
    ProjectDirs::from(
        DEFAULT_VALQERON_QUALIFIER,
        DEFAULT_VALQERON_ORGANIZATION,
        DEFAULT_VALQERON_APP,
    )
}

/// Truthy [`ENGINE_DURABLE_ENV`] enables strict durability; unset, empty, or
/// an off-value (`off`/`false`/`0`/`none`) keeps the relaxed default.
fn durable_from(env_value: Option<OsString>) -> bool {
    env_value
        .map(|value| !value.is_empty() && !valqeron_common::os_str_is_off(&value))
        .unwrap_or(false)
}

/// Whole-second interval from an environment variable. Unset or empty means
/// the default; anything else must parse as `u64`, otherwise the engine
/// refuses to start (misconfiguration must not silently become a default).
fn interval_secs_from(
    var: &'static str,
    env_value: Option<OsString>,
    default_secs: u64,
) -> EngineResult<u64> {
    let Some(value) = env_value else {
        return Ok(default_secs);
    };
    let text = value.to_str().ok_or_else(|| {
        EngineError::Config(format!("{var} must be whole seconds (not valid UTF-8)"))
    })?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(default_secs);
    }
    trimmed.parse::<u64>().map_err(|e| {
        EngineError::Config(format!("{var} must be whole seconds, got {trimmed:?}: {e}"))
    })
}

/// Extract trimmed UTF-8 text from an env value: `Ok(None)` for unset or
/// empty (use the default), `Err` for non-UTF-8.
fn env_text(var: &'static str, env_value: Option<OsString>) -> EngineResult<Option<String>> {
    let Some(value) = env_value else {
        return Ok(None);
    };
    let text = value
        .to_str()
        .ok_or_else(|| EngineError::Config(format!("{var} is not valid UTF-8")))?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed.to_owned()))
}

/// A sync source is enabled by default; unset or empty keeps it enabled,
/// an off-value (`off`/`false`/`0`/`none`) disables it.
fn sync_enabled_from(env_value: Option<OsString>) -> bool {
    env_value
        .map(|value| value.is_empty() || !valqeron_common::os_str_is_off(&value))
        .unwrap_or(true)
}

/// Market-local `HH:MM` from an environment variable; unset or empty means
/// [`DEFAULT_SYNC_AT`]. Anything else must parse, otherwise the engine
/// refuses to start.
fn sync_time_from(var: &'static str, env_value: Option<OsString>) -> EngineResult<NaiveTime> {
    let Some(text) = env_text(var, env_value)? else {
        let (h, m) = DEFAULT_SYNC_AT;
        return NaiveTime::from_hms_opt(h, m, 0)
            .ok_or_else(|| EngineError::Config(format!("{var} default is invalid")));
    };
    NaiveTime::parse_from_str(&text, "%H:%M")
        .map_err(|e| EngineError::Config(format!("{var} must be HH:MM, got {text:?}: {e}")))
}

/// Recurrence from an environment variable: unset/empty/`daily` →
/// [`Recurrence::Daily`]; `weekly:<mon..sun>` → weekly on that day.
fn recurrence_from(var: &'static str, env_value: Option<OsString>) -> EngineResult<Recurrence> {
    let Some(text) = env_text(var, env_value)? else {
        return Ok(Recurrence::Daily);
    };
    let lowered = text.to_ascii_lowercase();
    if lowered == "daily" {
        return Ok(Recurrence::Daily);
    }
    if let Some(day) = lowered.strip_prefix("weekly:") {
        let on = match day {
            "mon" => Weekday::Mon,
            "tue" => Weekday::Tue,
            "wed" => Weekday::Wed,
            "thu" => Weekday::Thu,
            "fri" => Weekday::Fri,
            "sat" => Weekday::Sat,
            "sun" => Weekday::Sun,
            other => {
                return Err(EngineError::Config(format!(
                    "{var} weekday must be one of mon..sun, got {other:?}"
                )));
            }
        };
        return Ok(Recurrence::Weekly { on });
    }
    Err(EngineError::Config(format!(
        "{var} must be \"daily\" or \"weekly:<mon..sun>\", got {text:?}"
    )))
}

/// `u32` from an environment variable; unset or empty means the default.
fn u32_from(var: &'static str, env_value: Option<OsString>, default: u32) -> EngineResult<u32> {
    let Some(text) = env_text(var, env_value)? else {
        return Ok(default);
    };
    text.parse::<u32>().map_err(|e| {
        EngineError::Config(format!("{var} must be a whole number, got {text:?}: {e}"))
    })
}

/// The CVM sync configuration, or `None` when disabled. Pure so tests never
/// touch process-global environment state.
fn cvm_sync_from(
    enabled: Option<OsString>,
    at: Option<OsString>,
    schedule: Option<OsString>,
    cooldown: Option<OsString>,
    max_backfill: Option<OsString>,
) -> EngineResult<Option<SyncSettings>> {
    if !sync_enabled_from(enabled) {
        return Ok(None);
    }
    Ok(Some(SyncSettings {
        at: sync_time_from(ENGINE_SYNC_CVM_AT_ENV, at)?,
        recurrence: recurrence_from(ENGINE_SYNC_CVM_SCHEDULE_ENV, schedule)?,
        cooldown_secs: u32_from(
            ENGINE_SYNC_CVM_COOLDOWN_ENV,
            cooldown,
            DEFAULT_SYNC_COOLDOWN_SECS,
        )?,
        max_backfill_days: u32_from(
            ENGINE_SYNC_MAX_BACKFILL_DAYS_ENV,
            max_backfill,
            DEFAULT_SYNC_MAX_BACKFILL_DAYS,
        )?,
    }))
}

fn server_exit_error(
    joined: Result<Result<(), tonic::transport::Error>, tokio::task::JoinError>,
) -> EngineError {
    match joined {
        Ok(Ok(())) => EngineError::Io("gRPC server exited unexpectedly".to_string()),
        Ok(Err(e)) => EngineError::Io(format!("gRPC server failed: {e}")),
        Err(e) => EngineError::Io(format!("gRPC server task failed: {e}")),
    }
}

fn server_join_outcome(
    joined: Result<Result<(), tonic::transport::Error>, tokio::task::JoinError>,
) -> Result<(), String> {
    match joined {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(e) => Err(e.to_string()),
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    /// Builds a config directly (bypassing env resolution) to exercise the
    /// derived accessors; the resolution cores are tested separately through
    /// their injected-value helpers.
    fn config_with(db: &str, durable: bool) -> EngineConfig {
        EngineConfig {
            db_path: DatabasePath(PathBuf::from(db)),
            socket_path: SocketPath(PathBuf::from("/tmp/x.sock")),
            log_file: None,
            durable,
            maintenance_interval: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(20),
            cvm_sync: None,
        }
    }

    #[test]
    fn config_exposes_the_derived_values() {
        let config = config_with("/tmp/x.db", true);

        assert_eq!(config.db_path(), Path::new("/tmp/x.db"));
        assert_eq!(config.socket_path(), Path::new("/tmp/x.sock"));
        assert_eq!(config.lock_path(), PathBuf::from("/tmp/x.db.lock"));
        assert_eq!(config.log_file(), None);
        assert_eq!(config.synchronous(), Synchronous::Full);
        assert_eq!(config.durability_label(), "full");
        assert_eq!(config.maintenance_interval(), Duration::from_secs(10));
        assert_eq!(config.heartbeat_interval(), Duration::from_secs(20));
    }

    #[test]
    fn lock_path_appends_to_the_full_file_name() {
        let config = config_with("/data/valqeron.db", false);
        assert_eq!(config.lock_path(), PathBuf::from("/data/valqeron.db.lock"));
    }

    #[test]
    fn default_durability_is_relaxed() {
        let config = config_with("/tmp/y.db", false);
        assert_eq!(config.synchronous(), Synchronous::Normal);
        assert_eq!(config.durability_label(), "normal");
    }

    #[test]
    fn database_path_errors_when_no_env_or_project_dir() {
        let result = DatabasePath::resolve_with(None, None);
        assert!(matches!(
            result,
            Err(EngineError::InvalidDataDirectory {
                app: DEFAULT_VALQERON_APP,
                env_var: DB_PATH_ENV,
            })
        ));
    }

    #[test]
    fn database_path_prefers_the_env_value() {
        let result = DatabasePath::resolve_with(
            Some(OsString::from("/tmp/override.db")),
            Some(PathBuf::from("/data")),
        )
        .unwrap();
        assert_eq!(result.0, PathBuf::from("/tmp/override.db"));
    }

    #[test]
    fn database_path_falls_back_to_the_project_dir() {
        let result = DatabasePath::resolve_with(None, Some(PathBuf::from("/data"))).unwrap();
        assert_eq!(
            result.0,
            PathBuf::from("/data").join(DEFAULT_ENGINE_DB_NAME)
        );
    }

    #[test]
    fn log_file_off_value_disables_logging() {
        for off in ["off", "OFF", "false", "0", "none"] {
            let result =
                LogFilePath::resolve_with(Some(OsString::from(off)), Some(PathBuf::from("/data")))
                    .unwrap();
            assert_eq!(result, None, "{off:?} must disable file logging");
        }
    }

    #[test]
    fn log_file_env_path_wins() {
        let result = LogFilePath::resolve_with(
            Some(OsString::from("/tmp/engine.log")),
            Some(PathBuf::from("/data")),
        )
        .unwrap();
        assert_eq!(result, Some(LogFilePath(PathBuf::from("/tmp/engine.log"))));
    }

    #[test]
    fn log_file_unset_or_empty_uses_the_default_path() {
        let default = Some(LogFilePath(
            PathBuf::from("/data").join(DEFAULT_ENGINE_LOG_FILE_NAME),
        ));
        for env_value in [None, Some(OsString::new())] {
            let result =
                LogFilePath::resolve_with(env_value.clone(), Some(PathBuf::from("/data"))).unwrap();
            assert_eq!(result, default, "{env_value:?} must use the default path");
        }
    }

    #[test]
    fn log_file_errors_when_no_env_or_project_dir() {
        let result = LogFilePath::resolve_with(None, None);
        assert!(matches!(
            result,
            Err(EngineError::InvalidDataDirectory {
                app: DEFAULT_VALQERON_APP,
                env_var: ENGINE_LOG_FILE_ENV,
            })
        ));
    }

    #[test]
    fn durable_requires_a_truthy_value() {
        for truthy in ["1", "true", "yes", "full"] {
            assert!(
                durable_from(Some(OsString::from(truthy))),
                "{truthy:?} must enable durability"
            );
        }
        for falsy in ["off", "false", "0", "none", ""] {
            assert!(
                !durable_from(Some(OsString::from(falsy))),
                "{falsy:?} must keep the relaxed default"
            );
        }
        assert!(!durable_from(None), "unset must keep the relaxed default");
    }

    #[test]
    fn intervals_default_when_unset_or_empty_and_parse_when_set() {
        for env_value in [None, Some(OsString::new()), Some(OsString::from("  "))] {
            assert_eq!(
                interval_secs_from(ENGINE_MAINTENANCE_INTERVAL_ENV, env_value, 3600).unwrap(),
                3600
            );
        }
        assert_eq!(
            interval_secs_from(
                ENGINE_MAINTENANCE_INTERVAL_ENV,
                Some(OsString::from(" 42 ")),
                3600
            )
            .unwrap(),
            42
        );
    }

    #[test]
    fn invalid_intervals_are_config_errors_naming_the_variable() {
        for bad in ["abc", "-1", "1.5", "10s"] {
            let err = interval_secs_from(
                ENGINE_HEARTBEAT_INTERVAL_ENV,
                Some(OsString::from(bad)),
                300,
            )
            .unwrap_err();
            assert_eq!(err.exit_code(), exit_code::CONFIG, "{bad:?} must be CONFIG");
            assert!(
                err.to_string().contains(ENGINE_HEARTBEAT_INTERVAL_ENV),
                "error must name the variable: {err}"
            );
        }
    }

    #[test]
    fn sync_enabled_defaults_on_and_honors_off_values() {
        assert!(sync_enabled_from(None), "unset means enabled");
        assert!(
            sync_enabled_from(Some(OsString::new())),
            "empty means enabled"
        );
        for on in ["on", "1", "true", "yes"] {
            assert!(
                sync_enabled_from(Some(OsString::from(on))),
                "{on:?} must keep the source enabled"
            );
        }
        for off in ["off", "OFF", "false", "0", "none"] {
            assert!(
                !sync_enabled_from(Some(OsString::from(off))),
                "{off:?} must disable the source"
            );
        }
    }

    #[test]
    fn sync_time_defaults_parses_and_rejects_garbage() {
        let default = NaiveTime::from_hms_opt(DEFAULT_SYNC_AT.0, DEFAULT_SYNC_AT.1, 0).unwrap();
        for env_value in [None, Some(OsString::new()), Some(OsString::from("  "))] {
            assert_eq!(
                sync_time_from(ENGINE_SYNC_CVM_AT_ENV, env_value).unwrap(),
                default
            );
        }
        assert_eq!(
            sync_time_from(ENGINE_SYNC_CVM_AT_ENV, Some(OsString::from(" 06:30 "))).unwrap(),
            NaiveTime::from_hms_opt(6, 30, 0).unwrap()
        );
        for bad in ["25:00", "07:60", "seven", "07", "07:00:00extra"] {
            let err =
                sync_time_from(ENGINE_SYNC_CVM_AT_ENV, Some(OsString::from(bad))).unwrap_err();
            assert_eq!(err.exit_code(), exit_code::CONFIG, "{bad:?} must be CONFIG");
            assert!(
                err.to_string().contains(ENGINE_SYNC_CVM_AT_ENV),
                "error must name the variable: {err}"
            );
        }
    }

    #[test]
    fn recurrence_parses_daily_and_weekly_and_rejects_garbage() {
        for env_value in [
            None,
            Some(OsString::new()),
            Some(OsString::from("daily")),
            Some(OsString::from(" DAILY ")),
        ] {
            assert_eq!(
                recurrence_from(ENGINE_SYNC_CVM_SCHEDULE_ENV, env_value).unwrap(),
                Recurrence::Daily
            );
        }
        assert_eq!(
            recurrence_from(
                ENGINE_SYNC_CVM_SCHEDULE_ENV,
                Some(OsString::from("weekly:fri"))
            )
            .unwrap(),
            Recurrence::Weekly { on: Weekday::Fri }
        );
        assert_eq!(
            recurrence_from(
                ENGINE_SYNC_CVM_SCHEDULE_ENV,
                Some(OsString::from("WEEKLY:Mon"))
            )
            .unwrap(),
            Recurrence::Weekly { on: Weekday::Mon }
        );
        for bad in ["monthly", "weekly", "weekly:funday", "daily:mon"] {
            let err = recurrence_from(ENGINE_SYNC_CVM_SCHEDULE_ENV, Some(OsString::from(bad)))
                .unwrap_err();
            assert_eq!(err.exit_code(), exit_code::CONFIG, "{bad:?} must be CONFIG");
            assert!(
                err.to_string().contains(ENGINE_SYNC_CVM_SCHEDULE_ENV),
                "error must name the variable: {err}"
            );
        }
    }

    #[test]
    fn u32_values_default_and_reject_garbage() {
        for env_value in [None, Some(OsString::new())] {
            assert_eq!(
                u32_from(ENGINE_SYNC_CVM_COOLDOWN_ENV, env_value, 300).unwrap(),
                300
            );
        }
        assert_eq!(
            u32_from(
                ENGINE_SYNC_CVM_COOLDOWN_ENV,
                Some(OsString::from(" 42 ")),
                300
            )
            .unwrap(),
            42
        );
        for bad in ["abc", "-1", "1.5"] {
            let err =
                u32_from(ENGINE_SYNC_CVM_COOLDOWN_ENV, Some(OsString::from(bad)), 300).unwrap_err();
            assert_eq!(err.exit_code(), exit_code::CONFIG, "{bad:?} must be CONFIG");
        }
    }

    #[test]
    fn cvm_sync_resolves_defaults_when_fully_unset() {
        let settings = cvm_sync_from(None, None, None, None, None)
            .unwrap()
            .expect("enabled by default");
        assert_eq!(
            settings.at,
            NaiveTime::from_hms_opt(DEFAULT_SYNC_AT.0, DEFAULT_SYNC_AT.1, 0).unwrap()
        );
        assert_eq!(settings.recurrence, Recurrence::Daily);
        assert_eq!(settings.cooldown_secs, DEFAULT_SYNC_COOLDOWN_SECS);
        assert_eq!(settings.max_backfill_days, DEFAULT_SYNC_MAX_BACKFILL_DAYS);
    }

    #[test]
    fn cvm_sync_disabled_short_circuits_before_validating_the_rest() {
        // With the source off, even invalid sibling values must not stop
        // the engine.
        let resolved = cvm_sync_from(
            Some(OsString::from("off")),
            Some(OsString::from("garbage")),
            Some(OsString::from("garbage")),
            Some(OsString::from("garbage")),
            Some(OsString::from("garbage")),
        );
        assert!(matches!(resolved, Ok(None)));
    }

    #[test]
    fn cvm_sync_invalid_values_refuse_to_start() {
        let err = cvm_sync_from(None, Some(OsString::from("nope")), None, None, None).unwrap_err();
        assert_eq!(err.exit_code(), exit_code::CONFIG);
    }

    #[test]
    fn exit_codes_map_per_error_class() {
        assert_eq!(
            EngineError::Config("bad".into()).exit_code(),
            exit_code::CONFIG
        );
        assert_eq!(
            EngineError::InvalidDataDirectory {
                app: DEFAULT_VALQERON_APP,
                env_var: DB_PATH_ENV
            }
            .exit_code(),
            exit_code::CONFIG
        );
        assert_eq!(
            EngineError::Io("boom".into()).exit_code(),
            exit_code::RUNTIME
        );
        assert_eq!(EngineError::ForcedShutdown.exit_code(), exit_code::RUNTIME);
    }

    #[test]
    fn already_running_maps_to_its_dedicated_exit_code_and_names_the_holder() {
        let err = EngineError::AlreadyRunning {
            db_path: PathBuf::from("/tmp/x.db"),
            pid: "123".to_string(),
        };
        assert_eq!(err.exit_code(), exit_code::ALREADY_RUNNING);
        let msg = err.to_string();
        assert!(msg.contains("/tmp/x.db"), "message names the db: {msg}");
        assert!(msg.contains("123"), "message names the pid: {msg}");
    }
}

#[cfg(test)]
mod lock_tests {
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
        let lock = EngineLock::acquire(db.clone(), lock_path.clone()).unwrap();
        let pid = EngineLock::read_lock_pid(&lock_path).expect("pid recorded");
        assert_eq!(pid, std::process::id().to_string());
        drop(lock);
    }

    #[test]
    fn second_acquire_fails_with_already_running() {
        let (_dir, db, lock_path) = temp_paths();
        let _held = EngineLock::acquire(db.clone(), lock_path.clone()).unwrap();

        let err = EngineLock::acquire(db.clone(), lock_path).unwrap_err();
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
        let lock = EngineLock::acquire(db.clone(), lock_path.clone()).unwrap();
        drop(lock);
        assert!(!lock_path.exists(), "lock file removed on clean release");

        // Reacquire works immediately.
        let again = EngineLock::acquire(db.clone(), lock_path.clone());
        assert!(again.is_ok(), "lock must be reacquirable after release");
    }

    #[test]
    fn stale_lock_file_with_dead_content_does_not_block_acquisition() {
        let (_dir, db, lock_path) = temp_paths();
        // Simulate SIGKILL residue: a file exists but nobody holds the flock.
        std::fs::write(&lock_path, "99999999").unwrap();

        let lock = EngineLock::acquire(db.clone(), lock_path.clone()).unwrap();
        let pid = EngineLock::read_lock_pid(&lock_path).expect("pid overwritten");
        assert_eq!(pid, std::process::id().to_string());
        drop(lock);
    }
}

#[cfg(test)]
mod boot_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Builds a config pointing entirely inside `dir`, bypassing `resolve()`
    /// (and therefore env vars / OS project-dir lookups) so each test is
    /// fully isolated and safe to run in parallel.
    fn test_config(dir: &Path) -> EngineConfig {
        EngineConfig {
            db_path: DatabasePath(dir.join("test.db")),
            socket_path: SocketPath(dir.join("run").join("test.sock")),
            log_file: None,
            durable: false,
            maintenance_interval: Duration::from_secs(3600),
            heartbeat_interval: Duration::from_secs(3600),
            cvm_sync: None,
        }
    }

    #[test]
    fn boot_chain_acquires_resources_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        let locked = Bootstrap::new(&config).acquire_lock().unwrap();
        assert!(config.lock_path().exists(), "lock file after acquire_lock");
        assert!(!config.db_path().exists(), "no db before open_database");

        let engine = locked
            .prepare_socket()
            .unwrap()
            .open_database()
            .unwrap()
            .bind_socket()
            .unwrap()
            .build_runtime()
            .unwrap();
        assert!(config.db_path().exists(), "db exists after open_database");
        assert!(
            config.socket_path().exists(),
            "socket exists after bind_socket"
        );

        drop(engine);
        assert!(
            !config.lock_path().exists(),
            "dropping the boot chain releases the lock"
        );
    }

    #[test]
    fn socket_dir_and_file_have_restricted_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        let engine = ValqeronEngine::boot(&config).unwrap();

        let dir_mode = std::fs::metadata(config.socket_path().parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700, "socket parent dir should be 0700");

        let file_mode = std::fs::metadata(config.socket_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600, "socket file should be 0600");

        drop(engine);
    }

    #[test]
    fn second_boot_against_same_db_fails_while_first_is_running() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        let _first = ValqeronEngine::boot(&config).expect("first boot should succeed");
        let second = ValqeronEngine::boot(&config);

        assert!(matches!(second, Err(EngineError::AlreadyRunning { .. })));
    }

    #[test]
    fn boot_succeeds_again_after_previous_engine_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        let first = ValqeronEngine::boot(&config).expect("first boot should succeed");
        drop(first);

        let second = ValqeronEngine::boot(&config);
        assert!(
            second.is_ok(),
            "boot should succeed again once the lock is released"
        );
    }

    #[test]
    fn stale_socket_file_does_not_block_a_fresh_boot() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        // Simulate a leftover socket file from a crashed engine (no lock held).
        std::fs::create_dir_all(config.socket_path().parent().unwrap()).unwrap();
        std::fs::write(config.socket_path(), b"not a real socket").unwrap();

        let engine = ValqeronEngine::boot(&config);
        assert!(
            engine.is_ok(),
            "a stale socket file must not block a fresh boot"
        );
    }

    #[test]
    fn missing_parent_directories_are_created() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("does").join("not").join("exist");
        let config = EngineConfig {
            db_path: DatabasePath(nested.join("test.db")),
            socket_path: SocketPath(nested.join("run").join("test.sock")),
            log_file: None,
            durable: false,
            maintenance_interval: Duration::from_secs(3600),
            heartbeat_interval: Duration::from_secs(3600),
            cvm_sync: None,
        };

        let engine = ValqeronEngine::boot(&config);
        assert!(
            engine.is_ok(),
            "boot should create missing db and socket parent directories"
        );
    }
}
