# Valqeron Database Layer — Connections, Locks & Sharing

Status: **implemented**. This describes the system as built, not a plan.

Scope: the SQLite connection layer in `valqeron-infrastructure`
(`crates/infrastructure/src/sqlite/database.rs` — one module holding connections, pool,
guards, pragmas, and dry-run) — how connections are opened, pooled, shared,
guarded, and shut down. The gRPC/async edge that sits above it is covered by
[engine.md](engine.md); this document is the layer underneath.

## One mode: file-backed WAL

There is exactly **one** connection mode. `Database` (`sqlite/database.rs`) always opens a
file-backed SQLite database in WAL journal mode. There is no in-memory variant — tests get the
same topology through a temporary directory (`Database::open_temp()`, see
[Testing](#testing)), so what tests exercise is byte-for-byte what production runs.

```
Database
├── writer   Arc<Mutex<Connection>>      1 × read/write   (WAL writer)
└── readers  Arc<ReaderPool>             N × read-only    (WaitPool<Connection>)
```

- **1 writer**: a single read/write connection behind a `Mutex`. All writes in the process
  serialize on this mutex, which is what lets SQLite's own locking never see contention from
  within one process (`SQLITE_BUSY` between our own connections is designed out, not retried
  around).
- **N readers**: `reader_pool_size` read-only connections in a `Condvar`-based wait pool.
  Reads run concurrently with each other **and** with the writer — WAL guarantees readers a
  consistent snapshot and never blocks them on the writer.

Everything is shared by `Arc`: `Database::handle()` hands out cloneable `DbHandle`s that hold
`Arc` clones of the writer mutex and the reader pool. Repositories own handles, not
connections; dropping the `Database` struct is independent of handles still in flight (the
`Arc`s keep connections alive), but the closing checkpoint runs in `Database::drop`.

## Opening sequence

`Database::open_with_config(path, DatabaseConfig)`:

1. Reject `reader_pool_size < 1` (`SqliteError::InvalidPoolSize`).
2. Open the **writer** (`SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE`), apply writer pragmas.
3. Run **embedded migrations** on the writer (see [Migrations](#migrations)).
4. Open each **reader** (`SQLITE_OPEN_READ_ONLY`), apply reader pragmas.

Order matters: migrations run before any reader opens, so a reader never observes a
half-migrated schema, and a brand-new database file exists (created by the writer) before the
read-only opens would otherwise fail.

Two `Database` instances on the same file are legal (WAL supports multiple connections across
processes); the engine's single-instance lock (below) is what makes this not happen in
production.

## Connection roles & pragmas

The pragma layer knows two roles — invalid combinations are unrepresentable:

| Pragma | Writer | Reader | Why |
|---|---|---|---|
| open flags | `READ_WRITE \| CREATE` | `READ_ONLY` | readers physically cannot write |
| `journal_mode` | set to `WAL` | read (a DB-file property; read-only conns can't set it) | concurrent readers + one writer |
| `query_only` | — | `ON` | second, SQLite-level belt on top of the read-only open flags; a write on a reader fails with `SQLITE_READONLY` |
| `synchronous` | `NORMAL` (default) or `FULL` | same | `NORMAL` is safe in WAL (no corruption, bounded loss on power cut); a truthy `VALQERON_ENGINE_DURABLE` flips to `FULL` |
| `foreign_keys` | `ON` | `ON` | FK enforcement is per-connection in SQLite |
| `busy_timeout` | 5 s (default) | 5 s | see [Cross-process contention](#cross-process-contention) |
| `cache_size` | −64 000 (64 000 KiB ≈ 64 MB) | same | page cache per connection |
| `temp_store` | `MEMORY` | same | temp b-trees off disk |
| `mmap_size` | 256 MiB | same | read via page-cache mapping |
| `wal_autocheckpoint` | 1000 pages | same | background WAL containment between maintenance runs |
| statement cache | 64 prepared statements | same | rusqlite `prepare_cached` backing |

## Locks — the complete inventory

Four locking layers, from innermost to outermost:

1. **Writer mutex** (`Mutex<Connection>`). One writer at
   a time in-process. `lock_writer` recovers a **poisoned** mutex (a thread panicked
   mid-write): it takes the guard via `into_inner`, and if the connection is not in
   autocommit — i.e. the panicking thread stranded an open transaction — it forces `ROLLBACK`
   before handing the connection out. A panic can therefore never leak a half-open transaction
   into the next writer (unit-tested in `poisoned_writer_with_open_transaction_is_healed_on_next_write`).

2. **Reader pool** (`WaitPool<Connection>`): a `Mutex<Vec<Connection>>`
   plus a `Condvar`.
   - `take()` pops a free connection (LIFO — the most recently used connection keeps the
     warmest cache) or parks on the `Condvar` until `put()` returns one; `put()` pushes and
     `notify_one`s.
   - Checkout is wrapped in `PooledReader`, which **checks the connection back in on `Drop`** —
     a leaked guard is the only way to shrink the pool, and a checkout that has waited more
     than 5 s logs a warning naming that exact suspicion.
   - The pool's poisoned-mutex recovery is plain `into_inner` (a `Vec` cannot be left in a
     broken state the way a connection can).
   - The `Mutex`/`Condvar` here (and on the writer) are swapped for **loom**'s versions under
     `#[cfg(loom)]`; `just test-loom` model-checks that a pooled item is
     never lost or duplicated under contention and that a blocked `take()` is always woken by a
     later `put()`.

3. **SQLite file locks (WAL)**. Within the process the two layers above make SQLite's locking
   invisible; they still matter across processes — see
   [Cross-process contention](#cross-process-contention).

4. **Engine single-instance lock** (`EngineLock` in `crates/engine/src/engine.rs`, detailed in
   [engine.md](engine.md)). A kernel advisory lock (`File::try_lock`) on `<db>.lock` next to
   the database file guarantees at most one engine per database. It is *not* part of this
   layer, but it is the reason the layer may assume exclusive ownership in production
   (migrations, checkpoints, `Drop` behavior).

**Lock ordering / deadlock freedom:** a `read()` touches only the pool; a `write()` touches
only the writer mutex; nothing acquires both at once. The one deliberate exception is
`dry_run`, which holds the writer mutex for its whole scope — its reads are answered by the
writer connection itself, never the pool, so it cannot form a cycle (nesting `dry_run` *would*
self-deadlock on the non-reentrant writer mutex and is rejected by a `debug_assert`).

## Handles and guards

Repositories never see `rusqlite::Connection` directly; they go through
`DbHandle` and the `Db` trait:

```rust
pub(crate) enum DbHandle {
    Live { writer: SharedConnection, readers: Arc<ReaderPool> },
    DryRun,
}

pub(crate) trait Db {
    fn write(&self) -> WriteGuard<'_>;  // locks the writer mutex
    fn read(&self) -> ReadGuard<'_>;    // checks a reader out of the pool
}
```

The guards deref to `&Connection` and encode how the connection is
held:

| Guard | Variant | Backing |
|---|---|---|
| `WriteGuard` | `Locked` | writer `MutexGuard` |
| | `Borrowed` | the dry-run connection (see below) |
| `ReadGuard` | `Pooled` | `PooledReader` (checked back in on drop) |
| | `Borrowed` | the dry-run connection |

**Runaway-query watchdog:** every guard installs a SQLite *progress handler* on creation and
clears it on drop. Every 5 000 VM instructions the handler checks a
15 s deadline; past it, the statement is interrupted (`SQLITE_INTERRUPT`) and the error
surfaces to the caller. This bounds the time any single operation can pin the writer mutex or
a pooled reader.

## Dry-run: savepoint-scoped execution

`Database::dry_run(f)` runs a full domain operation and guarantees none of it persists:

1. Lock the writer mutex — held for the entire dry run, so it **serializes against real
   writers** and sees a stable world.
2. `SAVEPOINT valqeron_dry_run`.
3. Publish the locked connection in a **thread-local** and run `f`
   with `DbHandle::DryRun`. Both `read()` and `write()` on that handle return `Borrowed`
   guards over the *same* writer connection — the dry run reads its own uncommitted writes,
   exactly like the real command would inside its transaction. The thread-local stores a raw
   `*const Connection` published only while the mutex guard is alive and cleared on unwind
   (restore-on-drop), so a dangling pointer is unobservable.
4. `ROLLBACK TO valqeron_dry_run; RELEASE valqeron_dry_run` — unconditionally, even if `f`
   returned an application error. A failed rollback logs and attempts a plain `RELEASE` as a
   fallback.

Concurrent real writes interleave safely before/after (never during) a dry run; the stress
test `dry_run_does_not_race_concurrent_writes` (`--ignored`) hammers exactly that.

## Cross-process contention

Inside one process, `SQLITE_BUSY` cannot arise between our own connections (single writer
mutex). Across processes — e.g. `sqlite3` poking a live database, or the historical
CLI-opens-DB phase — two defenses remain:

- **`busy_timeout` = 5 s** on every connection: SQLite blocks-and-retries internally before
  surfacing `SQLITE_BUSY`.
- **`with_busy_retry`** (`sqlite/support.rs`) on repository write paths: up to 5 attempts with
  linear backoff (10 ms, 20 ms, 30 ms, 40 ms) for errors that still escape as
  `DatabaseBusy`/`DatabaseLocked`.

In production neither should fire (the engine lock makes the process exclusive); they are
insurance, and each retry logs a warning so firing is visible.

## Migrations

`sqlite/migrations.rs`; SQL files live in `/migrations` but are **embedded at compile time**
(`include_str!` into the `MIGRATIONS` array — the binary carries its schema).

- Versioning via `PRAGMA user_version`; position in the array = version.
- Each pending migration runs in its own `BEGIN EXCLUSIVE` transaction: apply the SQL, bump
  `user_version`, commit — crash-safe at every boundary.
- A `user_version` **newer** than the binary knows is a hard error
  (`SqliteError::UnknownSchemaVersion`), never silently skipped: an old binary must not touch a
  future schema.
- Migrations run on the writer inside `Database::open_with_config`, before any reader exists.
  The engine is the sole migration runner in production (it owns the file exclusively).

Schema inventory: `issuer` (001), `security` (002), and `background_task` (003) — the
engine-internal task queue/history (`STRICT, WITHOUT ROWID`, BLOB uuid PK, RFC 3339 TEXT
timestamps, CHECK-constrained status, `version` for guarded writes; indexed on
`(status, scheduled_at)` for the dispatcher's due-claim and `(kind, scheduled_at)` for
per-kind history).

## Maintenance & shutdown

Two WAL-containment paths, deliberately different in aggressiveness:

| When | What | Why this flavor |
|---|---|---|
| Periodic (`Database::run_maintenance`) | `PRAGMA optimize` + `wal_checkpoint(PASSIVE)` | `PASSIVE` never blocks other readers/writers; safe while serving traffic. Returns `WalCheckpointStats { busy, log_frames, checkpointed_frames }` for the audit log. |
| `Database::drop` | `PRAGMA optimize` + `wal_checkpoint(TRUNCATE)` | at shutdown nothing else is running in-process; `TRUNCATE` fully rewinds the `-wal` file so the database is left as a clean single file. |

The engine drives the periodic path on a jittered interval and orders shutdown so the `Drop`
checkpoint cannot race in-flight writes (drain semaphore → reclaim engine → drop → release
lock; see engine.md). `wal_autocheckpoint = 1000` keeps the WAL bounded between runs;
`wal_file_stays_bounded_after_drop` and
`maintenance_checkpoints_the_wal_and_keeps_it_bounded_across_cycles` pin the behavior.

## Configuration

`DatabaseConfig` (`sqlite/database.rs`):

| Field | Default | Production (engine) |
|---|---|---|
| `reader_pool_size` | `min(available_parallelism, 6)`, fallback 2 | `READER_POOL_SIZE = 4` |
| `synchronous` | `Normal` | `Normal`, or `Full` with a truthy `VALQERON_ENGINE_DURABLE` |
| `busy_timeout` | 5 s | 5 s |

Sizing note: the engine's async facade admits storage closures through two lanes that mirror
this layer exactly — a read lane of `READER_POOL_SIZE` permits and a write lane of one (the
single writer) — so an admitted closure never waits on the pool's `Condvar` or the writer mutex
in-process. That bridge (lane semaphores + `spawn_blocking`) is documented in engine.md and
runtime.md; this layer stays fully synchronous.

## Testing

- **`Database::open_temp()` / `open_temp_with_config`** (`#[cfg(test)]`): a fresh database in
  its own temporary directory, returned as `TempDatabase` — a fixture owning both the
  `TempDir` and the `Database`, `Deref`ing to `Database`. Field order guarantees the database
  (and its closing `TRUNCATE` checkpoint) drops before the directory disappears. Because the
  only mode is file-backed, unit tests exercise the real pool, real read-only readers, and the
  real WAL.
- **Loom** (`just test-loom`): model-checks the `WaitPool` under `#[cfg(loom)]`. Required
  after touching the connection layer in `sqlite/database.rs`.
- **Stress/soak** (`#[ignore]`, run with `--ignored`): `dry_run_does_not_race_concurrent_writes`
  and `mixed_read_write_soak` (8 threads of mixed inserts/updates/reads asserting no lost or
  phantom writes).
