# Valqeron Engine — Internals Deep-Dive

Status: **implemented**. This describes the system as built, not a plan.

Scope: the engineering level below [runtime.md](runtime.md) — the exact thread/task inventory,
how the tokio runtime itself works and how the engine uses it, the life of an RPC end to end,
the storage access machinery (guards, pool, dry-run), the decision record behind this
architecture, and the consolidated invariant list with enforcement points. Crate-level
architecture is in [engine.md](engine.md); the SQLite layer in [database.md](database.md).

## 1. System at a glance

The full platform map (all processes, crates, and boundaries, with per-layer explanations)
lives in [platform.md](platform.md); below is the engine-centric slice this document dissects.

```
                 ┌─────────────────────── process (1 per DB, flock-enforced) ───────────────────────┐
valqeron CLI     │  main thread              tokio workers (N=cores)         blocking pool (≤7)     │
┌────────────┐   │  ┌───────────────┐        ┌───────────────────────┐       ┌─────────────────┐    │
│ Client      │  │  │ bootstrap     │        │ conn task ── req task │       │ storage closure │    │
│ (blocking,  │──UDS─▶ block_on(    │        │ conn task ── req task │─lane─▶│ f(&Repositories)│    │
│ current_    │  │  │   run_loop)   │        │ task tickers (3)      │permits│ ≤4 read, ≤1 wr  │    │
│ thread rt)  │  │  │ teardown      │        │ task dispatcher       │       └───────┬─────────┘    │
└────────────┘   │  └───────────────┘        │ signal streams        │               │              │
                 │                           └───────────────────────┘               ▼              │
                 │                                             SQLite: writer Mutex + 4-reader      │
                 │                                             Condvar pool, WAL, one file          │
                 └───────────────────────────────────────────────────────────────────────────────────┘
```

Two worlds, one crossing. Everything below `AsyncStorage` is ordinary synchronous Rust
(`core`, `infrastructure` — enforced tokio-free by `just deps-check`, model-checked by loom).
Everything async lives at the engine binary's edge. The single crossing is `spawn_blocking`
behind lane semaphores.

## 2. Thread & task inventory

### 2.1 OS threads

| Thread | Count | Born | Dies | Does |
|---|---|---|---|---|
| **main** | 1 | process start | process exit | argv guard (no arguments) + env config → bootstrap phases (lock, migrations, bind — all blocking) → `block_on(run_loop)` → polls `run_loop` itself → ordered teardown |
| **`valqeron-worker`** | N = available cores | the bootstrap `build_runtime` phase | `runtime.shutdown_timeout(20s)` | h2 protocol work, RPC handler futures, job timers, signal streams, semaphore waiters |
| **blocking pool** | 0→7, lazily spawned, idle-reaped | first `spawn_blocking` | runtime shutdown | storage closures only. Capped at `MAX_BLOCKING_THREADS = 4+1+2` — tokio's default is 512; the cap makes thread explosion structurally impossible |
| **tracing-appender worker** | 0 or 1 | `logging::init` when a log file is configured | `WorkerGuard` drop (held in `main.rs::dispatch`) | drains the non-blocking channel into the JSON log file, so no handler ever blocks on log disk I/O |

Notably absent: SQLite spawns no threads (rusqlite is in-process, `bundled`), and there are no
dedicated database threads — connections are *passive objects* owned by a mutex and a pool;
whichever blocking-pool thread holds the guard executes on them.

### 2.2 Tokio tasks (steady state)

| Task | Spawned at | Cardinality | Role |
|---|---|---|---|
| `run_loop` future | not spawned — driven by `block_on` on the main thread | 1 | `select!` over SIGTERM / SIGINT / server-task exit; owns shutdown orchestration |
| server task | `tokio::spawn(Server::…serve_with_incoming_shutdown)` | 1 | accept loop over the UDS listener stream; graceful shutdown via oneshot |
| connection tasks | by hyper, per accepted UDS connection | 1 per client | own the HTTP/2 connection state machine (framing, flow control, stream multiplexing) |
| request tasks | by hyper's executor, per h2 stream | 1 per in-flight RPC | poll the tonic router → service handler future |
| periodic tickers (`db_maintenance`, `heartbeat`, `task_prune`) | `BackgroundTasksManager::start` in `run_loop` | 1 each | durable ticks enqueue a `background_task` row (gated against pileup); ephemeral ticks run inline |
| task dispatcher | `BackgroundTasksManager::start` in `run_loop` | 1 | claims due rows (Notify-woken + 1s fallback poll), executes ≤2 concurrently, records outcomes/retries |
| storage closures | `spawn_blocking` inside `AsyncStorage` | ≤5 concurrent | **not** worker-pool tasks — blocking-pool jobs |

Background tasks are structured, not free-floating: tickers and the dispatcher live in a
`JoinSet`, share a `watch` shutdown channel, and every durable run is a version-guarded row in
the `background_task` table (`tasks.rs`) — which is why same-kind runs cannot overlap or pile
up, why a crash mid-run is recoverable at boot, and why `BackgroundTasksManager::drain`
deterministically waits for a mid-flight body.

## 3. How the tokio runtime works (and how the engine uses it)

The engine builds one `multi_thread` runtime (the bootstrap `build_runtime` phase) with
`enable_all()`, `thread_name("valqeron-worker")`, and `max_blocking_threads(7)`. What that
actually instantiates:

### 3.1 The worker scheduler

- N worker threads (default: available parallelism), each with a **local run queue** (fixed
  capacity 256) plus one shared **injection (global) queue**. `tokio::spawn` from a worker
  pushes to that worker's local queue; spawns from outside (e.g. the main thread inside
  `block_on`) go to the injection queue.
- **Work stealing:** an idle worker first drains its local queue, periodically checks the
  injection queue, then steals half of another worker's queue. This is why load spreads without
  any placement logic in engine code.
- **LIFO slot:** each worker keeps the most recently woken task in a one-item slot polled next,
  which optimizes wake-then-await message-passing chains (semaphore release → waiter runs).
- **Cooperative scheduling:** a task runs until it returns `Poll::Pending` — awaits are the
  *only* preemption points. Tokio adds a per-poll **coop budget** (~128 resource operations):
  once exhausted, tokio resources (channels, locks, semaphores) return `Pending` even when
  ready, forcing a yield so one busy task cannot starve a worker. The budget does **not** help
  against genuinely blocking calls — a thread stuck in SQLite for 15 s is invisible to the
  scheduler. That asymmetry is the entire reason the storage bridge exists.

### 3.2 The drivers (I/O, time, signal)

- **I/O driver:** one mio-based reactor per runtime (kqueue on macOS, epoll on Linux). When
  `run_loop` converts the pre-bound listener via `UnixListener::from_std`, the fd is registered
  with this reactor. A parked worker sleeps *in* the reactor poll; readiness events fire wakers,
  wakers reschedule tasks. Accept-readiness wakes the server task; per-connection socket
  readiness wakes that connection's task. Nothing in the engine polls anything — it is all
  waker-driven.
- **Time driver:** a hierarchical timing wheel checked when workers park. `interval_at` (jobs),
  `sleep` (drain deadlines), and `timeout` (lane admission, `wait_idle`) all register wheel
  entries; expiry fires wakers. `MissedTickBehavior::Skip` on the job tickers means a late tick
  is dropped, never bursted.
- **Signal driver:** `signal(SignalKind::…)` registers with a process-global signal handler that
  writes to a self-wake pipe registered with the I/O driver; `sigterm.recv()` in the `select!`
  is just another waker-driven stream. No signal-unsafe code runs in the handler itself.

### 3.3 `block_on` on a multi-thread runtime

`runtime.block_on(run_loop(…))` does **not** donate the main thread as a worker. The main
thread polls exactly that one future, parking on a condvar between wakes; everything
`tokio::spawn`ed runs on the workers. Consequences the engine relies on:

- Orchestration (`run_loop`'s `select!`) and RPC execution are physically decoupled — a flood of
  RPCs cannot delay signal handling, because the signal wakes the main thread directly.
- When `run_loop` returns, the workers still exist; teardown then explicitly bounds their death
  with `shutdown_timeout(20s)`.

### 3.4 The blocking pool

`spawn_blocking(f)` queues `f` for a **separate** thread pool: threads are spawned on demand up
to `max_blocking_threads`, reaped after an idle keep-alive (10 s default), and never run async
tasks. The returned `JoinHandle` is a future, so an async caller awaits completion without
occupying a worker. Two properties matter here:

- Blocking-pool jobs are **not cancellable** — dropping the `JoinHandle` detaches, it does not
  abort. `AsyncStorage` documents and tests this: a dropped RPC lets its closure finish; the
  lane permit releases at closure end.
- The pool is a queue, not a scheduler: without external bounding, 500 concurrent calls means
  500 threads. The engine bounds *admission* with the lane semaphores (≤5 in flight) and the
  *pool itself* at 7, so both layers are capped independently.

### 3.5 Task-model summary for this engine

```
waker flow:   fd readiness ──▶ I/O driver ──▶ waker ──▶ run queue ──▶ worker polls task
lane flow:    handler ──await semaphore──▶ permit ──▶ spawn_blocking ──▶ JoinHandle.await
                    (suspended future,          (blocking thread          (handler resumes
                     zero threads)               runs closure)             on any worker)
```

## 4. Life of an RPC

### 4.1 Connection + handshake (before any command)

Client side (`valqeron-client`, blocking facade over a private `current_thread` runtime):

1. Resolve the socket path via the **shared** convention in `valqeron-proto`
   (flag > `VALQERON_SOCKET` > platform runtime dir) — both sides use the same function, so
   they cannot disagree.
2. `!socket.exists()` → immediate typed `NotRunning` — no runtime built. This is why the engine
   binds the socket only *after* migrations: file presence is a truthful readiness signal.
3. Connect with a tower connector that dials `UnixStream` (the `http://valqeron.engine` URI is
   an h2-required placeholder). Connect timeout 2 s, per-RPC timeout 30 s.
4. **Version handshake:** `AdminService::Health` returns `(engine_version, protocol_version)`;
   a mismatch with `PROTOCOL_VERSION` is a typed `VersionMismatch` and the client refuses to
   operate.

Engine side: the listener was bound during bootstrap (nonblocking `std` listener) and registered
with the reactor at the top of `run_loop`. A client connecting in the boot window sits in the
kernel backlog and is served the moment tonic starts — connect never races the socket file into
existence.

### 4.2 The request path (register, the richest case)

```
 1 client   proto request built; block_on(issuer.register(req))        [client current_thread rt]
 2 kernel   h2 HEADERS+DATA frames over the UDS
 3 engine   connection task decodes h2 → request task polls the router [worker]
 4 handler  IssuerGrpc::register (grpc/issuer.rs)
     4a     req.dry_run captured
     4b     register_request_to_issuer(&req)  ← proto→domain, FALLIBLE: every identifier
            revalidated (CNPJ check digits, LEI, country), domain builders enforce
            invariants. Invalid wire data cannot become a domain value.
 5 admit    storage.write("issuer.register", dry_run, closure)
     5a     write-lane semaphore (1 permit), FIFO, acquired asynchronously —
            waiting costs a suspended future, no thread
     5b     5s timeout → Overloaded → ResourceExhausted
            lane closed → ShuttingDown → Unavailable
 6 execute  spawn_blocking closure                                     [blocking-pool thread]
     6a     dry_run=false: f(&engine.repositories())
            dry_run=true : engine.dry_run(f)   ← same closure, savepoint scope
     6b     closure = register_issuer(&repos.issuers, &issuer)
            = the WHOLE domain op: uniqueness checks (CNPJ, LEI) + insert, atomic
              within one closure execution — no interleaving possible
 7 result   permit released; handler's JoinHandle resolves             [worker]
 8 respond  Ok(proto) — or HandlerError → into_status(): one tonic::Code +
            the domain error's thiserror message (plain Status, no payload)
 9 client   classifies the status → ClientError::Rpc { code, message };
            the CLI prints the message and exits 1
```

Reads (`get`, `list`) take the read lane (`storage.read`, 4 permits); a saturated writer never
delays a read (pinned by the `saturated_write_lane_still_serves_reads` test). Optimistic
concurrency is an *outcome*, not an error: `patch`/`delete` return
`Applied | VersionMismatch{expected,actual} | Missing` in the response body.

### 4.3 Backpressure, end to end

```
client 30s RPC timeout (mutations never retried)
 → h2 flow control over UDS
  → lane semaphore      FIFO; 5s → typed Overloaded         ← the *designed* failure point
   → blocking pool      ≤7 threads, ≤5 ever in storage
    → writer mutex / reader pool    never contended in-process (permits ≡ connections)
     → SQLite busy_timeout 5s + with_busy_retry (5 × linear 10–40ms backoff)
                                    cross-process insurance only; warn-logged when it fires
      → progress-handler watchdog   every 5 000 VM ops, 15s deadline → SQLITE_INTERRUPT
```

Each layer fails faster than the one below it, so overload surfaces as one clean typed error at
the queueing stage instead of cascading timeouts.

## 5. The storage access interface

### 5.1 Ownership topology

```
AsyncStorage (Clone) ──┬─ IssuerGrpc             engine: Arc<SqliteStorageEngine>
                       ├─ BackgroundTasksManager             │
                       └─ run_loop                           ▼
                                                   Database
                                                   ├─ writer:  Arc<Mutex<Connection>>  (1, READ_WRITE|CREATE)
                                                   └─ readers: Arc<ReaderPool>         (4, READ_ONLY, query_only=ON)
                                                        ▲
         Repositories are built PER CLOSURE:            │
         SqliteIssuerRepository { db: DbHandle::Live { writer, readers } }  ← Arc clones, no connections
```

Repositories are cheap, transient views: `engine.repositories()` constructs them fresh inside
every closure; they hold `DbHandle`s (two `Arc` clones), never connections. Connections are
touched only through guards, inside a closure, on a blocking thread.

### 5.2 Guards: how a connection is held is encoded in a type

```rust
trait Db { fn write(&self) -> WriteGuard<'_>; fn read(&self) -> ReadGuard<'_>; }

enum DbHandle        { Live { writer, readers }, DryRun }
enum WriteGuard<'a>  { Locked(MutexGuard<'a, Connection>), Borrowed(&'a Connection) }
enum ReadGuard<'a>   { Pooled(PooledReader),               Borrowed(&'a Connection) }
```

Every acquisition does two things:

1. **RAII custody.** `Pooled` checks the connection back into the pool on `Drop` (a checkout
   blocked > 5 s logs a warning naming a leaked or long-held guard as the suspect); `Locked`
   releases the writer mutex on drop; `Borrowed` is the dry-run case.
2. **Arms the watchdog.** Guard creation installs a SQLite progress handler — every 5 000 VM
   instructions it checks a 15 s deadline and interrupts the statement past it; guard drop
   clears it. No operation can pin the writer or a reader indefinitely.

Pool mechanics (`WaitPool`): `Mutex<Vec<Connection>>` + `Condvar`; `take()` pops **LIFO** (the
most recently returned connection has the warmest page/statement caches) or parks; `put()`
pushes + `notify_one`. Poison recovery is asymmetric by design: the pool does plain
`into_inner` (a `Vec` cannot be left broken), while `lock_writer` checks `is_autocommit()` and
forces `ROLLBACK` if a panicking thread stranded an open transaction — a panic can never leak a
half-open transaction to the next writer.

### 5.3 Repository execution

- All SQL goes through `prepare_cached` (64-statement cache per connection): steady-state
  queries skip parse/plan.
- Write paths wrap statements in `with_busy_retry` (5 attempts, linear backoff) and
  disambiguate zero-affected-row guarded writes via a version re-read
  (`VersionMismatch` vs `Missing`).
- Eager loads run two statements on one reader guard without a transaction — WAL gives each
  statement a consistent snapshot, and hydration explicitly tolerates the inter-statement race
  (documented trade, not an accident).

### 5.4 Dry-run: savepoint + thread-local borrow

```
lock_writer()                          ← held for the WHOLE scope: serializes vs real writes
  SAVEPOINT valqeron_dry_run
  publish &Connection in a thread-local (raw ptr, restore-on-drop)
  f(&DbHandle::DryRun)                 ← read() AND write() return Borrowed guards over the
                                          same writer connection → the dry run reads its own
                                          uncommitted writes, exactly like the real command
  ROLLBACK TO …; RELEASE …             ← unconditional, even on application error
```

The thread-local is the reason this layer must stay synchronous: the pointer is valid precisely
because one OS thread runs the whole closure while the mutex guard is alive. Nesting is rejected
by a `debug_assert` — and made unreachable at the engine edge, because handler closures receive
`&Repositories`, not the engine.

## 6. Why this architecture — decision record

### Context and constraints

- The store is **embedded SQLite in WAL mode**: physics allow exactly 1 writer + N concurrent
  readers per file. No admission scheme adds parallelism beyond that.
- rusqlite is blocking. Any async design still executes SQLite calls on real threads.
- The workspace lint regime denies `unwrap`/`panic`/unchecked arithmetic — the codebase is
  built for verifiability, and the concurrency core is loom model-checked.
- The gRPC edge (tonic) is async; something must bridge the worlds.

### Options considered

| Option | Outcome | Why |
|---|---|---|
| **A. Call storage directly from handlers** | rejected | blocking calls on workers starve the runtime (no preemption without `.await`); the pool `Condvar` is invisible to tokio; a guard held across an await is one refactor away from a hard deadlock |
| **B. Raw `spawn_blocking` per handler** | rejected | tokio's blocking pool defaults to 512 threads: unbounded thread growth queueing invisibly on the writer mutex; no typed backpressure; the shutdown protocol (close → wait idle → reclaim → checkpoint) would be rebuilt ad hoc in every caller |
| **C. Async pool + async ports** | rejected | `spawn_blocking` multiplies (one crossing per repository call instead of one per domain op); multi-step ops either interleave or hold connections across `.await` — reintroducing the async-pool deadlock the design eliminates; async traits contaminate `core` |
| **D. Fuse the facade into infrastructure** | rejected | drags tokio into `core`'s ports, turns every storage test into a runtime test, breaks `just deps-check` containment |
| **E. Actor threads owning connections** | rejected (least bad) | keeps whole-op closures and makes dry-run trivially safe, but is functionally the current design with extra plumbing (boxed closures, reply channels) and discards the loom-verified pool |
| **F. Sync layer + lane-bounded facade (chosen)** | — | one bridge crossing per domain operation; admission ≡ resources; the dangerous states are unrepresentable (below) |

All async variants (C–E) also lose loom verification: loom swaps std-shaped primitives, and
`tokio::sync` types cannot be model-checked — the composition would be verified only by
integration tests.

### The deciding principle

Make the sharpest bug classes **unrepresentable rather than discouraged**:

- Domain closures contain no `.await` → a lock held across a suspension point *cannot be
  written*.
- Closures receive `&Repositories` → handlers *cannot reach* `dry_run` (whose nesting
  self-deadlocks) or the raw engine.
- Lane permits ≡ connections → an admitted closure *cannot block* on in-process locks; all
  queueing happens where waiting is free (suspended futures at the semaphore).
- The bootstrap typestate chain → resource mis-ordering (bind before migrations, DB before
  lock) *does not compile*.
- `Arc::try_unwrap` at teardown → the final `TRUNCATE` checkpoint *cannot race* a write; the
  mechanism is simultaneously the proof.

### Accepted costs

- Closure-shipping ergonomics at the edge (`Send + 'static`, move captures in/out).
- A dropped RPC cannot cancel its closure — bounded by permits × watchdog = 5 × 15 s.
- The decision flips only if the backend itself changes to real async I/O with high fan-out (a
  network database); that would be a new backend crate behind the same evaluation, not an
  asyncification of this one.

### Related decision: the async→sync crossing lives in the engine, not in infrastructure

"Why isn't `AsyncStorage` part of `valqeron-infrastructure`, next to the pool it guards?" Four
reasons, in order of weight:

1. **Verifiability.** Infrastructure's entire sync core (`Arc`/`Condvar`/`Mutex`) is swapped
   for loom models under `--cfg loom`; the pool's guarantees are *model-checked*. Loom cannot
   model tokio primitives — an async facade inside that crate would be the one concurrency
   structure the checker cannot see.
2. **Containment.** `just deps-check` denies tokio/tonic in core and infrastructure.
   `AsyncStorage` is tokio through and through (`Semaphore` lanes, `spawn_blocking`,
   `timeout`); moving it moves tokio in, and the invariant dies by definition.
3. **Runtime ownership.** `spawn_blocking` runs on the *engine's* runtime. A facade in
   infrastructure would panic unless called from inside some tokio context — an invisible
   dependency worse than the explicit layering.
4. **Policy vs mechanism.** Queue timeout, `Overloaded`/`ShuttingDown` and their gRPC-code
   mapping, close/drain/reclaim-for-checkpoint are the engine's admission and shutdown
   *contract*; infrastructure's contract is "1 writer + N readers, safely".

The two layers still compose as sealed builders: infrastructure's `Database`/`DbHandle`/
`ReaderPool`/guards are all `pub(crate)` — its only public surface is
`SqliteStorageEngine::open` (pragmas + migrations) plus handle accessors — and in the engine,
`AsyncStorage::open` is the single construction path, so an ungoverned engine handle never
exists outside `storage.rs` (it leaves only via `into_engine` for the final checkpoint).

### Related decision: service registration moved out of the binary

`install`/`uninstall`/`status` subcommands were originally kept in-binary; that decision was
**reversed**: installation/upgrade has a different lifecycle from the daemon (it happens at
deployment time, not runtime), so the binary takes no arguments at all (clap removed; any argv
is rejected as a config error; all configuration is `VALQERON_*` env vars carried by the
service definition), and registration is owned
by the `just engine-install` / `just engine-uninstall` recipes — the development stand-in for
future packaging (brew services, deb/rpm postinst). The cost accepted with this move: the
recipes mirror the engine's `ProjectDirs`/env-precedence resolution rather than sharing code
with it, so path-resolution changes in `engine.rs` must be reflected there (both sides honor
the same `VALQERON_DB`/`VALQERON_SOCKET`/`VALQERON_ENGINE_LOG_FILE` overrides, which bounds the
drift risk: pinning the env vars in the unit definition makes both resolutions agree by
construction). The `Type=notify` ⇔ sd_notify lockstep still holds — the recipe lives in the
same repository and changes with the engine. Runtime liveness probing moved to the client
(`valqeron engine ping`/`status`), where it belongs: it is an RPC concern, not a
service-manager one.

## 7. Invariants (consolidated)

| # | Invariant | Enforced by |
|---|---|---|
| 1 | At most one engine process per database file | exclusive advisory flock on `<db>.lock`; exit 3 when held |
| 2 | `core` and `infrastructure` are tokio/tonic-free | `just deps-check` (CI-of-record is local `just`) |
| 3 | `infrastructure` is reachable only from `engine` | workspace dependency graph |
| 4 | Runtime workers never execute SQLite/pool code | all storage flows through `AsyncStorage::read`/`write`/`maintenance` → `spawn_blocking`; closures are the only code that sees `Repositories` |
| 5 | One closure = one whole domain operation; no lock across `.await` | closures are sync (`FnOnce(&Repositories) -> T`), no await points exist inside |
| 6 | Admission ≡ resources: read permits ≡ reader pool, 1 write permit ≡ writer | by construction: `AsyncStorage::new` derives read permits from `SqliteStorageEngine::reader_pool_size()`; the write lane is a fixed 1 in `storage.rs` |
| 7 | Backpressure is typed, never thread pile-up | 5 s lane timeout → `Overloaded`; blocking pool capped at 7 |
| 8 | Dry runs never persist and never nest | savepoint always rolled back; `debug_assert` in `Database::dry_run`; handlers cannot reach `dry_run` (they hold `&Repositories`) |
| 9 | Socket file exists ⇒ database is open and migrated | bootstrap typestate: `bind_socket` consumes `DatabaseOpen` |
| 10 | Readiness is deterministic: lock ∧ migrated ∧ bound ∧ serving | `engine_ready` audit event + sd_notify `READY=1` at one code point in `run_loop` |
| 11 | Migrations run exactly once, before any reader, only in the engine | writer opens → migrations → readers open (`Database::open_with_config`); unknown future `user_version` is a hard error |
| 12 | Final checkpoint cannot race in-flight writes | shutdown ladder drains lanes; engine reclaimed via `Arc::try_unwrap` before `Drop` checkpoint; lock released last |
| 13 | A panic never leaks a half-open transaction | `lock_writer` poison recovery forces `ROLLBACK` on non-autocommit connections |
| 14 | No operation pins a connection unboundedly | progress-handler watchdog: 15 s → `SQLITE_INTERRUPT`, armed/cleared by guard RAII |
| 15 | Pool never loses or duplicates a connection; blocked `take` is always woken | loom model checks (`just test-loom`); `PooledReader` check-in on `Drop` |
| 16 | Wire compatibility: `PROTOCOL_VERSION` handshake (v2: plain-Status errors) | client refuses on mismatch; error contract = gRPC code + message, codes are the machine surface |
| 17 | Mutations are never retried by the client | client design; "response lost after commit" surfaces honestly |
| 18 | Background jobs never overlap and never burst | job body awaited inline on its own task; `MissedTickBehavior::Skip` |

## 8. Verification map

| Property | Assured by |
|---|---|
| Pool correctness under contention | loom (`#[cfg(loom)]` primitives swap, `just test-loom`) |
| Lane semantics: typed backpressure, reads survive writer saturation, cancel safety, drain | `storage.rs` unit tests (2-worker runtimes vs 16 calls, blocked-lane probes) |
| Boot order, exit codes, lock/socket cleanup, WAL truncated on clean exit | `tests/lifecycle.rs` — black-box against the real binary |
| Full client path: lifecycle, dry-run persistence, typed error codes, reader-pool fan-out | `tests/grpc.rs` — real binary + real `valqeron-client` over UDS |
| Dry-run vs concurrent writers; mixed read/write soak | `#[ignore]` stress tests (`cargo test -- --ignored`) |
| Fallibility discipline (no unwrap/panic/indexing/unchecked arithmetic) | workspace `deny` lints |
| Async containment | `just deps-check` |
| sd_notify protocol (`READY=1`/`WATCHDOG=1`/`STOPPING=1`) | unit tests + fake-`NOTIFY_SOCKET` e2e in `tests/lifecycle.rs`; real systemd `Type=notify`/`WatchdogSec` remains on the manual Linux checklist |
