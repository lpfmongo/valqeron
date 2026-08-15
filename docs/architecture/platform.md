# Valqeron Platform — Architecture Overview

Status: **implemented**. This describes the system as built, not a plan.

Scope: the whole platform on one page — every process, crate, and boundary, with the diagram as the map and a short
explanation per layer. The deeper references are
[engine.md](engine.md) (crate-level architecture, wire contract),
[database.md](database.md) (SQLite layer), [runtime.md](runtime.md) (runtime & concurrency),
and [internals.md](internals.md) (thread inventory, tokio mechanics, decision record, invariants).

## The map

```
════════════════════════════════ USER SPACE (one user, one machine) ═════════════════════════════════

                CLIENT PROCESS (per invocation)                      SERVICE MANAGER
┌──────────────────────────────────────────────────────┐   ┌──────────────────────────────────┐
│  valqeron / vq  (crates/cli)                         │   │ launchd (macOS) │ systemd --user │
│  clap · pre-validation (UX only) · JSON envelope     │   │   LaunchAgent   │  Type=notify   │
│  RFC-7807 rendering · problem.status = exit code     │   │   RunAtLoad     │  READY=1 ◄──┐  │
└──────────────────────┬───────────────────────────────┘   │   KeepAlive     │  STOPPING=1 │  │
                       │ blocking calls                    └──────────┬────────────────────┼──┘
┌──────────────────────▼───────────────────────────────┐              │ spawns the engine  │
│  valqeron-client  (crates/client)                    │              │ registered by      │
│  hidden current_thread runtime · block_on adapter    │              │ just engine-       │
│  connect(2s) → Health handshake (PROTOCOL_VERSION=1) │              │ install/uninstall  │
│  per-RPC 30s · mutations NEVER retried               │              │                    │
│  missing socket file ⇒ typed NotRunning (no RPC)     │              │              notify.rs
└──────────────────────┬───────────────────────────────┘              │              sd_notify
                       │                                              │              datagram
     ══════════════════▼════════ WIRE CONTRACT ═══════════════════    │
     valqeron-proto (crates/proto)                                    │
     .proto (protox codegen) · domain⇄proto fallible mapping          │
     socket discovery (shared: flag > VALQERON_SOCKET > platform)     │
     ProblemDetail⇄Status codec · slugs are ABI                       │
     ══════════════════╤═══════════════════════════════════════════   │
                       │  gRPC / HTTP2                                │
                       ▼                                              │
              ╔════ UDS socket ════╗          socket exists ⇒ DB open & migrated
              ║ valqeron.sock 0600 ║          (bind happens AFTER migrations)
              ╚════════╤═══════════╝
═══════════════════════╪═══════ ENGINE PROCESS (exactly one per DB — flock) ═══════════════════════
                       │
  valqeron-engine (crates/engine)                                     <db>.lock ◄── exclusive
                       │                                                            advisory flock
  BOOTSTRAP (typestate chain — mis-ordering does not compile)                       exit 3 if held
  ┌────────────────────────────────────────────────────────────────┐
  │ Bootstrap → acquire_lock → prepare_socket → open_database      │
  │           → bind_socket → build_runtime → run()                │
  │             (migrations)   (std listener)  (named workers,     │
  │                                             blocking pool ≤7)  │
  └───────────────────────────────┬────────────────────────────────┘
                                  ▼
  TOKIO multi_thread RUNTIME ("valqeron-worker" × cores)
  ┌────────────────────────────────────────────────────────────────┐
  │ main thread ── block_on(run_loop): select!{SIGTERM,SIGINT,     │
  │                                            server exit}        │
  │ server task ── tonic: IssuerService · AdminService             │
  │ conn task(s) ── h2 per connection ── req task per RPC          │
  │ JobSet (JoinSet + watch shutdown)                              │
  │   ├─ db_maintenance  jittered ±10%, bodies never overlap       │
  │   └─ heartbeat       log line                                  │
  └───────┬───────────────────────────────────────┬────────────────┘
          │ handlers: proto→domain parse          │ maintenance()
          ▼                                       ▼
  ASYNC→SYNC BRIDGE  AsyncStorage (storage.rs) — the ONLY crossing
  ┌────────────────────────────────────────────────────────────────┐
  │   READ LANE ════ 4 permits ≡ reader pool                       │
  │   WRITE LANE ═══ 1 permit  ≡ the writer (dry_run routed here)  │
  │   queue 5s → Overloaded → ResourceExhausted (engine/overloaded)│
  │   closed   → ShuttingDown → Unavailable                        │
  │   spawn_blocking: WHOLE domain op in ONE closure               │
  │   closures see &Repositories — never the engine, no .await     │
  └───────────────────────────────┬────────────────────────────────┘
                                  ▼ blocking pool (≤ 4+1+2 threads)
═══════════════ SYNC WORLD (tokio-free, enforced by just deps-check) ══════════════════
                                  │
  valqeron-infrastructure (private to engine)      valqeron-core (domain)
  ┌─────────────────────────────────────────┐      ┌──────────────────────────┐
  │ SqliteStorageEngine                     │      │ Issuer · Security ·      │
  │  ├─ writer  Arc<Mutex<Connection>> ──┐  │ impl │ Listing · Venue          │
  │  │   poison ⇒ forced ROLLBACK        │  │◄─────│ ports: *Repository,      │
  │  ├─ readers WaitPool ×4 (Condvar,    │  │      │ StorageEngine (dry_run)  │
  │  │   LIFO, loom-verified)            │  │      └───────────┬──────────────┘
  │  ├─ guards: RAII checkin + 15s       │  │                  │
  │  │   progress-handler watchdog       │  │      valqeron-identifiers
  │  ├─ dry_run: SAVEPOINT + thread-local│  │      Cnpj · Lei · Isin · Cfi ·
  │  │   (always ROLLBACK)               │  │      Mic · CountryCode (fuzzed,
  │  └─ embedded migrations (user_version)  │      generated tables committed)
  └──────────────────┬──────────────────────┘
                     ▼
            ┌─────────────────┐    WAL mode · 1 writer + N reader snapshots
            │  valqeron.db    │    busy_timeout 5s (cross-process insurance)
            │  (-wal · -shm)  │    Drop ⇒ optimize + checkpoint(TRUNCATE)
            └─────────────────┘

  shutdown ladder: STOPPING=1 → drain RPCs 10s → drain jobs 10s → close lanes →
                   wait idle 10s → runtime 20s → unlink socket → drop engine
                   (checkpoint) → release lock LAST
```

## Reading the map

The three `══` double lines are the boundaries where rules live; everything else is ordinary code:

| Boundary                                 | What crosses it                  | The rule                                                                                                              |
|------------------------------------------|----------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| **UDS socket** (process boundary)        | gRPC/HTTP2 frames                | one engine owns the database; every other process is a client. The socket file doubles as a truthful readiness signal |
| **Wire contract** (`valqeron-proto`)     | proto messages, problem details  | compatibility surface: `PROTOCOL_VERSION` handshake, problem slugs are ABI, IDs/timestamps have fixed canonical forms |
| **Async/sync frontier** (`AsyncStorage`) | one closure per domain operation | the only place async and blocking code meet; admission is lane-bounded to mirror the SQLite resources exactly         |

Arrows follow one request end to end: CLI → client `block_on` → UDS/h2 → handler (proto→domain parse) → lane permit →
blocking closure → guard → connection → file — and the response retraces it. `valqeron-common` (small shared helpers) is
omitted from the map; it sits beside proto with no architecturally interesting edges.

## Layer walkthrough

### Client process: `valqeron` / `vq` (crates/cli)

A thin, short-lived binary. It pre-validates input purely for UX (fast feedback, nice messages) — **the engine is
authoritative** and re-validates everything. Output is a JSON envelope or human text; engine failures arrive as RFC-7807
problem documents and are rendered verbatim, with `problem.status` doubling as the process exit code so scripts can
branch on failure *class*. The CLI contains no database code and cannot function without a running engine.

### `valqeron-client` — the blocking facade (crates/client)

Callers stay synchronous; each `Client` owns a private `current_thread` tokio runtime that drives tonic inside
`block_on` (never call it from inside another runtime). Three behaviors define it:

- **Missing socket file ⇒ immediate typed `NotRunning`** — no runtime built, no RPC attempted. This works because the
  engine binds the socket only after migrations succeed.
- **Version handshake on connect**: `AdminService::Health` must return
  `protocol_version == PROTOCOL_VERSION` or the client refuses to operate.
- **Mutations are never retried.** A lost response after a commit surfaces honestly instead of being papered over by a
  re-send that could double-apply.

### The wire contract (crates/proto)

The single source of truth both sides compile against: `.proto` files (built by `protox`, no system `protoc`), fallible
domain⇄proto mapping (invalid wire data cannot become a domain value), the `ProblemDetail`⇄`tonic::Status` codec, and —
deliberately — **socket discovery**, because engine and clients must resolve the same path (flag > `VALQERON_SOCKET` >
platform runtime dir). Renaming a problem slug or changing a canonical form is a breaking change; bump
`PROTOCOL_VERSION`.

### Service manager (launchd / systemd --user)

Registration is a *separate lifecycle* from the daemon: the binary takes no arguments (configuration is `VALQERON_*`
env vars carried by the service definition); `just engine-install` /
`just engine-uninstall` copy a static, machine-local service definition (LaunchAgent plist / systemd user unit created
once from the committed `.example` under `scripts/install/`, gitignored, carrying the paths and `VALQERON_*` overrides)
into place and register/deregister it, tearing the old instance fully down before starting the new one so at most one
engine runs at any time — the
development stand-in for future packaging (see the decision note in [internals.md](internals.md)). Runtime liveness is
probed through the client (`valqeron engine ping`/`status`). On Linux the unit is `Type=notify`: the engine sends
`READY=1` when it is actually serving and `STOPPING=1` when shutdown begins (`notify.rs`, hand-rolled datagram, silent
no-op elsewhere).

### Engine bootstrap (typestate chain)

Startup is a chain where each phase consumes the previous, so resource mis-ordering is a compile error, not a
code-review catch: lock → socket dir + stale-socket removal → database open (migrations run here, before any reader
connection exists) → bind (nonblocking `std`
listener, `0600`) → runtime. Two consequences worth internalizing: the socket's existence *proves* the database is
migrated, and readiness is a deterministic point rather than a race — early connections queue in the kernel backlog
until serving starts.

### Tokio runtime and its tasks

One `multi_thread` runtime (workers named `valqeron-worker`, blocking pool capped at lanes + margin instead of tokio's
default 512). The main thread drives `run_loop` — a pure signal/server watcher — while workers run the tonic server
task, per-connection h2 tasks, and per-RPC handler futures. Background work is structured: `PeriodicJob`s on a `JoinSet`
with a
`watch` shutdown; each job awaits its body inline on its own task, so overlapping runs are impossible and missed ticks
are skipped, never bursted. Details and tokio mechanics:
[runtime.md](runtime.md), [internals.md](internals.md) §3.

### The async→sync bridge (`AsyncStorage`) — the heart of the design

Runtime workers never touch SQLite; every storage call is one closure on tokio's blocking pool, admitted through a lane
that mirrors the real resource: **4 read permits ≡ the reader pool, 1 write permit ≡ the single WAL writer**. Because
permits map 1:1 onto connections, an admitted closure never blocks on in-process locks — queueing happens at the async
semaphore where a waiter is a suspended future, not a burned thread. Backpressure is typed (`Overloaded` →
`ResourceExhausted` after 5 s; closed lanes → `Unavailable`), the whole domain operation lives in one closure (no
interleaving, no lock across `.await` — there are no awaits), and closures receive `&Repositories` rather than the
engine, which makes the self-deadlocking nested dry-run unwritable. Dry runs ride the write lane through an
always-rolled-back savepoint.

### The sync world: core, infrastructure, identifiers

Everything below the bridge is plain synchronous Rust, enforced tokio-free by
`just deps-check` and model-checked by loom where it counts:

- **`valqeron-core`** — aggregates (`Issuer`, `Security`, `Listing`, `Venue`) and blocking ports (`*Repository`,
  `StorageEngine`). Pure domain logic, no I/O.
- **`valqeron-infrastructure`** — the SQLite adapter, private to the engine: one writer behind a poison-healing mutex (a
  panicked writer's stranded transaction is force-rolled-back), a LIFO `Condvar` reader pool whose guards check
  themselves back in on `Drop`, a 15 s progress-handler watchdog armed by every guard, savepoint-scoped dry-run, and
  compile-time embedded migrations versioned by `PRAGMA user_version`.
- **`valqeron-identifiers`** — fully validated identifier types (invalid state unrepresentable), fuzzed, with committed
  generated lookup tables (CFI taxonomy, MIC registry).

### The database file

One file-backed SQLite database in WAL mode — never in-memory, even in tests, so tests exercise the production topology
byte for byte. WAL gives readers consistent snapshots that never block on the writer. `busy_timeout` and bounded write
retries exist only as cross-process insurance; in-process, the single-writer mutex designs `SQLITE_BUSY` out. Periodic
maintenance runs `PRAGMA optimize` + a passive checkpoint; `Drop` runs a `TRUNCATE`
checkpoint that leaves a clean single file.

### Shutdown

The ladder at the bottom of the map runs strictly in order, each step bounded: notify
`STOPPING=1` → drain in-flight RPCs (10 s; a second signal forces exit 1) → drain jobs (10 s) → close the lanes and wait
idle (10 s) → runtime shutdown (20 s) → unlink socket → reclaim the engine via `Arc::try_unwrap` and drop it (final
checkpoint) → **release the lock last**. The
`try_unwrap` is both mechanism and proof: it can only succeed when nothing still references the engine, so the final
checkpoint cannot race a write.

## Where to go deeper

| Question                                                                   | Document                     |
|----------------------------------------------------------------------------|------------------------------|
| Crate graph, request path, wire/error contract, extending the API          | [engine.md](engine.md)       |
| Connections, locks, guards, dry-run, migrations, WAL maintenance           | [database.md](database.md)   |
| Boot phases, lanes and the backpressure chain, shutdown budgets            | [runtime.md](runtime.md)     |
| Thread/task inventory, tokio internals, decision record, the 18 invariants | [internals.md](internals.md) |
