# Valqeron Engine — Runtime & Concurrency

Status: **implemented**. This describes the system as built, not a plan.

Scope: how the engine boots, which threads and tasks exist at steady state, how async RPC work
crosses into the synchronous storage layer, and the exact shutdown order. The storage layer
underneath is covered by [database.md](database.md); the crate-level architecture by
[engine.md](engine.md). One level deeper — tokio scheduler mechanics, the full RPC walkthrough,
guard/pool internals, the architecture decision record, and the consolidated invariant list —
lives in [internals.md](internals.md).

## The five layers

```
1. Process        exclusive flock on <db>.lock            at most one engine per database
2. Main thread    synchronous bootstrap + teardown        crates/engine/src/engine.rs
3. Tokio workers  multi_thread runtime ("valqeron-worker") gRPC protocol, signals, job timers
4. Blocking pool  spawn_blocking, capped, lane-bounded    every storage closure
5. SQLite         1 writer mutex + 4-reader Condvar pool  loom-verified, fully synchronous
```

Only layer 3 is async. Layers 1–2 and 4–5 are ordinary blocking code; the single crossing point
between the worlds is `AsyncStorage` (layer 3 → 4), and everything below it is the untouched,
loom-verified model from `valqeron-infrastructure`.

## Bootstrap: typed phases (`engine.rs`)

Startup is a typestate chain — each phase consumes the previous, so the resource order cannot be
rearranged without failing to compile:

```
Bootstrap::new(config)        startup banner
  .acquire_lock()?            exclusive flock on <db>.lock (exit 3 when held elsewhere)
  .prepare_socket()?          0700 socket dir; unlink stale socket (safe: lock held)
  .open_database()?           open SQLite + run migrations; wrap in AsyncStorage
  .bind_socket()?             bind std UnixListener, nonblocking, chmod 0600
  .build_runtime()?           multi_thread tokio, named threads, capped blocking pool
→ ValqeronEngine::serve()     block_on(run_loop) + ordered teardown
```

Two properties are load-bearing:

- **The bind follows `open_database`.** A present socket file therefore proves the database is
  open and migrated — clients keep their cheap `NotRunning` detection (missing socket file, no
  RPC needed).
- **The bind precedes the runtime.** Readiness is a deterministic point in the boot sequence; a
  client connecting in the window before serving queues in the listener backlog instead of racing
  the socket file into existence. `run_loop` registers the already-bound listener with the
  reactor via `UnixListener::from_std`.

The runtime is built with `thread_name("valqeron-worker")` and
`max_blocking_threads(reads + writes + 2)`: the storage lanes bound admission, this bounds the
pool itself (tokio's default would allow 512 blocking threads).

### Readiness

The moment the server task is serving, the engine emits the `engine_ready` audit event and sends
`READY=1` over `$NOTIFY_SOCKET` (`notify.rs` — hand-rolled sd_notify, ~40 lines over
`std::os::unix::net::UnixDatagram`, silent no-op when the variable is unset). When shutdown
begins it sends `STOPPING=1`. The systemd user unit is `Type=notify`, so `systemctl start`
returns only once the engine actually serves; launchd has no readiness concept and is
unaffected. Tests wait on the `engine_ready` line instead of heartbeat proxies.

## Steady state: tasks and threads

```
main thread          parked in block_on(run_loop)
tokio workers        h2/protocol work, RPC handlers, timers, signal handling
  ├─ run_loop        select! over { SIGTERM, SIGINT, server task exit }
  ├─ server task     tonic serve_with_incoming_shutdown over the UDS
  ├─ task tickers    db_maintenance · heartbeat · task_prune (periodic schedules)
  └─ task dispatcher claims/executes/records rows of the background_task queue
blocking pool        ≤ 5 storage closures (4 read + 1 write) + margin
```

Background work is registered on the `BackgroundTasksManager` (`tasks.rs`): handlers by kind,
periodic schedules on their own ticker tasks (missed ticks are skipped, never bursted; first
ticks land one full period after spawn; ±10% optional jitter from clock sub-second noise — no
RNG dependency). A *durable* tick enqueues a row in the `background_task` table (gated on an
active-row check, so one kind never piles up or overlaps); the dispatcher claims due rows in
version-guarded batches, executes them with bounded concurrency, and records the outcome —
retrying failures with capped exponential backoff and recovering rows left `RUNNING` by a
previous process at boot. *Ephemeral* ticks (the heartbeat) run inline and persist nothing.
New background work registers a handler + schedule (or enqueues one-shot rows) instead of
growing the select loop.

## The async→sync bridge: storage lanes (`storage.rs`)

The reader pool checks out via a `Condvar` and the writer is a `std::Mutex` — both invisible to
tokio. A handler calling them on a runtime worker would block that worker for up to 15 s (the
SQLite watchdog bound); a handful of concurrent slow calls would starve accepts, timers, and
signal handling outright. So runtime workers never touch SQLite; every storage call is one
closure on the blocking pool, admitted through a lane that mirrors the real resource:

| Lane | Permits | Mirrors | Entry points |
|---|---|---|---|
| read | `reader_pool_size()` (4 in production) | reader connections | `AsyncStorage::read` |
| write | 1 (fixed) | the single WAL writer | `AsyncStorage::write`, `maintenance` |

Because permits map 1:1 onto connections, an **admitted** closure never waits on the pool's
`Condvar` or the writer mutex in-process; queueing happens exclusively at the async semaphore,
where a waiter is a suspended future (cheap), not a blocked thread (burned). Without the split,
a burst of writes could hold every slot while serializing on the writer mutex, starving reads
whose reader connections sat idle.

The backpressure chain, end to end:

```
client (30s RPC timeout, mutations never retried)
  → h2 over UDS
    → lane semaphore   FIFO; 5s queue timeout → Overloaded → gRPC ResourceExhausted
                       closed lane            → ShuttingDown → gRPC Unavailable
      → spawn_blocking (pool capped at lanes + margin)
        → writer mutex / reader pool   (never contended in-process — permits match)
          → SQLite busy_timeout 5s + bounded write retry   (cross-process insurance only)
            → 15s progress-handler watchdog → SQLITE_INTERRUPT
```

Interface contract:

- Closures receive `&Repositories<SqliteStorageEngine>`, never the engine. Handlers cannot call
  `StorageEngine::dry_run` at the wrong depth — the self-deadlocking nested dry run is
  unrepresentable at this edge.
- `write(operation, dry_run, closure)` routes the *same* closure through the always-rolled-back
  savepoint when `dry_run` is set; a failure of the savepoint machinery surfaces as
  `E::from(StorageError)`.
- One closure carries the **whole** domain operation (e.g. register's check-then-insert), so
  multistep services never interleave across executions — and no lock is ever held across an
  `.await`, because the closure contains no `.await`.
- `maintenance()` is `pub(crate)` and write-lane: engine-level work (checkpoints,
  `PRAGMA optimize`) delays writes, never reads.

### Cancellation semantics

Dropping an RPC future does not cancel its closure: `spawn_blocking` tasks run to completion and
release their permit at the end. Orphaned work after a client disconnect is therefore bounded by
*permits × watchdog*: at most 5 closures, each interrupted after 15 s. Lane acquisition itself is
cancel-safe (a dropped waiter simply leaves the queue).

## Shutdown ladder

```
signal (SIGTERM/SIGINT)
  └─ Ready → Stopping (fires STOPPING=1); stop accepting;    ≤ 10s  (2nd signal → exit 1,
     drain in-flight RPCs                                            Stopping → Failed)
      └─ drain background tasks (tickers + dispatcher stop;   ≤ 10s
         in-flight runs finish and record their outcome)
          └─ storage.close(): new calls → ShuttingDown
              └─ storage.wait_idle()                          ≤ 10s
                  └─ runtime.shutdown_timeout()               ≤ 20s  (blocking-pool backstop)
                      └─ unlink socket
                          └─ reclaim engine (Arc::try_unwrap) → drop:
                             PRAGMA optimize + wal_checkpoint(TRUNCATE)
                              └─ release <db>.lock — last, after the checkpoint
                                 proves the database is quiesced
                                  └─ Stopping → Stopped (clean) or → Failed
```

The lifecycle FSM behind these labels lives in `lifecycle.rs` (exhaustive transition table,
watch-based observers, sd_notify fired on transition) — see [engine.md](engine.md) §Lifecycle.

The lane drain is what makes the final checkpoint deterministic: `Arc::try_unwrap` succeeds only
when no closure still holds the engine. The service-manager stop timeout (60 s in the unit
templates) exceeds the sum of the internal budgets, so a graceful stop is never truncated
mid-checkpoint; SIGKILL remains the final backstop for truly stuck work. Exit codes: `0` clean,
`1` runtime/forced, `2` config, `3` already running.

## Client-side runtime

`valqeron-client` is blocking by design: each `Client` owns a private `current_thread` runtime
that drives tonic inside `block_on`. It is an adapter for synchronous callers, not a shared
runtime — never call the client from inside another tokio runtime.

## Why the storage layer stays synchronous

Making `valqeron-infrastructure` tokio-aware comes up periodically; it was evaluated and
rejected. The three shapes considered:

1. **Async pool + async ports.** rusqlite is blocking, so `spawn_blocking` does not disappear —
   it multiplies, from one crossing per domain operation (optimal) to one per repository call,
   with async composition *between* the crossings. Multi-step operations then either interleave
   or hold a checked-out connection across an `.await`, which compiles fine and quietly
   reintroduces the classic async-pool deadlock. Today "lock across await" is unrepresentable
   because domain closures contain no awaits.
2. **Fusing the facade into infrastructure.** Moves `AsyncStorage` down a layer, drags tokio into
   `core`'s ports (they live in `core/src/storage.rs`), turns every infrastructure test into a
   runtime test, and breaks the `just deps-check` containment invariant.
3. **Actor threads owning connections.** The least-bad variant — whole-op closures survive and
   dry-run is trivially safe — but it is functionally the current design with extra plumbing
   (boxed closures, reply channels) and it discards the loom-verified pool.

All three lose loom verification (loom swaps std-shaped primitives; `tokio::sync` types cannot be
model-checked), and none adds throughput: concurrency is bounded by SQLite/WAL physics — 4
readers + 1 writer on a local file — not by admission mechanics. Additionally, the `dry_run`
mechanism publishes the locked writer connection in a **thread-local** for the closure's scope;
tasks migrate across threads at `.await` points, so an async dry run would require redesigning
the most safety-critical mechanism in the layer. The two real costs of the sync design — permit
sizing and closure ergonomics — are paid once, at this edge, by the lanes and the
`&Repositories` closure interface.

The decision flips only if the backend itself changes to something with real async I/O and high
fan-out (a network database, many attached files) — and that would be a new backend crate behind
the same evaluation, not an asyncification of this one.
