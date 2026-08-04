---
id: 7
title: "StorageActor core (async→sync bridge)"
milestone: "M3 — StorageActor bridge"
area: area:engine
size: L
risk: high
depends_on: [5]
blocks: [8, 9, 10, 19, 22]
status: todo
assignee:
---

# #7 — StorageActor core (async→sync bridge)

## Context

**This is the highest-risk item in the backlog.** See engine.md §2.1.

`IssuerRepository` and `StorageEngine` are blocking traits. The SQLite adapter uses a
`Condvar`-based reader pool (`crates/infrastructure/src/sqlite/connection/pool.rs`) and a single
writer behind a `Mutex` — a model verified with `loom` tests. tonic handlers are `async`.

Calling the blocking pool from a tokio worker thread would stall the runtime, and because the
reader pool blocks on a `Condvar` when exhausted, it can deadlock outright: all workers parked
waiting for readers, no worker left to release one.

The resolution is an actor. The synchronous `PersistenceManager<SqliteStorageEngine>` is owned
by dedicated OS threads. Async handlers send a command over a bounded channel and `await` a
`oneshot` reply. The loom-verified pool is used exactly as designed — untouched.

Cross-reference: upstream
[#5](https://github.com/lfreitas2012/valqeron/issues/5) (detect modified migration scripts) —
the engine becomes the sole migration runner, since migrations execute on `Database::open` here.

## Tasks

- [ ] Define a `StorageCommand` enum covering the `IssuerRepository` surface: `find_by_id`,
      `list_paged`, `exists`, `exists_by_cnpj`, `exists_by_lei`, `insert`, `apply_patch`,
      `update`, `delete`. Each variant carries a `oneshot::Sender` for its reply.
- [ ] Open `SqliteStorageEngine` at engine startup (before serving), wrap in
      `PersistenceManager`, and hand ownership to the actor. Migrations run here.
- [ ] Spawn dedicated blocking threads sized to `reader_pool_size + 1` — enough to saturate the
      reader pool plus one writer — and document the reasoning inline.
- [ ] Use a **bounded** `mpsc` channel; a full queue must surface as a typed backpressure error,
      never an unbounded memory grow.
- [ ] Expose an async `StorageHandle` (cloneable) as the only way to reach storage. Nothing else
      in the engine may hold the `PersistenceManager`.
- [ ] Route the domain service `register_issuer` (which performs CNPJ/LEI uniqueness checks then
      inserts) as a **single command** so the check-then-insert is not split across two
      round-trips and cannot interleave.
- [ ] Handle reply-channel drop (client cancelled/disconnected) without panicking or poisoning
      the worker.
- [ ] Ensure the actor survives a panicking command: a poisoned writer mutex is already
      auto-recovered by the infrastructure layer, but the actor thread must not die silently.
- [ ] Emit tracing spans per command with the request-id from
      [#12](0012-interceptors-auth-tracing.md) once available.

## Acceptance criteria

- No `async fn` in the engine ever calls a blocking repository method directly — enforced by
  review and by [#9](0009-bridge-concurrency-tests.md).
- Reads execute concurrently across the reader pool; writes serialise on the writer mutex, with
  behaviour identical to direct use of the infrastructure crate.
- A full command queue returns a typed error promptly rather than blocking the caller
  indefinitely.
- Dropping a `oneshot` receiver mid-flight does not kill the actor or leak a connection.
- Exactly one `SqliteStorageEngine` exists per process.
- `valqeron-core` and `valqeron-infrastructure` remain unmodified.

## Test strategy

- **Integration:** drive the handle from many concurrent tokio tasks; assert results match
  direct synchronous repository calls.
- **Deadlock probe:** issue more concurrent reads than `reader_pool_size` from a runtime with
  deliberately few worker threads (e.g. 2). Must complete, not hang — this is the failure mode
  the design exists to prevent.
- **Backpressure:** fill the queue and assert the typed error, not a hang.
- **Cancellation:** drop reply receivers mid-flight; assert the actor keeps serving.
- Deeper concurrency verification lives in [#9](0009-bridge-concurrency-tests.md).

## Notes

Prototype the bridge before building the full command surface. If the actor shape proves
awkward, the alternative is `tokio::task::spawn_blocking` against a semaphore-limited pool —
evaluate it early, while the change is still cheap.
