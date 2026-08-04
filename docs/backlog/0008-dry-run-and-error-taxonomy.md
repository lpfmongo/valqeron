---
id: 8
title: "Dry-run support and error taxonomy"
milestone: "M3 — StorageActor bridge"
area: area:engine
size: M
depends_on: [7]
blocks: [11]
status: todo
assignee:
---

# #8 — Dry-run support and error taxonomy

## Context

The CLI's `--dry-run` works by wrapping a closure in a writer `SAVEPOINT`
(`Database::dry_run` in `crates/infrastructure/src/sqlite/connection/database.rs`): it takes the
writer mutex, opens `SAVEPOINT valqeron_dry_run`, runs the closure, then rolls back.

That closure-scoped design does not survive IPC (engine.md R2). Over gRPC, dry-run becomes a
**per-request flag executed inside a single StorageActor command** — one RPC equals one dry-run
scope. Multi-RPC dry-run sessions are explicitly out of scope: they would require holding the
writer mutex across client round-trips, blocking all writes on a network peer.

Note the existing `debug_assert!` guarding against nested `dry_run` — it deadlocks on the writer
mutex. The actor must make nesting structurally impossible.

## Tasks

- [ ] Add a `dry_run: bool` to every mutating `StorageCommand` variant.
- [ ] When set, execute the command inside `PersistenceManager::dry_run` so the savepoint wraps
      exactly one command.
- [ ] Guarantee dry-run commands cannot nest — the actor executes commands one at a time on a
      given writer, but assert it explicitly rather than relying on the current shape.
- [ ] Define an engine-level error enum distinguishing at minimum: not found, version mismatch
      (carrying expected and actual), uniqueness violation (CNPJ/LEI), validation failure,
      backpressure/queue full, timeout, and internal storage fault.
- [ ] Map `WriteOutcome::VersionMismatch` and `WriteOutcome::Missing` into that taxonomy —
      `WriteOutcome` is `#[must_use]`, so a silent discard is a bug.
- [ ] Map `StorageFault` (an opaque boxed error) into the internal-fault variant **without
      leaking internal detail to clients**; log the full fault server-side at ERROR.
- [ ] Preserve the domain's `RegisterIssuerError` distinctions (duplicate CNPJ vs duplicate LEI)
      so [#11](0011-grpc-error-mapping.md) can produce actionable messages.
- [ ] Confirm dry-run and real writes serialise correctly under concurrency — the existing
      `dry_run_serializes_against_a_concurrent_writer` test documents the expected behaviour.

## Acceptance criteria

- A dry-run mutation is observable within its own command and leaves no trace afterwards.
- A dry-run RPC never leaves a savepoint open, even when the command errors mid-flight.
- Every error the actor can produce maps to exactly one taxonomy variant; none collapse into a
  generic "internal error" except genuine faults.
- `StorageFault` internals never reach a client response.
- Concurrent real writes are unaffected by an in-flight dry-run.

## Test strategy

- **Integration:** dry-run insert, then assert a subsequent read sees nothing.
- **Integration:** dry-run a command that fails partway; assert no savepoint leaks and the next
  write succeeds.
- **Concurrency:** dry-run in one task while another writes; assert the real write survives and
  the dry-run does not — mirroring the existing infrastructure test.
- **Unit:** exhaustive match over the error taxonomy so a new variant cannot be added without
  updating the mapping.
