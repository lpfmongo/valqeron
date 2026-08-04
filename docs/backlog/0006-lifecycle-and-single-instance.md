---
id: 6
title: "Lifecycle: single-instance guard and graceful shutdown"
milestone: "M2 — Engine daemon skeleton"
area: area:engine
size: L
depends_on: [5]
blocks: [16, 17]
status: done
assignee:
---

# #6 — Lifecycle: single-instance guard and graceful shutdown

## Context

Decision D1 says the engine owns the database exclusively. That guarantee needs enforcement:
two engines on one DB would reintroduce exactly the multi-writer problem the daemon exists to
prevent.

Socket binding alone is insufficient — a crashed daemon leaves a stale socket file behind
(engine.md R4). The authority is an **advisory `flock`** held for the process lifetime; the
socket is secondary and gets cleaned up when the lock proves the previous owner is gone.

Shutdown ordering matters because `Database`'s `Drop` impl runs `PRAGMA optimize` and a
`wal_checkpoint(TRUNCATE)`. That must happen after all work stops, or the checkpoint races
in-flight writes.

## Tasks

- [x] Acquire an advisory exclusive `flock` on a lock file next to the DB at startup; hold it
      for the process lifetime.
- [x] If the lock is held, exit non-zero with a clear message naming the DB path and (if
      obtainable) the holding PID. Do not silently proceed.
- [ ] On successful lock acquisition, remove any stale socket file before binding.
      *Deferred to the gRPC edge — no socket exists yet.*
- [ ] Bind the Unix domain socket with a parent directory at mode 0700; set restrictive
      permissions on the socket itself. *Deferred to the gRPC edge.*
- [x] Install SIGTERM and SIGINT handlers via `tokio::signal`.
- [x] Implement ordered graceful shutdown: (1) stop scheduling new jobs, (2) drain the
      in-flight job with a bounded deadline, (3) shut the runtime down, (4) drop the
      storage engine so WAL checkpoint runs, (5) release the lock and remove the lock file.
- [x] Bound the drain with a deadline; log a warning when it expires. *(Fixed 10s constant;
      configurability deferred until requests exist.)*
- [x] Ensure shutdown is idempotent — a second signal during the drain escalates to an
      immediate non-zero exit instead of corrupting the sequence.

## Acceptance criteria

- A second engine instance against the same DB fails fast with a clear diagnostic and exit
  code (`3`). ✅ integration-tested.
- A stale lock **file** left by `SIGKILL` does not prevent the next start (the kernel lock
  dies with the process; the file is diagnostic only). ✅ integration-tested. *Socket
  equivalent deferred with the socket itself.*
- SIGTERM triggers the full ordered sequence; the WAL file is checkpointed and bounded
  afterwards (mirroring the existing `wal_file_stays_bounded_after_drop` test).
  ✅ integration-tested.
- The lock file is removed on clean exit. ✅ integration-tested.
- ~~The socket is not accessible to other users on the machine.~~ *Deferred with the socket.*

## Delivery note (minimal engine slice)

Landed in `crates/engine` (`lockfile.rs`, `runtime.rs`) with end-to-end coverage in
`crates/engine/tests/lifecycle.rs`. The lock uses `std::fs::File::try_lock` (stable since
Rust 1.89) — no extra locking dependency. Everything socket-related moves to the milestone
that introduces the gRPC listener.

**Scope note (phase 1 ownership):** the lock guards *engine vs engine* only. The CLI keeps
opening the database directly; cross-process safety comes from SQLite WAL + busy timeouts.
Exclusive ownership (D1/R5) activates once the client library and CLI dispatch exist.

## Test strategy

- **Integration:** start two instances, assert the second fails; `SIGKILL` the first, then start
  a third and assert it recovers the stale socket.
- **Integration:** send SIGTERM mid-request and assert the request either completes or fails
  cleanly — never a partial write.
- **Manual/scripted:** verify socket and lock file permissions.
- Reuse the WAL-size assertion approach from
  `crates/infrastructure/src/sqlite/connection/database.rs` tests.
