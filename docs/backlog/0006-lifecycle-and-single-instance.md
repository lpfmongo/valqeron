---
id: 6
title: "Lifecycle: single-instance guard and graceful shutdown"
milestone: "M2 — Engine daemon skeleton"
area: area:engine
size: L
depends_on: [5]
blocks: [16, 17]
status: todo
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

- [ ] Acquire an advisory exclusive `flock` on a lock file next to the DB at startup; hold it
      for the process lifetime.
- [ ] If the lock is held, exit non-zero with a clear message naming the DB path and (if
      obtainable) the holding PID. Do not silently proceed.
- [ ] On successful lock acquisition, remove any stale socket file before binding.
- [ ] Bind the Unix domain socket with a parent directory at mode 0700; set restrictive
      permissions on the socket itself.
- [ ] Install SIGTERM and SIGINT handlers via `tokio::signal`.
- [ ] Implement ordered graceful shutdown: (1) stop accepting new connections, (2) drain
      in-flight requests with a bounded deadline, (3) stop background workers, (4) drop the
      storage engine so WAL checkpoint runs, (5) release the lock and remove the socket.
- [ ] Make the drain deadline configurable; log a warning listing what was still in flight if
      it expires.
- [ ] Ensure shutdown is idempotent — a second signal during shutdown must not corrupt the
      sequence (consider escalating to immediate exit on a second SIGINT).

## Acceptance criteria

- A second engine instance against the same DB fails fast with a clear diagnostic and exit code.
- A stale socket left by `SIGKILL` does not prevent the next start.
- SIGTERM triggers the full ordered sequence; the WAL file is checkpointed and bounded
  afterwards (mirroring the existing `wal_file_stays_bounded_after_drop` test).
- The socket file and lock file are removed on clean exit.
- The socket is not accessible to other users on the machine.

## Test strategy

- **Integration:** start two instances, assert the second fails; `SIGKILL` the first, then start
  a third and assert it recovers the stale socket.
- **Integration:** send SIGTERM mid-request and assert the request either completes or fails
  cleanly — never a partial write.
- **Manual/scripted:** verify socket and lock file permissions.
- Reuse the WAL-size assertion approach from
  `crates/infrastructure/src/sqlite/connection/database.rs` tests.
