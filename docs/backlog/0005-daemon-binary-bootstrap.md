---
id: 5
title: "Daemon binary bootstrap"
milestone: "M2 — Engine daemon skeleton"
area: area:engine
size: M
depends_on: [2]
blocks: [6, 7]
status: done
assignee:
---

# #5 — Daemon binary bootstrap

## Context

The engine needs a startable process before anything can be layered on it. Configuration must
resolve the same way the CLI does today (`crates/cli/src/config.rs`: `directories::ProjectDirs`
with qualifier `io`, org `valqeron`, plus `VALQERON_DB` env and flag overrides) so both binaries
agree on which database file they mean — otherwise the exclusive-ownership guarantee in
[#6](0006-lifecycle-and-single-instance.md) is meaningless.

Logging reuses the existing dual-layer `tracing` setup (compact stderr + JSON file via
`tracing-appender`), including the dedicated `valqeron::audit` target.

On the critical path. Runs in parallel with M1.

## Tasks

- [x] Add a `main` for the `valqeron-engine` binary that builds a tokio runtime.
- [x] Implement config resolution mirroring the CLI: DB path, synchronous
      mode (`--durable`), log paths. Precedence: flag > env > `ProjectDirs` default.
- [x] Add engine-specific config: maintenance and heartbeat intervals. (Socket path, blocking
      pool size and request deadline arrive with the gRPC edge.)
- [x] Factor the shared path-resolution logic so the CLI and engine cannot drift — extract to a
      common location rather than copy-pasting.
- [x] Initialise `tracing` with the existing dual-layer setup; keep the `valqeron::audit`
      target intact.
- [x] Add `--version` and `--help` via `clap`, consistent with the CLI's conventions.
- [x] Log a startup banner at INFO: version, resolved DB path, lock path, intervals, pool size.
- [ ] Ensure runtime worker-thread count is configurable and defaults sensibly (do not let it
      collide with the blocking pool sizing in [#7](0007-storage-actor-core.md)). *Deferred:
      moot on the `current_thread` runtime; revisit when tonic forces `multi_thread`.*

## Acceptance criteria

- `valqeron-engine --help` and `--version` work; the binary starts and stays running.
- Given identical env/flags, the engine and CLI resolve the **same** DB path — asserted by a
  test, not by inspection.
- Logs appear on stderr and in the JSON log file, matching the CLI's format.
- ~~No database is opened yet — that arrives with [#7](0007-storage-actor-core.md).~~
  *Superseded:* the minimal engine opens the database at startup (migrations) and runs
  periodic maintenance — sequenced ahead of the StorageActor by decision.
- Process exits 0 on SIGINT even before graceful shutdown lands in
  [#6](0006-lifecycle-and-single-instance.md).

## Delivery note (minimal engine slice)

Landed as `crates/engine` with the shared resolution extracted to `crates/config`
(`valqeron-config`). Deviations from the original text, by decision:

- **Runtime:** tokio `current_thread`, not multi-threaded — tokio's only consumers today are
  timers and signal streams; `multi_thread` arrives with tonic. Blocking DB work already goes
  through `spawn_blocking`, previewing the [#7](0007-storage-actor-core.md) bridge shape.
- **Database opens at startup:** the engine's first useful job is periodic maintenance
  (upstream #4), which needs the database. Exclusive ownership (D1) remains deferred — the
  lock guards engine-vs-engine only; the CLI keeps direct access until the client library
  lands.
- Engine logging uses `VALQERON_ENGINE_LOG_FILE` / `VALQERON_ENGINE_LOG_LEVEL` and its own
  log file, so the two binaries never interleave writes; `VALQERON_DB` stays shared.

## Test strategy

- **Unit:** config precedence (flag beats env beats default); path resolution parity between CLI
  and engine.
- **Integration:** spawn the binary, assert it starts and emits the startup banner, then
  terminate it.
