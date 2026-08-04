---
id: 5
title: "Daemon binary bootstrap"
milestone: "M2 — Engine daemon skeleton"
area: area:engine
size: M
depends_on: [2]
blocks: [6, 7]
status: todo
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

- [ ] Add a `main` for the `valqeron-engine` binary that builds a multi-threaded tokio runtime.
- [ ] Implement config resolution mirroring the CLI: DB path, reader pool size, synchronous
      mode (`--durable`), log paths. Precedence: flag > env > `ProjectDirs` default.
- [ ] Add engine-specific config: socket path, blocking pool size, request deadline.
- [ ] Factor the shared path-resolution logic so the CLI and engine cannot drift — extract to a
      common location rather than copy-pasting.
- [ ] Initialise `tracing` with the existing dual-layer setup; keep the `valqeron::audit`
      target intact.
- [ ] Add `--version` and `--help` via `clap`, consistent with the CLI's conventions.
- [ ] Log a startup banner at INFO: version, resolved DB path, socket path, pool sizes.
- [ ] Ensure runtime worker-thread count is configurable and defaults sensibly (do not let it
      collide with the blocking pool sizing in [#7](0007-storage-actor-core.md)).

## Acceptance criteria

- `valqeron-engine --help` and `--version` work; the binary starts and stays running.
- Given identical env/flags, the engine and CLI resolve the **same** DB path — asserted by a
  test, not by inspection.
- Logs appear on stderr and in the JSON log file, matching the CLI's format.
- No database is opened yet — that arrives with [#7](0007-storage-actor-core.md).
- Process exits 0 on SIGINT even before graceful shutdown lands in
  [#6](0006-lifecycle-and-single-instance.md).

## Test strategy

- **Unit:** config precedence (flag beats env beats default); path resolution parity between CLI
  and engine.
- **Integration:** spawn the binary, assert it starts and emits the startup banner, then
  terminate it.
