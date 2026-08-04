---
id: 15
title: "valqeron engine subcommands (status, ping)"
milestone: "M5 — CLI client mode"
area: area:cli
size: S
depends_on: [13]
blocks: []
status: todo
assignee:
---

# #15 — `valqeron engine` subcommands (status, ping)

## Context

Once a daemon holds the database, users need a way to ask whether it is running and healthy —
otherwise the first diagnostic step is reading log files or `ps`.

Small item, but it is the primary troubleshooting entry point and the first consumer of
`AdminService` ([#24](0024-admin-service.md)). `ping` can ship against a minimal health
endpoint before the full AdminService lands.

## Tasks

- [ ] Add a `valqeron engine` subcommand group.
- [ ] `engine ping`: connect, round-trip a health check, report latency. Exit non-zero when no
      daemon is reachable.
- [ ] `engine status`: report version, uptime, resolved database path, socket path, and — once
      [#24](0024-admin-service.md) lands — pool and queue statistics.
- [ ] Emit output through the existing `{success, dry_run, data}` JSON envelope; respect
      `--pretty`.
- [ ] Distinguish "no daemon running" (an expected state, clear message) from "daemon
      unreachable" (an error) using the typed errors from
      [#13](0013-valqeron-client-library.md).
- [ ] Use distinct exit codes for the two cases so scripts can branch on them.

## Acceptance criteria

- `engine ping` succeeds against a running daemon and fails clearly against none.
- `engine status` reports the same database path the daemon actually opened — the check that
  catches config drift between CLI and engine.
- Output uses the standard JSON envelope and honours `--pretty`.
- "Not running" and "unreachable" have distinct messages and distinct exit codes.

## Test strategy

- **Integration:** both subcommands against a running engine; assert reported paths match the
  daemon's actual configuration.
- **Negative:** with no daemon, assert the message and exit code; with a stale socket file,
  assert the unreachable path.
- **Envelope:** assert JSON shape matches other commands.
