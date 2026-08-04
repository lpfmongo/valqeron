---
id: 16
title: "launchd plist (macOS)"
milestone: "M6 — Service management"
area: area:ops
size: M
depends_on: [6]
blocks: [18]
status: in-progress
assignee:
---

# #16 — launchd plist (macOS)

## Context

macOS is the primary development platform for this project, so launchd support comes first.

The engine runs as a **user agent** (`~/Library/LaunchAgents`), not a system daemon — the
database lives under the user's data directory via `directories::ProjectDirs`, and the
peer-credential policy in [#12](0012-interceptors-auth-tracing.md) assumes a single owning user.
A system-wide daemon would contradict both.

`KeepAlive` needs care: an engine that exits because another instance holds the lock
([#6](0006-lifecycle-and-single-instance.md)) must not be restarted in a tight loop.

## Tasks

- [x] Write a `.plist` template with `Label`, `ProgramArguments`, `RunAtLoad`, and a
      crash-only `KeepAlive` (`SuccessfulExit: false`) so clean exits are not restarted.
- [x] Add `ThrottleInterval` to prevent restart storms on repeated startup failure.
- [x] Point `StandardOutPath`/`StandardErrorPath` at the log directory resolved in
      [#5](0005-daemon-binary-bootstrap.md); document interaction with the JSON log file.
- [x] Parameterise the binary path and any env overrides (`VALQERON_DB`) rather than hardcoding.
- [x] Verify SIGTERM from `launchctl` triggers the graceful shutdown sequence from
      [#6](0006-lifecycle-and-single-instance.md). *Verified 2026-08-04: `launchctl kill
      -TERM` → ordered shutdown, WAL truncated to 0 bytes, lock file removed.*
- [x] Provide install/uninstall — implemented as `valqeron-engine install|uninstall|status`
      subcommands. *Rationale:* a built-in command renders the template from the actual
      binary location (`current_exe`) and cannot drift from the shipped binary, unlike
      copy-pasted `launchctl` docs; `--force` re-renders after moving/rebuilding the binary.
- [x] Document how to inspect state (`launchctl print`) and read logs — `install` prints the
      inspection commands; `valqeron-engine status` reports registration + liveness.

## Acceptance criteria

- The agent starts at login and survives a crash.
- A clean exit (`engine shutdown`) does **not** trigger a restart.
- Repeated startup failure is throttled, not looped.
- `launchctl kill -TERM` produces an ordered shutdown with a checkpointed WAL.
- The plist contains no absolute paths specific to one developer's machine.
- Uninstall leaves no residue: no plist, no socket, no lock file.

## Test strategy

Manual on macOS, with a documented checklist (CI cannot cover launchd).

- [x] Install, verify running, `SIGKILL` and confirm restart.
- [x] Clean stop (`launchctl kill -TERM`) and confirm **no** restart — the most likely
      misconfiguration.
- [x] Force a startup failure (hold the lock from another process) and confirm throttling.
- [x] Confirm the WAL is checkpointed after `launchctl bootout` / clean stop.
- [ ] Agent starts at login (requires a logout/login cycle).

## Delivery note

Implementation landed (`crates/engine/src/service/launchd.rs` + embedded template;
template rendering is unit-tested and the run/signal lifecycle is integration-tested).

**Manual checklist executed 2026-08-04 on macOS:**

- `SIGKILL` → launchd respawned the agent within ~4s (new pid); the stale lock file never
  blocked the restart.
- `launchctl kill -TERM` → ordered shutdown (drain → final `wal_checkpoint(TRUNCATE)` →
  lock removed); agent stayed stopped for >40s (`SuccessfulExit: false` correctly leaves
  clean exits alone).
- Lock held by a foreground engine + `launchctl kickstart` → exit-3 startup failures were
  throttled to one attempt per ~30s (3 attempts / 75s, each naming the holder pid), then
  recovered automatically on the first attempt after the lock freed.
- `uninstall` → no residue: plist removed, agent unloaded, lock file gone, WAL 0 bytes.

**Remaining before `done`: verify the agent starts after a logout/login cycle.**
