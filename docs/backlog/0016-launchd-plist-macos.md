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
- [ ] Verify SIGTERM from `launchctl` triggers the graceful shutdown sequence from
      [#6](0006-lifecycle-and-single-instance.md). *Manual checklist below — pending.*
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

- Install, verify running, `SIGKILL` and confirm restart.
- `engine shutdown` and confirm **no** restart — the most likely misconfiguration.
- Force a startup failure (hold the lock from another process) and confirm throttling.
- Confirm the WAL is checkpointed after `launchctl bootout`.

## Delivery note

Implementation landed (`crates/engine/src/service/launchd.rs` + embedded template;
template rendering is unit-tested and the run/signal lifecycle is integration-tested).
**Remaining before `done`: execute the manual checklist above on macOS.** One nuance to
verify deliberately: a lock-held startup failure exits `3`, which `SuccessfulExit: false`
treats as restartable — `ThrottleInterval` (30s) is what keeps that loop tame.
