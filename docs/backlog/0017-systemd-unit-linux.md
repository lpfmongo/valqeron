---
id: 17
title: "systemd unit (Linux)"
milestone: "M6 — Service management"
area: area:ops
size: S
depends_on: [6]
blocks: [18]
status: in-progress
assignee:
---

# #17 — systemd unit (Linux)

## Context

The Linux counterpart to [#16](0016-launchd-plist-macos.md). A **user unit**
(`systemctl --user`) for the same reason: the database is user-scoped and the auth policy
assumes a single owning uid.

systemd offers sandboxing directives that launchd lacks. They are worth applying — but the
engine needs write access to its data directory, its log directory, and its socket path, so
`ProtectSystem`/`ProtectHome` must be configured deliberately rather than set to the strictest
value and left broken.

## Tasks

- [x] Write a user unit with `Type=simple`, `ExecStart`, and `Restart=on-failure` (not
      `always` — a clean shutdown must stay shut down).
- [x] Set `RestartSec` and a start-limit to prevent restart storms.
- [x] Apply hardening: `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=strict`,
      `ProtectHome=read-only` with explicit `ReadWritePaths` for the data and log
      directories (socket directory arrives with the gRPC edge).
- [x] Set `KillSignal=SIGTERM` and a `TimeoutStopSec` (40s) longer than the engine's drain
      (10s) + runtime shutdown (20s) bounds from [#6](0006-lifecycle-and-single-instance.md),
      so shutdown is never truncated mid-checkpoint.
- [x] Add `WantedBy=default.target` for the user session.
- [x] Document `systemctl --user enable/start/status` and `journalctl --user -u` usage —
      `install` prints them; boot-start-without-login documented via
      `loginctl enable-linger`.
- [ ] Confirm the hardening directives do not break database or log writes — verify, do not
      assume. *Manual, pending — no Linux host exercised yet.*

## Acceptance criteria

- The unit starts, restarts on crash, and does **not** restart after clean shutdown.
- Hardening is applied without breaking database, log, or socket access.
- `TimeoutStopSec` exceeds the drain deadline; shutdown completes with a checkpointed WAL.
- Start-limit throttling prevents loops when the lock is held.
- Logs are visible via `journalctl --user`.

## Test strategy

Manual on Linux with a documented checklist.

- `systemd-analyze verify` on the unit file.
- Start, `SIGKILL`, confirm restart; clean shutdown, confirm no restart.
- Confirm each `ReadWritePaths` entry is genuinely required — remove one and observe the failure
  to prove the sandbox is real.
- Confirm the WAL is checkpointed after `systemctl --user stop`.

## Delivery note

Implementation landed (`crates/engine/src/service/systemd.rs` + embedded template; template
rendering is unit-tested cross-platform, and the run/signal lifecycle is integration-tested).
**Remaining before `done`: the manual checklist above on a Linux host** — in particular the
`ProtectHome=read-only` + `ReadWritePaths` interaction, which is asserted here but not yet
proven against a real systemd.
