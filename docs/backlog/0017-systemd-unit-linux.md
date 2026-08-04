---
id: 17
title: "systemd unit (Linux)"
milestone: "M6 — Service management"
area: area:ops
size: S
depends_on: [6]
blocks: [18]
status: todo
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

- [ ] Write a user unit with `Type=simple`, `ExecStart`, and `Restart=on-failure` (not
      `always` — a clean shutdown must stay shut down).
- [ ] Set `RestartSec` and a start-limit to prevent restart storms.
- [ ] Apply hardening: `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=strict`,
      `ProtectHome=read-only` with explicit `ReadWritePaths` for the data, log, and socket
      directories.
- [ ] Set `KillSignal=SIGTERM` and a `TimeoutStopSec` longer than the drain deadline from
      [#6](0006-lifecycle-and-single-instance.md), so shutdown is never truncated mid-checkpoint.
- [ ] Add `WantedBy=default.target` for the user session.
- [ ] Document `systemctl --user enable/start/status` and `journalctl --user -u` usage.
- [ ] Confirm the hardening directives do not break database or log writes — verify, do not
      assume.

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
