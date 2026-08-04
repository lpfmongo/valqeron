---
id: 14
title: "CLI dispatch switch (engine vs direct)"
milestone: "M5 — CLI client mode"
area: area:cli
size: L
depends_on: [11, 13]
blocks: [21]
status: todo
assignee:
---

# #14 — CLI dispatch switch (engine vs direct)

## Context

Final item on the critical path — this is where the work becomes user-visible.

The CLI currently opens SQLite directly (`crates/cli/src/store.rs`) and dispatches through a
`Command` trait with an `AccessMode`. It gains a second path: talk to the engine when one is
running, fall back to direct DB access when not.

The tension with decision D1 is real (engine.md R5). If the daemon holds the database and the
CLI also opens it, the single-writer guarantee breaks. Direct mode must therefore **check the
engine's flock and refuse** when the daemon is live. Direct mode exists for offline and admin
use, not as a concurrent alternative.

The output contract is non-negotiable: the `{success, dry_run, data}` JSON envelope, the
RFC-7807 error document, and the sysexits-style exit codes must be identical in both modes.
Users and scripts cannot be able to tell which path ran.

## Tasks

- [ ] Add mode resolution: engine mode when the socket is live, direct mode otherwise; explicit
      `--direct` and (optionally) `--engine` to force either.
- [ ] In direct mode, check the engine's advisory flock before opening the database. If held,
      fail with a clear message directing the user to the engine.
- [ ] Refactor the `Command` trait so each command can execute against either a
      `valqeron-client` handle or the existing local `Repos` façade.
- [ ] Map client errors back into `AppError`/`ProblemDetail` so error output is unchanged.
- [ ] Preserve exit codes exactly across both modes.
- [ ] Preserve `--dry-run` semantics in engine mode via the per-request flag from
      [#8](0008-dry-run-and-error-taxonomy.md).
- [ ] Keep `--db-path`, `--reader-pool-size`, and `--durable` meaningful in direct mode; warn
      clearly when they are passed in engine mode where they have no effect.
- [ ] Document the mode-selection rules in `--help`.

## Acceptance criteria

- Identical stdout JSON for the same command in engine and direct mode — byte-for-byte for
  equivalent data.
- Identical exit codes across modes for equivalent outcomes.
- Direct mode refuses to open the database while the engine holds the lock.
- With no daemon running, the CLI behaves exactly as it does today.
- `--dry-run` works in both modes and persists nothing in either.
- Flags that do not apply to the active mode produce a warning, not silent no-ops.

## Test strategy

- **Golden-output:** run a matrix of commands in both modes and diff stdout. This is the primary
  regression guard for the output contract.
- **Integration:** start the engine, run the CLI, assert engine mode was used (via daemon logs);
  stop the engine, assert fallback to direct.
- **Guard test:** with the engine running, force `--direct` and assert a clean refusal.
- **Exit codes:** assert per failure class in both modes.
- Reuse existing CLI tests unchanged wherever possible — if they need edits, the contract moved.
