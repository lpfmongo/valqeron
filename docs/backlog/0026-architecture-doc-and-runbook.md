---
id: 26
title: "Architecture doc and ops runbook"
milestone: "M9 — Admin, hardening, docs"
area: area:docs
size: M
depends_on: [24, 25]
blocks: []
status: todo
assignee:
---

# #26 — Architecture doc and ops runbook

## Context

`depends_on` lists [#24](0024-admin-service.md) and [#25](0025-backpressure-and-limits.md) as
the last items to land, but this effectively depends on all preceding work — it documents the
system as built.

[`docs/architecture/engine.md`](../architecture/engine.md) describes the system as *designed*.
Implementation will have diverged: decisions revisited, limits tuned, approaches replaced. A
design document that no longer matches the code is worse than none, because it misleads.

The runbook is the genuinely new artefact. The repo has no operational documentation, and the
daemon is the first component that can be running-but-wrong rather than simply absent.

**Cross-reference:** upstream
[#6](https://github.com/lfreitas2012/valqeron/issues/6) (Litestream replication) — the backup and
recovery section should note how replication would interact with WAL checkpointing from
[#22](0022-maintenance-scheduler.md).

## Tasks

- [ ] Reconcile engine.md against the implementation; correct every divergence.
- [ ] Update the architecture diagram to reflect what was actually built.
- [ ] Record decisions that changed during implementation and why — the reasoning is what future
      readers need.
- [ ] Write an ops runbook covering: install/uninstall on both platforms, start/stop/status,
      log locations and interpretation, config reference with defaults.
- [ ] Document failure modes and their diagnosis: daemon will not start (lock held, stale
      socket, migration failure), slow requests (queue saturation, pool exhaustion, background
      jobs), client cannot connect (socket permissions, version skew).
- [ ] Document the upgrade procedure, including version skew between CLI and daemon.
- [ ] Document backup and recovery, noting the WAL and the Litestream interaction (upstream #6).
- [ ] Add a "first 5 minutes" section: what to check first when something is wrong.
- [ ] Add a README section pointing at the engine docs — the repo currently has no README.
- [ ] Update the backlog README, marking all items done and noting follow-up work.

## Acceptance criteria

- engine.md matches the implementation; no stale claims remain.
- The runbook lets someone who did not build the system install, operate, and diagnose it.
- Every documented failure mode has a concrete diagnostic step, not just a description.
- Config reference lists every option with its default and effect.
- Upgrade and version-skew handling is documented.
- A developer can follow the install instructions on a clean machine without prior knowledge.

## Test strategy

Documentation — validated by review and by execution.

- **Walkthrough:** someone other than the implementer follows the install instructions on a
  clean machine. Every stumble is a documentation bug.
- **Failure drill:** deliberately induce each documented failure mode and confirm the runbook's
  diagnostic steps actually identify it.
- **Config audit:** cross-check the reference against the code so no option is missing or
  misdescribed.
