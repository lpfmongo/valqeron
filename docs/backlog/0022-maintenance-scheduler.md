---
id: 22
title: "Maintenance scheduler"
milestone: "M8 — Background subsystem"
area: area:engine
size: M
depends_on: [7]
blocks: [23]
status: todo
assignee:
---

# #22 — Maintenance scheduler

## Context

Today the CLI is a one-shot process: `PRAGMA optimize` and `wal_checkpoint(TRUNCATE)` run in
`Database`'s `Drop` impl, which is adequate when the process lives for seconds. A daemon may run
for weeks — the WAL grows and query plans staleness accumulates with nothing to correct them.

**Cross-reference:** upstream
[#4](https://github.com/lfreitas2012/valqeron/issues/4) — "Add periodic SQLite `PRAGMA optimize`
for long-lived database processes" — anticipates exactly this. That issue is largely satisfied
by this item; the daemon is the long-lived process it describes. Reference it in the PR and
propose closing it.

**Cross-reference:** upstream
[#6](https://github.com/lfreitas2012/valqeron/issues/6) (Litestream replication) interacts here.
Litestream reads the WAL; aggressive `TRUNCATE` checkpointing can interfere with replication.
If Litestream is adopted, checkpoint policy must be revisited.

## Tasks

- [ ] Add a scheduler that runs periodic jobs as tokio tasks, each with a configurable interval.
- [ ] Add a periodic `PRAGMA optimize` job (addresses upstream #4).
- [ ] Add a periodic WAL checkpoint job; choose the mode deliberately (`PASSIVE` is safer under
      concurrency than `TRUNCATE`) and document the reasoning.
- [ ] Route all jobs through the StorageActor — never open a second connection.
- [ ] Add jitter to intervals so jobs do not synchronise with client traffic bursts.
- [ ] Make jobs shutdown-aware: they must observe the shutdown signal and stop promptly rather
      than delay the drain in [#6](0006-lifecycle-and-single-instance.md).
- [ ] Skip a scheduled run if the previous run is still in flight; log when this happens.
- [ ] Log each run at DEBUG with duration; log failures at WARN without killing the scheduler.
- [ ] Make every job individually disableable via config.
- [ ] Document the Litestream interaction (upstream #6) even though replication is not in scope.

## Acceptance criteria

- Jobs run at their configured intervals with jitter applied.
- Maintenance never blocks client requests for a perceptible period — measure, do not assume.
- A failing job logs and retries next interval; it does not kill the scheduler or the daemon.
- Shutdown stops jobs promptly; a running job does not extend the drain deadline unboundedly.
- Overlapping runs are prevented.
- WAL size stays bounded over a long run with sustained writes — the property the existing
  `wal_file_stays_bounded_after_drop` test asserts at shutdown, now maintained continuously.

## Test strategy

- **Integration:** short intervals in test config; assert jobs execute.
- **Soak:** sustained writes over an extended run; assert WAL size stays bounded. Mark
  `#[ignore]` per the existing stress-test convention.
- **Shutdown:** trigger shutdown with a job mid-run; assert prompt, clean stop.
- **Failure:** inject a failing job; assert the scheduler survives and retries.
- **Interference:** measure request latency with and without maintenance running; document the
  delta.
