---
id: 24
title: "AdminService"
milestone: "M9 — Admin, hardening, docs"
area: area:engine
size: M
depends_on: [12]
blocks: [26]
status: todo
assignee:
---

# #24 — AdminService

## Context

Operating a daemon requires visibility into it. Without introspection, diagnosing "the CLI feels
slow" means guessing between a saturated command queue, an exhausted reader pool, a long-running
background job, or genuine disk contention.

The stats worth exposing map directly to the system's known bottlenecks: the bounded command
channel ([#7](0007-storage-actor-core.md)), the reader pool, subscriber count
([#20](0020-event-service-streaming.md)), and background job state
([#22](0022-maintenance-scheduler.md)).

`Shutdown` needs care — it stops a service other clients may be using. It must be authorised
strictly and must route through the graceful sequence from
[#6](0006-lifecycle-and-single-instance.md), never an abrupt exit.

## Tasks

- [ ] Implement `Health` as a cheap liveness check with no database access — it must stay
      responsive even when storage is saturated, otherwise it cannot distinguish "hung" from
      "busy".
- [ ] Implement `Readiness` separately, verifying the database is actually reachable.
- [ ] Implement `Status`: version, uptime, database path, socket path, command-queue depth and
      capacity, reader-pool utilisation, active subscriber count, background job last-run and
      last-result.
- [ ] Implement `Shutdown` with strict authorisation, triggering the graceful sequence and
      responding before the socket closes.
- [ ] Ensure `Status` collection is cheap and cannot itself contend on the writer mutex.
- [ ] Expose counters useful for diagnosis: total requests, errors by taxonomy variant, dropped
      events.
- [ ] Wire `Status` into `valqeron engine status` from
      [#15](0015-cli-engine-subcommands.md).

## Acceptance criteria

- `Health` responds promptly even under heavy storage load — the defining property.
- `Readiness` fails when the database is unreachable while `Health` still succeeds.
- `Status` reports accurate queue depth and pool utilisation under load.
- `Shutdown` requires authorisation and performs an ordered shutdown.
- `Status` adds no measurable contention.
- Reported database path matches the one actually opened.

## Test strategy

- **Integration:** saturate the command queue, then call `Health`; assert prompt response. Then
  assert `Status` reflects the saturation.
- **Readiness:** make the database unreachable; assert readiness fails and health does not.
- **Shutdown:** authorised call triggers ordered shutdown and returns a response; unauthorised
  call is rejected and the daemon keeps running.
- **Accuracy:** drive known load and assert reported counters match expectations.
