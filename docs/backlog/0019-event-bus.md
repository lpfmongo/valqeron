---
id: 19
title: "EventBus"
milestone: "M7 — Events"
area: area:engine
size: M
depends_on: [7]
blocks: [20, 23]
status: todo
assignee:
---

# #19 — EventBus

## Context

Change notification is what makes a desktop app viable: a UI cannot poll a local database
efficiently, and without push it will either feel stale or waste cycles.

The emission point is a real design decision (engine.md R3). `rusqlite`'s `hooks` feature is
already enabled and offers `update_hook`, but it reports only table name and rowid — no typed
domain data, and it fires inside the SQLite write path where doing work is dangerous. Emitting
from the **StorageActor at commit** instead gives typed events, and naturally covers mutations
from background workers ([#23](0023-ingestion-worker-stub.md)) as well as clients.

The subtlety: the actor must emit **only after the write is durably committed**, never for a
dry-run, and never for a write that failed or hit a version mismatch.

## Tasks

- [ ] Add a `tokio::sync::broadcast` channel with a configurable buffer capacity.
- [ ] Emit `ChangeEvent` from the StorageActor after each successful mutation, carrying kind,
      issuer id, resulting version, and timestamp.
- [ ] Suppress emission for dry-run commands — they roll back, so an event would be a lie.
- [ ] Suppress emission when `WriteOutcome` is `VersionMismatch` or `Missing`.
- [ ] Ensure emission cannot block or fail the write path: a full broadcast buffer must drop for
      slow subscribers, not stall the actor.
- [ ] Guarantee event ordering is consistent with commit order — the version field must be
      monotonic per issuer.
- [ ] Expose a subscribe API for [#20](0020-event-service-streaming.md) and internal consumers.
- [ ] Log at DEBUG when events are dropped due to a lagging subscriber; this is the signal a
      client is falling behind.

## Acceptance criteria

- Every successful mutation produces exactly one event — no duplicates, no misses.
- Dry-run mutations produce no events.
- Failed and version-mismatched writes produce no events.
- A slow or absent subscriber never blocks or slows a write.
- Per-issuer event ordering matches commit ordering.
- Events carry enough data for a client to update its view without an immediate re-fetch.

## Test strategy

- **Integration:** subscribe, perform each mutation kind, assert one event each with correct
  fields.
- **Negative:** dry-run mutation and a stale-version patch; assert silence.
- **Slow subscriber:** subscribe and never consume; assert writes continue at full speed and the
  drop is logged.
- **Ordering:** rapid sequential updates to one issuer; assert versions arrive monotonically.
- **Coverage:** confirm a mutation from a background worker also emits.
