---
id: 23
title: "IngestionWorker stub"
milestone: "M8 — Background subsystem"
area: area:engine
size: M
depends_on: [19, 22]
blocks: []
status: todo
assignee:
---

# #23 — IngestionWorker stub

## Context

Decision D4 includes domain data sync as engine responsibility, but
[#1](0001-adr-engine-architecture.md) is what decides the actual sources. Until that lands, this
item builds the **structure** without committing to any specific provider.

Building the seam now is worthwhile: it forces the questions that matter regardless of source —
how ingested writes reach storage (through the actor, never a second connection), how they emit
events ([#19](0019-event-bus.md)), and how conflicts with user edits resolve.

That last question is the substantive one. If ingestion overwrites an issuer a user just edited,
optimistic concurrency turns it into a version mismatch. The policy — user wins, source wins, or
flag for review — is a product decision that must be recorded, not defaulted.

Scope this to a trait, config gating, and a no-op or fixture implementation. Real sources are a
follow-up once the ADR resolves.

## Tasks

- [ ] Define a worker trait: run on a schedule, report progress, respond to shutdown.
- [ ] Register ingestion with the scheduler from [#22](0022-maintenance-scheduler.md).
- [ ] Gate ingestion behind config, disabled by default — an engine must never make unexpected
      outbound network calls.
- [ ] Route all writes through the StorageActor so events emit and single-writer holds.
- [ ] Define and document the conflict policy for ingested data colliding with user edits.
- [ ] Define the batching strategy — ingestion must not monopolise the writer and starve
      interactive requests.
- [ ] Provide a fixture-backed implementation (reading a local file) so the path is testable
      without a network dependency.
- [ ] Add structured logging: records processed, created, updated, skipped, failed.
- [ ] Document what a real implementation must supply once the ADR names sources.

## Acceptance criteria

- Ingestion is disabled by default and requires explicit configuration.
- The fixture implementation ingests records end-to-end, emitting events for each write.
- Ingested writes respect optimistic concurrency; conflicts follow the documented policy.
- Batching demonstrably yields to client requests — measure writer contention.
- Shutdown interrupts ingestion cleanly, mid-batch, without partial-record corruption.
- Failures are logged and retried next interval; one bad record does not abort the run.

## Test strategy

- **Integration:** fixture ingestion; assert records land and events emit.
- **Conflict:** pre-edit a record, then ingest a colliding version; assert the documented policy
  applies.
- **Interference:** run ingestion while issuing client writes; assert client latency stays
  acceptable.
- **Shutdown:** interrupt mid-batch; assert no partial record and clean stop.
- **Resilience:** include malformed records in the fixture; assert they are skipped and logged,
  not fatal.
