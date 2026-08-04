---
id: 9
title: "Bridge concurrency tests"
milestone: "M3 — StorageActor bridge"
area: area:engine
size: M
risk: high
depends_on: [7]
blocks: [25]
status: todo
assignee:
---

# #9 — Bridge concurrency tests

## Context

The bridge's failure modes are the kind that pass in development and deadlock in production
under load. The infrastructure crate sets the standard here: it already has `loom` model
checking, a mixed read/write soak test, and a dry-run race test (all in
`crates/infrastructure/src/sqlite/connection/database.rs`). The bridge deserves equivalent
rigour.

The specific hazard: tokio's runtime has a fixed worker count. If blocking work leaks onto
workers, a burst of concurrent requests parks every worker on the reader `Condvar` and the
runtime stops making progress. This does not reproduce reliably at low concurrency — it must be
tested deliberately.

## Tasks

- [ ] Write a test that runs the engine on a **deliberately small** runtime (1–2 worker threads)
      and issues far more concurrent storage operations than there are workers. Must complete.
- [ ] Write a test that saturates the reader pool while a write is in flight, asserting reads
      still make progress and no task is starved.
- [ ] Add a mixed read/write soak test modelled on the existing `mixed_read_write_soak`, driven
      through the async handle instead of directly.
- [ ] Verify the final row count equals successful inserts — no lost writes, no phantom rows —
      reusing the invariant style from the infrastructure soak test.
- [ ] Add a backpressure test: overfill the bounded command channel and assert callers receive
      the typed error promptly rather than hanging.
- [ ] Add a cancellation storm test: spawn many requests and drop them mid-flight; assert no
      leaked connections and a still-healthy actor.
- [ ] Investigate detecting blocking-on-async mechanically (e.g. `tokio-console`, or a debug
      assertion in the handle). Document the finding even if no automated guard is adopted.
- [ ] Mark long-running tests `#[ignore]` with a documented `--ignored` invocation, matching the
      existing convention for stress and soak tests.

## Acceptance criteria

- All tests pass reliably, including under `--test-threads=1` and on a constrained runtime.
- The small-runtime saturation test provably fails if a blocking call is reintroduced onto an
  async worker — verify by temporarily doing so.
- Soak test asserts data-integrity invariants, not merely absence of panics.
- No test hangs indefinitely; every one has a bounded timeout.
- Stress and soak tests are `#[ignore]`d and documented in the `Justfile`.

## Test strategy

This item *is* the test strategy. Key point: each test must be shown to fail when the property
under test is broken. A concurrency test that has never failed proves nothing.

Consider whether `loom` can model the actor's channel handoff. It likely cannot cover tokio
itself, but the handoff protocol between the async handle and the blocking worker may be
tractable — evaluate and document the conclusion.
