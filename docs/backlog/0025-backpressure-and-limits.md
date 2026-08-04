---
id: 25
title: "Backpressure and limits"
milestone: "M9 — Admin, hardening, docs"
area: area:engine
size: M
depends_on: [9, 20]
blocks: [26]
status: todo
assignee:
---

# #25 — Backpressure and limits

## Context

Individual limits were introduced piecemeal — a bounded command channel in
[#7](0007-storage-actor-core.md), a subscriber cap in [#20](0020-event-service-streaming.md), a
request deadline in [#10](0010-issuer-service-unary-rpcs.md). This item reviews them as a system
and closes the gaps.

The failure mode being defended against: unbounded resource growth under load, where the daemon
degrades into unresponsiveness instead of shedding load. A daemon that rejects work clearly is
strictly better than one that accepts everything and stops responding.

The existing SQLite progress handler enforces a 15s per-operation timeout. Every other timeout
in the system should be reasoned about relative to it, so a request cannot outlive the operation
backing it — or vice versa.

## Tasks

- [ ] Audit every queue, pool, and buffer for an explicit bound; document each default and its
      rationale.
- [ ] Align the timeout hierarchy: client deadline ≤ request deadline ≤ storage operation
      timeout (15s progress handler). Document the intended relationship.
- [ ] Add a maximum concurrent connection limit; reject beyond it clearly.
- [ ] Add a maximum concurrent in-flight request limit, independent of connection count.
- [ ] Verify every limit surfaces as `ResourceExhausted` with an actionable message per
      [#11](0011-grpc-error-mapping.md) — never a hang or silent drop.
- [ ] Make limits configurable with defaults sized against the reader pool.
- [ ] Add load-shedding: when the command queue is near capacity, reject new work fast rather
      than queueing it to timeout later.
- [ ] Ensure rejections are counted and visible in
      [#24](0024-admin-service.md) `Status`.
- [ ] Verify no limit can be exceeded by a client that ignores errors and keeps retrying.

## Acceptance criteria

- Every unbounded resource is either bounded or documented as safely unbounded with reasoning.
- Under sustained overload the daemon rejects work and stays responsive — it does not degrade
  into unresponsiveness or grow memory without bound.
- Timeouts nest correctly: no request outlives its storage operation, no storage operation
  outlives its request.
- Every limit breach produces `ResourceExhausted` with an actionable message.
- Rejection counters appear in `Status`.
- Memory stays bounded under a load test that ignores all errors.

## Test strategy

- **Overload:** drive far beyond capacity; assert rejections, bounded memory, and continued
  responsiveness of `Health`.
- **Timeout nesting:** issue a request with a short client deadline against a slow operation;
  assert clean cancellation at the right layer with no orphaned work.
- **Limits:** exceed each limit individually; assert the correct error.
- **Abusive client:** ignore all errors and retry aggressively; assert the daemon survives and
  memory stays flat. Mark `#[ignore]` per the stress-test convention.
- Build on the harness from [#9](0009-bridge-concurrency-tests.md) rather than duplicating it.
