---
id: 20
title: "EventService streaming"
milestone: "M7 — Events"
area: area:engine
size: L
depends_on: [12, 19]
blocks: [21, 25]
status: todo
assignee:
---

# #20 — EventService streaming

## Context

Exposes [#19](0019-event-bus.md) over gRPC server-streaming.

The hard part is **lag**. `tokio::sync::broadcast` drops messages for subscribers that fall
behind, surfacing as `RecvError::Lagged(n)`. Silently swallowing that leaves the client with a
view it believes is current but is not — the worst possible outcome for a UI.

The contract must make lag explicit: on `Lagged`, send a resync marker telling the client its
stream has gaps and it must re-fetch. Correctness over convenience.

This is also the first long-lived connection type, which changes shutdown: streams must be
terminated cleanly during the drain phase from [#6](0006-lifecycle-and-single-instance.md)
rather than left hanging.

## Tasks

- [ ] Implement the `Subscribe` server-streaming RPC over the bus.
- [ ] Support the filter message from [#3](0003-proto-v1-definitions.md) (at minimum by change
      kind; by issuer id if the ADR calls for it).
- [ ] On `RecvError::Lagged(n)`, emit a resync marker carrying the number of missed events —
      never silently continue.
- [ ] Document the client contract for resync: on receipt, re-fetch current state and resume.
- [ ] Detect client disconnect promptly and release the subscription — no leaked subscribers.
- [ ] Enforce a maximum concurrent subscriber count; reject beyond it with `ResourceExhausted`.
- [ ] Terminate streams cleanly during graceful shutdown with an appropriate status, not an
      abrupt socket close.
- [ ] Apply the auth interceptor from [#12](0012-interceptors-auth-tracing.md) to subscriptions.
- [ ] Consider an initial-snapshot option (`send current state, then stream`) — decide and
      document, since without it every client must fetch-then-subscribe with a race in between.

## Acceptance criteria

- A subscriber receives events for mutations made by other clients.
- A deliberately slow subscriber receives a resync marker rather than silently missing events.
- Disconnected clients are cleaned up; subscriber count returns to zero.
- Subscriber limit is enforced.
- Graceful shutdown closes streams with a clear status.
- Unauthorised subscribe attempts are rejected before any event is sent.

## Test strategy

- **Integration:** two clients — one subscribes, the other mutates; assert delivery.
- **Lag:** subscribe with a tiny buffer, flood mutations without consuming, assert the resync
  marker arrives with a plausible count.
- **Leak:** connect and drop many subscribers; assert the count returns to zero.
- **Limit:** exceed the maximum; assert `ResourceExhausted`.
- **Shutdown:** SIGTERM with active streams; assert clean termination, no hang.
- **Race:** if initial-snapshot is implemented, assert no event is lost between snapshot and
  stream start.
