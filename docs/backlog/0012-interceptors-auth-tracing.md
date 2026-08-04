---
id: 12
title: "Interceptors: auth, request-id, tracing"
milestone: "M4 — IssuerService end-to-end"
area: area:engine
size: M
depends_on: [10]
blocks: [20, 24]
status: todo
assignee:
---

# #12 — Interceptors: auth, request-id, tracing

## Context

A Unix socket restricted to mode 0700 already limits access to the owning user, but that is
filesystem policy, not application policy. Peer-credential verification via `SO_PEERCRED`
(Linux) / `LOCAL_PEERCRED` (macOS) makes the engine's own trust decision explicit and gives
audit records a real actor identity.

Request-ids matter more here than in the CLI: a single daemon serves many clients, and without
correlation a log file is an unreadable interleaving. The existing `valqeron::audit` target
needs an actor and a request-id attached to remain useful.

If [#1](0001-adr-engine-architecture.md) chose a TCP-loopback listener for the desktop app, this
item also needs a token or mTLS path — peer credentials do not exist over TCP.

## Tasks

- [ ] Implement a peer-credential interceptor for UDS connections; capture uid/gid/pid.
- [ ] Enforce an authorisation policy — by default, only the uid owning the daemon. Make it
      configurable, and log every rejection.
- [ ] Accept a client-supplied request-id if present; otherwise generate one. Return it on every
      response, success or failure.
- [ ] Create a per-request `tracing` span carrying request-id, RPC method, and peer identity;
      ensure it propagates into StorageActor commands.
- [ ] Attach the peer identity to `valqeron::audit` records for mutations.
- [ ] Add the token/mTLS path **only if** the ADR selected a TCP-loopback listener; otherwise
      record explicitly that it is out of scope.
- [ ] Ensure interceptor failures produce a clean `Unauthenticated` / `PermissionDenied`
      `Status` consistent with [#11](0011-grpc-error-mapping.md).

## Acceptance criteria

- A connection from an unauthorised uid is rejected before reaching any handler.
- Every log line emitted during a request carries its request-id, including logs from the
  blocking storage threads.
- Audit records identify the acting peer.
- A client-supplied request-id is echoed back unchanged; a generated one is returned when absent.
- Rejections are logged with enough detail to diagnose, without leaking to the client.

## Test strategy

- **Integration:** authorised connection succeeds; assert request-id round trip.
- **Integration:** if feasible in CI, connect as a different uid and assert rejection. If not,
  unit-test the policy function directly and document the CI gap.
- **Log assertion:** drive one request and confirm every emitted span shares the request-id —
  including across the async→sync boundary, which is the easy place to lose it.
- **Platform:** peer-credential APIs differ between Linux and macOS; test both or gate clearly.
