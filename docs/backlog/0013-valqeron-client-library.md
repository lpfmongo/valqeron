---
id: 13
title: "valqeron-client library"
milestone: "M5 — CLI client mode"
area: area:client
size: M
depends_on: [4]
blocks: [14, 15]
status: todo
assignee:
---

# #13 — `valqeron-client` library

## Context

Both the CLI and the future desktop app need to reach the engine. Duplicating connection
handling, retry policy, and version negotiation across two codebases guarantees drift.

[#1](0001-adr-engine-architecture.md) decides whether this crate ships now or is extracted after
the CLI works. If the ADR chose "inline first", this item narrows to a module inside the CLI —
the tasks below still apply, only the location changes.

Version negotiation matters because the daemon is long-running and independently installed: a
CLI upgraded ahead of the daemon must fail with a clear diagnostic rather than a confusing
decode error.

## Tasks

- [ ] Implement socket discovery matching the engine's resolution logic from
      [#5](0005-daemon-binary-bootstrap.md) — same precedence, same defaults.
- [ ] Implement connect with a bounded timeout; distinguish "no daemon running" (socket absent
      or refused) from "daemon unreachable" (present but failing).
- [ ] Add a retry policy for transient failures only, with backoff. Never retry non-idempotent
      mutations automatically — a retried `Register` could duplicate.
- [ ] Implement a version handshake against `AdminService` and fail with a clear message on
      incompatibility.
- [ ] Expose typed methods mirroring `IssuerService`, returning domain types via the
      [#4](0004-codegen-and-mapping-layer.md) mapping — callers should not handle protobuf.
- [ ] Surface the RFC-7807 detail from [#11](0011-grpc-error-mapping.md) as a typed client error.
- [ ] Support the streaming subscribe call for [#20](0020-event-service-streaming.md) — design
      the API now even if the server side lands later.
- [ ] Keep the crate free of CLI-specific concerns (no `clap`, no stdout formatting) so the
      desktop app can use it unchanged.

## Acceptance criteria

- "No daemon running" is a distinct, matchable error — [#14](0014-cli-dispatch-switch.md)
  depends on this to choose its dispatch path.
- Connection failures never hang indefinitely.
- Mutations are not silently retried.
- A version mismatch produces an actionable message naming both versions.
- The crate compiles without the CLI and has no `clap` dependency.

## Test strategy

- **Integration:** against a real engine — connect, call each RPC, assert typed results.
- **Negative:** point at a non-existent socket, a stale socket file, and a socket owned by an
  unrelated process; assert each produces the correct distinguishable error.
- **Version skew:** stub an incompatible version response and assert the handshake fails
  clearly.
- **Retry:** verify idempotent reads retry and mutations do not.
