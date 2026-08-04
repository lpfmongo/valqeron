---
id: 3
title: "Proto v1 service and message definitions"
milestone: "M1 — Contract"
area: area:proto
size: M
depends_on: [2]
blocks: [4]
status: todo
assignee:
---

# #3 — Proto v1 service and message definitions

## Context

`valqeron-proto` is the single wire contract shared by the engine, the CLI, and the future
desktop app. Getting it right early matters because all three consume it.

The domain has specifics that must survive the wire crossing: `Versioned<T>` optimistic
concurrency (`version: u32`), `WriteOutcome` (`Applied` / `VersionMismatch` / `Missing`),
keyset pagination via `list_paged(after, limit)`, and validated value objects (`IssuerName`
≤ 200 chars, `Cnpj`, `Lei`, `CountryCode`, `IssuerStatus`).

## Tasks

- [ ] Create `proto/valqeron/v1/` with an explicit `v1` package for forward compatibility.
- [ ] Define `Issuer` message: id (UUIDv7), name, status enum, created_at, version, and optional
      cnpj / lei / country_code.
- [ ] Define `IssuerPatch` with per-field presence semantics so "clear this field" and "leave
      unchanged" are distinguishable (`optional` / wrapper types, not bare scalars).
- [ ] Define `WriteOutcome` mirroring the domain enum, carrying `expected` and `actual` on
      version mismatch.
- [ ] Define `ChangeEvent` for [#19](0019-event-bus.md): kind (created/updated/deleted),
      issuer id, resulting version, timestamp.
- [ ] Define an error detail message carrying RFC-7807 fields (type, title, detail, instance) so
      [#11](0011-grpc-error-mapping.md) can preserve the CLI's existing error contract.
- [ ] Define `IssuerService`: Register, Get, List (keyset paginated), Patch, Delete — all unary.
- [ ] Define `EventService`: Subscribe (server-streaming `ChangeEvent`) with a filter message.
- [ ] Define `AdminService`: Health, Status, Shutdown.
- [ ] Add a `dry_run` field to every mutating request (engine.md R2 — one RPC equals one
      dry-run scope).
- [ ] Document the ID encoding (UUID as 16 bytes vs canonical string) and timestamp encoding
      (the storage layer uses millisecond, Z-suffixed RFC-3339 UTC).

## Acceptance criteria

- `.proto` files are organised under a versioned package and lint cleanly (`buf lint` or an
  equivalent agreed convention).
- Every mutating RPC accepts a `dry_run` flag.
- `IssuerPatch` can express "set field to null" distinctly from "do not touch field".
- Optimistic-concurrency version fields appear on every read and every mutating request.
- No `.proto` construct forces a breaking change to `valqeron-core`.

## Test strategy

No runtime tests at this stage — codegen and round-trip mapping are verified in
[#4](0004-codegen-and-mapping-layer.md). Review gate: walk each existing `IssuerRepository`
method and confirm the proto surface can express it, including `exists_by_cnpj` / `exists_by_lei`
uniqueness failures.
