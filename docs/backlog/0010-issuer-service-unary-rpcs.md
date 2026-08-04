---
id: 10
title: "IssuerService unary RPCs"
milestone: "M4 — IssuerService end-to-end"
area: area:engine
size: L
depends_on: [4, 7]
blocks: [11, 12]
status: todo
assignee:
---

# #10 — IssuerService unary RPCs

## Context

This closes the loop: gRPC request → mapping → StorageActor → SQLite → response. It is the last
item on the critical path before the CLI can talk to the engine
([#14](0014-cli-dispatch-switch.md)).

Handlers should stay thin. Validation belongs in the mapping layer
([#4](0004-codegen-and-mapping-layer.md)), storage semantics in the actor
([#7](0007-storage-actor-core.md)), and error translation in
[#11](0011-grpc-error-mapping.md). A handler that does more than translate-dispatch-translate is
a smell.

Pagination must preserve the existing keyset scheme — `list_paged(after: Option<IssuerId>,
limit: u32)` — not offset pagination.

## Tasks

- [ ] Implement the tonic server for `IssuerService` and serve it over the Unix socket bound in
      [#6](0006-lifecycle-and-single-instance.md).
- [ ] `Register`: route through the domain `register_issuer` service as a single actor command
      so the CNPJ/LEI uniqueness check and insert stay atomic.
- [ ] `Get`: return the issuer with its `version`; absent issuer is a typed not-found, not an
      empty success.
- [ ] `List`: keyset pagination with `after` and `limit`; enforce a maximum limit and a
      documented default. Return a next-page cursor.
- [ ] `Patch`: accept `expected_version`, map to the domain typestate `IssuerPatch`, and
      propagate `WriteOutcome`.
- [ ] `Delete`: accept `expected_version`; distinguish "already gone" from "version mismatch".
- [ ] Honour the `dry_run` flag on every mutating RPC via [#8](0008-dry-run-and-error-taxonomy.md).
- [ ] Enforce a per-request deadline consistent with the existing 15s SQLite progress-handler
      timeout; respect client-supplied gRPC deadlines where shorter.
- [ ] Emit audit records for mutations on the existing `valqeron::audit` target.

## Acceptance criteria

- All five RPCs work end-to-end against a real SQLite file.
- Optimistic concurrency is enforced: a stale `expected_version` fails and does not write.
- `List` paginates correctly across a dataset larger than one page, with a stable order and no
  duplicates or gaps at page boundaries.
- `limit` above the maximum is clamped or rejected — documented either way, never unbounded.
- Mutations with `dry_run` leave the database unchanged.
- Handlers contain no business logic.

## Test strategy

- **Integration:** full round trip over a real socket against a temp-file database, one test per
  RPC.
- **Concurrency:** two clients patch the same issuer with the same `expected_version`; exactly
  one succeeds.
- **Pagination:** seed more rows than one page; walk all pages; assert the union equals the seed
  set exactly.
- **Deadline:** confirm a slow operation is cut off rather than hanging the connection.
- Consider `scripts/dev_load_data.sh` as a realistic seed source for list/pagination tests.
