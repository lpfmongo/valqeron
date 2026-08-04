---
id: 11
title: "gRPC error mapping"
milestone: "M4 — IssuerService end-to-end"
area: area:engine
size: M
depends_on: [8, 10]
blocks: [14]
status: todo
assignee:
---

# #11 — gRPC error mapping

## Context

The CLI has an established error contract: `AppError` → `ProblemDetail` (RFC 7807) → JSON on
stderr, with sysexits-style exit codes (`crates/cli/src/error/`). Users and scripts depend on it.

Routing through gRPC must not degrade that. A `Status` code alone is too coarse — "failed
precondition" cannot tell a user *which* version mismatched, or whether a duplicate was a CNPJ
or a LEI. The structured detail defined in [#3](0003-proto-v1-definitions.md) carries the
RFC-7807 payload so [#14](0014-cli-dispatch-switch.md) can reconstruct byte-identical output.

Two audiences, one mechanism: the `Status` code serves programmatic clients, the detail payload
serves humans.

## Tasks

- [ ] Map the [#8](0008-dry-run-and-error-taxonomy.md) taxonomy to `tonic::Status` codes:
      not found → `NotFound`; version mismatch → `FailedPrecondition` (or `Aborted` — decide and
      document); uniqueness violation → `AlreadyExists`; validation → `InvalidArgument`;
      backpressure → `ResourceExhausted`; timeout → `DeadlineExceeded`; storage fault →
      `Internal`.
- [ ] Attach the RFC-7807 detail payload to every error `Status`.
- [ ] Include machine-readable specifics: expected/actual versions on mismatch, the offending
      identifier type on uniqueness violations, the field name on validation failures.
- [ ] Ensure `Internal` responses carry a correlation id (the request-id from
      [#12](0012-interceptors-auth-tracing.md)) and nothing else — the full fault is logged
      server-side only.
- [ ] Document the code↔taxonomy table in the crate so it stays discoverable.
- [ ] Verify parity with the CLI's existing exit-code mapping so
      [#14](0014-cli-dispatch-switch.md) can preserve exit codes.

## Acceptance criteria

- Every taxonomy variant maps to exactly one `Status` code; the mapping is exhaustive and
  compiler-enforced.
- No internal error text, file path, or SQL fragment reaches a client.
- A version-mismatch error tells the client both the expected and actual version.
- A duplicate error names whether it was CNPJ or LEI.
- The RFC-7807 detail is sufficient to reproduce the CLI's current error output for equivalent
  failures.

## Test strategy

- **Unit:** exhaustive match over the taxonomy — adding a variant without a mapping must fail to
  compile.
- **Integration:** trigger each real failure (duplicate CNPJ, stale version, missing id,
  over-long name) and assert both the `Status` code and the detail payload.
- **Leak test:** force a storage fault and assert the client-visible message contains no
  internal detail while the server log contains the full fault.
