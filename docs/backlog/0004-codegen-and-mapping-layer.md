---
id: 4
title: "Codegen pipeline and domain⇄proto mapping"
milestone: "M1 — Contract"
area: area:proto
size: M
depends_on: [3]
blocks: [10, 13]
status: todo
assignee:
---

# #4 — Codegen pipeline and domain⇄proto mapping

## Context

`tonic-build` output violates several workspace-denied lints — most notably `as_conversions`,
and potentially `indexing_slicing` (engine.md R1). Generated code must be quarantined rather
than met by relaxing lints globally.

The mapping layer is where domain invariants are re-established on the way in: proto messages
are unvalidated bags of fields, while `Issuer` is only constructible through a validating
builder. Every inbound conversion is a fallible parse, not a cast.

## Tasks

- [ ] Wire `build.rs` with `tonic-build` to compile the `.proto` files from
      [#3](0003-proto-v1-definitions.md).
- [ ] Quarantine generated code in a dedicated module with scoped `#[allow(...)]`, per the
      strategy chosen in [#2](0002-workspace-scaffolding.md). Document why each allow exists.
- [ ] Implement `TryFrom<proto::Issuer>` for the domain `Issuer`, routing through
      `IssuerBuilder` so validation (name length, CNPJ⇒BR country rule) is enforced.
- [ ] Implement `From<&Issuer>` for `proto::Issuer` including the `version` from
      `Versioned<Issuer>`.
- [ ] Map `proto::IssuerPatch` to the domain typestate `IssuerPatch` builder, rejecting an
      all-empty patch (the domain makes empty patches unconstructible).
- [ ] Map `WriteOutcome` in both directions.
- [ ] Map `Cnpj`, `Lei`, `CountryCode`, `IssuerStatus` with parse errors surfaced as typed
      conversion failures — never a panic.
- [ ] Ensure conversions perform no arithmetic that could violate `arithmetic_side_effects`
      (watch `u32`/`u64` version and limit fields).

## Acceptance criteria

- `cargo clippy --workspace` is clean; no workspace-level lint was weakened.
- Round-trip property: domain → proto → domain is lossless for every valid `Issuer`.
- Invalid proto input (over-long name, malformed CNPJ, CNPJ with non-BR country, empty patch)
  produces a typed error, not a panic or a silently-coerced value.
- Mapping code lives outside the generated module and is fully lint-compliant.

## Test strategy

- **Unit:** one negative test per validation rule, asserting the specific error variant.
- **Round-trip:** table-driven over representative issuers (with/without CNPJ, with/without LEI,
  both statuses); property-based if a suitable generator is cheap to write.
- **Timestamp fidelity:** confirm the millisecond Z-suffixed RFC-3339 representation survives
  the round trip, including the legacy offset formats the repository still parses.
