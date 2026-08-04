---
id: 2
title: "Workspace scaffolding for engine crates"
milestone: "M0 — Decisions & scaffolding"
area: area:engine
size: M
depends_on: [1]
blocks: [3, 5]
status: todo
assignee:
---

# #2 — Workspace scaffolding for engine crates

## Context

The workspace currently has three members: `valqeron-core`, `valqeron-infrastructure`,
`valqeron-cli` (root `Cargo.toml`). The engine needs three more —
`valqeron-proto`, `valqeron-engine`, `valqeron-client` — wired into the workspace with their
async dependencies, without leaking `tokio` into `core` or `infrastructure` (engine.md R6).

This is the first item on the critical path.

## Tasks

- [ ] Add `crates/proto`, `crates/engine`, `crates/client` and register them in the workspace
      `members`.
- [ ] Add workspace dependencies: `tokio` (rt-multi-thread, macros, signal, sync, net),
      `tonic`, `prost`, and `tonic-build` as a build-dependency.
- [ ] Set `publish = false` on the new crates, matching the convention used by `core` and `cli`.
- [ ] Establish the generated-code lint strategy (engine.md R1): decide between a scoped
      `#[allow(...)]` wrapper module or per-crate lint relaxation, and document the choice in
      the crate's `lib.rs`. Do not weaken lints workspace-wide.
- [ ] Each crate compiles as an empty skeleton: `proto` and `client` as libs, `engine` as a
      binary named `valqeron-engine`.
- [ ] Add a CI/Justfile check asserting `valqeron-core` and `valqeron-infrastructure` have no
      `tokio`/`tonic` in their dependency trees (e.g. `cargo tree -p ... -i tokio` must fail to
      find it).
- [ ] Extend the `Justfile` so `test`, `lint`, and `format` cover the new crates.
- [ ] Verify `cargo deny` still passes with the new dependency set; update `deny.toml`
      allowances if the licence set changed.

## Acceptance criteria

- `cargo build --workspace` and `cargo clippy --workspace` succeed with zero warnings under the
  existing denied lints.
- The async-containment check fails the build if someone adds `tokio` to `core` or
  `infrastructure`.
- `cargo deny check` passes.
- No changes to `core` or `infrastructure` source files.

## Test strategy

Build-level verification. The async-containment check is the meaningful test — confirm it
actually fails by temporarily adding `tokio` to `core`, observing the failure, then reverting.
