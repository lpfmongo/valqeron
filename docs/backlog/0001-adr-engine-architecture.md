---
id: 1
title: "ADR: engine architecture decisions"
milestone: "M0 — Decisions & scaffolding"
area: area:docs
size: S
depends_on: [ ]
blocks: [ 2 ]
status: todo
assignee:
---

# #1 — ADR: engine architecture decisions

## Context

[`docs/architecture/engine.md`](../architecture/engine.md) records decisions D1–D4 (engine owns the DB, gRPC over UDS,
tokio at the edge only, background scope) but leaves four questions open in §6. Those answers change crate boundaries
and the auth design, so they must be settled before scaffolding lands.

This item produces the first ADR and establishes `docs/adr/` as the home for future ones.

## Tasks

- [ ] Create `docs/adr/` with a short README explaining the ADR format and numbering.
- [ ] Write `docs/adr/0001-engine-architecture.md` restating D1–D4 with their rationale and consequences.
- [ ] Resolve open question 1 — **desktop transport**: same Unix socket, or an additional TCP-loopback + TLS listener?
  Record the auth implication (peer credentials vs token/mTLS).
- [ ] Resolve open question 2 — **ingestion sources**: which external source (s) feed issuer sync, or
  is [#23](0023-ingestion-worker-stub.md) a documented stub for now?
- [ ] Resolve open question 3 — **`valqeron-client` timing**: ship the shared crate in the first slice, or inline into
  the CLI and extract later?
- [ ] Resolve open question 4 — **Windows support**: in scope (named pipes) or deferred?
- [ ] Decide the **GitHub target repo** for this backlog (fork with issues enabled, or upstream)
  — see the backlog README "Filing to GitHub" section.
- [ ] Update engine.md §6 to point at the ADR instead of listing the questions as open.

## Acceptance criteria

- `docs/adr/0001-engine-architecture.md` exists and answers all four open questions with a stated rationale, not just a
  verdict.
- Each answer names the items it affects (e.g. a TCP-loopback decision changes
  [#12](0012-interceptors-auth-tracing.md) and [#13](0013-valqeron-client-library.md)).
- engine.md §6 no longer presents these as unresolved.
- Any backlog item invalidated by a decision is updated in the same PR.

## Test strategy

Documentation only — no automated tests. Review gate: a developer unfamiliar with the prior discussion can read the ADR
and correctly answer "what transport does the desktop app use, and why?"
