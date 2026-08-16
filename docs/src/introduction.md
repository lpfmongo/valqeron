# Introduction

Engineering documentation for **Valqeron** — a securities reference-data
platform built as a Rust workspace: a user-scoped daemon (`valqeron-engine`)
that exclusively owns an embedded SQLite database and serves gRPC over a Unix
domain socket, plus a thin CLI client (`vq`).

This book documents the parts of the system that need explanation beyond what
the code comments carry: the invariants, the reasons behind the design, and the
operational behaviour you get as a consequence.

## What is here today

**[Background Tasks](./tasks/overview.md)** — the engine's task framework:
how work is registered, scheduled, executed, tracked, and observed. Read
[Overview](./tasks/overview.md) first for orientation, then
[Architecture](./tasks/architecture.md) for the boundaries that make the
framework extensible.

## Audience and conventions

Written for engineers working *on* the codebase, not for end users.

- Code references use paths relative to the workspace root
  (`crates/engine/src/tasks/mod.rs`) and avoid line numbers, which drift.
- Diagrams are Mermaid where they show structure or flow, and ASCII where
  precise column alignment carries meaning.
- SQL is shown against the engine's own tables, which live in the same SQLite
  file as domain data (see
  [Catalog § Why one file](./tasks/catalog.md#why-one-file)).

## Related material

The workspace-level contributor guide (build commands, lint constraints,
architecture invariants) lives in `AGENTS.md` at the repository root. The
service definitions used to install the engine are in `scripts/install/`.
