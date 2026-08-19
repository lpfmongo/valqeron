# Introduction

Engineering documentation for **Valqeron**.

This book documents the parts of the system that need explanation beyond what the code comments carry: the invariants,
the reasons behind the design, and the operational behaviour you get as a consequence.

## What is here today

1. **[Background Tasks Architecture](./tasks/overview.md)**: the engine's task framework architecture: how work is
   registered, scheduled, executed, tracked, and observed. Read [Overview](./tasks/overview.md) first for orientation,
   then [Architecture](./tasks/architecture.md) for the boundaries that make the framework extensible.

## Audience and conventions

Written for engineers working *on* the codebase, not for end users.

- Code references use paths relative to the workspace root.
- Diagrams are Mermaid where they show structure or flow, and ASCII where precise column alignment carries meaning.
