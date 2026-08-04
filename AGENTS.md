# AGENTS.md

Valqeron: a one-shot Rust CLI (binary `valqeron`, alias `vq`) that manages financial-instrument
issuers in a local SQLite database. Cargo workspace, edition 2024, ports-and-adapters layout.

## Crate boundaries

- `crates/core` (`valqeron-core`) — domain (`Issuer`) + ports (`IssuerRepository`, `StorageEngine`). No I/O.
- `crates/infrastructure` (`valqeron-infrastructure`) — SQLite adapter: single-writer connection behind a
  `Mutex` + `Condvar` reader pool, WAL, embedded migrations.
- `crates/config` (`valqeron-config`) — shared DB/log path resolution. CLI and engine **must** resolve
  paths through it so they agree on the database file; don't reimplement resolution in a binary.
- `crates/cli` (`valqeron-cli`) — CLI composition root (binary `valqeron`, alias `vq`).
- `crates/engine` (`valqeron-engine`) — daemon binary: single-instance lock, migrations at startup,
  periodic DB maintenance, launchd/systemd install. See "Engine" below.

**tokio is allowed in `crates/engine` only** (`current_thread`; `multi_thread` arrives with tonic).
`core`, `infrastructure`, `config`, and `cli` stay sync/async-free (see `docs/architecture/engine.md`
R6). The gRPC surface in `docs/architecture/engine.md` is still future — `valqeron-proto` and
`valqeron-client` do not exist yet.

## Commands

- Lint: `just lint` (`cargo clippy --workspace --all-targets --all-features -- -D warnings`)
- Format: `just format` / `just format-check`
- Tests (fast iteration): `cargo test --workspace` — `just test` installs cargo-llvm-cov and runs coverage instead
- Single test: `cargo test -p valqeron-cli <name_substring>`
- Loom concurrency tests (not run by plain `cargo test`): `just test-loom`
  (`RUSTFLAGS="--cfg loom" cargo test -p valqeron-infrastructure --lib loom_tests`).
  Run these after touching `crates/infrastructure/src/sqlite/connection/` — the pool swaps in
  loom's `Mutex`/`Condvar` via `#[cfg(loom)]` (`connection/sync.rs`).
- Stress/soak tests in `connection/database.rs` are `#[ignore]`d; run with `--ignored` only when relevant.

## Denied lints (will bite you)

Workspace denies `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `unreachable`,
`indexing_slicing`, `string_slice`, `arithmetic_side_effects` (use `saturating_*`/`checked_*`),
`as_conversions` (use `From`/`try_from`), `exit`, `panic_in_result_fn`. Non-test code must comply.

Test code is exempt via `#![cfg_attr(test, allow(...))]` at each crate root — extend that list rather
than sprinkling per-function allows in tests.

## SQLite & migrations

- Migrations live in `/migrations` but are **embedded at compile time**: adding one means creating the
  SQL file **and** appending it to the `MIGRATIONS` array in
  `crates/infrastructure/src/sqlite/migrations.rs`. Ordering = array index; versioning via
  `PRAGMA user_version`. Never edit an already-applied migration.
- Seed dev data: `scripts/dev_load_data.sh <path_to_db> issuer` (needs `sqlite3`).

## CLI contract

- Success: JSON envelope on stdout (`--output FILE` to redirect). Errors: RFC-7807 `ProblemDetail`
  wrapped in `{"success": false, ...}` on **stderr**, with problem-specific exit codes. Logs go to
  stderr + a JSON log file (on by default). Preserve this envelope when changing commands.
- DB path resolution: `--db-path` flag > `VALQERON_DB` env > platform data dir
  (`directories` crate, app `io.valqeron.valqeron`). `VALQERON_LOG_FILE=off` disables file logging;
  `VALQERON_LOG_LEVEL` sets file log level.
- `--dry-run` runs the real command inside a rolled-back savepoint (rejected for `init`).

## Engine

- `valqeron-engine run|install|uninstall|status`. Exit codes: `0` ok, `1` runtime/not-running,
  `2` config, `3` already running (lock held), `4` service-manager failure. Preserve these —
  launchd/systemd restart policies key off nonzero exits.
- Single-instance guard: `std::fs::File::try_lock` on `<db>.lock` next to the DB (kernel lock =
  authority; PID inside is diagnostic only). **Phase 1: guards engine-vs-engine only** — the CLI
  still opens the DB directly; do not add exclusive-ownership checks until the client library
  lands (engine.md D1/R5).
- Engine logging envs are `VALQERON_ENGINE_LOG_FILE`/`VALQERON_ENGINE_LOG_LEVEL` (own log file);
  `VALQERON_DB` is shared with the CLI. Engine stderr defaults to `info` (daemon banner), unlike
  the CLI's `warn`; `valqeron::audit` stays visible on engine stderr.
- launchd plist / systemd unit templates are **embedded** (`crates/engine/src/service/templates/`,
  `{{PLACEHOLDER}}` substitution; rendering unit-tested cross-platform). Service-manager behavior
  itself can't run in CI — manual checklists live in backlog #16/#17 (both `in-progress` until
  executed).
- Integration tests (`crates/engine/tests/lifecycle.rs`) spawn the real binary and send signals;
  they are timing-tolerant but keep them serial-ish if adding more. `tests/` files need their own
  `#![allow(...)]` header (crate-root `cfg_attr(test, ...)` doesn't reach integration tests).

## Workflow

- Conventional commit messages (`feat:`, `refactor:`, `docs:`, `chore:`), PRs to `main`.
- Backlog items in `docs/backlog/` carry YAML front matter; when working one, set
  `status: in-progress` → `done` in the same PR that lands it (see `docs/backlog/README.md`).
- CI only runs `cargo audit`; clippy/fmt/tests are enforced locally — run `just lint`,
  `just format-check`, and tests before committing. `deny.toml` supports `cargo deny check` (optional).
