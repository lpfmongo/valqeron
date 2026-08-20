# AGENTS.md

Valqeron: securities reference-data system in Rust. `valqeron-engine` is a daemon that exclusively owns a SQLite database and serves gRPC over a Unix domain socket; the CLI (`valqeron`, alias `vq`) is a pure client with no database code.

Engineering docs are an mdBook under `docs/` (`just docs-serve`). The Background Tasks book (`docs/src/tasks/`) is authoritative for the engine's task scheduler — read `adding-a-task.md` before adding a background task.

## Commands (Justfile)

- `just lint` — clippy, workspace, all targets/features, `-D warnings`
- `just format` / `just format-check`
- `just test` — runs `cargo llvm-cov` (installs it first; slow). For iteration use `cargo test --workspace` or `cargo test -p <crate> <test_name>`
- `just test-loom` — loom model checks (`RUSTFLAGS="--cfg loom"`, infrastructure only). **Required after touching the connection layer in `crates/infrastructure/src/sqlite/database.rs`** (pool, guards, dry-run)
- `just deps-check` — asserts `core` and `infrastructure` have no tokio/tonic in their dep tree
- `just docs-check` — mdBook build + doctest check; run after editing `docs/`
- Stress/soak tests are `#[ignore]`d; run with `cargo test -- --ignored`
- Fuzzing: `just fuzz-all` or `just --justfile crates/identifiers/Justfile fuzz <target> [seconds]` (auto-installs nightly + cargo-fuzz)

## Hard lint constraints

Workspace denies (not warns): `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `indexing_slicing`, `string_slice`, `arithmetic_side_effects`, `as_conversions`, `exit`. Write fallible code accordingly: `?`/pattern matching instead of unwrap, `.get()` instead of `[i]`, `checked_*`/`saturating_*` arithmetic, `From`/`TryFrom` instead of `as`. Generated proto code in `valqeron-proto` is the one scoped `#[allow]` exception.

## Architecture invariants (enforced, do not break)

- `core` and `infrastructure` are fully synchronous — no tokio/tonic (checked by `just deps-check`). Async lives only at the edges: `engine` (multi_thread runtime), `client` (hidden current_thread runtime), `proto` (types only).
- `infrastructure` is private to the engine; no other crate may depend on it.
- All engine storage calls go through `AsyncStorage::read`/`write` (`crates/engine/src/storage.rs`): the **whole** domain operation in one `spawn_blocking` closure, lane-bounded (read permits mirror the reader pool, 1 write permit mirrors the single writer). Never call SQLite/reader-pool code directly from an async task.
- Every mutating RPC supports `dry_run` (savepoint that always rolls back); handlers pass the flag to `AsyncStorage::write`, which routes the same closure through `StorageEngine::dry_run`. Nesting `dry_run` self-deadlocks — closures receive `&Repositories`, never the engine, precisely so handlers cannot do this.
- Errors travel as plain gRPC statuses (tonic code + message) — no structured detail payload. Bump `PROTOCOL_VERSION` in `valqeron-proto` on breaking `.proto` changes.
- Background tasks: `crate::scheduler` is the feature's only door (`Scheduler` + `TaskDefinition` builders + `TaskHandler`; the `trigger/` module is private — never import past the façade). One module per task in `crates/engine/src/tasks/` exporting `register(builder, ...)`, composed in `engine.rs`; task `kind` strings are persisted primary keys — renaming one retires the old row. Every task registers unconditionally: enablement (`enabled`) and tunable settings live on `task_registry` (boot-preserved; code declares only defaults) — never behind env vars. While the engine runs, `enabled` is engine-owned memory (`Scheduler::set_enabled`: commit, then publish); raw-SQL edits are stopped-engine only. Full checklist: `docs/src/tasks/adding-a-task.md`.

## Codegen & migrations (committed artifacts)

- Migrations: SQL files in `/migrations` are embedded at compile time. Adding one requires both the file **and** appending it to the `MIGRATIONS` array in `crates/infrastructure/src/sqlite/migrations.rs` (array position = schema version). The engine is the sole migration runner.
- Proto: compiled by `protox` in `crates/proto/build.rs` — no system `protoc` needed.
- `crates/identifiers` has committed generated tables: `src/cfi/table.rs` (from `data/cfi.json`) and `src/mic/table.rs` (from `data/mic.csv`). Never hand-edit them; after changing the data files run `just --justfile crates/identifiers/Justfile cfi-generate` / `mic-generate` (`cfi-check`/`mic-check` verify freshness).

## Testing notes

- There is no in-memory SQLite mode. Tests use `Database::open_temp()` (`TempDatabase` fixture) — real file-backed WAL, real reader pool.
- Engine integration tests live in `crates/engine/tests/` (`grpc.rs`, `lifecycle.rs`).

## Running the engine locally

- `just engine-install` / `engine-uninstall` manage a launchd/systemd **user** service. The service definitions under `scripts/install/` are machine-local (gitignored), created once from the committed `.example` files — absolute paths only, launchd expands nothing. The recipe validates that the definition's binary path exists and is executable before touching the running service.
- Exactly one engine may run per database: the loser of the lock exits with code 3 (`ALREADY_RUNNING`; 1 = runtime failure, 2 = config). A manually launched engine (e.g. `cargo run -p valqeron-engine`) will block a service-managed one and vice versa.
- Engine config is env-only (`VALQERON_DB`, `VALQERON_ENGINE_LOG_FILE`, `VALQERON_ENGINE_LOG_LEVEL`, `VALQERON_ENGINE_DURABLE` — see `pub const *_ENV` in `crates/engine/src/engine.rs`); defaults come from `ProjectDirs("io","valqeron","valqeron")`. Structured JSON logs go to `engine.log` in that data dir. Background-task enablement/scheduling is **not** env config — it lives in the `task_registry` table.

## Conventions

- Conventional commits (`feat:`, `chore:`, `ci:`, `docs:`); branches named `feat/...`, `docs/...`.
- CI only runs `cargo audit` (`.github/workflows/audit.yaml`); lint/test discipline is local via `just`.
- Licenses limited by the `deny.toml` allowlist (MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, Unicode-3.0, Zlib); new deps must comply.
