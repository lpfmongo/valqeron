# Reference

## Tuning constants

| Constant | Value | Where | Meaning |
|---|---|---|---|
| `DISPATCH_MAX_SLEEP` | 600s | `scheduler/mod.rs` | Cap on the dispatcher's watermark sleep (self-healing fallback; bounds post-suspend lateness in awake time) |
| `DISPATCH_MIN_SLEEP` | 10ms | `scheduler/mod.rs` | Floor: a stale past watermark naps, never busy-loops |
| `CLAIM_BATCH` | 8 | `scheduler/mod.rs` | Rows claimed per write-lane call |
| `EXECUTION_CONCURRENCY` | 2 | `scheduler/mod.rs` | Concurrent handlers per batch |
| `RETIRED_ERROR` | `"retired: kind no longer registered"` | `scheduler/mod.rs` | Recorded on cancelled rows |
| `INTERRUPTED_ERROR` | `"interrupted: the engine stopped…"` | `scheduler/mod.rs` | Recorded by crash recovery |
| `SEED_FALLBACK_INTERVAL` | 3600s | `scheduler/trigger/mod.rs` | Wall-clock seeder fallback cap (cooldowns wake precisely via the pass hint; = `MAX_COOLDOWN`, so every hint fits one sleep) |
| `ESCALATE_AFTER_FAILURES` | 5 | `scheduler/trigger/sync.rs` | `warn` → `error`; the `halted` threshold |
| Sync builder defaults | 3×300s retry · 300s cooldown · 90d cap | `scheduler/task.rs` | Every source inherits them unless overridden |
| `MAX_BACKOFF` | 1h | `core/src/tasks` | Retry backoff cap |
| `MAX_COOLDOWN` | 1h | `core/src/tasks` | Sync cooldown cap |
| `TASK_KIND_MAX_LEN` | 100 | `core/src/tasks` | Kind length limit |
| `MAX_SCAN_DAYS` | 366 / 400 | `core/calendar.rs`, `core/schedule.rs` | Bounded calendar scans |
| `DEFAULT_MAINTENANCE_INTERVAL` | 28800s (8h) | `tasks/system.rs` | `db_maintenance` code default |
| `DEFAULT_HEARTBEAT_INTERVAL` | 300s | `tasks/system.rs` | `heartbeat` code default |
| `TASK_RETENTION_DAYS` | 7 | `tasks/system.rs` | History kept before pruning |
| `TASK_PRUNE_AT` | 03:00 UTC | `tasks/system.rs` | Prune occurrence |
| `DRAIN_TIMEOUT` | 10s | `engine.rs` | Graceful shutdown budget |
| `RUNTIME_SHUTDOWN_TIMEOUT` | 20s | `engine.rs` | Blocking-pool drain bound |

Task settings (period, time of day, recurrence, cooldown, backfill cap) are **not** environment configuration: code
declares defaults, the `task_registry` row owns the effective values — see
[Operations § Configuration](./operations.md#configuration). Audit events are in
[Operations § Logging control](./operations.md#logging-control).

## Built-in tasks

| Kind | Category | Trigger | Descriptor | Tracking | Log policy |
|---|---|---|---|---|---|
| `db_maintenance` | ENGINE_SYSTEM | Interval | `interval:28800s±10%` | Durable | ALL |
| `heartbeat` | ENGINE_SYSTEM | Interval | `interval:300s` | Ephemeral | FAILURES_ONLY |
| `sd_watchdog` | ENGINE_SYSTEM | Interval | `interval:<Ns>` | Ephemeral | FAILURES_ONLY |
| `task_prune` | ENGINE_SYSTEM | Recurring | `recurring:daily@03:00+00:00` | Durable | ALL |
| `cvm_daily_sync` | FINANCE_DATA_SYNC | Sync | `sync:daily@07:00-03:00` | Durable | ALL |

## Enumerations

| Type | Values |
|---|---|
| `TaskCategory` | `ENGINE_SYSTEM`, `FINANCE_DATA_SYNC`, `OTHER` |
| `TaskTrigger` | `INTERVAL`, `RECURRING`, `SYNC` |
| `TaskTracking` | `DURABLE`, `EPHEMERAL` |
| `LogPolicy` | `ALL`, `FAILURES_ONLY` |
| `TaskStatus` (queue) | `PENDING`, `RUNNING` — terminal states live in the history, not the queue |
| `ExecutionOutcome` (history + stats) | `SUCCEEDED`, `NOT_READY`, `FAILED` |
| `SyncOutcomeKind` (cursor) | `SYNCED`, `NOT_READY`, `FAILED` |
| `DerivedTaskStatus` | `retired`, `disabled`, `running`, `halted`, `cooling_down`, `catching_up`, `waiting`, `due`, `idle` |
| `Recurrence` | `Daily`, `Weekly { on: Weekday }` — round-trips through `daily` / `weekly:<mon..sun>` |

## Schedule descriptors

| Trigger | Format | Example |
|---|---|---|
| Interval | `interval:{secs}s` | `interval:300s` |
| Interval + jitter | `interval:{secs}s±10%` | `interval:3600s±10%` |
| Recurring | `recurring:{recurrence}@{HH:MM}{offset}` | `recurring:daily@03:00+00:00` |
| Sync | `sync:{recurrence}@{HH:MM}{offset}` | `sync:weekly:mon@08:00-03:00` |

Display-only; never parsed back.

## Tables

| Table | Migration | Purpose | Pruned | Schema shown in |
|---|---|---|---|---|
| `task_registry` | 001 | Catalog: declaration + operator intent + settings | never | [Architecture § The data model](./architecture.md#the-data-model) |
| `task_queue` | 001 | Live work only (`PENDING`/`RUNNING`) | never needs it — terminal runs leave | — |
| `task_execution` | 001 | Terminal run history | after 7 days | — |
| `task_stat` | 001 | Prune-proof per-kind aggregates | never | — |
| `sync_cursor` | 001 | Per-source sync progress | never | [Triggers § Sync](./triggers.md#sync) |

All `STRICT, WITHOUT ROWID`, in the same SQLite file as domain data. The pre-1.0 migration history was squashed into
the single consolidated `001_create_initial_schema.sql` (the engine is pre-version-0; databases from before the
squash carry a higher `user_version` and are rejected loudly — delete and re-create them).

```sql
-- task_queue indexes
idx_task_queue_due  (status, scheduled_at)   -- claim_due
idx_task_queue_kind (kind, scheduled_at)     -- exists_active, find_active

-- task_execution indexes
idx_task_execution_kind     (kind, finished_at)   -- recent runs per kind
idx_task_execution_finished (finished_at)         -- retention pruning
```

Timestamps are RFC 3339, millisecond precision, `Z`-suffixed UTC — `2026-08-12T10:00:00.000Z` — uniform, so
lexicographic `TEXT` comparison is time order. Civil dates use `NaiveDate`'s canonical `YYYY-MM-DD`.

## Repository ports

All five live in `crates/core/src/tasks/repository.rs`:

| Port | Key methods |
|---|---|
| `BackgroundTaskRepository` (queue) | `insert`, `claim_due` (skips disabled kinds), `next_due_at` (the dispatcher's sleep watermark), `complete` (Terminal = guarded DELETE, Retry = UPDATE), `exists_active`, `find_active`, `find_by_id`, `list_queued`, `take_pending`, `requeue_interrupted`, `take_exhausted_running` |
| `TaskExecutionRepository` (history) | `insert`, `find_by_id`, `list_recent`, `prune_finished` |
| `TaskStatRepository` (aggregates) | `record_run`, `get`, `list` |
| `TaskRegistryRepository` (catalog) | `declare` (COALESCE-fills NULL settings), `retire_missing`, `get`, `list`, `is_enabled`, `set_enabled`, `update_settings` |
| `SyncCursorRepository` (progress) | `get`, `upsert` |

Each carries `#[cfg_attr(test, mockall::automock)]` **and** a hand-written `delegate_*!` macro for `Box`/`Rc`/`Arc` — a
trait change must update both.

## File map

```text
crates/core/src/tasks/
  mod.rs                   the whole domain: queue entity + retry arithmetic,
                           execution + stats records, catalog entity + settings,
                           derive_status + the status read model,
                           sync cursor + cooldown policy
  error.rs                 every task error enum
  repository.rs            the five ports + mocks + delegates

crates/infrastructure/src/sqlite/
  task/                    task_queue adapter
  task_execution/          task_execution adapter
  task_stat/               task_stat adapter
  task_registry/           task_registry adapter
  sync_cursor/             sync_cursor adapter
  migrations.rs            MIGRATIONS array (index = schema version)

crates/engine/src/
  scheduler/mod.rs         the facade: Scheduler(Builder), boot reconcile,
                           TaskWorkerManager (stoppable seeders + dispatcher),
                           TaskContextRunner (storage gateway + execution)
  scheduler/task.rs        TaskDefinition + typed builders + TaskHandler
  scheduler/trigger/       PRIVATE — trait Trigger + interval/recurring/sync
  tasks/system.rs          ENGINE_SYSTEM tasks
  tasks/cvm.rs             CVM sync source
  engine.rs                composition + config resolution

migrations/
  001_create_initial_schema.sql            the consolidated schema (pre-1.0
                                           history squashed); future changes
                                           append 002, 003, …
```

## Invariants

1. `core` and `infrastructure` stay free of tokio/tonic — enforced by `just deps-check`.
2. `crate::scheduler` is the only door: `trigger/` is private, `scheduler/` imports nothing from `tasks/`, and a grep
   for `cvm` in `scheduler/` returns only test fixture strings.
3. A migration requires both the `.sql` file **and** an append to `MIGRATIONS`; the array index is the schema version.
4. Engine tables share one SQLite file with domain data — required for atomic data + cursor commits (WAL cannot commit
   across attached files).
5. Every durable trigger gates on `exists_active`: one run per kind in flight.
6. The enable gate is the in-memory image of the committed registry flag, checked before the seed transaction; writes
   go through `Scheduler::set_enabled` (commit first, publish after), and the claim's SQL filter backstops it.
7. A terminal completion is one transaction: guarded queue delete + history insert + stats fold — applied only when the
   delete matched.
8. Status is derived, never stored; stats are folded forward, never recomputed from history.
9. Sync handlers must be idempotent (at-least-once execution).
10. Handlers derive dates from `RunWindow`, never from the clock.
11. Stats count *runs*: recovery-interrupted terminal rows count; retirement cancellations do not.
12. Every task registers unconditionally; the registry row (not env, not code) decides whether and how it runs. The
    reconcile rewrites identity columns, COALESCE-fills NULL settings with code defaults, and never touches `enabled`
    or non-NULL settings.

## Not yet built

| Item | Notes |
|---|---|
| `ListTasks` RPC + `vq engine tasks` | `Scheduler::statuses()` is the in-process read model; additive proto change, no `PROTOCOL_VERSION` bump |
| Enable/disable + settings RPC | `Scheduler::set_enabled` is the complete backend (commit + publish + wake); settings edits need only `update_settings` + a restart |
| One-shot range backfill | `backfill_run` table + one reconciler branch |
| B3 holiday calendar | Seam is `MarketCalendar::is_business_day` |
| Real CVM ingestion | Changes only `tasks/cvm.rs` (+ its own tables); the handler becomes a struct `TaskHandler` |
| Live settings reload | Settings are read once at boot; a running engine picks up `enabled` immediately but schedule changes at the next start |
| IANA timezones | Fixed offsets today; exact for Brazil (no DST since 2019) |
