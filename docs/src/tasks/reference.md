# Reference

## Tuning constants

| Constant | Value | Where | Meaning |
|---|---|---|---|
| `DISPATCH_POLL_INTERVAL` | 1s | `tasks/mod.rs` | Dispatcher fallback poll (clock-due rows) |
| `CLAIM_BATCH` | 8 | `tasks/mod.rs` | Rows claimed per write-lane call |
| `EXECUTION_CONCURRENCY` | 2 | `tasks/mod.rs` | Concurrent handlers per batch |
| `RETIRED_ERROR` | `"retired: kind no longer registered"` | `tasks/mod.rs` | Recorded on cancelled rows |
| `INTERRUPTED_ERROR` | `"interrupted: the engine stopped…"` | `tasks/mod.rs` | Recorded by crash recovery |
| `RECONCILE_INTERVAL` | 60s | `tasks/trigger/mod.rs` | Wall-clock seeder fallback tick |
| `ESCALATE_AFTER_FAILURES` | 5 | `tasks/trigger/sync.rs` | `warn` → `error`; the `halted` threshold |
| Sync builder defaults | 3×300s retry · 300s cooldown · 90d cap | `tasks/task.rs` | Every source inherits them unless overridden |
| `MAX_BACKOFF` | 1h | `core/src/tasks` | Retry backoff cap |
| `MAX_COOLDOWN` | 1h | `core/src/tasks` | Sync cooldown cap |
| `TASK_KIND_MAX_LEN` | 100 | `core/src/tasks` | Kind length limit |
| `MAX_SCAN_DAYS` | 366 / 400 | `core/calendar.rs`, `core/schedule.rs` | Bounded calendar scans |
| `TASK_RETENTION_DAYS` | 7 | `jobs/system.rs` | History kept before pruning |
| `TASK_PRUNE_AT` | 03:00 UTC | `jobs/system.rs` | Prune occurrence |
| `DRAIN_TIMEOUT` | 10s | `engine.rs` | Graceful shutdown budget |
| `RUNTIME_SHUTDOWN_TIMEOUT` | 20s | `engine.rs` | Blocking-pool drain bound |

Environment variables are documented in [Operations § Configuration](./operations.md#configuration); audit events in
[Operations § Logging control](./operations.md#logging-control).

## Built-in tasks

| Kind | Category | Trigger | Descriptor | Tracking | Log policy |
|---|---|---|---|---|---|
| `db_maintenance` | ENGINE_SYSTEM | Interval | `interval:3600s±10%` | Durable | ALL |
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
| `DerivedTaskStatus` | `retired`, `disabled`, `paused`, `running`, `halted`, `cooling_down`, `catching_up`, `waiting`, `due`, `idle` |
| `Recurrence` | `Daily`, `Weekly { on: Weekday }` |

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
| `task_registry` | 006 | Catalog: declaration + operator intent | never | [Architecture § The data model](./architecture.md#the-data-model) |
| `task_queue` | 006 | Live work only (`PENDING`/`RUNNING`) | never needs it — terminal runs leave | — |
| `task_execution` | 006 | Terminal run history | after 7 days | — |
| `task_stat` | 006 | Prune-proof per-kind aggregates | never | — |
| `sync_cursor` | 004 | Per-source sync progress | never | [Triggers § Sync](./triggers.md#sync) |

All `STRICT, WITHOUT ROWID`, in the same SQLite file as domain data. Migration 006 reorganized the original fused
`background_task` + `task_registration` pair into the four task tables, carrying all data over.

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
| `BackgroundTaskRepository` (queue) | `insert`, `claim_due`, `complete` (Terminal = guarded DELETE, Retry = UPDATE), `exists_active`, `find_active`, `find_by_id`, `list_queued`, `take_pending`, `requeue_interrupted`, `take_exhausted_running` |
| `TaskExecutionRepository` (history) | `insert`, `find_by_id`, `list_recent`, `prune_finished` |
| `TaskStatRepository` (aggregates) | `record_run`, `get`, `list` |
| `TaskRegistrationRepository` (catalog) | `declare`, `retire_missing`, `get`, `list`, `is_paused`, `set_paused` |
| `SyncCursorRepository` (progress) | `get`, `upsert` |

Each carries `#[cfg_attr(test, mockall::automock)]` **and** a hand-written `delegate_*!` macro for `Box`/`Rc`/`Arc` — a
trait change must update both.

## File map

```text
crates/core/src/tasks/
  mod.rs                   the whole domain: queue entity + retry arithmetic,
                           execution + stats records, catalog entity,
                           derive_status + the status read model,
                           sync cursor + cooldown policy
  error.rs                 every task error enum
  repository.rs            the five ports + mocks + delegates

crates/infrastructure/src/sqlite/
  task/                    task_queue adapter
  task_execution/          task_execution adapter
  task_stat/               task_stat adapter
  task_registration/       task_registry adapter
  sync_cursor/             sync_cursor adapter
  migrations.rs            MIGRATIONS array (index = schema version)

crates/engine/src/
  tasks/mod.rs             the facade: BackgroundTasks(Builder),
                           TaskWorkerManager (stoppable seeders + dispatcher),
                           TaskContextRunner (storage gateway + execution)
  tasks/task.rs            TaskDefinition + typed builders + TaskHandler
  tasks/trigger/           PRIVATE — trait Trigger + interval/recurring/sync
  jobs/system.rs           ENGINE_SYSTEM tasks
  jobs/cvm.rs              CVM sync source
  engine.rs                composition + config resolution

migrations/
  003_create_background_task_schema.sql    superseded by 006
  004_create_sync_cursor_schema.sql
  005_create_task_registration_schema.sql  superseded by 006
  006_reorganize_task_schema.sql           the four task tables
```

## Invariants

1. `core` and `infrastructure` stay free of tokio/tonic — enforced by `just deps-check`.
2. `crate::tasks` is the only door: `trigger/` is private, `tasks/` imports nothing from `jobs/`, and a grep for `cvm`
   in `tasks/` returns only test fixture strings.
3. A migration requires both the `.sql` file **and** an append to `MIGRATIONS`; the array index is the schema version.
4. Engine tables share one SQLite file with domain data — required for atomic data + cursor commits (WAL cannot commit
   across attached files).
5. Every durable trigger gates on `exists_active`: one run per kind in flight.
6. The pause gate and the trigger's reconcile share one transaction.
7. A terminal completion is one transaction: guarded queue delete + history insert + stats fold — applied only when the
   delete matched.
8. Status is derived, never stored; stats are folded forward, never recomputed from history.
9. Sync handlers must be idempotent (at-least-once execution).
10. Handlers derive dates from `RunWindow`, never from the clock.
11. Stats count *runs*: recovery-interrupted terminal rows count; retirement cancellations do not.

## Not yet built

| Item | Notes |
|---|---|
| `ListTasks` RPC + `vq engine tasks` | `BackgroundTasks::statuses()` is the in-process read model; additive proto change, no `PROTOCOL_VERSION` bump |
| Pause/resume RPC | Flip `paused` + `kick(kind)`; worker `stop`/`start` already exist for runtime control |
| One-shot range backfill | `backfill_run` table + one reconciler branch |
| B3 holiday calendar | Seam is `MarketCalendar::is_business_day` |
| Real CVM ingestion | Changes only `jobs/cvm.rs` (+ its own tables); the handler becomes a struct `TaskHandler` |
| DB-driven schedule overrides | Env remains the config source today |
| IANA timezones | Fixed offsets today; exact for Brazil (no DST since 2019) |
