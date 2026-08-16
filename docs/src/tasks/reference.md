# Reference

## Tuning constants

| Constant | Value | Where | Meaning |
|---|---|---|---|
| `DISPATCH_POLL_INTERVAL` | 1s | `tasks/mod.rs` | Dispatcher fallback poll (clock-due rows) |
| `CLAIM_BATCH` | 8 | `tasks/mod.rs` | Rows claimed per write-lane call |
| `EXECUTION_CONCURRENCY` | 2 | `tasks/mod.rs` | Concurrent handlers per batch |
| `RETIRED_ERROR` | `"retired: kind no longer registered"` | `tasks/mod.rs` | Recorded on cancelled rows |
| `RECONCILE_INTERVAL` | 60s | `plane/mod.rs` | Wall-clock seeder fallback tick |
| `ESCALATE_AFTER_FAILURES` | 5 | `plane/sync.rs` | `warn` → `error`; the `halted` threshold |
| `MAX_BACKOFF` | 1h | `core/task.rs` | Retry backoff cap |
| `MAX_COOLDOWN` | 1h | `core/sync/cooldown.rs` | Sync cooldown cap |
| `TASK_KIND_MAX_LEN` | 100 | `core/task.rs` | Kind length limit |
| `MAX_SCAN_DAYS` | 366 / 400 | `core/calendar.rs`, `core/schedule.rs` | Bounded calendar scans |
| `TASK_RETENTION_DAYS` | 7 | `jobs/system.rs` | History kept before pruning |
| `TASK_PRUNE_AT` | 03:00 UTC | `jobs/system.rs` | Prune occurrence |
| `DRAIN_TIMEOUT` | 10s | `engine.rs` | Graceful shutdown budget |
| `RUNTIME_SHUTDOWN_TIMEOUT` | 20s | `engine.rs` | Blocking-pool drain bound |

## Environment variables

| Variable | Default | Invalid value |
|---|---|---|
| `VALQERON_ENGINE_MAINTENANCE_INTERVAL` | `3600` | refuse to start |
| `VALQERON_ENGINE_HEARTBEAT_INTERVAL` | `300` | refuse to start |
| `VALQERON_ENGINE_SYNC_CVM` | enabled | — (`off`/`false`/`0`/`none` disables) |
| `VALQERON_ENGINE_SYNC_CVM_AT` | `07:00` | refuse to start |
| `VALQERON_ENGINE_SYNC_CVM_SCHEDULE` | `daily` | refuse to start |
| `VALQERON_ENGINE_SYNC_CVM_COOLDOWN` | `300` | refuse to start |
| `VALQERON_ENGINE_SYNC_MAX_BACKFILL_DAYS` | `90` | refuse to start |

Unset or empty always means "use the default"; only a *set, non-empty, invalid*
value is fatal.

## Built-in tasks

| Kind | Category | Plane | Descriptor | Tracking | Log policy |
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
| `TaskTier` | `INTERVAL`, `RECURRING`, `SYNC` |
| `TaskTracking` | `DURABLE`, `EPHEMERAL` |
| `LogPolicy` | `ALL`, `FAILURES_ONLY` |
| `RunOutcome` | `SUCCEEDED`, `FAILED` |
| `TaskStatus` | `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED` |
| `SyncOutcomeKind` | `SYNCED`, `NOT_READY`, `FAILED` |
| `DerivedTaskStatus` | `retired`, `disabled`, `paused`, `running`, `halted`, `cooling_down`, `catching_up`, `waiting`, `due`, `idle` |
| `Recurrence` | `Daily`, `Weekly { on: Weekday }` |

## Schedule descriptors

| Plane | Format | Example |
|---|---|---|
| Interval | `interval:{secs}s` | `interval:300s` |
| Interval + jitter | `interval:{secs}s±10%` | `interval:3600s±10%` |
| Recurring | `recurring:{recurrence}@{HH:MM}{offset}` | `recurring:daily@03:00+00:00` |
| Sync | `sync:{recurrence}@{HH:MM}{offset}` | `sync:weekly:mon@08:00-03:00` |

Display-only; never parsed back.

## Tables

| Table | Migration | Purpose | Pruned |
|---|---|---|---|
| `background_task` | 003 | Queue + run history | after 7 days (terminal rows) |
| `sync_cursor` | 004 | Per-source sync progress | never |
| `task_registration` | 005 | Task catalog + intent + summary | never |

All `STRICT, WITHOUT ROWID`, in the same SQLite file as domain data.

### `background_task` indexes

```sql
idx_background_task_due  (status, scheduled_at)   -- claim_due
idx_background_task_kind (kind, scheduled_at)     -- exists_active, find_active
```

### Timestamp format

RFC 3339, millisecond precision, `Z`-suffixed UTC —
`2026-08-12T10:00:00.000Z`. Uniform, so lexicographic `TEXT` comparison is time
order. Civil dates use `NaiveDate`'s canonical `YYYY-MM-DD`.

## Repository ports

| Port | Key methods |
|---|---|
| `BackgroundTaskRepository` | `insert`, `claim_due`, `complete`, `exists_active`, `find_active`, `fail_pending`, `reset_stale_running`, `prune_finished`, `list_recent`, `find_by_id` |
| `SyncCursorRepository` | `get`, `upsert` |
| `TaskRegistrationRepository` | `declare`, `retire_missing`, `get`, `list`, `is_paused`, `set_paused`, `record_run` |

Each carries `#[cfg_attr(test, mockall::automock)]` **and** a hand-written
`delegate_*!` macro for `Box`/`Rc`/`Arc` — a trait change must update both.

## Audit operations

`task_registry_reconcile` · `task_seed` · `task_run` · `task_recovery` ·
`task_prune` · `db_maintenance` · `sync_seed` · `sync_not_ready` · `sync_skip` ·
`sync_halted`

All under `target = "valqeron::audit"`.

## File map

```text
crates/core/src/
  calendar.rs              MarketCalendar, business-day math
  schedule.rs              Recurrence, Schedule, TargetPeriod, descriptors
  task.rs                  BackgroundTask, retry arithmetic
  task/repository.rs       queue port
  sync.rs                  SyncCursor, SyncOutcome, SyncSource
  sync/cooldown.rs         CooldownPolicy
  sync/repository.rs       cursor port
  task_registration.rs     catalog entity, derive_status
  task_registration/service.rs   list_task_statuses (read model)

crates/infrastructure/src/sqlite/
  task/                    background_task adapter
  sync_cursor/             sync_cursor adapter
  task_registration/       task_registration adapter
  migrations.rs            MIGRATIONS array (index = schema version)

crates/engine/src/
  tasks/mod.rs             manager kernel
  tasks/plane/mod.rs       Plane trait, contract types
  tasks/plane/interval.rs
  tasks/plane/recurring.rs
  tasks/plane/sync.rs
  jobs/system.rs           ENGINE_SYSTEM tasks
  jobs/cvm.rs              CVM sync source
  engine.rs                composition + config resolution

migrations/
  003_create_background_task_schema.sql
  004_create_sync_cursor_schema.sql
  005_create_task_registration_schema.sql
```

## Invariants

1. `core` and `infrastructure` stay free of tokio/tonic — enforced by
   `just deps-check`.
2. `tasks/` imports nothing from `jobs/`; the manager imports nothing
   tier-specific from `plane/sync.rs` beyond the trait object.
3. A migration requires both the `.sql` file **and** an append to `MIGRATIONS`;
   the array index is the schema version.
4. Engine tables share one SQLite file with domain data — required for atomic
   data + cursor commits (WAL cannot commit across attached files).
5. Every durable plane gates on `exists_active`: one run per kind in flight.
6. The pause gate and the plane's reconcile share one transaction.
7. Completion and `record_run` share one transaction.
8. Status is derived, never stored.
9. Sync handlers must be idempotent (at-least-once execution).
10. Handlers derive dates from `RunWindow`, never from the clock.

## Not yet built

| Item | Notes |
|---|---|
| `ListTasks` RPC + `vq engine tasks` | Additive proto change; no `PROTOCOL_VERSION` bump |
| Pause/resume RPC | Would also enable cancelling an armed row on pause |
| One-shot range backfill | `backfill_run` table + one reconciler branch |
| B3 holiday calendar | Seam is `MarketCalendar::is_business_day` |
| Real CVM ingestion | Changes only `jobs/cvm.rs` (+ its own tables) |
| DB-driven schedule overrides | Env remains the config source today |
| IANA timezones | Fixed offsets today; exact for Brazil (no DST since 2019) |
