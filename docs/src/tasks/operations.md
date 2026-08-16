# Operations

Running, tuning, and troubleshooting the task framework.

## Configuration

All configuration is environment variables, set in the service definition
(`scripts/install/*.example`). Invalid values make the engine **refuse to
start** — a misconfiguration must never silently become a default.

### Engine-wide

| Variable | Default | Meaning |
|---|---|---|
| `VALQERON_ENGINE_MAINTENANCE_INTERVAL` | `3600` | `db_maintenance` period, seconds |
| `VALQERON_ENGINE_HEARTBEAT_INTERVAL` | `300` | `heartbeat` period, seconds |
| `VALQERON_ENGINE_LOG_LEVEL` | `info` | JSON file-log filter (see below) |
| `VALQERON_ENGINE_LOG_FILE` | `<data>/engine.log` | `off`/`false`/`0`/`none` disables |

### Sync sources

Namespaced per source, so adding a source adds a namespace:

| Variable | Default | Meaning |
|---|---|---|
| `VALQERON_ENGINE_SYNC_CVM` | enabled | `off`/`false`/`0`/`none` disables |
| `VALQERON_ENGINE_SYNC_CVM_AT` | `07:00` | market-local `HH:MM` |
| `VALQERON_ENGINE_SYNC_CVM_SCHEDULE` | `daily` | `daily` or `weekly:<mon..sun>` |
| `VALQERON_ENGINE_SYNC_CVM_COOLDOWN` | `300` | failure-cooldown base, seconds |
| `VALQERON_ENGINE_SYNC_MAX_BACKFILL_DAYS` | `90` | unattended catch-up bound |

Disabling a source does not erase it: the catalog keeps a row with
`config_enabled = 0`, status `disabled`, and its cursor and history intact.

## Pause and resume

`paused` is operator intent, persisted across restarts and independent of
configuration.

> **Interim story:** there is no CLI or RPC yet, so pause is flipped by writing
> the row directly. This is safe against a live engine — WAL mode plus the
> repositories' busy-retry handle the concurrency.

```sql
-- pause
UPDATE task_registration
   SET paused = 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
 WHERE kind = 'cvm_daily_sync';

-- resume
UPDATE task_registration
   SET paused = 0, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
 WHERE kind = 'cvm_daily_sync';
```

### Semantics

```text
pause  ─► stops SEEDING within ≤60s (the seeder's fallback tick)
       ─► an already-armed row still fires
       ─► an in-flight run finishes normally

resume ─► seeding restarts at the next tick
       ─► sync sources catch up sequentially from the cursor
```

Pause stops the *scheduler*, not the *runtime*. Restarting the engine is never
required in either direction.

For a sync source, pausing and resuming days later costs nothing extra: resume
walks the missed periods in order, using exactly the same catch-up path as an
outage. See [Scenarios § Pause and resume](./scenarios.md#s2--pause-and-resume).

**Ephemeral tasks ignore `paused`.** `heartbeat` and `sd_watchdog` are liveness
work; pausing the watchdog would let systemd's `WatchdogSec` kill the engine.

## Logging control

Three independent dials, no bespoke machinery.

### 1. Structured context

Every run executes inside a span, and audit events carry the category:

```text
task_run{kind="cvm_daily_sync", category="FINANCE_DATA_SYNC"}
```

### 2. `EnvFilter` — per task or per category

```bash
# one task verbose, everything else at info
VALQERON_ENGINE_LOG_LEVEL='info,[task_run{kind=cvm_daily_sync}]=debug'

# a whole category
VALQERON_ENGINE_LOG_LEVEL='info,[task_run{category=FINANCE_DATA_SYNC}]=trace'

# stderr uses RUST_LOG with the same syntax
RUST_LOG='warn,[task_run{kind=task_prune}]=debug'
```

### 3. `log_policy` — manager noise per task

| Value | Effect |
|---|---|
| `ALL` | log every run-finished line |
| `FAILURES_ONLY` | log failures only |

Failures are **always** logged regardless of policy. Defaults:
`FAILURES_ONLY` for `heartbeat` and `sd_watchdog`, `ALL` for everything else.

### Audit events

All carry `target = "valqeron::audit"` and appear in the JSON file log (the CLI
suppresses this target on stderr):

| `operation` | Emitted when |
|---|---|
| `task_registry_reconcile` | boot; declared / retired / cancelled counts |
| `task_seed` | recurring plane armed the next occurrence |
| `sync_seed` | sync plane seeded a run (with `pending_days`) |
| `sync_not_ready` | source has not published yet |
| `sync_skip` | stale cursor clamped, history skipped |
| `sync_halted` | terminal failure; `warn`, escalating to `error` at 5 |
| `task_run` | a run finished (subject to `log_policy`) |
| `task_recovery` | boot requeued/failed orphaned `RUNNING` rows |
| `task_prune` | history rows deleted |
| `db_maintenance` | WAL checkpoint + optimize |

## Troubleshooting

### A task is not running

```sql
SELECT kind, registered, config_enabled, paused, last_outcome, last_error
FROM task_registration WHERE kind = '<kind>';
```

| Symptom | Cause | Fix |
|---|---|---|
| `registered = 0` | kind removed from code | expected after an upgrade |
| `config_enabled = 0` | disabled by env | unset the `*_SYNC_*` off-value |
| `paused = 1` | operator paused it | resume (above) |
| all clean, no rows | possibly cooling down | check `sync_cursor` |

### A sync source is stuck

```sql
SELECT source, through_slot, through_target,
       consecutive_failures, cooldown_until, last_outcome, last_error
FROM sync_cursor WHERE source = 'cvm';
```

- `consecutive_failures ≥ 5` → **halted**: the same period retries after each
  cooldown and nothing after it runs. Fix the underlying cause; the next success
  clears the counter and catch-up resumes.
- `last_outcome = 'NOT_READY'` → the source has not published yet. Normal;
  it retries after `cooldown_until`.
- `through_target` far behind → catch-up in progress, or the gap exceeded
  `max_backfill_days` and was skipped (look for `operation="sync_skip"`).

### Rows are piling up

They should not — every durable plane gates on `exists_active`. If a kind has
many `PENDING` rows, they were almost certainly inserted outside the framework.

```sql
SELECT kind, status, COUNT(*) FROM background_task GROUP BY kind, status;
```

### History is missing

`task_prune` deletes terminal rows after 7 days. Long-term facts live on the
registration (`last_run_at`, `total_runs`, `total_failures`) precisely because
history does not survive.

## Backup note

`task_registration` and `sync_cursor` are part of the engine's operational
state. Restoring a database backup restores sync progress with it — a backup
from ten days ago will make sources catch up ten days of periods (bounded by
`max_backfill_days`).
