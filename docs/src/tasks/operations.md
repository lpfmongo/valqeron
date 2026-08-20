# Operations

Running, tuning, and troubleshooting the task scheduler.

## Configuration

Task configuration is **registry state, not environment state**. Every task the code ships registers at every boot;
the `task_registry` row owns whether it runs (`enabled`) and its tunable settings. Code declares only defaults: on
first boot they are seeded into the row, and from then on the row is the truth — the boot reconcile rewrites identity
columns but never touches `enabled` or a non-NULL setting.

| Column | Applies to | Meaning |
|---|---|---|
| `enabled` | all | `0` stops seeding **and** dispatching; preserved across boots |
| `period_secs` | interval | seconds between runs (`db_maintenance` default 28800 = 8h, `heartbeat` 300) |
| `at_local` | recurring, sync | market-local `HH:MM` (`cvm_daily_sync` default `07:00`) |
| `recurrence` | recurring, sync | `daily` or `weekly:<mon..sun>` |
| `cooldown_secs` | sync | failure-cooldown base, seconds (default 300) |
| `max_backfill_days` | sync | unattended catch-up bound (default 90) |

`NULL` means code-owned: the next boot refills the code default (so clearing a column is "reset to default"), and a
task that pins a knob — `sd_watchdog`'s period follows the systemd `WATCHDOG_USEC` contract — keeps it `NULL` forever.
Unparseable hand-edits are logged and read as unset; they never stop the engine.

**While the engine runs, the engine owns the row.** `enabled` is read into memory at boot and mutated only through
`Scheduler::set_enabled` (the future RPC's backend): committed to the registry first, published to the in-memory gate
after, effective immediately. Settings are read once at boot and apply at the next start. Raw-SQL edits against a
*live* engine are unsupported — edit with the engine stopped. The only task-adjacent environment variables left are
the engine-wide ones (`VALQERON_ENGINE_LOG_LEVEL`, `VALQERON_ENGINE_LOG_FILE` — see
[Logging control](#logging-control)).

Disabling a task does not erase it: the catalog keeps the row (`enabled = 0`, status `disabled`) and its cursor,
stats, and history stay intact.

## Status

A task's status is **never stored** — it is derived on every read by `derive_status`, a pure function in
`crates/core/src/tasks/mod.rs` (no clock, no I/O, exhaustively unit-tested), so it cannot go stale. In-process,
`Scheduler::statuses()` assembles the full view (registration + status + next run + stats + cursor).

```mermaid
flowchart LR
    A["task_registry<br/><i>declaration + intent</i>"] --> D["derive_status()"]
    B["task_queue<br/><i>earliest live row</i>"] --> D
    C["sync_cursor<br/><i>sync tasks only</i>"] --> D
    D --> E["DerivedTaskStatus"]
    F["task_stat<br/><i>totals, last run</i>"] -.->|display| E
```

First match wins — the order encodes precedence (operator decisions beat queue contents; "executing right now" beats
sync detail; `halted` is the escalated form of `cooling_down`):

| # | Status | Condition |
|---|---|---|
| 1 | `retired` | `registered = 0` |
| 2 | `disabled` | `enabled = 0` |
| 3 | `running` | a queue row is `RUNNING` |
| 4 | `halted` | sync: `consecutive_failures ≥ 5` |
| 5 | `cooling_down` | sync: `cooldown_until > now` |
| 6 | `catching_up` | sync: a queued row is past due |
| 7 | `waiting` | queued row `scheduled_at > now` |
| 8 | `due` | queued row past due (non-sync) |
| 9 | `idle` | nothing queued — normal for ephemeral tasks, transient for durable ones |

A healthy engine mid-morning on a Wednesday, and the same engine after CVM has been failing:

| kind | status | next run | last run | outcome | runs/fails |
|---|---|---|---|---|---|
| `db_maintenance` | `waiting` | 12:38Z | 11:41Z | SUCCEEDED | 162/0 |
| `heartbeat` | `idle` | — | — | — | — |
| `task_prune` | `waiting` | Thu 03:00Z | Wed 03:00Z | SUCCEEDED | 7/0 |
| `cvm_daily_sync` | **`halted`** | after cooldown | 07:00 BRT | FAILED | 31/5 — `last_error="connect timeout"` |
| `old_cleanup` | `retired` | — | Aug 12 | SUCCEEDED | 41/2 — preserved after removal from code |

Until the `ListTasks` RPC and `vq engine tasks` land, the raw joins:

```sql
-- catalog + stats + next run
SELECT r.kind, r.category, r.schedule,
       r.registered, r.enabled,
       s.last_outcome, s.total_runs, s.total_failures, s.last_success_at,
       (SELECT MIN(scheduled_at) FROM task_queue q
         WHERE q.kind = r.kind AND q.status = 'PENDING') AS next_run_at
FROM task_registry r
LEFT JOIN task_stat s ON s.kind = r.kind
ORDER BY r.category, r.kind;

-- recent runs (history, pruned after 7 days)
SELECT kind, outcome, finished_at, attempts, duration_ms, error
FROM task_execution
ORDER BY finished_at DESC
LIMIT 20;

-- sync detail
SELECT source, through_slot, through_target,
       consecutive_failures, cooldown_until, last_outcome, last_error
FROM sync_cursor;
```

## Enable, disable, and worker control

`enabled` is operator intent, persisted across restarts. Disabling stops the *scheduling*, not the *runtime* —
restarting the engine is never required in either direction.

```text
set_enabled(kind, false) ─► registry committed, then the in-memory gate published
                         ─► seeding stops immediately (zero DB work per skipped pass)
                         ─► dispatching stops at the next claim: armed rows freeze
                         ─► an in-flight run finishes normally

set_enabled(kind, true)  ─► committed + published + seeder and dispatcher woken
                         ─► frozen rows thaw and dispatch immediately
                         ─► sync sources catch up sequentially from the cursor
```

> **Interim story:** the CLI/RPC surface is not built yet; `Scheduler::set_enabled` is its complete in-process
> backend. On a *stopped* engine the flag can be edited directly (`UPDATE task_registry SET enabled = 0, updated_at =
> strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE kind = '…';`) — the next boot reads it into memory. Editing a **live**
> engine's row is unsupported: seeding follows the in-memory flag (only the claim's SQL filter would notice).

Settings edits are stopped-engine edits too (`UPDATE task_registry SET period_secs = 600 WHERE kind =
'db_maintenance';`) and apply at the next start — triggers are built from the row once, at boot.

A disabled-then-re-enabled sync source costs nothing extra: it walks the missed periods in order through exactly the
same catch-up path as an outage. **Ephemeral tasks ignore `enabled`** — `heartbeat` and `sd_watchdog` are liveness
work; disabling the watchdog would let systemd's `WatchdogSec` kill the engine.

In-process, `Scheduler` additionally exposes **runtime worker control** — `stop(kind)` / `start(kind)` halt and
respawn one kind's seeder loop, and `kick(kind)` wakes a seeder ahead of its fallback sleep.

## Logging control

Three independent dials, no bespoke machinery:

| Dial | Mechanism | Example |
|---|---|---|
| Structured context | every run is inside `task_run{kind, category}`; audit events carry the category | `task_run{kind="cvm_daily_sync", category="FINANCE_DATA_SYNC"}` |
| `EnvFilter` per task/category | span-scoped directives in `VALQERON_ENGINE_LOG_LEVEL` (file log) or `RUST_LOG` (stderr) | `'info,[task_run{kind=cvm_daily_sync}]=debug'` · `'info,[task_run{category=FINANCE_DATA_SYNC}]=trace'` |
| `log_policy` per task | `ALL` logs every run-finished line; `FAILURES_ONLY` logs failures only (failures are **always** logged) | defaults: `FAILURES_ONLY` for ephemeral tasks, `ALL` otherwise |

Audit events all carry `target = "valqeron::audit"` and appear in the JSON file log (the CLI suppresses this target on
stderr):

| `operation` | Emitted when |
|---|---|
| `task_registry_reconcile` | boot; declared / retired / cancelled counts |
| `task_seed` | recurring trigger armed the next occurrence |
| `sync_seed` | sync trigger seeded a run (with `pending_days`) |
| `sync_not_ready` | source has not published yet |
| `sync_skip` | stale cursor clamped, history skipped |
| `sync_halted` | terminal failure; `warn`, escalating to `error` at 5 |
| `task_run` | a run finished (subject to `log_policy`; outcome `succeeded`/`not_ready`/`retry`/`failed`) |
| `task_recovery` | boot requeued/recorded orphaned `RUNNING` rows |
| `task_prune` | history rows deleted |
| `db_maintenance` | WAL checkpoint + optimize |

## Troubleshooting

**A task is not running:**

```sql
SELECT r.registered, r.enabled, r.schedule, s.last_outcome, s.last_error
FROM task_registry r LEFT JOIN task_stat s ON s.kind = r.kind
WHERE r.kind = '<kind>';
```

| Symptom | Cause | Fix |
|---|---|---|
| `registered = 0` | kind removed from code | expected after an upgrade |
| `enabled = 0` | operator disabled it | re-enable (above) |
| unexpected `schedule` | a stored settings override | clear the column to NULL to reset to the code default |
| all clean, no rows | possibly cooling down | check `sync_cursor` |

**A sync source is stuck** — query `sync_cursor` (see [Status](#status)):

| Observation | Meaning |
|---|---|
| `consecutive_failures ≥ 5` | **halted**: the same period retries after each cooldown, nothing after it runs; the next success clears the counter and catch-up resumes |
| `last_outcome = 'NOT_READY'` | source has not published yet — normal; retries after `cooldown_until` |
| `through_target` far behind | catch-up in progress, or the gap exceeded `max_backfill_days` and was skipped (look for `operation="sync_skip"`) |

**Rows are piling up** — they should not: every durable trigger gates on `exists_active`, and terminal runs leave the
queue entirely. More than a handful of `task_queue` rows means rows were inserted outside the framework
(`SELECT kind, status, COUNT(*) FROM task_queue GROUP BY kind, status;`).

**History is missing** — `task_prune` deletes `task_execution` rows after 7 days. Long-term facts live on `task_stat`
(totals, failure counts, `last_success_at`, durations) precisely because history does not survive.

**Backups:** `task_registry`, `task_stat`, and `sync_cursor` are part of the engine's operational state. Restoring a
ten-day-old backup makes sources catch up ten days of periods (bounded by `max_backfill_days`).

## Scenarios

Each is covered by an automated test; the mechanics behind them are in [Triggers](./triggers.md) and
[Architecture](./architecture.md).

| Scenario | What happens | Test |
|---|---|---|
| **Ten-day outage** | Every missed business day synced in order, one at a time, paced by handler speed; then the future slot is armed | `ten_missed_business_days_backfill_sequentially_in_order` |
| **One success per day** | A successful sync advances the cursor past today's slot and arms strictly the next business-day occurrence — the same day can never run twice successfully | `successful_run_arms_the_next_business_day_not_today` |
| **Disable Tue, re-enable Fri** | Wed's already-armed row freezes (the claim skips it); no seeding while disabled; re-enable through `set_enabled` thaws the row immediately and walks Thu+Fri via the normal catch-up path | `disabled_source_resumes_with_sequential_catchup`, `disabled_kind_seeds_nothing_until_reenabled`, `armed_rows_of_a_disabled_kind_freeze_until_reenabled` |
| **Operator tunes a schedule** | Stopped engine: `UPDATE task_registry SET period_secs = …` (or `at_local`/`recurrence`); the next boot builds the trigger from the stored value and writes the effective descriptor back | `stored_settings_override_code_defaults_at_boot` |
| **A future row comes due** | No polling: the dispatcher sleeps until the queue's earliest `scheduled_at` (watermark), capped at 60s | `future_row_dispatches_via_the_watermark_not_the_fallback` |
| **Source starts failing** | 3 attempts (+5/+10 min), terminal `FAILED` moves to history; cursor holds; cooldown doubles per failure (5→10→20→40→60 min capped); at 5 failures log escalates to `error`, status `halted`; nothing after the failing period runs | `terminal_failure_halts_on_the_same_slot_with_cooldown`, `zero_cooldown_retries_the_same_slot_and_counts_failures` |
| **Not published yet** | `NotReady`: history records `NOT_READY`, cursor holds, cooldown set, failures **not** counted, status `cooling_down`; retried after the cooldown | `not_ready_holds_the_cursor_without_burning_failures` |
| **Upgrade removes a task** | Boot reconcile retires the kind, moves its `PENDING` rows to history as cancellations (stats untouched — they never ran), preserves catalog + stats forever; a later re-add revives it with totals intact | `retired_kinds_are_marked_and_their_pending_rows_cancelled` |
| **Fresh install** | Cold start: every task registers enabled with its code defaults; a sync cursor is seeded one occurrence back — exactly one run (the latest period), then steady state | `boot_reconcile_registers_the_catalog`, `cold_start_runs_only_the_most_recent_period` |
| **Cursor 412 days behind** | Beyond `max_backfill_days`: reseeded to cold start, one run, `WARN operation="sync_skip"` — no unattended grind through a year | `stale_cursor_beyond_the_cap_skips_ahead_to_the_latest` |
| **Crash between cursor advance and completion** | Recovery requeues the row; the same period re-runs from its payload; the idempotent upsert and cursor advance are no-ops | `crash_between_handler_success_and_completion_reruns_idempotently` |
| **Crash mid-run, budget spent** | Recovery moves the row to history as `FAILED "interrupted…"` — and it counts in the stats | `startup_recovers_rows_left_running_by_a_previous_process` |
| **Worker stopped at runtime** | `stop(kind)` halts that seeder (armed rows still fire); `start(kind)` respawns it; other kinds unaffected | `worker_handles_stop_and_restart_one_kind` |
| **Two sources, one behind** | Independent cursors, seeders, cooldowns; a halted source never blocks another; the dispatcher interleaves at concurrency 2 | `two_sources_advance_independently` |
