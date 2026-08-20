# Valqeron Background Tasks — Architecture Walkthrough

> Personal reference note. The canonical documentation is the mdBook under
> `docs/src/tasks/` (`just docs-serve`): Architecture, Triggers, Operations,
> Reference, and the Adding a Task checklist.

## The big picture

Three-role kernel behind one façade (`crate::scheduler`), with SQLite as the
single source of truth. Everything is **event-driven**: rows in the DB are the
durable alarms, tokio `Notify`/watch channels are the wakes, and the only
polling left is two capped self-healing sweeps.

```text
                        ┌─────────────────────────────────────────────────────────────┐
                        │                      valqeron-engine                        │
                        │                                                             │
  engine.rs composes:   │  Scheduler::builder().task(..).task(..).start(storage)      │
                        └───────────────────────────┬─────────────────────────────────┘
                                                    │ boot
                    ┌───────────────────────────────▼───────────────────────────────┐
                    │ MANAGER  (Scheduler / SchedulerBuilder)          mod.rs        │
                    │  1 validate+dedupe  2 crash recovery  3 registry reconcile     │
                    │  set_enabled() ── DB commit → publish atomic → wake ──┐        │
                    │  statuses()  stop()/start()  drain(deadline)          │        │
                    └───────┬──────────────────────────────┬────────────────┼────────┘
                            │ spawns                       │ spawns         │
        ┌───────────────────▼────────────────┐   ┌─────────▼────────────────▼───────┐
        │ SEEDERS (one loop per kind)        │   │ DISPATCHER (single loop)         │
        │                                    │   │                                  │
        │ select! {                          │   │ boot sweep: drain_due()          │
        │   shutdown,                        │   │ loop select! {                   │
        │   trigger.wake_notified(),  ◄──────┼─┐ │   shutdown,                      │
        │   sleep_until(next_pass)           │ │ │   runner.wake.notified(), ◄──┐   │
        │ }                                  │ │ │   sleep(dispatch_sleep())    │   │
        │ → seed_pass / inline_run           │ │ │ } → drain_due()              │   │
        │ next_pass = hint.min(cap=1h)       │ │ └───────────┬──────────────────┼───┘
        └──────────────┬─────────────────────┘ │             │                  │
                       │                       │             │                  │
        ┌──────────────▼───────────────────────┴─────────────▼──────────────────┴───┐
        │ RUNNER (TaskContextRunner)                                                │
        │  enabled: AtomicBool gate (per kind) ── memory image of registry flag     │
        │  next_due: Mutex<Option<DateTime>>  ── dispatcher's sleep watermark       │
        │  wake: Notify ── seed → dispatcher                                        │
        │  seed_pass → trigger.reconcile   execute_one → handler → interpret        │
        └──────────────────────────────┬────────────────────────────────────────────┘
                                       │ every DB touch: one closure, one lane
        ┌──────────────────────────────▼────────────────────────────────────────────┐
        │ AsyncStorage.write("op", dry_run=false, |repos| ...)   (1 write permit)   │
        └──────────────────────────────┬────────────────────────────────────────────┘
                                       ▼ SQLite (WAL)
   ┌────────────────┬──────────────────┬───────────────────┬────────────┬───────────┐
   │ task_registry  │ background_task  │ task_execution    │ task_stat  │sync_cursor│
   │ catalog:       │ queue:           │ history: every    │ per-kind   │ sync tier │
   │ enabled,       │ PENDING/RUNNING  │ terminal run      │ aggregates │ position +│
   │ settings,      │ rows = durable   │ (audit trail)     │            │ cooldown  │
   │ descriptor     │ alarms           │                   │            │ state     │
   └────────────────┴──────────────────┴───────────────────┴────────────┴───────────┘
```

**Trigger tiers** (private behind `trigger/`; the kernel never sees cursors or
payloads):

| Tier | Semantics | Registered tasks |
|---|---|---|
| `Interval` | monotonic since boot, ±10% jitter option; `Durable` (rows) or `Ephemeral` (inline, no row) | `db_maintenance` 8h durable; `heartbeat` 300s ephemeral; `sd_watchdog` ephemeral, `.pinned()` (period from `WATCHDOG_USEC`, never DB-tunable) |
| `Recurring` | wall-clock business-day schedule; next occurrence always pre-armed as a row | `task_prune` daily 03:00 UTC |
| `Sync` | cursor-driven, sequential catch-up, cooldown + halt-on-failure | `cvm_daily_sync` daily 07:00 São Paulo, B3 calendar |

## Step-by-step behavior

### Boot (`SchedulerBuilder::start`, `crates/engine/src/scheduler/mod.rs:147`)

1. **Validate + dedupe** definitions (a kind registered twice would race to
   seed).
2. **Crash recovery** (`mod.rs:365`): `RUNNING` rows are orphans (the
   single-instance lock guarantees no live owner) — attempts left → requeued
   `PENDING`; exhausted → moved to history as `Failed` + stats.
3. **Registry reconcile** (`mod.rs:415`), one transaction: for each kind, read
   stored row → adopt its `enabled` → fold stored settings into the code
   config (`with_settings`: non-NULL columns override, NULL keeps code
   defaults) → `declare` upsert (descriptor computed from the *effective*
   config). Kinds that vanished from code are **retired** and their pending
   rows cancelled to history (no stats — a row that never ran is not a run).
4. Build one `Registration` per kind: `enabled: AtomicBool` (from the
   committed row), handler, trigger runtime.
5. Spawn the **dispatcher** (with an immediate boot sweep — recovery may have
   requeued due-now rows, and the watermark must come from the real queue) and
   **one seeder per kind**.

### Seeding (per-kind loop, `mod.rs:897`)

1. Sleep until: a **wake** (`trigger.wake_notified()`) or **next_pass** — the
   trigger's hint clamped to `SEED_FALLBACK_INTERVAL` (1h). First pass is
   immediate at boot.
2. `Ephemeral` kinds run **inline** on the seeder task itself (no row, runs of
   one kind never overlap; deliberately *not* gated on `enabled` — liveness
   work).
3. Durable kinds run `seed_pass` (`mod.rs:502`): **memory gate first** —
   `enabled.load()` false ⇒ zero DB. Otherwise one write transaction runs
   `trigger.reconcile(repos, now)`:
   - *Recurring*: is the next occurrence armed? If not, insert a `PENDING`
     row with `scheduled_at` = next schedule slot.
   - *Sync*: read `sync_cursor` → first uncovered occurrence strictly after
     the cursor → if none pending and not cooling down, arm one row whose
     **payload carries the target period** (a catch-up run days late still
     targets its own dates); catch-up is one occurrence at a time.
4. Result: `Seeded` → `wake.notify_one()` (dispatcher claims immediately);
   `Idle { next_pass_at }` → precise sleep hint (a cooldown expiry, or
   `Some(now)` after a slot advance to chain immediately).

### Dispatching (single loop, `mod.rs:929`)

1. Sleep `dispatch_sleep()` = time until the **watermark** (queue's earliest
   `scheduled_at`), floored 10ms, capped `DISPATCH_MAX_SLEEP` (600s) — or wake
   early on `runner.wake`.
2. `drain_due` (`mod.rs:582`): loop —
   - One write transaction: `claim_due(now, 8)` flips due `PENDING` →
     `RUNNING` (version-guarded), **skipping kinds whose registry row is
     disabled** (SQL backstop; frozen rows thaw on re-enable). If empty, the
     *same closure* runs `next_due_at()` → refresh the watermark → return.
     Retries and rows armed during this drain are all visible to that final
     query.
   - Execute the batch with bounded parallelism (`JoinSet` + semaphore of 2);
     all outcomes recorded before the next claim, so zero-delay retries are
     picked up in the same drain, bounded by attempt budgets.

### One run (`execute_one`, `mod.rs:639`)

1. `trigger.window_for(payload)` → typed `RunWindow` (sync: the slot + civil
   dates from the *row*, never from `now`).
2. Handler runs inside a `task_run` span → `TaskOutcome::{Done, NotReady,
   Failed}`.
3. `trigger.interpret` applies tier side effects: sync **advances the cursor**
   on `Done` (this is why ≤1 success/day is structural) or **holds it + sets a
   cooldown** on `NotReady`.
4. Completion, one transaction: terminal runs **move** — guarded queue delete
   + history insert + stats fold; retryable failures go back `PENDING` with
   `retry_at` (exponential backoff, capped at 1h).
5. Hooks: terminal failure → `trigger.on_terminal` (sync cooldown escalation;
   halts after `ESCALATE_AFTER_FAILURES`), then `trigger.wake()` — the seeder
   re-arms the next occurrence within milliseconds.

### Operator control (`set_enabled`, `mod.rs:277`)

DB commit → publish to the `AtomicBool` → wake seeder (re-enable seeds
immediately) + dispatcher (frozen rows thaw) → audit log. Memory is strictly a
cache of committed state; raw-SQL edits are stopped-engine only.

### Why an idle engine costs ~220 DB calls/day

Every scheduled event is wake- or watermark-driven and exact. The caps (600s
dispatcher, 1h seeders) exist only to self-heal abnormal cases — clock jumps
across laptop suspend, a transiently failed pass — degrading to *lateness
bounded by one cap*, never a missed run, because the rows themselves are
durable.

| Actor | Cadence | Calls/day |
|---|---|---|
| Dispatcher fallback sweep | 600s cap | 144 |
| task_prune + cvm seeders | 3600s cap × 2 | 48 |
| db_maintenance seeder | 8h period | 3 |
| Actual runs (claim/complete/cursor txs) | event-driven | ~25 |
| **Total** | | **~220** (was ~89k with 1s polling) |
