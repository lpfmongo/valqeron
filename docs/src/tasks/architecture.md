# Architecture

Three layers, two hard boundaries: **the scheduler knows nothing about any individual task**, and **tasks know nothing
about scheduling**. Adding a data source (ANBIMA, B3, SEC) is one new file plus one composition line — it inherits
catch-up, cooldowns, enable/disable, status, and log filtering unchanged.

`crate::scheduler` is the feature's only door. Task modules and the engine composition see the façade — `Scheduler`,
`TaskDefinition` builders, and the handler contract — and nothing else; the `trigger` module is private.

```text
═══════════ LAYER 1: TASK IMPLEMENTATIONS — the scheduler must NOT know these ═════════
                                                                                       
  tasks/system.rs                    tasks/cvm.rs                   tasks/<future>.rs  
  ENGINE_SYSTEM                      FINANCE_DATA_SYNC              OTHER              
  db_maintenance, heartbeat,         CVM handler                    e.g. report_export 
  task_prune, sd_watchdog            + later: its own tables                           
                                       (migration + repo slot,                         
  each exports: register(builder)       referenced ONLY here)                          
        │                                   │                            │             
════════╪═══════════════════════════════════╪════════════════════════════╪════════════
        ▼   the ONLY contract: TaskDefinition (builder) + TaskHandler                   
═══════════ LAYER 2: TRIGGERS — scheduling semantics, private module ══════════════════
                                                                                       
  trigger/interval.rs       trigger/recurring.rs        trigger/sync.rs                
  monotonic ticker,         wall-clock occurrence,      cursor-driven: catch-up,       
  durable | ephemeral       seed-ahead alarm            cooldown, halt, NotReady;      
                                                        owns sync_cursor + payload     
        │                          │                            │                      
        └────────────┬─────────────┴──────────────┬─────────────┘                      
                     ▼  trait Trigger: cadence · mode · reconcile · window_for          
                        · interpret · on_terminal · wake                               
═══════════ LAYER 3: THE RUNTIME — generic, knows nothing above ═══════════════════════
                                                                                       
   Scheduler(Builder)         TaskWorkerManager        TaskContextRunner               
   catalog reconcile          one stoppable seeder     storage gateway; seed passes,   
   (settings merge),          loop per kind + the      claim, execute in task_run      
   crash recovery,            dispatcher; per-task     spans, terminal move, trigger   
   start/drain facade,        stop · start · drain     hooks                           
   stop/start/kick/statuses                                                            
                                                                                       
═══════════════════════════ ENGINE DB (valqeron.db, WAL) ══════════════════════════════
  task_registry     task_queue      task_execution   task_stat      <task-owned tables>
  catalog + intent  live work only  run history      aggregates,    invisible to L2/L3 
  + settings                        (pruned @ 7d)    never pruned                      
```

| Layer | Knows | Must never know |
|---|---|---|
| **Runtime** | kind, category, trigger config, log policy, an opaque `Arc<dyn TaskHandler>` | handler internals, payload formats, cursors, CVM, any task table |
| **Trigger** | its own scheduling semantics and state table (sync → `sync_cursor`) | what handlers do, task-owned tables |
| **Task** | its handler, its config, its own tables | other tasks, runtime internals |

Enforced structurally, not by convention: the `trigger` module is private to `scheduler/`, `scheduler/` imports
nothing from `tasks/`, and a grep for `cvm` in `scheduler/` returns only test fixture strings. `core` holds every pure
decision (business-day math, retry backoff, cooldown growth, status derivation) in one module — `core/src/tasks/` —
and stays tokio-free, enforced by `just deps-check`; the engine owns the clocks, the loops, and the I/O.

## The contract

Everything crossing the Layer 1 ⇄ Layer 2 boundary:

```rust,ignore
// How a task runs: object-safe, so trivial tasks stay closures (blanket
// impl) and stateful ones implement it on a struct.
trait TaskHandler: Send + Sync + 'static {
    fn run<'a>(&'a self, ctx: TaskContext) -> BoxFuture<'a, TaskOutcome>;
}

// What the framework hands a handler
TaskContext {
    storage: AsyncStorage,
    window: RunWindow,           // None | Period { slot, target }
}

// What a handler answers
enum TaskOutcome {
    Done,
    NotReady { retry_after_secs: u32 },
    Failed(String),
}
```

One handler contract for every task under every trigger. Handlers never parse payloads — the trigger does that once
and hands over typed values in `window`. What a task *is* travels as a `TaskDefinition`, produced by one typed builder
per trigger kind (see [Adding a Task](./adding-a-task.md)); the builders carry the framework defaults and capture the
task's code-default `TaskSettings`, which the boot reconcile seeds into the registry.

The seam that erases trigger-specific knowledge from the runtime is the `Trigger` trait
(`crates/engine/src/scheduler/trigger/mod.rs`):

```rust,ignore
trait Trigger: Send + Sync {
    fn cadence(&self) -> (tokio::time::Instant, Duration);
    fn mode(&self) -> TickMode;                    // Seed | Inline

    // Runs INSIDE the runner's single write transaction, after the
    // enable gate — so gate and insert cannot race.
    fn reconcile(&self, repos: &Repositories<..>, now) -> Result<SeedPass, StorageError>;

    fn window_for(&self, payload: Option<&str>) -> Result<RunWindow, String>;
    // Ok carries what the history records: Completed or NotReady.
    fn interpret(&self, storage, window, outcome) -> BoxFuture<Result<Interpretation, TaskFailure>>;
    fn on_terminal(&self, storage, error: String) -> BoxFuture<()>;
    fn wake(&self);
    fn wake_notified(&self) -> BoxFuture<()>;
}
```

Because `reconcile` receives the repositories and returns a verdict, the runner can wrap it in a transaction and an
enable check without understanding what the trigger did inside.

## Runtime topology

Each registration gets one seeder loop — individually stoppable via `Scheduler::stop/start(kind)` — and there is
exactly one dispatcher.

```mermaid
flowchart TB
    subgraph tokio["engine runtime"]
        S1["seeder: db_maintenance"]
        S2["seeder: task_prune"]
        S3["seeder: cvm_daily_sync"]
        S4["seeder: heartbeat<br/>(inline mode)"]
        D["dispatcher"]
    end
    DB[("valqeron.db")]

    S1 -->|"seed row"| DB
    S2 -->|"seed row"| DB
    S3 -->|"seed row"| DB
    S1 -.->|notify| D
    S2 -.->|notify| D
    S3 -.->|notify| D
    D -->|"claim_due(8)"| DB
    D -->|"execute ≤2 concurrent"| H["handlers"]
    H -->|"outcome"| D
    D -->|"terminal move:<br/>delete + history + stats"| DB
    D -.->|"trigger.wake()"| S3

    S4 -->|"runs handler inline<br/>no rows"| H
```

The runtime is event-driven; polling exists only as capped self-healing fallbacks. A seeder that inserted a row wakes
the dispatcher immediately; a completed run wakes its trigger's seeder (so catch-up is paced by handler speed). For
pure clock edges: the dispatcher sleeps until the queue's earliest `scheduled_at` (the **watermark**, refreshed inside
every empty claim, capped at `DISPATCH_MAX_SLEEP = 60s`), and a wall-clock seeder sleeps until its pass hint (a sync
cooldown expiry, capped at `SEED_FALLBACK_INTERVAL = 600s`). An idle engine touches the database a couple of times per
hour instead of every second — and a wrong hint degrades to one capped sleep, never a missed run, because the rows
themselves are the durable alarms.

## The data model

One concern per table; a terminal run **moves** rather than mutates in place:

```text
┌─ task_registry ─────────────┐   ┌─ task_queue ────────────────┐
│ catalog + operator intent   │   │ LIVE work only              │
│ identity (code-owned,       │   │ status ∈ PENDING | RUNNING  │
│  overwritten at boot):      │   │ attempts/max, retry delay,  │
│  category trigger_kind      │   │ payload, version guard      │
│  tracking schedule source   │   │ rows leave on completion ──┐│
│  log_policy                 │   └─────────────────────────── ││
│ intent (preserved): enabled │                    terminal move│
│ settings (NULL=code-owned): │   ┌─ task_execution ───────────▼┐
│  period_secs at_local       │   │ history: 1 row per terminal │
│  recurrence cooldown_secs   │   │  run; outcome ∈ SUCCEEDED | │
│  max_backfill_days          │   │  NOT_READY | FAILED;        │
│ registered (boot-owned)     │   │  attempts, duration_ms      │
└─────────────────────────────┘   │ pruned after 7 days         │
┌─ task_stat ─────────────────┐   └─────────────────────────────┘
│ prune-proof aggregates:     │◄── terminal runs fold here
│ totals, durations, last_*   │
│ NEVER pruned                │
└─────────────────────────────┘
          sync_cursor — per-source progress, never pruned
```

```sql
CREATE TABLE task_registry (
    kind                TEXT NOT NULL PRIMARY KEY,
    category            TEXT NOT NULL,   -- ENGINE_SYSTEM|FINANCE_DATA_SYNC|OTHER
    trigger_kind        TEXT NOT NULL,   -- INTERVAL|RECURRING|SYNC
    tracking            TEXT NOT NULL,   -- DURABLE|EPHEMERAL
    schedule            TEXT NOT NULL,   -- display-only descriptor of the EFFECTIVE schedule
    source              TEXT,            -- sync trigger: the sync_cursor key
    log_policy          TEXT NOT NULL DEFAULT 'ALL',
    enabled             INTEGER NOT NULL DEFAULT 1,   -- operator intent, boot-preserved
    period_secs         INTEGER,         -- interval trigger        ─┐
    at_local            TEXT,            -- recurring/sync, HH:MM    │ settings:
    recurrence          TEXT,            -- daily | weekly:<day>     │ NULL = code-owned,
    cooldown_secs       INTEGER,         -- sync                     │ non-NULL = DB truth
    max_backfill_days   INTEGER,         -- sync                    ─┘
    registered          INTEGER NOT NULL DEFAULT 1,   -- 0 = retired
    first_registered_at TEXT NOT NULL,
    updated_at          TEXT NOT NULL
) STRICT, WITHOUT ROWID;
```

Facts that matter:

- **`runs = registered && enabled`.** Two switches, deliberately distinct: *"does the code still ship this?"* and
  *"did an operator turn it off?"*. There is no env switch — every task the code ships registers at every boot.
- **Every catalog column has one owner.** Identity columns are rewritten by the boot reconcile; `enabled` and non-NULL
  settings survive it (each settings column is `COALESCE`-filled from the code default only while NULL). Run
  aggregates live on `task_stat`, so the reconcile never has to tiptoe around columns it does not own.
- **`NULL` settings mean code-owned.** A task that declares a knob tunable seeds its default on first boot; a task
  that pins a knob (`sd_watchdog`'s period follows the systemd contract) leaves the column NULL forever. Clearing a
  column back to NULL is "reset to default" — the next boot refills it. Unparseable hand-edits are logged and read as
  unset, never a boot failure.
- **`task_stat` is the prune-proof memory.** `task_prune` deletes `task_execution` rows after 7 days; the stats row —
  totals, failure counts, durations, `last_success_at` — is never deleted and survives retirement, exactly like the
  sync cursor.
- **Disabled is still declared.** A disabled task keeps its catalog row (`enabled = 0`), so an operator can tell
  "turned off" from "never existed" — history, stats, and cursor stay intact for re-enabling.

### Boot reconcile

Once at startup, in one write transaction, **after** crash recovery (so a retired kind's requeued rows get cancelled
too):

```mermaid
sequenceDiagram
    participant B as builder.start()
    participant R as recover_stale_running
    participant T as reconcile_registry (1 txn)
    participant DB as valqeron.db

    B->>R: RUNNING rows are orphans
    R->>DB: requeue (attempts left), or move to<br/>task_execution as FAILED + count in stats
    B->>T: validated definitions from code
    loop each definition
        T->>DB: read stored settings
        T->>T: merge into the code config<br/>(effective schedule)
        T->>DB: declare (upsert; preserve enabled,<br/>COALESCE-fill NULL settings)
    end
    T->>DB: retire_missing(kinds) → retired[]
    loop each retired kind
        T->>DB: take_pending → task_execution rows<br/>("retired: kind no longer registered")
    end
    T-->>B: effective configs per kind
    B->>B: build triggers from effective configs,<br/>spawn seeders + dispatcher
```

The triggers are built **after** the reconcile, from the merged (DB-effective) configs — an operator's stored period
or schedule override is what actually runs, and the `schedule` descriptor written back shows it. If the reconcile
transaction itself cannot run, the engine boots on code defaults rather than refusing to start.

**Retirement, not deletion.** A kind that disappears from code is marked `registered = 0`; its catalog row and stats
survive forever, and its leftover `PENDING` rows move to the history as failed — recorded as cancellations, **not**
counted in the stats (a row that never ran is not a run). A kind that comes back is revived by the next `declare` with
its totals intact.

> **Renaming a kind is retire + create.** Run totals restart. Sync *progress* does not, because `sync_cursor` is keyed
> by `source`, not by task kind.

### Why one file

The engine tables live in the **same SQLite file** as domain data. That is a correctness requirement, not convenience:
WAL mode cannot commit atomically across attached database files. One file is what allows an ingesting handler to write
its data *and* advance its cursor in a single transaction — the exactly-once seam. A separate `engine.db` would
permanently foreclose that.

## Lifecycle of a run

```mermaid
sequenceDiagram
    autonumber
    participant S as seeder
    participant DB as valqeron.db
    participant D as dispatcher
    participant T as trigger
    participant H as handler

    Note over S: sleep elapsed, wake, or boot
    S->>S: enabled? (in-memory gate)
    S->>DB: txn { trigger.reconcile }
    DB-->>S: Seeded
    S-->>D: notify

    D->>DB: claim_due(now, 8)
    Note over DB: PENDING → RUNNING<br/>attempts += 1
    DB-->>D: claimed rows

    D->>T: window_for(payload)
    T-->>D: RunWindow
    Note over D,H: span task_run{kind, category}
    D->>H: handler.run(TaskContext)
    H-->>D: TaskOutcome

    D->>T: interpret(storage, window, outcome)
    Note over T: sync: advance / hold cursor
    T-->>D: Ok(Completed | NotReady) | Err(TaskFailure)

    D->>DB: txn { queue delete · task_execution insert · task_stat fold }
    opt terminal failure
        D->>T: on_terminal(error)
        Note over T: sync: count failure,<br/>set cooldown
    end
    D->>T: wake()
    T-->>S: re-arm seeder
```

The non-obvious guarantees at each stage:

| Stage | Guarantee |
|---|---|
| Seed (1–4) | The enable gate is the in-memory image of the *committed* registry flag (writes go through `Scheduler::set_enabled`: commit first, publish after) — a disabled kind's pass costs one atomic load and zero DB work |
| Claim (5–6) | Select-then-claim under one writer guard; kinds with `enabled = 0` are skipped in SQL (armed rows freeze until re-enabled — the DB-truth backstop); the per-row `WHERE status='PENDING'` guard makes double-dispatch impossible; batch runs at `EXECUTION_CONCURRENCY = 2` *across* kinds, serial within one; the final empty claim refreshes the dispatcher's sleep watermark |
| Execute (7–10) | Handler runs inside a `task_run{kind, category}` span; sees typed `RunWindow`, never a payload |
| Interpret (11–12) | Trigger applies its side effects and reports what the history should record — `NotReady` is a first-class result ("waited" is not "worked"), never a failure; a failed cursor write turns a successful handler into a failed run |
| Complete (13) | The terminal **move** is one transaction: version-guarded queue `DELETE`, `task_execution` `INSERT`, `task_stat` fold — applied only when the guarded delete matched. Retries stay in the queue as an `UPDATE`; only terminal outcomes reach history and stats |
| Hooks (14–16) | `on_terminal` lets the trigger count failures / set cooldowns, then `wake()` re-arms the seeder within milliseconds |

### Retries, crash recovery, shutdown

Retries live on the queue row itself:

```text
attempts < max_attempts  →  TaskCompletion::Retry
                            status → PENDING
                            scheduled_at = now + retry_delay_secs · 2^(attempts−1)
                            capped at MAX_BACKOFF = 1 hour

attempts = max_attempts  →  TaskCompletion::Terminal  (row moves to history)
```

With `retry_delay_secs = 300` and 3 attempts: fail → +5 min → fail → +10 min → fail → terminal.

At boot, `RUNNING` rows are orphans by definition (the single-instance lock guarantees no other process owns them):

```mermaid
flowchart LR
    A["RUNNING row at boot"] --> B{"attempts <<br/>max_attempts?"}
    B -->|yes| C["→ PENDING, due now<br/><i>requeued</i>"]
    B -->|no| D["→ task_execution FAILED<br/>'interrupted…' — counted in stats"]
```

Shutdown is bounded and symmetric with crash recovery:

```text
SIGTERM / SIGINT
   │
   ├─ lifecycle → Stopping (sd_notify STOPPING=1)
   ├─ gRPC server stops accepting, drains in-flight RPCs
   ├─ Scheduler::drain — watch flip stops every seeder + the dispatcher
   ├─ bounded drain (DRAIN_TIMEOUT = 10s) waits for running handlers
   └─ storage closed, runtime shut down (RUNTIME_SHUTDOWN_TIMEOUT = 20s)
```

A handler cut off by the drain deadline leaves its row `RUNNING` — exactly the crash-recovery case, handled at the next
boot.
