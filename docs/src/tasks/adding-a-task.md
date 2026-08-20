# Adding a Task

The whole procedure, for each trigger.

## The contract

Two things define a task: **how it runs** — the `TaskHandler` trait — and **what/when it runs** — a `TaskDefinition`
built by one typed builder per trigger. Both live behind `crate::scheduler`; that import path is the entire surface.

```rust,ignore
// How it runs. Closures implement this automatically (blanket impl);
// stateful tasks (an HTTP client, a parser) implement it on a struct.
trait TaskHandler: Send + Sync + 'static {
    fn run<'a>(&'a self, ctx: TaskContext) -> BoxFuture<'a, TaskOutcome>;
}

pub(crate) struct TaskContext {
    pub storage: AsyncStorage,   // the storage facade
    pub window: RunWindow,       // what this run is responsible for
}

pub(crate) enum RunWindow {
    None,                                  // interval, recurring
    Period {                               // sync
        slot: DateTime<Utc>,               // the occurrence this run belongs to
        target: TargetPeriod,              // { from: NaiveDate, to: NaiveDate }
    },
}

pub(crate) enum TaskOutcome {
    Done,
    NotReady { retry_after_secs: u32 },    // "expected, try later" — NOT an error
    Failed(String),
}
```

The trigger parses the row's payload **once** and hands over typed values — handlers never touch raw payloads. How each
trigger interprets the outcome is in [Triggers § one outcome vocabulary](./triggers.md#choosing-one).

Builders carry the framework defaults, so a task states only what makes it different:

| Builder | Defaults | Overrides |
|---|---|---|
| `TaskDefinition::interval(kind, period)` | durable, ±10% jitter, log `ALL`, category `Other` | `.ephemeral()` (inline, no jitter, `FAILURES_ONLY`), `.no_jitter()`, `.pinned()` (period stays code-owned), `.log_all()`, `.log_failures_only()`, `.category(..)` |
| `TaskDefinition::recurring(kind, schedule)` | no retries (the next occurrence is the retry), log `ALL` | `.retry(..)`, `.log_failures_only()`, `.category(..)` |
| `TaskDefinition::sync(kind, source, schedule)` | retry 3×300s, cooldown 300s, backfill cap 90d, category `FinanceDataSync` | `.retry(..)`, `.cooldown_secs(..)`, `.max_backfill_days(..)`, `.category(..)` |

`.run(handler)` yields the `TaskDefinition`. What you pass the builder are **code defaults**: the boot reconcile seeds
them into the registry row once, and from then on the row (operator-editable, boot-preserved) is what actually
schedules the task — including whether it runs at all (`enabled`). There is no env-var or code path for turning a
task off; every task the code ships registers at every boot.

## The shape

Every task module exports one `register` function that takes the builder and returns it, composed in `engine.rs`,
which does nothing else:

```rust,ignore
// crates/engine/src/tasks/<name>.rs
pub(crate) fn register(builder: SchedulerBuilder) -> SchedulerBuilder {
    builder.task(TaskDefinition::…(..).run(handler))
}

// crates/engine/src/engine.rs
fn scheduler(..) -> SchedulerBuilder {
    let builder = Scheduler::builder();
    let builder = crate::tasks::system::register(builder, started, state);
    let builder = crate::tasks::cvm::register(builder);
    crate::tasks::my_task::register(builder)   // ← your line
}
```

Checklist:

1. Pick a **trigger** — see [Choosing one](./triggers.md#choosing-one).
2. Pick a **category** — `EngineSystem`, `FinanceDataSync`, or `Other`.
3. Choose a **kind**: `snake_case`, stable, ≤100 chars. It is a primary key and a log field; renaming it retires the
   old row.
4. Write the module in `crates/engine/src/tasks/`, with its scheduling defaults as in-module constants.
5. Add the `mod` declaration in `tasks/mod.rs`.
6. Add one composition line in `engine.rs`.
7. Write tests.

Nothing else changes. The catalog row appears at the next boot with your defaults seeded into its settings columns;
status, enable/disable, operator tuning, worker stop/start, and log filtering come for free.

## Example 1 — interval (ephemeral)

A liveness probe that should never persist rows:

```rust,ignore
// crates/engine/src/tasks/probe.rs
use std::time::Duration;
use valqeron_core::TaskCategory;

use crate::scheduler::{SchedulerBuilder, TaskContext, TaskDefinition, TaskOutcome};

pub(crate) const PROBE_TASK: &str = "upstream_probe";

/// Code default; the registry's `period_secs` is the effective value.
const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) fn register(builder: SchedulerBuilder) -> SchedulerBuilder {
    builder.task(
        TaskDefinition::interval(PROBE_TASK, DEFAULT_PROBE_INTERVAL)
            .category(TaskCategory::EngineSystem)
            .ephemeral()
            .run(|_ctx: TaskContext| async {
                match probe_upstream().await {
                    Ok(()) => TaskOutcome::Done,
                    Err(e) => TaskOutcome::Failed(e.to_string()),
                }
            }),
    )
}
```

> Closures annotate the context type (`|_ctx: TaskContext|`) — the blanket `TaskHandler` impl cannot drive closure
> inference on its own.

## Example 2 — recurring

A weekly report every Monday at 06:00 UTC, with history:

```rust,ignore
// crates/engine/src/tasks/report.rs
use chrono::{NaiveTime, Weekday};
use valqeron_core::{MarketCalendar, Recurrence, Schedule, TaskCategory};

use crate::scheduler::{RetryPolicy, SchedulerBuilder, TaskContext, TaskDefinition, TaskOutcome};

pub(crate) const REPORT_TASK: &str = "weekly_report";

pub(crate) fn register(builder: SchedulerBuilder) -> SchedulerBuilder {
    let at = NaiveTime::from_hms_opt(6, 0, 0).unwrap_or_default();
    builder.task(
        TaskDefinition::recurring(
            REPORT_TASK,
            Schedule::new(MarketCalendar::UTC, at, Recurrence::Weekly { on: Weekday::Mon }),
        )
        .retry(RetryPolicy { max_attempts: 2, retry_delay_secs: 600 })
        .run(|ctx: TaskContext| async move {
            match build_report(&ctx.storage).await {
                Ok(rows) => {
                    tracing::info!(rows, "weekly report written");
                    TaskOutcome::Done
                }
                Err(e) => TaskOutcome::Failed(e.to_string()),
            }
        }),
    )
}
```

Catalog descriptor: `recurring:weekly:mon@06:00+00:00`. An operator who tunes `at_local`/`recurrence` on the row
changes the effective schedule at the next boot — the code's 06:00 Monday stays the default for fresh databases.

## Example 3 — sync source, with a struct handler

A second data source, inheriting catch-up, cooldowns, and halt semantics. Real ingestion carries state (a client, a
parser), so the handler is a struct:

```rust,ignore
// crates/engine/src/tasks/anbima.rs
use chrono::{NaiveTime, Weekday};
use valqeron_core::{MarketCalendar, Recurrence, Schedule, SyncSource};

use crate::scheduler::{
    BoxFuture, RunWindow, SchedulerBuilder, TaskContext, TaskDefinition, TaskHandler,
    TaskOutcome,
};

pub(crate) const ANBIMA_SYNC_TASK: &str = "anbima_weekly_sync";
pub(crate) const ANBIMA_SOURCE: &str = "anbima";

struct AnbimaSync {
    client: IngestClient,
}

impl TaskHandler for AnbimaSync {
    fn run<'a>(&'a self, ctx: TaskContext) -> BoxFuture<'a, TaskOutcome> {
        Box::pin(async move {
            let RunWindow::Period { slot, target } = ctx.window else {
                return TaskOutcome::Failed("sync run without a period window".into());
            };
            tracing::info!(%slot, %target.from, %target.to, "ingesting ANBIMA week");

            match self.client.ingest(&ctx.storage, target.from, target.to).await {
                Ok(()) => TaskOutcome::Done,
                Err(Error::NotPublished) => TaskOutcome::NotReady { retry_after_secs: 3600 },
                Err(e) => TaskOutcome::Failed(e.to_string()),
            }
        })
    }
}

pub(crate) fn register(builder: SchedulerBuilder) -> SchedulerBuilder {
    let Ok(source) = SyncSource::new(ANBIMA_SOURCE) else {
        tracing::error!(source = ANBIMA_SOURCE, "invalid sync source name");
        return builder;
    };
    let at = NaiveTime::from_hms_opt(8, 0, 0).unwrap_or_default();
    builder.task(
        TaskDefinition::sync(
            ANBIMA_SYNC_TASK,
            source,
            Schedule::new(MarketCalendar::B3, at, Recurrence::Weekly { on: Weekday::Mon }),
        )
        .run(AnbimaSync { client: IngestClient::new() }),
    )
}
```

There is no disabled-registration path to write: the source always registers, and turning it off is an operator
action on the row (`UPDATE task_registry SET enabled = 0 WHERE kind = 'anbima_weekly_sync';`) — cursor, stats, and
history stay intact either way.

Because the recurrence is weekly, each run's `target` spans the whole preceding business week — the framework computes
that; the handler just honours it.

## Reaching storage

Handlers use `ctx.storage`, whose `write`/`read` closures run on the blocking pool and receive `&Repositories`. Note
the double `Result`: the outer one is the storage facade's admission control (`Overloaded`, `ShuttingDown`), the inner
one is the operation itself:

```rust,ignore
|ctx: TaskContext| async move {
    let cutoff = Utc::now() - Duration::days(7);
    let pruned = ctx
        .storage
        .write("my_task", false, move |repos| {
            repos
                .executions
                .prune_finished(cutoff)
                .map_err(StorageError::from)
        })
        .await;

    match pruned {
        Ok(Ok(removed)) => {
            tracing::info!(removed, "pruned rows");
            TaskOutcome::Done
        }
        Ok(Err(e)) => TaskOutcome::Failed(e.to_string()),   // storage error
        Err(e) => TaskOutcome::Failed(e.to_string()),       // backpressure
    }
}
```

Every run executes inside a `task_run{kind, category}` span, so anything a handler logs is automatically attributed —
per-task and per-category verbosity needs no code change; see
[Operations § Logging control](./operations.md#logging-control).

## Task-owned tables

When a task needs its own storage, it owns the whole vertical slice — but the engine remains the sole migration runner,
and `Repositories` is compile-time typed:

```text
migrations/002_anbima_schema.sql          ← new migration file (next array slot)
crates/infrastructure/src/sqlite/anbima/  ← adapter (model/queries/repository)
crates/core/src/anbima.rs                 ← entity + repository port
crates/core/src/storage.rs                ← Repositories.anbima + StorageEngine::Anbima
crates/engine/src/tasks/anbima.rs         ← the ONLY place that references it
```

- Append the migration to `MIGRATIONS` in `crates/infrastructure/src/sqlite/migrations.rs`. **The array index is the
  schema version** — append only, never reorder.
- Reference the repository exclusively from the task's own `tasks/` module. Neither the runtime nor any trigger may
  know it exists.
- Because everything shares one SQLite file, the handler can write its data and advance its own state in a single
  transaction.

## Testing

Follow the existing patterns in `crates/engine/src/scheduler/mod.rs` and
`crates/engine/src/scheduler/trigger/sync.rs`:

```rust,ignore
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn my_task_runs_and_records() {
    let (_dir, storage) = storage();          // tempfile-backed real DB
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);

    let manager = Scheduler::builder()
        .task(my_definition().run(move |_ctx: TaskContext| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                TaskOutcome::Done
            }
        }))
        .start(storage.clone())
        .await;

    let probe = Arc::clone(&runs);
    wait_until(5, async || probe.load(Ordering::SeqCst) >= 1).await;
    assert!(manager.drain(Duration::from_secs(2)).await);

    let stats = stats_row(&storage, "my_task").await.unwrap();
    assert_eq!(stats.total_runs, 1);
}
```

Useful helpers already available in those modules: `storage()`, `wait_until()`, `seed_task()`, `registration_row()`,
`execution_row()`, `stats_row()`, `queued_tasks()`, and for sync — `seed_cursor()`, `get_cursor()`,
`occurrence_back()`, `recording_handler()`. Tests use real file-backed SQLite (never in-memory) so WAL behaviour
matches production. `manager.kick(kind)` wakes a seeder ahead of its fallback sleep; `manager.stop/start(kind)`
exercise the per-task worker handles; `manager.set_enabled(kind, …)` is the committed enable/disable path. Tests that
insert queue rows directly (bypassing the seeders' dispatcher notifications) shrink the fallback sweep with
`Scheduler::builder().dispatch_max_sleep(…)`.

## Rules and common mistakes

| Rule | Breaking it means |
|---|---|
| Derive dates from `ctx.window`, never from `Utc::now()` | every catch-up run syncs the same day |
| Sync writes must be idempotent (upsert-by-date, never append) | duplicates after a crash between cursor advance and completion — see [Triggers § At-least-once](./triggers.md#at-least-once) |
| Return `NotReady` for "not published yet", not `Failed` | burns the retry budget, logs at error level, escalates to `halted` |
| Never panic — return `TaskOutcome::Failed` | no outcome recorded; the row is cleaned up only by crash recovery at next boot (the workspace denies `unwrap`/`expect`/`panic` anyway) |
| No blocking I/O directly on the runtime — use `ctx.storage` or `spawn_blocking` | stalls the executor |
| Don't schedule "daily" work on the interval trigger | anchored to boot: never fires on a machine restarting more often than the period |
| One registration per kind | the second registration is rejected with an error log |

A long-running handler blocks only its own next occurrence (the `exists_active` gate), never other kinds.
