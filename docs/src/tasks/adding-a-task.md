# Adding a Task

The whole procedure, for each trigger.

## The contract

Two things define a task: **how it runs** — the `TaskHandler` trait — and **what/when it runs** — a `TaskDefinition`
built by one typed builder per trigger. Both live behind `crate::tasks`; that import path is the entire surface.

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

Builders carry the framework defaults, so a job states only what makes it different:

| Builder | Defaults | Overrides |
|---|---|---|
| `TaskDefinition::interval(kind, period)` | durable, ±10% jitter, log `ALL`, category `Other` | `.ephemeral()` (inline, no jitter, `FAILURES_ONLY`), `.no_jitter()`, `.log_all()`, `.log_failures_only()`, `.category(..)` |
| `TaskDefinition::recurring(kind, schedule)` | no retries (the next occurrence is the retry), log `ALL` | `.retry(..)`, `.log_failures_only()`, `.category(..)` |
| `TaskDefinition::sync(kind, source, schedule)` | retry 3×300s, cooldown 300s, backfill cap 90d, category `FinanceDataSync` | `.retry(..)`, `.cooldown_secs(..)`, `.max_backfill_days(..)`, `.category(..)` |

Every builder has two terminals: `.run(handler)` yields the `TaskDefinition`, and `.disabled()` yields the catalog
declaration for a task configured off this boot — same source of truth, no hand-rolled declarations.

## The shape

Every task module exports one `register` function that takes the builder and returns it, composed in `engine.rs`,
which does nothing else:

```rust,ignore
// crates/engine/src/jobs/<name>.rs
pub(crate) fn register(
    builder: BackgroundTasksBuilder,
    config: &EngineConfig,
) -> BackgroundTasksBuilder {
    builder.task(TaskDefinition::…(..).run(handler))
}

// crates/engine/src/engine.rs
fn background_tasks(..) -> BackgroundTasksBuilder {
    let builder = BackgroundTasks::builder();
    let builder = crate::jobs::system::register(builder, config, started, state);
    let builder = crate::jobs::cvm::register(builder, config);
    crate::jobs::my_task::register(builder, config)   // ← your line
}
```

Checklist:

1. Pick a **trigger** — see [Choosing one](./triggers.md#choosing-one).
2. Pick a **category** — `EngineSystem`, `FinanceDataSync`, or `Other`.
3. Choose a **kind**: `snake_case`, stable, ≤100 chars. It is a primary key and a log field; renaming it retires the
   old row.
4. Write the module in `crates/engine/src/jobs/`.
5. Add the `mod` declaration in `jobs/mod.rs`.
6. Add one composition line in `engine.rs`.
7. If configurable, add env parsing to `EngineConfig` and document the variables in `scripts/install/*.example`.
8. Write tests.

Nothing else changes. The catalog row appears at the next boot; status, pause/resume, worker stop/start, and log
filtering come for free.

## Example 1 — interval (ephemeral)

A liveness probe that should never persist rows:

```rust,ignore
// crates/engine/src/jobs/probe.rs
use std::time::Duration;
use valqeron_core::TaskCategory;

use crate::tasks::{BackgroundTasksBuilder, TaskContext, TaskDefinition, TaskOutcome};

pub(crate) const PROBE_TASK: &str = "upstream_probe";

pub(crate) fn register(builder: BackgroundTasksBuilder) -> BackgroundTasksBuilder {
    builder.task(
        TaskDefinition::interval(PROBE_TASK, Duration::from_secs(60))
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
// crates/engine/src/jobs/report.rs
use chrono::{NaiveTime, Weekday};
use valqeron_core::{MarketCalendar, Recurrence, Schedule, TaskCategory};

use crate::tasks::{BackgroundTasksBuilder, RetryPolicy, TaskContext, TaskDefinition, TaskOutcome};

pub(crate) const REPORT_TASK: &str = "weekly_report";

pub(crate) fn register(builder: BackgroundTasksBuilder) -> BackgroundTasksBuilder {
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

Catalog descriptor: `recurring:weekly:mon@06:00+00:00`.

## Example 3 — sync source, with a struct handler

A second data source, inheriting catch-up, cooldowns, and halt semantics. Real ingestion carries state (a client, a
parser), so the handler is a struct — and the env-disabled path reuses the same builder via `.disabled()`:

```rust,ignore
// crates/engine/src/jobs/anbima.rs
use chrono::{NaiveTime, Weekday};
use valqeron_core::{MarketCalendar, Recurrence, Schedule, SyncSource};

use crate::engine::EngineConfig;
use crate::tasks::{
    BackgroundTasksBuilder, BoxFuture, RunWindow, TaskContext, TaskDefinition, TaskHandler,
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

pub(crate) fn register(
    builder: BackgroundTasksBuilder,
    config: &EngineConfig,
) -> BackgroundTasksBuilder {
    let Ok(source) = SyncSource::new(ANBIMA_SOURCE) else {
        tracing::error!(source = ANBIMA_SOURCE, "invalid sync source name");
        return builder;
    };
    let at = NaiveTime::from_hms_opt(8, 0, 0).unwrap_or_default();
    let definition = TaskDefinition::sync(
        ANBIMA_SYNC_TASK,
        source,
        Schedule::new(MarketCalendar::B3, at, Recurrence::Weekly { on: Weekday::Mon }),
    );

    match config.anbima_sync() {
        // Configured off: cataloged as `disabled`, cursor and stats intact.
        None => builder.declare(definition.disabled()),
        Some(settings) => builder.task(
            definition
                .cooldown_secs(settings.cooldown_secs)
                .run(AnbimaSync { client: IngestClient::new(settings) }),
        ),
    }
}
```

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
migrations/00N_anbima_schema.sql          ← new migration file
crates/infrastructure/src/sqlite/anbima/  ← adapter (model/queries/repository)
crates/core/src/anbima.rs                 ← entity + repository port
crates/core/src/storage.rs                ← Repositories.anbima + StorageEngine::Anbima
crates/engine/src/jobs/anbima.rs          ← the ONLY place that references it
```

- Append the migration to `MIGRATIONS` in `crates/infrastructure/src/sqlite/migrations.rs`. **The array index is the
  schema version** — append only, never reorder.
- Reference the repository exclusively from the task's own `jobs/` module. Neither the runtime nor any trigger may
  know it exists.
- Because everything shares one SQLite file, the handler can write its data and advance its own state in a single
  transaction.

## Testing

Follow the existing patterns in `crates/engine/src/tasks/mod.rs` and `crates/engine/src/tasks/trigger/sync.rs`:

```rust,ignore
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn my_task_runs_and_records() {
    let (_dir, storage) = storage();          // tempfile-backed real DB
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);

    let manager = BackgroundTasks::builder()
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
matches production. `manager.kick(kind)` wakes a seeder immediately instead of waiting for its 60-second tick;
`manager.stop/start(kind)` exercise the per-task worker handles.

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
