# Adding a Task

The whole procedure, for each plane.

## The shape

Every task module exports one `register` function that takes the builder and
returns it:

```rust,ignore
// crates/engine/src/jobs/<name>.rs
pub(crate) fn register(
    builder: BackgroundTasksBuilder,
    config: &EngineConfig,
) -> BackgroundTasksBuilder {
    builder.register(TaskSpec { .. }, |ctx| async move { .. })
}
```

Composed in `engine.rs`, which does nothing else:

```rust,ignore
fn background_tasks(
    config: &EngineConfig,
    started: Instant,
    state: watch::Receiver<LifecycleState>,
) -> BackgroundTasksBuilder {
    let builder = BackgroundTasksManager::builder();
    let builder = crate::jobs::system::register(builder, config, started, state);
    let builder = crate::jobs::cvm::register(builder, config);
    crate::jobs::my_task::register(builder, config)   // ← your line
}
```

## Checklist

1. Pick a **plane** — see [Choosing one](./planes.md#choosing-one).
2. Pick a **category** — `EngineSystem`, `FinanceDataSync`, or `Other`.
3. Choose a **kind**: `snake_case`, stable, ≤100 chars. It is a primary key and
   a log field; renaming it retires the old row.
4. Write the module in `crates/engine/src/jobs/`.
5. Add the `mod` declaration in `jobs/mod.rs`.
6. Add one composition line in `engine.rs`.
7. If configurable, add env parsing to `EngineConfig` and document the variables
   in `scripts/install/*.example`.
8. Write tests.

Nothing else changes. The catalog row appears at the next boot; status,
pause/resume, and log filtering come for free.

---

## Example 1 — interval (ephemeral)

A liveness probe that should never persist rows:

```rust,ignore
// crates/engine/src/jobs/probe.rs
use std::time::Duration;
use valqeron_core::{LogPolicy, TaskCategory};

use crate::tasks::plane::{PlaneConfig, TaskOutcome, Tracking};
use crate::tasks::{BackgroundTasksBuilder, TaskSpec};

pub(crate) const PROBE_TASK: &str = "upstream_probe";

pub(crate) fn register(builder: BackgroundTasksBuilder) -> BackgroundTasksBuilder {
    builder.register(
        TaskSpec {
            kind: PROBE_TASK,
            category: TaskCategory::EngineSystem,
            plane: PlaneConfig::Interval {
                period: Duration::from_secs(60),
                jitter: true,
                tracking: Tracking::Ephemeral,
            },
            log_policy: LogPolicy::FailuresOnly,
        },
        |_ctx| async {
            match probe_upstream().await {
                Ok(()) => TaskOutcome::Done,
                Err(e) => TaskOutcome::Failed(e.to_string()),
            }
        },
    )
}
```

## Example 2 — recurring

A weekly report every Monday at 06:00 UTC, with history:

```rust,ignore
// crates/engine/src/jobs/report.rs
use chrono::{NaiveTime, Weekday};
use valqeron_core::{LogPolicy, MarketCalendar, Recurrence, Schedule, TaskCategory};

use crate::tasks::plane::{PlaneConfig, RetryPolicy, TaskOutcome};
use crate::tasks::{BackgroundTasksBuilder, TaskSpec};

pub(crate) const REPORT_TASK: &str = "weekly_report";

pub(crate) fn register(builder: BackgroundTasksBuilder) -> BackgroundTasksBuilder {
    let at = NaiveTime::from_hms_opt(6, 0, 0).unwrap_or_default();
    builder.register(
        TaskSpec {
            kind: REPORT_TASK,
            category: TaskCategory::Other,
            plane: PlaneConfig::Recurring {
                schedule: Schedule::new(
                    MarketCalendar::UTC,
                    at,
                    Recurrence::Weekly { on: Weekday::Mon },
                ),
                retry: RetryPolicy { max_attempts: 2, retry_delay_secs: 600 },
            },
            log_policy: LogPolicy::All,
        },
        |ctx| async move {
            match build_report(&ctx.storage).await {
                Ok(rows) => {
                    tracing::info!(rows, "weekly report written");
                    TaskOutcome::Done
                }
                Err(e) => TaskOutcome::Failed(e.to_string()),
            }
        },
    )
}
```

Catalog descriptor: `recurring:weekly:mon@06:00+00:00`.

## Example 3 — sync source

A second data source, inheriting catch-up, cooldowns, and halt semantics:

```rust,ignore
// crates/engine/src/jobs/anbima.rs
use chrono::{NaiveTime, Weekday};
use valqeron_core::{
    CooldownPolicy, LogPolicy, MarketCalendar, Recurrence, Schedule, SyncSource, TaskCategory,
};

use crate::engine::EngineConfig;
use crate::tasks::plane::{PlaneConfig, RetryPolicy, RunWindow, TaskOutcome};
use crate::tasks::{BackgroundTasksBuilder, TaskSpec};

pub(crate) const ANBIMA_SYNC_TASK: &str = "anbima_weekly_sync";
pub(crate) const ANBIMA_SOURCE: &str = "anbima";

pub(crate) fn register(
    builder: BackgroundTasksBuilder,
    _config: &EngineConfig,
) -> BackgroundTasksBuilder {
    let Ok(source) = SyncSource::new(ANBIMA_SOURCE) else {
        tracing::error!(source = ANBIMA_SOURCE, "invalid sync source name");
        return builder;
    };
    let at = NaiveTime::from_hms_opt(8, 0, 0).unwrap_or_default();

    builder.register(
        TaskSpec {
            kind: ANBIMA_SYNC_TASK,
            category: TaskCategory::FinanceDataSync,
            plane: PlaneConfig::Sync {
                source,
                schedule: Schedule::new(
                    MarketCalendar::B3,
                    at,
                    Recurrence::Weekly { on: Weekday::Mon },
                ),
                retry: RetryPolicy { max_attempts: 3, retry_delay_secs: 300 },
                cooldown: CooldownPolicy::new(300),
                max_backfill_days: 90,
            },
            log_policy: LogPolicy::All,
        },
        |ctx| async move {
            let RunWindow::Period { slot, target } = ctx.window else {
                return TaskOutcome::Failed("sync run without a period window".into());
            };
            tracing::info!(%slot, %target.from, %target.to, "ingesting ANBIMA week");

            match ingest(&ctx.storage, target.from, target.to).await {
                Ok(()) => TaskOutcome::Done,
                Err(Error::NotPublished) => TaskOutcome::NotReady { retry_after_secs: 3600 },
                Err(e) => TaskOutcome::Failed(e.to_string()),
            }
        },
    )
}
```

Because the recurrence is weekly, each run's `target` spans the whole preceding
business week — the framework computes that; the handler just honours it.

---

## Task-owned tables

When a task needs its own storage, it owns the whole vertical slice — but the
engine remains the sole migration runner, and `Repositories` is compile-time
typed. So:

```text
migrations/00N_anbima_schema.sql          ← new migration file
crates/infrastructure/src/sqlite/anbima/  ← adapter (model/queries/repository)
crates/core/src/anbima.rs                 ← entity + repository port
crates/core/src/storage.rs                ← Repositories.anbima + StorageEngine::Anbima
crates/engine/src/jobs/anbima.rs          ← the ONLY place that references it
```

Rules:

- Append the migration to `MIGRATIONS` in
  `crates/infrastructure/src/sqlite/migrations.rs`. **The array index is the
  schema version** — append only, never reorder.
- Reference the repository exclusively from the task's own `jobs/` module.
  Neither the manager nor any plane may know it exists.
- Because everything shares one SQLite file, the handler can write its data and
  advance its own state in a single transaction.

## Testing

Follow the existing patterns in `crates/engine/src/tasks/mod.rs` and
`crates/engine/src/tasks/plane/sync.rs`:

```rust,ignore
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn my_task_runs_and_records() {
    let (_dir, storage) = storage();          // tempfile-backed real DB
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);

    let manager = BackgroundTasksManager::builder()
        .register(my_spec(), move |_ctx| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                TaskOutcome::Done
            }
        })
        .start(storage.clone())
        .await;

    let probe = Arc::clone(&runs);
    wait_until(5, async || probe.load(Ordering::SeqCst) >= 1).await;
    assert!(manager.drain(Duration::from_secs(2)).await);

    let registration = registration_row(&storage, "my_task").await.unwrap();
    assert_eq!(registration.total_runs(), 1);
}
```

Useful helpers already available in those modules: `storage()`, `wait_until()`,
`seed_task()`, `registration_row()`, and for sync — `seed_cursor()`,
`get_cursor()`, `occurrence_back()`, `recording_handler()`.

Tests use real file-backed SQLite (never in-memory) so WAL behaviour matches
production. `manager.kick(kind)` is a test-only hook to wake a seeder
immediately instead of waiting for its 60-second tick.

## Common mistakes

| Mistake | Consequence |
|---|---|
| `Duration::from_secs(86_400)` on the interval plane | never fires on a machine restarting daily — use recurring |
| Reading `Utc::now()` in a sync handler | every catch-up run syncs the same day |
| Returning `Failed` for "not published yet" | burns the retry budget, escalates to `halted` |
| Non-idempotent sync writes | duplicates after a crash between cursor advance and completion |
| Panicking in a handler | no outcome recorded; row cleaned up only at next boot |
| Reusing a kind across two registrations | the second is rejected with an error log |
