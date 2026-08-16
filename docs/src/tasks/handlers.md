# The Handler Contract

Every task in every plane has the same handler signature. This is the entire
surface a task author must learn.

```rust,ignore
Fn(TaskContext) -> impl Future<Output = TaskOutcome> + Send
```

## `TaskContext`

```rust,ignore
pub(crate) struct TaskContext {
    pub storage: AsyncStorage,   // the storage facade
    pub window: RunWindow,       // what this run is responsible for
}
```

### `RunWindow`

```rust,ignore
pub(crate) enum RunWindow {
    None,                                  // interval, recurring
    Period {                               // sync
        slot: DateTime<Utc>,               // the occurrence this run belongs to
        target: TargetPeriod,              // { from: NaiveDate, to: NaiveDate }
    },
}
```

The plane parses the row's payload **once** and hands over typed values.
Handlers never touch raw payloads, never decode timestamps, and never need to
know the payload format exists.

## `TaskOutcome`

```rust,ignore
pub(crate) enum TaskOutcome {
    Done,
    NotReady { retry_after_secs: u32 },
    Failed(String),
}
```

One vocabulary; each plane interprets it:

| Outcome | Interval | Recurring | Sync |
|---|---|---|---|
| `Done` | row `SUCCEEDED`, totals bumped | same | same **+ cursor advances** |
| `NotReady` | info log; retry is the next tick | info log; retry is the next occurrence | **cursor holds, cooldown set, no failure counted** |
| `Failed` | retry per budget → terminal | same | same **+ terminal holds cursor, counts failure, starts escalating cooldown** |

`NotReady` exists because *"the upstream source has not published yet"* is not
an error. Using `Failed` for it would burn the retry budget, log at error level,
and eventually mark the source `halted` — all wrong for an expected condition.

## Writing a handler

Handlers reach storage through `ctx.storage`, whose `write` / `read` closures
run on the blocking pool and receive `&Repositories`:

```rust,ignore
|ctx| async move {
    let cutoff = Utc::now() - Duration::days(7);
    let pruned = ctx
        .storage
        .write("my_task", false, move |repos| {
            repos.tasks.prune_finished(cutoff).map_err(StorageError::from)
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

Note the double `Result`: the outer one is the storage facade's admission
control (`Overloaded`, `ShuttingDown`), the inner one is the operation itself.

### A sync handler

```rust,ignore
|ctx| async move {
    let RunWindow::Period { slot, target } = ctx.window else {
        return TaskOutcome::Failed("sync run without a period window".into());
    };

    match fetch_and_upsert(&ctx.storage, target.from, target.to).await {
        Ok(_)                => TaskOutcome::Done,
        Err(Error::NotPublished) => TaskOutcome::NotReady { retry_after_secs: 3600 },
        Err(e)               => TaskOutcome::Failed(e.to_string()),
    }
}
```

## Rules

**1. Derive dates from `window`, never from the clock.**
A catch-up run executing days late must still sync *its own* period. Calling
`Utc::now()` to decide what to fetch makes every backfill run sync the same day.

**2. Sync handlers must be idempotent.**
The cursor advance and the completion record are separate transactions, so a
crash between them re-runs a period. Upsert by date; never blind-append.
See [Sync § At-least-once](./plane-sync.md#at-least-once-and-why-that-is-fine).

**3. Never panic.**
A panic escapes the outcome vocabulary and aborts the run task without
recording anything; the row is only cleaned up by crash recovery at the next
boot. Return `TaskOutcome::Failed` instead. (The workspace denies `unwrap`,
`expect`, and `panic` in non-test code, which enforces this.)

**4. Do not do blocking I/O directly on the runtime.**
Route it through `ctx.storage`, which uses `spawn_blocking` internally, or
`tokio::task::spawn_blocking` for non-storage blocking work.

**5. Long-running handlers block their own next occurrence, not others.**
The `exists_active` gate means a slow run skips its own subsequent ticks. Other
kinds are unaffected.

## Handler logging

Runs execute inside a span, so any event a handler emits is automatically
attributed:

```text
task_run{kind="cvm_daily_sync", category="FINANCE_DATA_SYNC"}
```

That makes per-task and per-category verbosity possible through `EnvFilter`
without any code change — see [Operations § Logging](./operations.md#logging-control).
