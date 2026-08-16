# Scenarios

Worked end to end, showing the three tables at each step. Every scenario here
is covered by an automated test.

---

## S1 — Ten-day outage

*A laptop is closed for ten days. On the next boot, every missed business day
must be synced, in order.*

Cursor at boot: `through_slot = Aug 3`, `through_target = Aug 2`.

| | `sync_cursor` | seeder action | dispatcher |
|---|---|---|---|
| boot | slot Aug 3 / target Aug 2 | seed **Aug 4** (past due) | runs now → covers Aug 3 |
| +ms | slot Aug 4 / target Aug 3 | seed **Aug 5** (past due) | runs now → covers Aug 4 |
| … | … | … | … |
| | slot Aug 13 / target Aug 12 | seed **Aug 14** (past due) | runs now → covers Aug 13 |
| done | slot Aug 14 / target Aug 13 | seed **Aug 17 07:00** (future) | waits |

```text
runs:  ●──●──●──●──●──●──●──●──●──●        then ○ (armed, waiting)
       Aug3 4  5  6  7 10 11 12 13        Aug 17
       └─── strictly ascending, one at a time ───┘
```

Why it holds:

- **sequential** — `exists_active` keeps exactly one row in flight;
- **chronological** — the cursor only advances on `Done`;
- **fast** — each completion wakes the seeder, so pacing is handler speed, not
  the 60-second tick;
- **bounded** — beyond `max_backfill_days` the gap is skipped with a warning
  instead of grinding through a year.

> Test: `ten_missed_business_days_backfill_sequentially_in_order`

---

## S2 — Pause and resume

*An operator pauses CVM on Tuesday and resumes on Friday.*

```text
Mon  cursor slot=Mon target=Fri(prev)   run covers Fri      ✓
Tue  09:00  UPDATE task_registration SET paused=1 …
     └─ Wed's row was already armed → it still fires (covers Tue)
     └─ afterwards: no new seeding. status = paused
Wed  ·  no seeding
Thu  ·  no seeding
Fri  08:00  UPDATE … SET paused=0 …
     ≤60s later:
       seed Thu slot (past due) → run → covers Wed
       seed Fri slot (past due) → run → covers Thu
       seed Mon slot (future)   → waits
```

Resume needs no new machinery: it walks the missed periods through exactly the
same catch-up path as S1. Status transitions `paused → catching_up → waiting`
within a minute.

> **Pause stops seeding, not armed rows.** A row already scheduled when the
> pause lands will still run. Strict "nothing runs after the flip" would require
> cancelling the armed row, which will land with the pause RPC.

> Tests: `paused_source_resumes_with_sequential_catchup`,
> `paused_kind_seeds_nothing_until_resumed`

---

## S3 — Disabled by configuration

*`VALQERON_ENGINE_SYNC_CVM=off`, engine restarted.*

```text
boot reconcile → declare_disabled(cvm_daily_sync, config_enabled = false)
              → no seeder spawned
status         → disabled
sync_cursor    → untouched
history        → untouched
```

The task stays **visible** with its full history rather than silently vanishing.
Re-enabling weeks later resumes from the cursor, bounded by
`max_backfill_days`.

> Test: `boot_reconcile_registers_the_catalog` (disabled declaration branch)

---

## S4 — A source starts failing

*CVM is unreachable on Monday.*

```text
Mon 07:00  run → Failed("connect timeout")
             attempt 1 of 3 … +5 min … attempt 2 … +10 min … attempt 3
             → terminal FAILED

           background_task:     row FAILED
           task_registration:   total_runs += 1, total_failures += 1,
                                last_outcome = FAILED, last_error = "connect timeout"
           sync_cursor:         through_slot UNCHANGED  ← halt
                                consecutive_failures = 1
                                cooldown_until = now + 5 min

cooldown elapses → the SAME slot is re-seeded
             failure 2 → cooldown 10 min
             failure 3 → 20 min
             failure 4 → 40 min
             failure 5 → 60 min (capped) + log escalates to ERROR
                       → status = halted
```

Nothing after the failing period runs. That is deliberate: for financial data,
stalling loudly beats silently skipping a day.

Recovery is automatic — the first success clears `consecutive_failures`, and
catch-up resumes from where it stopped.

> Tests: `terminal_failure_halts_on_the_same_slot_with_cooldown`,
> `zero_cooldown_retries_the_same_slot_and_counts_failures`

---

## S5 — The source has not published yet

*The 07:00 run finds no file for yesterday.*

```text
handler → TaskOutcome::NotReady { retry_after_secs: 3600 }

background_task:     row SUCCEEDED      ← not an error
task_registration:   total_runs += 1, total_failures unchanged
sync_cursor:         through_slot UNCHANGED
                     consecutive_failures = 0     ← not counted
                     last_outcome = NOT_READY
                     cooldown_until = now + 1h
status:              cooling_down
```

An hour later the same period is retried. A slow publisher never escalates to
`halted` and never fills the log with errors.

> Test: `not_ready_holds_the_cursor_without_burning_failures`

---

## S6 — An upgrade removes a task

*The new binary no longer registers `old_cleanup`.*

```text
boot: recover_stale_running   → any RUNNING row of the kind requeued to PENDING
      reconcile
        declare(current kinds)
        retire_missing        → old_cleanup.registered = 0
        fail_pending          → its PENDING rows → FAILED
                                 "retired: kind no longer registered"

status: retired
run summary: preserved forever (41 runs / 2 failures, last run Aug 12)
```

No orphan alarms, no confusing `"no handler registered"` failures, and the
historical record survives. If the kind returns in a later version, the next
`declare` revives it with its totals intact.

> Test: `retired_kinds_are_marked_and_their_pending_rows_cancelled`

---

## S7 — Fresh install

*First-ever boot on a Wednesday at 09:00 BRT.*

```text
no sync_cursor → cold start
   latest occurrence before now = Wed 07:00
   previous                     = Tue 07:00
   cursor: through_slot = Tue 07:00, through_target = Mon

first run: slot Wed 07:00, period [Tue, Tue]      ← the latest period only
then:      seed Thu 07:00 (future) → waits
```

Exactly one run, no history. Bulk historical loading is a separate concern from
the daily loop.

> Test: `cold_start_runs_only_the_most_recent_period`

---

## S8 — Cursor far beyond the cap

*A source was disabled for eight months, then re-enabled.*

```text
pending_days = 412  >  max_backfill_days = 90

→ cursor reseeded to cold start
→ WARN operation="sync_skip" skipped_days=412 max_backfill_days=90
→ exactly one run, covering the most recent period
→ steady state from there
```

The engine refuses to silently grind through 412 sequential fetches on an
unattended boot.

> Test: `stale_cursor_beyond_the_cap_skips_ahead_to_the_latest`

---

## S9 — Crash between cursor advance and completion

*The engine dies in the millisecond between the cursor write and the
completion record.*

```text
before crash:  cursor advanced past Aug 12; row still RUNNING
boot:          recovery requeues the row (attempts left)
               dispatcher re-runs the SAME period (from the payload)
               handler upserts Aug 12 again          ← idempotent, harmless
               cursor.advanced(Aug 12) again          ← no-op
```

At-least-once, never at-most-once. This is why sync handlers must be
idempotent.

> Test: `crash_between_handler_success_and_completion_reruns_idempotently`

---

## S10 — Two sources, independent

*CVM is three periods behind; a new source cold-starts. Both run concurrently.*

```text
alpha (3 behind):  ●──●──●──○        3 catch-up runs, then armed
beta  (cold):      ●──○              1 run, then armed

rows: alpha 4 (3 succeeded + 1 future), beta 2 (1 succeeded + 1 future)
```

Each source has its own cursor, its own seeder, and its own cooldown. A halted
source never blocks another; the dispatcher interleaves them at
`EXECUTION_CONCURRENCY = 2`.

> Test: `two_sources_advance_independently`
