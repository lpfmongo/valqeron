use crate::sqlite::database::{Db, DbHandle};
use crate::sqlite::support::{backend, with_busy_retry, write_outcome};
use crate::sqlite::task::model::TaskRow;
use crate::sqlite::task::queries;
use chrono::{DateTime, Utc};
use valqeron_core::{
    BackgroundTask, BackgroundTaskRepository, RepositoryResult, TaskCompletion, TaskId, TaskKind,
    Versioned, WriteOutcome,
};

pub struct SqliteBackgroundTaskRepository {
    db: DbHandle,
}

impl SqliteBackgroundTaskRepository {
    pub(crate) fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

fn reconstitute(row: TaskRow) -> Versioned<BackgroundTask> {
    let Versioned { data, version } = row.into_inner();
    Versioned {
        data: BackgroundTask::reconstitute(data),
        version,
    }
}

impl BackgroundTaskRepository for SqliteBackgroundTaskRepository {
    fn insert(&self, task: &BackgroundTask) -> RepositoryResult<()> {
        with_busy_retry(|| {
            let conn = self.db.write();
            queries::insert(&conn, task).map(|_| ())
        })
        .map_err(backend)
    }

    fn find_by_id(&self, id: &TaskId) -> RepositoryResult<Option<Versioned<BackgroundTask>>> {
        let conn = self.db.read();
        Ok(queries::find_by_id(&conn, id)
            .map_err(backend)?
            .map(reconstitute))
    }

    fn list_queued(&self, limit: u32) -> RepositoryResult<Vec<Versioned<BackgroundTask>>> {
        let conn = self.db.read();
        Ok(queries::list_queued(&conn, limit)
            .map_err(backend)?
            .into_iter()
            .map(reconstitute)
            .collect())
    }

    fn exists_active(&self, kind: &TaskKind) -> RepositoryResult<bool> {
        let conn = self.db.read();
        queries::exists_active(&conn, kind).map_err(backend)
    }

    fn find_active(&self, kind: &TaskKind) -> RepositoryResult<Option<Versioned<BackgroundTask>>> {
        let conn = self.db.read();
        Ok(queries::find_active(&conn, kind)
            .map_err(backend)?
            .map(reconstitute))
    }

    fn take_pending(&self, kind: &TaskKind) -> RepositoryResult<Vec<BackgroundTask>> {
        with_busy_retry(|| {
            // Read-then-delete under one writer guard, so the returned rows
            // are exactly the deleted ones.
            let conn = self.db.write();
            let rows = queries::pending_rows(&conn, kind)?;
            queries::delete_pending(&conn, kind)?;
            Ok(rows.into_iter().map(|row| reconstitute(row).data).collect())
        })
        .map_err(backend)
    }

    fn claim_due(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> RepositoryResult<Vec<Versioned<BackgroundTask>>> {
        with_busy_retry(|| {
            // One writer guard spans select-then-claim, so the batch is
            // atomic; the per-id PENDING guard in `mark_running` keeps a
            // claim idempotent regardless.
            let conn = self.db.write();
            let ids = queries::due_ids(&conn, now, limit)?;
            let mut claimed = Vec::with_capacity(ids.len());
            for id in ids {
                if queries::mark_running(&conn, &id, now)? == 0 {
                    continue;
                }
                if let Some(row) = queries::find_by_id(&conn, &id)? {
                    claimed.push(reconstitute(row));
                }
            }
            Ok(claimed)
        })
        .map_err(backend)
    }

    fn next_due_at(&self) -> RepositoryResult<Option<DateTime<Utc>>> {
        let conn = self.db.read();
        queries::next_due_at(&conn).map_err(backend)
    }

    fn complete(
        &self,
        id: &TaskId,
        expected_version: u32,
        completion: TaskCompletion,
    ) -> RepositoryResult<WriteOutcome> {
        with_busy_retry(|| {
            let conn = self.db.write();
            let affected = match &completion {
                TaskCompletion::Terminal { .. } => {
                    queries::delete_completed(&conn, id, expected_version)?
                }
                TaskCompletion::Retry {
                    error,
                    failed_at,
                    retry_at,
                } => queries::complete_retry(
                    &conn,
                    id,
                    expected_version,
                    error,
                    *failed_at,
                    *retry_at,
                )?,
            };
            match affected {
                0 => write_outcome(
                    &conn,
                    queries::TASK_VERSION_SQL,
                    id.as_bytes(),
                    expected_version,
                ),
                _ => Ok(WriteOutcome::Applied),
            }
        })
        .map_err(backend)
    }

    fn requeue_interrupted(&self, error: &str, now: DateTime<Utc>) -> RepositoryResult<u32> {
        with_busy_retry(|| {
            let conn = self.db.write();
            let requeued = queries::requeue_interrupted_running(&conn, error, now)?;
            Ok(u32::try_from(requeued).unwrap_or(u32::MAX))
        })
        .map_err(backend)
    }

    fn take_exhausted_running(&self, _now: DateTime<Utc>) -> RepositoryResult<Vec<BackgroundTask>> {
        with_busy_retry(|| {
            let conn = self.db.write();
            let rows = queries::exhausted_running_rows(&conn)?;
            queries::delete_exhausted_running(&conn)?;
            Ok(rows.into_iter().map(|row| reconstitute(row).data).collect())
        })
        .map_err(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::database::{Database, TempDatabase};
    use chrono::Duration;
    use valqeron_core::{ExecutionOutcome, TaskKind, TaskStatus};

    const INTERRUPTED: &str = "interrupted: the engine stopped while the task was running";

    fn test_repo() -> (TempDatabase, SqliteBackgroundTaskRepository) {
        let db = Database::open_temp();
        let repo = SqliteBackgroundTaskRepository::new(db.handle());
        (db, repo)
    }

    fn kind(name: &str) -> TaskKind {
        TaskKind::new(name).unwrap()
    }

    fn task_due_at(at: chrono::DateTime<Utc>, max_attempts: u32) -> BackgroundTask {
        BackgroundTask::builder()
            .kind(kind("test_task"))
            .scheduled_at(at)
            .max_attempts(max_attempts)
            .retry_delay_secs(30)
            .build()
            .unwrap()
    }

    fn terminal(outcome: ExecutionOutcome, finished_at: chrono::DateTime<Utc>) -> TaskCompletion {
        TaskCompletion::Terminal {
            outcome,
            error: None,
            finished_at,
        }
    }

    #[test]
    fn insert_then_find_round_trips_all_fields() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        let task = BackgroundTask::builder()
            .kind(kind("round_trip"))
            .payload(r#"{"limit":5}"#)
            .scheduled_at(now + Duration::minutes(5))
            .max_attempts(3)
            .retry_delay_secs(60)
            .created_at(now)
            .build()
            .unwrap();

        repo.insert(&task).unwrap();

        let found = repo.find_by_id(task.id()).unwrap().expect("task found");
        assert_eq!(found.version, 1);
        assert_eq!(found.data.kind().as_str(), "round_trip");
        assert_eq!(found.data.status(), TaskStatus::Pending);
        assert_eq!(found.data.payload(), Some(r#"{"limit":5}"#));
        assert_eq!(found.data.attempts(), 0);
        assert_eq!(found.data.max_attempts(), 3);
        assert_eq!(found.data.retry_delay_secs(), 60);
        assert!(found.data.started_at().is_none());
        assert!(found.data.last_error().is_none());
    }

    /// The watermark query: earliest PENDING `scheduled_at`, excluding
    /// kinds an operator disabled (their frozen rows must not produce a
    /// past watermark).
    #[test]
    fn next_due_at_reports_the_earliest_claimable_row() {
        let (db, repo) = test_repo();
        assert_eq!(repo.next_due_at().unwrap(), None, "empty queue");

        // Millisecond precision: stored timestamps are canonical `.3f`.
        let now = chrono::SubsecRound::trunc_subsecs(Utc::now(), 3);
        let sooner = now + Duration::minutes(5);
        let later = now + Duration::minutes(30);
        let frozen = BackgroundTask::builder()
            .kind(kind("frozen_kind"))
            .scheduled_at(now - Duration::minutes(1))
            .max_attempts(1)
            .build()
            .unwrap();
        repo.insert(&frozen).unwrap();
        repo.insert(&task_due_at(later, 1)).unwrap();
        repo.insert(&task_due_at(sooner, 1)).unwrap();

        // Without a registry row, every kind is claimable: the frozen
        // kind's past-due row is the minimum.
        let watermark = repo.next_due_at().unwrap().expect("rows queued");
        assert!(watermark < now, "past-due row wins");

        // Disable the kind: its rows leave the watermark entirely.
        {
            let handle = db.handle();
            let conn = handle.write();
            conn.execute_batch(
                "INSERT INTO task_registry
                     (kind, category, trigger_kind, tracking, schedule, log_policy,
                      enabled, first_registered_at, updated_at)
                 VALUES ('frozen_kind', 'OTHER', 'INTERVAL', 'DURABLE', 'interval:1s',
                         'ALL', 0, '2026-08-01T00:00:00.000Z', '2026-08-01T00:00:00.000Z');",
            )
            .unwrap();
        }
        assert_eq!(
            repo.next_due_at().unwrap(),
            Some(sooner),
            "disabled kinds are excluded; the earliest claimable row wins"
        );
    }

    #[test]
    fn claim_due_claims_oldest_first_and_counts_the_attempt() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        let older = task_due_at(now - Duration::minutes(10), 1);
        let newer = task_due_at(now - Duration::minutes(5), 1);
        repo.insert(&newer).unwrap();
        repo.insert(&older).unwrap();

        let claimed = repo.claim_due(now, 1).unwrap();
        assert_eq!(claimed.len(), 1);
        let first = &claimed[0];
        assert_eq!(first.data.id(), older.id(), "oldest due task claims first");
        assert_eq!(first.data.status(), TaskStatus::Running);
        assert_eq!(first.data.attempts(), 1);
        assert!(first.data.started_at().is_some());
        assert_eq!(first.version, 2, "claiming bumps the version");

        // The second claim picks the remaining task.
        let claimed = repo.claim_due(now, 8).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].data.id(), newer.id());

        // Nothing PENDING remains.
        assert!(repo.claim_due(now, 8).unwrap().is_empty());
    }

    #[test]
    fn exists_active_sees_every_queue_row_until_completion_removes_it() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        let task_kind = kind("test_task");
        assert!(!repo.exists_active(&task_kind).unwrap());

        repo.insert(&task_due_at(now, 1)).unwrap();
        assert!(repo.exists_active(&task_kind).unwrap(), "PENDING is active");

        let claimed = repo.claim_due(now, 1).unwrap().remove(0);
        assert!(repo.exists_active(&task_kind).unwrap(), "RUNNING is active");

        let outcome = repo
            .complete(
                claimed.data.id(),
                claimed.version,
                terminal(ExecutionOutcome::Succeeded, now),
            )
            .unwrap();
        assert!(outcome.applied());
        assert!(
            !repo.exists_active(&task_kind).unwrap(),
            "a terminal completion leaves the queue"
        );
    }

    #[test]
    fn claim_due_ignores_future_tasks() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        repo.insert(&task_due_at(now + Duration::minutes(5), 1))
            .unwrap();

        assert!(repo.claim_due(now, 8).unwrap().is_empty());
        assert_eq!(
            repo.claim_due(now + Duration::minutes(6), 8).unwrap().len(),
            1,
            "the same task claims once due"
        );
    }

    #[test]
    fn terminal_completion_deletes_the_row() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        repo.insert(&task_due_at(now, 1)).unwrap();
        let claimed = repo.claim_due(now, 1).unwrap().remove(0);

        let outcome = repo
            .complete(
                claimed.data.id(),
                claimed.version,
                terminal(ExecutionOutcome::Succeeded, now),
            )
            .unwrap();
        assert_eq!(outcome, WriteOutcome::Applied);
        assert!(
            repo.find_by_id(claimed.data.id()).unwrap().is_none(),
            "the queue holds live work only"
        );
    }

    #[test]
    fn complete_retry_requeues_at_the_backoff_time() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        repo.insert(&task_due_at(now, 3)).unwrap();
        let claimed = repo.claim_due(now, 1).unwrap().remove(0);

        let retry_at = claimed.data.next_retry_at(now);
        let outcome = repo
            .complete(
                claimed.data.id(),
                claimed.version,
                TaskCompletion::Retry {
                    error: "boom".into(),
                    failed_at: now,
                    retry_at,
                },
            )
            .unwrap();
        assert_eq!(outcome, WriteOutcome::Applied);

        let requeued = repo.find_by_id(claimed.data.id()).unwrap().unwrap();
        assert_eq!(requeued.data.status(), TaskStatus::Pending);
        assert_eq!(requeued.data.last_error(), Some("boom"));
        assert_eq!(requeued.data.attempts(), 1, "attempt count is preserved");

        // Not due before the backoff expires; due after.
        assert!(repo.claim_due(now, 8).unwrap().is_empty());
        let reclaimed = repo.claim_due(retry_at, 8).unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].data.attempts(), 2);
    }

    #[test]
    fn complete_reports_version_mismatch_on_stale_version() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        repo.insert(&task_due_at(now, 1)).unwrap();
        let claimed = repo.claim_due(now, 1).unwrap().remove(0);

        let stale = repo
            .complete(
                claimed.data.id(),
                claimed.version - 1,
                terminal(ExecutionOutcome::Succeeded, now),
            )
            .unwrap();
        assert_eq!(
            stale,
            WriteOutcome::VersionMismatch {
                expected: claimed.version - 1,
                actual: claimed.version,
            }
        );
        assert!(
            repo.find_by_id(claimed.data.id()).unwrap().is_some(),
            "a stale completion must not delete the row"
        );
    }

    #[test]
    fn complete_reports_missing_for_unknown_ids() {
        let (_db, repo) = test_repo();
        let outcome = repo
            .complete(
                &TaskId::new(),
                1,
                terminal(ExecutionOutcome::Succeeded, Utc::now()),
            )
            .unwrap();
        assert_eq!(outcome, WriteOutcome::Missing);
    }

    #[test]
    fn recovery_requeues_or_takes_by_attempt_budget() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        // Task A: 1 of 3 attempts spent → requeued.
        let a = task_due_at(now, 3);
        // Task B: 1 of 1 attempts spent → removed and returned.
        let b = task_due_at(now, 1);
        repo.insert(&a).unwrap();
        repo.insert(&b).unwrap();
        let claimed = repo.claim_due(now, 8).unwrap();
        assert_eq!(claimed.len(), 2, "both tasks are RUNNING now");

        let requeued = repo.requeue_interrupted(INTERRUPTED, now).unwrap();
        assert_eq!(requeued, 1);
        let exhausted = repo.take_exhausted_running(now).unwrap();
        assert_eq!(exhausted.len(), 1);
        assert_eq!(exhausted[0].id(), b.id());
        assert_eq!(exhausted[0].attempts(), 1);

        let a_row = repo.find_by_id(a.id()).unwrap().unwrap();
        assert_eq!(a_row.data.status(), TaskStatus::Pending);
        assert_eq!(a_row.data.last_error(), Some(INTERRUPTED));

        assert!(
            repo.find_by_id(b.id()).unwrap().is_none(),
            "exhausted rows leave the queue"
        );

        // Idempotent: nothing RUNNING remains.
        assert_eq!(repo.requeue_interrupted(INTERRUPTED, now).unwrap(), 0);
        assert!(repo.take_exhausted_running(now).unwrap().is_empty());
    }

    #[test]
    fn take_pending_removes_and_returns_only_pending_rows() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        let running = task_due_at(now - Duration::minutes(1), 1);
        let pending = task_due_at(now + Duration::minutes(5), 1);
        repo.insert(&running).unwrap();
        repo.insert(&pending).unwrap();
        assert_eq!(repo.claim_due(now, 1).unwrap().len(), 1);

        let taken = repo.take_pending(&kind("test_task")).unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].id(), pending.id());

        assert!(repo.find_by_id(pending.id()).unwrap().is_none());
        assert!(
            repo.find_by_id(running.id()).unwrap().is_some(),
            "RUNNING rows are not cancelled"
        );

        let conn = repo.db.read();
        assert_eq!(
            queries::count_by_kind(&conn, &kind("test_task")).unwrap(),
            1
        );
    }
}
