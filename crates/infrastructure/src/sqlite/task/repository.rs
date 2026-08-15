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

    fn list_recent(&self, limit: u32) -> RepositoryResult<Vec<Versioned<BackgroundTask>>> {
        let conn = self.db.read();
        Ok(queries::list_recent(&conn, limit)
            .map_err(backend)?
            .into_iter()
            .map(reconstitute)
            .collect())
    }

    fn exists_active(&self, kind: &TaskKind) -> RepositoryResult<bool> {
        let conn = self.db.read();
        queries::exists_active(&conn, kind).map_err(backend)
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

    fn complete(
        &self,
        id: &TaskId,
        expected_version: u32,
        completion: TaskCompletion,
    ) -> RepositoryResult<WriteOutcome> {
        with_busy_retry(|| {
            let conn = self.db.write();
            let affected = match &completion {
                TaskCompletion::Succeeded { finished_at } => {
                    queries::complete_succeeded(&conn, id, expected_version, *finished_at)?
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
                TaskCompletion::Failed { error, finished_at } => {
                    queries::complete_failed(&conn, id, expected_version, error, *finished_at)?
                }
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

    fn reset_stale_running(&self, now: DateTime<Utc>) -> RepositoryResult<u32> {
        with_busy_retry(|| {
            let conn = self.db.write();
            let failed = queries::fail_exhausted_running(&conn, now)?;
            let requeued = queries::requeue_interrupted_running(&conn, now)?;
            Ok(u32::try_from(failed.saturating_add(requeued)).unwrap_or(u32::MAX))
        })
        .map_err(backend)
    }

    fn prune_finished(&self, older_than: DateTime<Utc>) -> RepositoryResult<u32> {
        with_busy_retry(|| {
            let conn = self.db.write();
            let removed = queries::prune_finished(&conn, older_than)?;
            Ok(u32::try_from(removed).unwrap_or(u32::MAX))
        })
        .map_err(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::database::{Database, TempDatabase};
    use chrono::Duration;
    use valqeron_core::{TaskKind, TaskStatus};

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
        assert!(found.data.finished_at().is_none());
        assert!(found.data.last_error().is_none());
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
    fn exists_active_sees_pending_and_running_but_not_terminal() {
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
                TaskCompletion::Succeeded { finished_at: now },
            )
            .unwrap();
        assert!(outcome.applied());
        assert!(
            !repo.exists_active(&task_kind).unwrap(),
            "terminal rows are not active"
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
    fn complete_succeeded_terminalizes_and_clears_the_error() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        repo.insert(&task_due_at(now, 1)).unwrap();
        let claimed = repo.claim_due(now, 1).unwrap().remove(0);

        let outcome = repo
            .complete(
                claimed.data.id(),
                claimed.version,
                TaskCompletion::Succeeded { finished_at: now },
            )
            .unwrap();
        assert_eq!(outcome, WriteOutcome::Applied);

        let done = repo.find_by_id(claimed.data.id()).unwrap().unwrap();
        assert_eq!(done.data.status(), TaskStatus::Succeeded);
        assert!(done.data.finished_at().is_some());
        assert!(done.data.last_error().is_none());
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
        assert!(requeued.data.finished_at().is_none(), "not terminal");

        // Not due before the backoff expires; due after.
        assert!(repo.claim_due(now, 8).unwrap().is_empty());
        let reclaimed = repo.claim_due(retry_at, 8).unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].data.attempts(), 2);
    }

    #[test]
    fn complete_failed_is_terminal() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        repo.insert(&task_due_at(now, 1)).unwrap();
        let claimed = repo.claim_due(now, 1).unwrap().remove(0);

        let outcome = repo
            .complete(
                claimed.data.id(),
                claimed.version,
                TaskCompletion::Failed {
                    error: "fatal".into(),
                    finished_at: now,
                },
            )
            .unwrap();
        assert_eq!(outcome, WriteOutcome::Applied);

        let failed = repo.find_by_id(claimed.data.id()).unwrap().unwrap();
        assert_eq!(failed.data.status(), TaskStatus::Failed);
        assert_eq!(failed.data.last_error(), Some("fatal"));
        assert!(repo.claim_due(now, 8).unwrap().is_empty());
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
                TaskCompletion::Succeeded { finished_at: now },
            )
            .unwrap();
        assert_eq!(
            stale,
            WriteOutcome::VersionMismatch {
                expected: claimed.version - 1,
                actual: claimed.version,
            }
        );
    }

    #[test]
    fn complete_reports_missing_for_unknown_ids() {
        let (_db, repo) = test_repo();
        let outcome = repo
            .complete(
                &TaskId::new(),
                1,
                TaskCompletion::Succeeded {
                    finished_at: Utc::now(),
                },
            )
            .unwrap();
        assert_eq!(outcome, WriteOutcome::Missing);
    }

    #[test]
    fn reset_stale_running_requeues_or_fails_by_attempt_budget() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        // Task A: 1 of 3 attempts spent → requeued.
        let a = task_due_at(now, 3);
        // Task B: 1 of 1 attempts spent → terminal FAILED.
        let b = task_due_at(now, 1);
        repo.insert(&a).unwrap();
        repo.insert(&b).unwrap();
        let claimed = repo.claim_due(now, 8).unwrap();
        assert_eq!(claimed.len(), 2, "both tasks are RUNNING now");

        let touched = repo.reset_stale_running(now).unwrap();
        assert_eq!(touched, 2);

        let a_row = repo.find_by_id(a.id()).unwrap().unwrap();
        assert_eq!(a_row.data.status(), TaskStatus::Pending);
        assert_eq!(a_row.data.last_error(), Some(queries::INTERRUPTED_ERROR));

        let b_row = repo.find_by_id(b.id()).unwrap().unwrap();
        assert_eq!(b_row.data.status(), TaskStatus::Failed);
        assert_eq!(b_row.data.last_error(), Some(queries::INTERRUPTED_ERROR));

        // Idempotent: nothing RUNNING remains.
        assert_eq!(repo.reset_stale_running(now).unwrap(), 0);
    }

    #[test]
    fn prune_finished_removes_only_old_terminal_rows() {
        let (_db, repo) = test_repo();
        let now = Utc::now();

        // Old succeeded row (prunable).
        repo.insert(&task_due_at(now, 1)).unwrap();
        let old_done = repo.claim_due(now, 1).unwrap().remove(0);
        let outcome = repo
            .complete(
                old_done.data.id(),
                old_done.version,
                TaskCompletion::Succeeded {
                    finished_at: now - Duration::days(10),
                },
            )
            .unwrap();
        assert!(outcome.applied());

        // Fresh succeeded row (kept).
        repo.insert(&task_due_at(now, 1)).unwrap();
        let fresh_done = repo.claim_due(now, 1).unwrap().remove(0);
        let outcome = repo
            .complete(
                fresh_done.data.id(),
                fresh_done.version,
                TaskCompletion::Succeeded { finished_at: now },
            )
            .unwrap();
        assert!(outcome.applied());

        // Pending row (kept regardless of age).
        let pending = task_due_at(now + Duration::minutes(1), 1);
        repo.insert(&pending).unwrap();

        let removed = repo.prune_finished(now - Duration::days(7)).unwrap();
        assert_eq!(removed, 1);
        assert!(repo.find_by_id(old_done.data.id()).unwrap().is_none());
        assert!(repo.find_by_id(fresh_done.data.id()).unwrap().is_some());
        assert!(repo.find_by_id(pending.id()).unwrap().is_some());
    }

    #[test]
    fn list_recent_orders_by_schedule_descending() {
        let (_db, repo) = test_repo();
        let now = Utc::now();
        let older = task_due_at(now - Duration::minutes(10), 1);
        let newer = task_due_at(now, 1);
        repo.insert(&older).unwrap();
        repo.insert(&newer).unwrap();

        let recent = repo.list_recent(10).unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].data.id(), newer.id());
        assert_eq!(recent[1].data.id(), older.id());

        let limited = repo.list_recent(1).unwrap();
        assert_eq!(limited.len(), 1);

        let conn = repo.db.read();
        assert_eq!(
            queries::count_by_kind(&conn, &kind("test_task")).unwrap(),
            2
        );
    }
}
