use chrono::{DateTime, Utc};
use valqeron_core::{RepositoryResult, TaskExecution, TaskExecutionRepository, TaskId};

use crate::sqlite::database::{Db, DbHandle};
use crate::sqlite::support::{backend, with_busy_retry};
use crate::sqlite::task_execution::queries;

pub struct SqliteTaskExecutionRepository {
    db: DbHandle,
}

impl SqliteTaskExecutionRepository {
    pub(crate) fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

impl TaskExecutionRepository for SqliteTaskExecutionRepository {
    fn insert(&self, execution: &TaskExecution) -> RepositoryResult<()> {
        with_busy_retry(|| {
            let conn = self.db.write();
            queries::insert(&conn, execution).map(|_| ())
        })
        .map_err(backend)
    }

    fn find_by_id(&self, id: &TaskId) -> RepositoryResult<Option<TaskExecution>> {
        let conn = self.db.read();
        Ok(queries::find_by_id(&conn, id)
            .map_err(backend)?
            .map(|row| row.into_inner()))
    }

    fn list_recent(&self, limit: u32) -> RepositoryResult<Vec<TaskExecution>> {
        let conn = self.db.read();
        Ok(queries::list_recent(&conn, limit)
            .map_err(backend)?
            .into_iter()
            .map(|row| row.into_inner())
            .collect())
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
    use chrono::{Duration, TimeZone};
    use valqeron_core::{ExecutionOutcome, TaskKind};

    fn test_repo() -> (TempDatabase, SqliteTaskExecutionRepository) {
        let db = Database::open_temp();
        let repo = SqliteTaskExecutionRepository::new(db.handle());
        (db, repo)
    }

    /// A fixed, millisecond-precision timestamp — the persisted canonical
    /// form — so round-trip equality holds.
    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
            .single()
            .unwrap()
    }

    fn execution(
        outcome: ExecutionOutcome,
        finished_at: DateTime<Utc>,
        error: Option<&str>,
    ) -> TaskExecution {
        TaskExecution {
            id: TaskId::new(),
            kind: TaskKind::new("test_task").unwrap(),
            outcome,
            payload: Some("slot|from|to".into()),
            scheduled_at: finished_at - Duration::minutes(1),
            started_at: Some(finished_at - Duration::seconds(30)),
            finished_at,
            attempts: 2,
            duration_ms: Some(1234),
            error: error.map(str::to_owned),
            created_at: finished_at - Duration::minutes(2),
        }
    }

    #[test]
    fn insert_then_find_round_trips_all_fields() {
        let (_db, repo) = test_repo();
        let record = execution(ExecutionOutcome::Failed, now(), Some("boom"));

        repo.insert(&record).unwrap();

        let found = repo.find_by_id(&record.id).unwrap().expect("row");
        assert_eq!(found, record);
    }

    #[test]
    fn list_recent_orders_by_finish_descending() {
        let (_db, repo) = test_repo();
        let now = now();
        let older = execution(
            ExecutionOutcome::Succeeded,
            now - Duration::minutes(10),
            None,
        );
        let newer = execution(ExecutionOutcome::NotReady, now, None);
        repo.insert(&older).unwrap();
        repo.insert(&newer).unwrap();

        let recent = repo.list_recent(10).unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].id, newer.id);
        assert_eq!(recent[0].outcome, ExecutionOutcome::NotReady);
        assert_eq!(recent[1].id, older.id);

        assert_eq!(repo.list_recent(1).unwrap().len(), 1);
    }

    #[test]
    fn prune_finished_removes_only_old_rows() {
        let (_db, repo) = test_repo();
        let now = now();
        let old = execution(ExecutionOutcome::Succeeded, now - Duration::days(10), None);
        let fresh = execution(ExecutionOutcome::Succeeded, now, None);
        repo.insert(&old).unwrap();
        repo.insert(&fresh).unwrap();

        let removed = repo.prune_finished(now - Duration::days(7)).unwrap();
        assert_eq!(removed, 1);
        assert!(repo.find_by_id(&old.id).unwrap().is_none());
        assert!(repo.find_by_id(&fresh.id).unwrap().is_some());
    }
}
