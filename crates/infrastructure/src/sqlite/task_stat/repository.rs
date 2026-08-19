use chrono::{DateTime, Utc};
use valqeron_core::{ExecutionOutcome, RepositoryResult, TaskKind, TaskStatRepository, TaskStats};

use crate::sqlite::database::{Db, DbHandle};
use crate::sqlite::support::{backend, with_busy_retry};
use crate::sqlite::task_stat::queries;

pub struct SqliteTaskStatRepository {
    db: DbHandle,
}

impl SqliteTaskStatRepository {
    pub(crate) fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

impl TaskStatRepository for SqliteTaskStatRepository {
    fn record_run(
        &self,
        kind: &TaskKind,
        outcome: ExecutionOutcome,
        error: Option<String>,
        duration_ms: Option<u64>,
        at: DateTime<Utc>,
    ) -> RepositoryResult<()> {
        with_busy_retry(|| {
            let conn = self.db.write();
            queries::record_run(&conn, kind, outcome, error.as_deref(), duration_ms, at).map(|_| ())
        })
        .map_err(backend)
    }

    fn get(&self, kind: &TaskKind) -> RepositoryResult<Option<TaskStats>> {
        let conn = self.db.read();
        Ok(queries::get(&conn, kind)
            .map_err(backend)?
            .map(|row| row.into_inner()))
    }

    fn list(&self) -> RepositoryResult<Vec<TaskStats>> {
        let conn = self.db.read();
        Ok(queries::list(&conn)
            .map_err(backend)?
            .into_iter()
            .map(|row| row.into_inner())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::database::{Database, TempDatabase};
    use chrono::TimeZone;

    fn test_repo() -> (TempDatabase, SqliteTaskStatRepository) {
        let db = Database::open_temp();
        let repo = SqliteTaskStatRepository::new(db.handle());
        (db, repo)
    }

    fn kind(name: &str) -> TaskKind {
        TaskKind::new(name).unwrap()
    }

    fn at(h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 17, h, 0, 0).single().unwrap()
    }

    #[test]
    fn record_run_folds_totals_and_last_state_forward() {
        let (_db, repo) = test_repo();
        let k = kind("t");
        assert!(repo.get(&k).unwrap().is_none());

        repo.record_run(&k, ExecutionOutcome::Succeeded, None, Some(100), at(9))
            .unwrap();
        repo.record_run(
            &k,
            ExecutionOutcome::Failed,
            Some("boom".into()),
            Some(50),
            at(10),
        )
        .unwrap();
        repo.record_run(&k, ExecutionOutcome::NotReady, None, None, at(11))
            .unwrap();

        let stats = repo.get(&k).unwrap().expect("row");
        assert_eq!(stats.total_runs, 3, "every terminal outcome counts");
        assert_eq!(stats.total_failures, 1, "only FAILED counts as a failure");
        assert_eq!(stats.total_duration_ms, 150, "unmeasured runs add nothing");
        assert_eq!(stats.last_run_at, Some(at(11)));
        assert_eq!(stats.last_outcome, Some(ExecutionOutcome::NotReady));
        assert_eq!(stats.last_error, None, "last error reflects the last run");
        assert_eq!(
            stats.last_success_at,
            Some(at(9)),
            "the last success survives later non-successes"
        );
        assert_eq!(stats.last_duration_ms, None);
    }

    #[test]
    fn list_orders_by_kind() {
        let (_db, repo) = test_repo();
        repo.record_run(&kind("b"), ExecutionOutcome::Succeeded, None, None, at(9))
            .unwrap();
        repo.record_run(&kind("a"), ExecutionOutcome::Succeeded, None, None, at(9))
            .unwrap();

        let listed = repo.list().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].kind.as_str(), "a");
        assert_eq!(listed[1].kind.as_str(), "b");
    }
}
