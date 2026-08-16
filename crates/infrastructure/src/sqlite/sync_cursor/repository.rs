use valqeron_core::{RepositoryResult, SyncCursor, SyncCursorRepository, SyncSource};

use crate::sqlite::database::{Db, DbHandle};
use crate::sqlite::support::{backend, with_busy_retry};
use crate::sqlite::sync_cursor::queries;

pub struct SqliteSyncCursorRepository {
    db: DbHandle,
}

impl SqliteSyncCursorRepository {
    pub(crate) fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

impl SyncCursorRepository for SqliteSyncCursorRepository {
    fn get(&self, source: &SyncSource) -> RepositoryResult<Option<SyncCursor>> {
        let conn = self.db.read();
        Ok(queries::get(&conn, source)
            .map_err(backend)?
            .map(|row| SyncCursor::reconstitute(row.into_inner())))
    }

    fn upsert(&self, cursor: &SyncCursor) -> RepositoryResult<()> {
        with_busy_retry(|| {
            let conn = self.db.write();
            queries::upsert(&conn, cursor).map(|_| ())
        })
        .map_err(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::database::{Database, TempDatabase};
    use chrono::{NaiveDate, TimeZone, Utc};
    use valqeron_core::SyncOutcomeKind;

    fn test_repo() -> (TempDatabase, SqliteSyncCursorRepository) {
        let db = Database::open_temp();
        let repo = SqliteSyncCursorRepository::new(db.handle());
        (db, repo)
    }

    fn source(name: &str) -> SyncSource {
        SyncSource::new(name).unwrap()
    }

    fn utc(y: i32, m: u32, d: u32, h: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0).single().unwrap()
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn get_returns_none_for_an_unknown_source() {
        let (_db, repo) = test_repo();
        assert!(repo.get(&source("cvm")).unwrap().is_none());
    }

    #[test]
    fn upsert_then_get_round_trips_all_fields() {
        let (_db, repo) = test_repo();
        let now = utc(2026, 8, 12, 12);
        let cursor =
            SyncCursor::seeded(source("cvm"), utc(2026, 8, 11, 10), date(2026, 8, 10), now).failed(
                "connection refused".into(),
                utc(2026, 8, 12, 13),
                now,
            );

        repo.upsert(&cursor).unwrap();

        let found = repo.get(&source("cvm")).unwrap().expect("cursor found");
        assert_eq!(found.source().as_str(), "cvm");
        assert_eq!(found.through_slot(), utc(2026, 8, 11, 10));
        assert_eq!(found.through_target(), date(2026, 8, 10));
        assert_eq!(found.cooldown_until(), Some(utc(2026, 8, 12, 13)));
        assert_eq!(found.consecutive_failures(), 1);
        assert_eq!(found.last_outcome(), Some(SyncOutcomeKind::Failed));
        assert_eq!(found.last_error(), Some("connection refused"));
        assert_eq!(found.updated_at(), now);
    }

    #[test]
    fn upsert_replaces_the_existing_row() {
        let (_db, repo) = test_repo();
        let now = utc(2026, 8, 12, 12);
        let seeded =
            SyncCursor::seeded(source("cvm"), utc(2026, 8, 11, 10), date(2026, 8, 10), now);
        repo.upsert(&seeded).unwrap();

        let advanced = seeded.advanced(utc(2026, 8, 12, 10), date(2026, 8, 11), now);
        repo.upsert(&advanced).unwrap();

        let found = repo.get(&source("cvm")).unwrap().expect("cursor found");
        assert_eq!(found.through_slot(), utc(2026, 8, 12, 10));
        assert_eq!(found.through_target(), date(2026, 8, 11));
        assert_eq!(found.last_outcome(), Some(SyncOutcomeKind::Synced));
        assert_eq!(found.cooldown_until(), None, "advance clears the cooldown");
    }

    #[test]
    fn sources_are_independent_rows() {
        let (_db, repo) = test_repo();
        let now = utc(2026, 8, 12, 12);
        let cvm = SyncCursor::seeded(source("cvm"), utc(2026, 8, 11, 10), date(2026, 8, 10), now);
        let b3 = SyncCursor::seeded(source("b3"), utc(2026, 8, 5, 10), date(2026, 8, 4), now);
        repo.upsert(&cvm).unwrap();
        repo.upsert(&b3).unwrap();

        let found_cvm = repo.get(&source("cvm")).unwrap().expect("cvm row");
        let found_b3 = repo.get(&source("b3")).unwrap().expect("b3 row");
        assert_eq!(found_cvm.through_target(), date(2026, 8, 10));
        assert_eq!(found_b3.through_target(), date(2026, 8, 4));
    }

    #[test]
    fn not_ready_state_round_trips_without_error_text() {
        let (_db, repo) = test_repo();
        let now = utc(2026, 8, 12, 12);
        let cursor =
            SyncCursor::seeded(source("cvm"), utc(2026, 8, 11, 10), date(2026, 8, 10), now)
                .held_not_ready(600, now);
        repo.upsert(&cursor).unwrap();

        let found = repo.get(&source("cvm")).unwrap().expect("cursor found");
        assert_eq!(found.last_outcome(), Some(SyncOutcomeKind::NotReady));
        assert_eq!(found.consecutive_failures(), 0);
        assert!(found.last_error().is_none());
        assert!(found.cooldown_until().is_some());
    }
}
