use chrono::{DateTime, Utc};
use valqeron_core::{
    RepositoryResult, TaskDeclaration, TaskKind, TaskRegistration, TaskRegistryRepository,
    TaskSettings,
};

use crate::sqlite::database::{Db, DbHandle};
use crate::sqlite::support::{backend, with_busy_retry};
use crate::sqlite::task_registry::queries;

pub struct SqliteTaskRegistryRepository {
    db: DbHandle,
}

impl SqliteTaskRegistryRepository {
    pub(crate) fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

impl TaskRegistryRepository for SqliteTaskRegistryRepository {
    fn declare(&self, declaration: &TaskDeclaration, now: DateTime<Utc>) -> RepositoryResult<()> {
        with_busy_retry(|| {
            let conn = self.db.write();
            queries::declare(&conn, declaration, now).map(|_| ())
        })
        .map_err(backend)
    }

    fn retire_missing(
        &self,
        kinds: &[TaskKind],
        now: DateTime<Utc>,
    ) -> RepositoryResult<Vec<TaskKind>> {
        with_busy_retry(|| {
            // One writer guard spans select-then-retire, so the set is
            // consistent.
            let conn = self.db.write();
            let missing = queries::registered_kinds_not_in(&conn, kinds)?;
            let mut retired = Vec::with_capacity(missing.len());
            for kind in missing {
                queries::retire(&conn, &kind, now)?;
                if let Ok(kind) = TaskKind::new(kind) {
                    retired.push(kind);
                }
            }
            Ok(retired)
        })
        .map_err(backend)
    }

    fn get(&self, kind: &TaskKind) -> RepositoryResult<Option<TaskRegistration>> {
        let conn = self.db.read();
        Ok(queries::get(&conn, kind)
            .map_err(backend)?
            .map(|row| TaskRegistration::reconstitute(row.into_inner())))
    }

    fn list(&self) -> RepositoryResult<Vec<TaskRegistration>> {
        let conn = self.db.read();
        Ok(queries::list(&conn)
            .map_err(backend)?
            .into_iter()
            .map(|row| TaskRegistration::reconstitute(row.into_inner()))
            .collect())
    }

    fn is_enabled(&self, kind: &TaskKind) -> RepositoryResult<bool> {
        let conn = self.db.read();
        Ok(queries::is_enabled(&conn, kind)
            .map_err(backend)?
            .unwrap_or(true))
    }

    fn set_enabled(
        &self,
        kind: &TaskKind,
        enabled: bool,
        now: DateTime<Utc>,
    ) -> RepositoryResult<bool> {
        with_busy_retry(|| {
            let conn = self.db.write();
            queries::set_enabled(&conn, kind, enabled, now).map(|affected| affected > 0)
        })
        .map_err(backend)
    }

    fn update_settings(
        &self,
        kind: &TaskKind,
        settings: &TaskSettings,
        now: DateTime<Utc>,
    ) -> RepositoryResult<bool> {
        with_busy_retry(|| {
            let conn = self.db.write();
            queries::update_settings(&conn, kind, settings, now).map(|affected| affected > 0)
        })
        .map_err(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::database::{Database, TempDatabase};
    use chrono::{NaiveTime, TimeZone};
    use valqeron_core::{
        LogPolicy, Recurrence, SyncSource, TaskCategory, TaskTracking, TaskTrigger,
    };

    fn test_repo() -> (TempDatabase, SqliteTaskRegistryRepository) {
        let db = Database::open_temp();
        let repo = SqliteTaskRegistryRepository::new(db.handle());
        (db, repo)
    }

    fn kind(name: &str) -> TaskKind {
        TaskKind::new(name).unwrap()
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 17, 9, 0, 0).single().unwrap()
    }

    fn default_settings() -> TaskSettings {
        TaskSettings {
            period_secs: None,
            at_local: NaiveTime::from_hms_opt(7, 0, 0),
            recurrence: Some(Recurrence::Daily),
            cooldown_secs: Some(300),
            max_backfill_days: Some(90),
        }
    }

    fn declaration(name: &str) -> TaskDeclaration {
        TaskDeclaration {
            kind: kind(name),
            category: TaskCategory::FinanceDataSync,
            trigger: TaskTrigger::Sync,
            tracking: TaskTracking::Durable,
            schedule: "sync:daily@07:00-03:00".into(),
            source: Some(SyncSource::new("cvm").unwrap()),
            log_policy: LogPolicy::All,
            settings: default_settings(),
        }
    }

    #[test]
    fn declare_then_get_round_trips_all_fields() {
        let (_db, repo) = test_repo();
        repo.declare(&declaration("cvm_daily_sync"), now()).unwrap();

        let found = repo.get(&kind("cvm_daily_sync")).unwrap().expect("row");
        assert_eq!(found.kind().as_str(), "cvm_daily_sync");
        assert_eq!(found.category(), TaskCategory::FinanceDataSync);
        assert_eq!(found.trigger(), TaskTrigger::Sync);
        assert_eq!(found.tracking(), TaskTracking::Durable);
        assert_eq!(found.schedule(), "sync:daily@07:00-03:00");
        assert_eq!(found.source().map(|s| s.as_str()), Some("cvm"));
        assert_eq!(found.log_policy(), LogPolicy::All);
        assert!(found.enabled());
        assert_eq!(*found.settings(), default_settings());
        assert!(found.registered());
        assert_eq!(found.first_registered_at(), now());
    }

    #[test]
    fn redeclare_preserves_operator_intent() {
        let (_db, repo) = test_repo();
        let k = kind("cvm_daily_sync");
        repo.declare(&declaration("cvm_daily_sync"), now()).unwrap();
        repo.set_enabled(&k, false, now()).unwrap();
        let overridden = TaskSettings {
            at_local: NaiveTime::from_hms_opt(9, 30, 0),
            ..default_settings()
        };
        repo.update_settings(&k, &overridden, now()).unwrap();

        // Next boot re-declares with a changed descriptor and changed code
        // defaults: identity is rewritten, operator values are not.
        let mut redeclared = declaration("cvm_daily_sync");
        redeclared.schedule = "sync:daily@08:30-03:00".into();
        redeclared.settings.at_local = NaiveTime::from_hms_opt(8, 30, 0);
        redeclared.settings.cooldown_secs = Some(600);
        let later = now() + chrono::Duration::hours(1);
        repo.declare(&redeclared, later).unwrap();

        let found = repo.get(&k).unwrap().expect("row");
        assert_eq!(
            found.schedule(),
            "sync:daily@08:30-03:00",
            "declaration updated"
        );
        assert!(!found.enabled(), "operator intent preserved");
        assert_eq!(
            found.settings().at_local,
            NaiveTime::from_hms_opt(9, 30, 0),
            "operator setting preserved over the new code default"
        );
        assert_eq!(
            found.settings().cooldown_secs,
            Some(300),
            "non-NULL setting keeps its original value"
        );
        assert_eq!(
            found.first_registered_at(),
            now(),
            "first registration timestamp preserved"
        );
    }

    #[test]
    fn cleared_setting_refills_from_the_code_default_at_redeclare() {
        let (_db, repo) = test_repo();
        let k = kind("cvm_daily_sync");
        repo.declare(&declaration("cvm_daily_sync"), now()).unwrap();

        // Operator resets cooldown to "code default" (NULL).
        let cleared = TaskSettings {
            cooldown_secs: None,
            ..default_settings()
        };
        repo.update_settings(&k, &cleared, now()).unwrap();
        assert_eq!(
            repo.get(&k).unwrap().expect("row").settings().cooldown_secs,
            None
        );

        // The next boot fills the NULL with the declared default.
        repo.declare(&declaration("cvm_daily_sync"), now()).unwrap();
        assert_eq!(
            repo.get(&k).unwrap().expect("row").settings().cooldown_secs,
            Some(300)
        );
    }

    #[test]
    fn code_owned_settings_stay_null_across_boots() {
        let (_db, repo) = test_repo();
        let k = kind("watchdog");
        let mut pinned = declaration("watchdog");
        pinned.trigger = TaskTrigger::Interval;
        pinned.source = None;
        pinned.settings = TaskSettings::default();
        repo.declare(&pinned, now()).unwrap();
        repo.declare(&pinned, now()).unwrap();

        let found = repo.get(&k).unwrap().expect("row");
        assert_eq!(*found.settings(), TaskSettings::default());
    }

    #[test]
    fn unparseable_settings_are_ignored_not_fatal() {
        let (db, repo) = test_repo();
        repo.declare(&declaration("t"), now()).unwrap();
        {
            let handle = db.handle();
            let conn = handle.write();
            conn.execute(
                "UPDATE task_registry SET at_local = '99:99', recurrence = 'fortnightly' \
                 WHERE kind = 't'",
                [],
            )
            .unwrap();
        }

        let found = repo.get(&kind("t")).unwrap().expect("row");
        assert_eq!(found.settings().at_local, None, "garbage reads as unset");
        assert_eq!(found.settings().recurrence, None, "garbage reads as unset");
        assert_eq!(found.settings().cooldown_secs, Some(300), "rest intact");
    }

    #[test]
    fn retire_missing_flags_only_absent_kinds_and_declare_revives() {
        let (_db, repo) = test_repo();
        repo.declare(&declaration("keep_me"), now()).unwrap();
        repo.declare(&declaration("retire_me"), now()).unwrap();

        let retired = repo.retire_missing(&[kind("keep_me")], now()).unwrap();
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].as_str(), "retire_me");

        assert!(repo.get(&kind("keep_me")).unwrap().unwrap().registered());
        assert!(!repo.get(&kind("retire_me")).unwrap().unwrap().registered());

        // Retiring again is a no-op (already retired rows are skipped).
        let again = repo.retire_missing(&[kind("keep_me")], now()).unwrap();
        assert!(again.is_empty());

        // A kind that returns to code is revived by declare.
        repo.declare(&declaration("retire_me"), now()).unwrap();
        assert!(repo.get(&kind("retire_me")).unwrap().unwrap().registered());
    }

    #[test]
    fn retire_missing_with_no_kinds_retires_everything() {
        let (_db, repo) = test_repo();
        repo.declare(&declaration("a"), now()).unwrap();
        repo.declare(&declaration("b"), now()).unwrap();
        let retired = repo.retire_missing(&[], now()).unwrap();
        assert_eq!(retired.len(), 2);
    }

    #[test]
    fn is_enabled_and_set_enabled_semantics() {
        let (_db, repo) = test_repo();
        let k = kind("t");
        assert!(repo.is_enabled(&k).unwrap(), "unknown kinds fail open");
        assert!(
            !repo.set_enabled(&k, false, now()).unwrap(),
            "no row to flip"
        );
        assert!(
            !repo
                .update_settings(&k, &default_settings(), now())
                .unwrap(),
            "no row to update"
        );

        repo.declare(&declaration("t"), now()).unwrap();
        assert!(repo.set_enabled(&k, false, now()).unwrap());
        assert!(!repo.is_enabled(&k).unwrap());
        assert!(repo.set_enabled(&k, true, now()).unwrap());
        assert!(repo.is_enabled(&k).unwrap());
    }

    #[test]
    fn list_orders_by_category_then_kind() {
        let (_db, repo) = test_repo();
        let mut sys = declaration("z_system");
        sys.category = TaskCategory::EngineSystem;
        sys.trigger = TaskTrigger::Interval;
        sys.source = None;
        repo.declare(&sys, now()).unwrap();
        repo.declare(&declaration("a_sync"), now()).unwrap();

        let listed = repo.list().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].kind().as_str(), "z_system", "ENGINE_SYSTEM first");
        assert_eq!(listed[1].kind().as_str(), "a_sync");
    }
}
