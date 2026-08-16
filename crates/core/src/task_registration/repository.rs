use chrono::{DateTime, Utc};
use std::rc::Rc;
use std::sync::Arc;

use crate::common::RepositoryResult;
use crate::task::TaskKind;
use crate::task_registration::{RunOutcome, TaskDeclaration, TaskRegistration};

/// Persistence port for the task catalog.
///
/// One row per kind, written only by the engine's task manager (single
/// writer under the instance lock), so plain upserts are enough — no
/// optimistic versioning. The `paused` flag is additionally flipped by
/// operators (SQL today, RPC later); those are single-column updates that
/// cannot conflict with the manager's writes.
#[cfg_attr(test, mockall::automock)]
pub trait TaskRegistrationRepository {
    /// Upsert a registration from its code declaration. On conflict only
    /// the declaration columns (category, tier, tracking, schedule, source,
    /// log policy, `config_enabled`, `registered = 1`) are rewritten —
    /// operator intent (`paused`) and the run summary are preserved.
    fn declare(&self, declaration: &TaskDeclaration, now: DateTime<Utc>) -> RepositoryResult<()>;

    /// Mark every currently registered kind NOT in `kinds` as retired.
    /// Returns the kinds that were retired by this call.
    fn retire_missing(
        &self,
        kinds: &[TaskKind],
        now: DateTime<Utc>,
    ) -> RepositoryResult<Vec<TaskKind>>;

    fn get(&self, kind: &TaskKind) -> RepositoryResult<Option<TaskRegistration>>;

    /// Every registration (including retired ones), ordered by category
    /// then kind.
    fn list(&self) -> RepositoryResult<Vec<TaskRegistration>>;

    /// Whether the kind is operator-paused. Unknown kinds are not paused.
    fn is_paused(&self, kind: &TaskKind) -> RepositoryResult<bool>;

    /// Flip the operator pause flag. Returns whether the row existed.
    fn set_paused(
        &self,
        kind: &TaskKind,
        paused: bool,
        now: DateTime<Utc>,
    ) -> RepositoryResult<bool>;

    /// Record a terminal run on the registration's prune-proof summary.
    fn record_run(
        &self,
        kind: &TaskKind,
        outcome: RunOutcome,
        error: Option<String>,
        at: DateTime<Utc>,
    ) -> RepositoryResult<()>;
}

macro_rules! delegate_task_registration_repository {
    ($ty:ty) => {
        impl<R: TaskRegistrationRepository + ?Sized> TaskRegistrationRepository for $ty {
            fn declare(
                &self,
                declaration: &TaskDeclaration,
                now: DateTime<Utc>,
            ) -> RepositoryResult<()> {
                (**self).declare(declaration, now)
            }
            fn retire_missing(
                &self,
                kinds: &[TaskKind],
                now: DateTime<Utc>,
            ) -> RepositoryResult<Vec<TaskKind>> {
                (**self).retire_missing(kinds, now)
            }
            fn get(&self, kind: &TaskKind) -> RepositoryResult<Option<TaskRegistration>> {
                (**self).get(kind)
            }
            fn list(&self) -> RepositoryResult<Vec<TaskRegistration>> {
                (**self).list()
            }
            fn is_paused(&self, kind: &TaskKind) -> RepositoryResult<bool> {
                (**self).is_paused(kind)
            }
            fn set_paused(
                &self,
                kind: &TaskKind,
                paused: bool,
                now: DateTime<Utc>,
            ) -> RepositoryResult<bool> {
                (**self).set_paused(kind, paused, now)
            }
            fn record_run(
                &self,
                kind: &TaskKind,
                outcome: RunOutcome,
                error: Option<String>,
                at: DateTime<Utc>,
            ) -> RepositoryResult<()> {
                (**self).record_run(kind, outcome, error, at)
            }
        }
    };
}

delegate_task_registration_repository!(Box<R>);
delegate_task_registration_repository!(Rc<R>);
delegate_task_registration_repository!(Arc<R>);
