//! The task status read model: the catalog joined with the queue and the
//! sync cursors, statuses derived per kind — assembled fresh on every read
//! so it can never go stale.

use chrono::{DateTime, Utc};

use crate::common::RepositoryResult;
use crate::sync::SyncCursor;
use crate::sync::repository::SyncCursorRepository;
use crate::task::TaskStatus;
use crate::task::repository::BackgroundTaskRepository;
use crate::task_registration::repository::TaskRegistrationRepository;
use crate::task_registration::{DerivedTaskStatus, TaskRegistration, derive_status};

/// One task's assembled view: the registration, its derived status, and
/// the live scheduling facts behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusEntry {
    pub registration: TaskRegistration,
    pub status: DerivedTaskStatus,
    /// The earliest queued run, when one exists.
    pub next_run_at: Option<DateTime<Utc>>,
    /// The sync cursor, for sync-tier tasks.
    pub cursor: Option<SyncCursor>,
}

/// Assemble the status of every cataloged task (including retired ones).
pub fn list_task_statuses(
    registry: &impl TaskRegistrationRepository,
    tasks: &impl BackgroundTaskRepository,
    cursors: &impl SyncCursorRepository,
    halted_after: u32,
    now: DateTime<Utc>,
) -> RepositoryResult<Vec<TaskStatusEntry>> {
    let registrations = registry.list()?;
    let mut entries = Vec::with_capacity(registrations.len());
    for registration in registrations {
        let active = tasks
            .find_active(registration.kind())?
            .map(|versioned| versioned.data);
        let cursor = match registration.source() {
            Some(source) => cursors.get(source)?,
            None => None,
        };
        let status = derive_status(
            &registration,
            active.as_ref(),
            cursor.as_ref(),
            halted_after,
            now,
        );
        let next_run_at = active
            .as_ref()
            .filter(|task| task.status() == TaskStatus::Pending)
            .map(|task| task.scheduled_at());
        entries.push(TaskStatusEntry {
            registration,
            status,
            next_run_at,
            cursor,
        });
    }
    Ok(entries)
}
