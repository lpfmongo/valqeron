use std::rc::Rc;
use std::sync::Arc;

use crate::common::RepositoryResult;
use crate::sync::{SyncCursor, SyncSource};

/// Persistence port for sync-source progress cursors.
///
/// One row per source, written only by the engine's sync reconciler and
/// completion hook (single writer under the engine's instance lock), so
/// plain upsert semantics are enough — no optimistic versioning.
#[cfg_attr(test, mockall::automock)]
pub trait SyncCursorRepository {
    fn get(&self, source: &SyncSource) -> RepositoryResult<Option<SyncCursor>>;

    /// Insert or fully replace the cursor row for `cursor.source()`.
    fn upsert(&self, cursor: &SyncCursor) -> RepositoryResult<()>;
}

macro_rules! delegate_sync_cursor_repository {
    ($ty:ty) => {
        impl<R: SyncCursorRepository + ?Sized> SyncCursorRepository for $ty {
            fn get(&self, source: &SyncSource) -> RepositoryResult<Option<SyncCursor>> {
                (**self).get(source)
            }
            fn upsert(&self, cursor: &SyncCursor) -> RepositoryResult<()> {
                (**self).upsert(cursor)
            }
        }
    };
}

delegate_sync_cursor_repository!(Box<R>);
delegate_sync_cursor_repository!(Rc<R>);
delegate_sync_cursor_repository!(Arc<R>);
