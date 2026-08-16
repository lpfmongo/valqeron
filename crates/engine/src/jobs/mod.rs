//! Task implementations, grouped by category. Each module owns its task
//! kinds end to end — spec, handler, configuration consumption, and (when
//! real ingestion lands) its own tables and repositories. The task manager
//! never imports from here; `engine::background_tasks` composes these
//! registrations into the builder.

pub(crate) mod cvm;
pub(crate) mod system;
