//! Task implementations, grouped by category. Each module owns its task
//! kinds end to end — spec, handler, defaults, and (when real ingestion
//! lands) its own tables and repositories. The scheduler never imports
//! from here; `engine::scheduler` composes these registrations into the
//! builder.

pub(crate) mod cvm;
pub(crate) mod system;
