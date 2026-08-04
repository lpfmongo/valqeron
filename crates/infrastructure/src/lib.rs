#![cfg_attr(
    test,
    allow(
        clippy::as_conversions,
        clippy::arithmetic_side_effects,
        clippy::clone_on_copy,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::items_after_test_module,
        clippy::panic,
        clippy::unwrap_used
    )
)]

mod sqlite;

pub use sqlite::{
    DatabaseConfig, SqliteError, SqliteStorageEngine, Synchronous, WalCheckpointStats,
};
