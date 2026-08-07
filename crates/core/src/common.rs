use crate::storage::StorageFault;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    pub data: T,
    pub version: u32,
}

/// Result alias shared by every repository port.
pub type RepositoryResult<T> = Result<T, StorageFault>;

#[must_use = "a write outcome may indicate the write did not apply"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Applied,

    VersionMismatch { expected: u32, actual: u32 },

    Missing,
}

impl WriteOutcome {
    #[must_use]
    pub const fn applied(self) -> bool {
        matches!(self, Self::Applied)
    }
}

/// Typestate markers shared by the patch builders: a patch can only be built
/// once at least one field has been set (`NonEmpty`).
pub struct Empty;
pub struct NonEmpty;
