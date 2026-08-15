mod mapping;

use directories::ProjectDirs;
use std::path::{Path, PathBuf};

// v2: removed the RFC-7807 `ProblemDetailProto` status-details payload —
// errors are a plain `tonic::Status` (code + message).
pub const PROTOCOL_VERSION: u32 = 2;

#[allow(
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice,
    clippy::unreachable,
    clippy::unwrap_used,
    clippy::panic_in_result_fn
)]
pub mod v1 {
    tonic::include_proto!("valqeron.v1");
}

//-------- Resolve socket path --------
pub const SOCKET_ENV: &str = "VALQERON_SOCKET";
pub const SOCKET_FILE_NAME: &str = "valqeron.sock";
const QUALIFIER: &str = "io";
const ORGANIZATION: &str = "valqeron";
const SHARED_APP: &str = "valqeron";

pub fn resolve_socket_path(flag: Option<PathBuf>) -> Result<PathBuf, ConnectionConfigError> {
    if let Some(path) = flag {
        return Ok(path);
    }
    if let Some(env) = std::env::var_os(SOCKET_ENV) {
        return Ok(PathBuf::from(env));
    }
    let dirs = ProjectDirs::from(QUALIFIER, ORGANIZATION, SHARED_APP)
        .ok_or(ConnectionConfigError::NoHomeDirectory)?;
    let dir = dirs
        .runtime_dir()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| dirs.data_dir().join("run"));
    Ok(dir.join(SOCKET_FILE_NAME))
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionConfigError {
    #[error("could not determine a home directory for the engine socket")]
    NoHomeDirectory,
}

pub use crate::mapping::issuer::{
    IssuerMappingError, PatchCommand, issuer_from_proto, issuer_to_proto,
    issuer_to_register_request, parse_issuer_id, patch_request_to_domain,
    register_request_to_issuer, write_outcome_from_proto, write_outcome_to_proto,
};

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod test_resolve_socket_path {
    use super::*;

    #[test]
    fn explicit_flag_wins() {
        let path = resolve_socket_path(Some(PathBuf::from("/tmp/custom.sock")));
        assert!(matches!(path, Ok(p) if p == Path::new("/tmp/custom.sock")));
    }

    #[test]
    fn default_ends_with_socket_file_name() {
        // The env var may leak from the calling shell; only assert the default shape when it is absent.
        if std::env::var_os(SOCKET_ENV).is_none() {
            let path = resolve_socket_path(None).unwrap();
            assert!(path.ends_with(SOCKET_FILE_NAME), "unexpected: {path:?}");
        }
    }

    #[test]
    fn resolution_is_deterministic() {
        let a = resolve_socket_path(Some(PathBuf::from("/tmp/x.sock"))).unwrap();
        let b = resolve_socket_path(Some(PathBuf::from("/tmp/x.sock"))).unwrap();
        assert_eq!(a, b);
    }
}
