//! Maps every failure a gRPC handler can produce to a plain `tonic::Status`:
//! one `tonic::Code` plus the error's own human-readable message.
//!
//! This is deliberately the whole error contract. The former RFC-7807
//! problem envelope (slugs, sysexits statuses, JSON extensions, cause
//! chains) was removed: nothing consumed it — the CLI prints the message
//! and exits 1 — and `thiserror` messages already carry the information.

use tonic::{Code, Status};
use valqeron_core::{RegisterIssuerError, StorageError, StorageFault};
use valqeron_proto::IssuerMappingError;

use crate::storage::StorageCallError;

/// Everything an engine gRPC handler can fail with.
#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error(transparent)]
    Mapping(#[from] IssuerMappingError),

    #[error(transparent)]
    Register(#[from] RegisterIssuerError),

    #[error(transparent)]
    Fault(#[from] StorageFault),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Call(#[from] StorageCallError),
}

impl HandlerError {
    /// Convert into the `tonic::Status` sent over the wire.
    pub fn into_status(self) -> Status {
        Status::new(self.code(), self.to_string())
    }

    fn code(&self) -> Code {
        match self {
            HandlerError::Mapping(_) => Code::InvalidArgument,
            HandlerError::Register(e) => match e {
                RegisterIssuerError::DuplicateCnpj | RegisterIssuerError::DuplicateLei => {
                    Code::AlreadyExists
                }
                RegisterIssuerError::Storage(_) => Code::Internal,
            },
            HandlerError::Fault(_) | HandlerError::Storage(_) => Code::Internal,
            HandlerError::Call(e) => match e {
                StorageCallError::Overloaded => Code::ResourceExhausted,
                StorageCallError::ShuttingDown => Code::Unavailable,
                StorageCallError::TaskFailed(_) => Code::Internal,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use valqeron_core::IssuerNameError;

    #[test]
    fn duplicate_identifiers_map_to_already_exists_with_the_domain_message() {
        let status = HandlerError::Register(RegisterIssuerError::DuplicateCnpj).into_status();
        assert_eq!(status.code(), Code::AlreadyExists);
        assert!(
            status.message().contains("CNPJ"),
            "message names the identifier: {}",
            status.message()
        );
    }

    #[test]
    fn validation_failures_map_to_invalid_argument() {
        let err = valqeron_core::Cnpj::parse("00000000000192").expect_err("invalid check digit");
        let status = HandlerError::Mapping(IssuerMappingError::Cnpj(err)).into_status();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(!status.message().is_empty());

        let err = IssuerMappingError::Name(IssuerNameError::TooLong { max: 200 });
        let status = HandlerError::Mapping(err).into_status();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(
            status.message().contains("200"),
            "message carries the limit: {}",
            status.message()
        );
    }

    #[test]
    fn backpressure_maps_to_resource_exhausted_and_shutdown_to_unavailable() {
        let status = HandlerError::Call(StorageCallError::Overloaded).into_status();
        assert_eq!(status.code(), Code::ResourceExhausted);

        let status = HandlerError::Call(StorageCallError::ShuttingDown).into_status();
        assert_eq!(status.code(), Code::Unavailable);
    }

    #[test]
    fn storage_faults_map_to_internal() {
        let fault = StorageFault::new(std::io::Error::other("disk on fire"));
        let status = HandlerError::Fault(fault).into_status();
        assert_eq!(status.code(), Code::Internal);
        assert!(status.message().contains("disk on fire"));
    }
}
