use tonic::{Request, Response, Status};
use v1::{
    DeleteIssuerRequestProto, DeleteIssuerResponseProto, IssuerProto, ListIssuersResponseProto,
    PatchIssuerResponseProto,
};
use valqeron_core::{IssuerRepository, LoadMode, Repositories, Versioned, register_issuer};
use valqeron_infrastructure::SqliteStorageEngine;
use valqeron_proto::v1::rpc_issuer_service_server::RpcIssuerService;
use valqeron_proto::{
    issuer_to_proto, parse_issuer_id, patch_request_to_domain, register_request_to_issuer, v1,
    write_outcome_to_proto,
};

use crate::grpc::error::HandlerError;
use crate::storage::AsyncStorage;

const DEFAULT_LIST_LIMIT: u32 = 50;
const MAX_LIST_LIMIT: u32 = 1_000;

pub struct IssuerGrpc {
    storage: AsyncStorage,
}

impl IssuerGrpc {
    pub fn new(storage: AsyncStorage) -> Self {
        Self { storage }
    }

    /// Run a read-only closure on the storage read lane, flattening admission
    /// failures and handler failures into one `Status`.
    async fn run_read<T, F>(&self, operation: &'static str, f: F) -> Result<T, Status>
    where
        F: FnOnce(&Repositories<SqliteStorageEngine>) -> Result<T, HandlerError> + Send + 'static,
        T: Send + 'static,
    {
        match self.storage.read(operation, f).await {
            Ok(inner) => inner.map_err(HandlerError::into_status),
            Err(call_err) => Err(HandlerError::from(call_err).into_status()),
        }
    }

    /// Run a mutating closure on the storage write lane. With `dry_run` the
    /// same closure executes inside the engine's always-rolled-back
    /// savepoint — handlers never route dry runs themselves.
    async fn run_write<T, F>(
        &self,
        operation: &'static str,
        dry_run: bool,
        f: F,
    ) -> Result<T, Status>
    where
        F: FnOnce(&Repositories<SqliteStorageEngine>) -> Result<T, HandlerError> + Send + 'static,
        T: Send + 'static,
    {
        match self.storage.write(operation, dry_run, f).await {
            Ok(inner) => inner.map_err(HandlerError::into_status),
            Err(call_err) => Err(HandlerError::from(call_err).into_status()),
        }
    }
}

#[tonic::async_trait]
impl RpcIssuerService for IssuerGrpc {
    async fn register(
        &self,
        request: Request<v1::RegisterIssuerRequestProto>,
    ) -> Result<Response<v1::RegisterIssuerResponseProto>, Status> {
        let req = request.into_inner();
        let dry_run = req.dry_run;
        let issuer =
            register_request_to_issuer(&req).map_err(|e| HandlerError::from(e).into_status())?;

        let registered = self
            .run_write("issuer.register", dry_run, move |repos| {
                register_issuer(&repos.issuers, &issuer)
                    .map_err(HandlerError::from)
                    .map(|()| {
                        issuer_to_proto(&Versioned {
                            data: issuer,
                            version: 1,
                        })
                    })
            })
            .await?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "issuer.register",
            id = %registered.id,
            dry_run,
            "registered issuer"
        );

        Ok(Response::new(v1::RegisterIssuerResponseProto {
            issuer: Some(registered),
        }))
    }

    async fn get(
        &self,
        request: Request<v1::GetIssuerRequestProto>,
    ) -> Result<Response<v1::GetIssuerResponseProto>, Status> {
        let req = request.into_inner();
        let id = parse_issuer_id(&req.id).map_err(|e| HandlerError::from(e).into_status())?;

        let issuer = self
            .run_read("issuer.get", move |repos| {
                repos
                    .issuers
                    .find_by_id(&id, LoadMode::Lazy)
                    .map(|found| found.as_ref().map(issuer_to_proto))
                    .map_err(HandlerError::from)
            })
            .await?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "issuer.info",
            id = %req.id,
            found = issuer.is_some(),
            "issuer lookup"
        );

        Ok(Response::new(v1::GetIssuerResponseProto { issuer }))
    }

    async fn list(
        &self,
        request: Request<v1::ListIssuersRequestProto>,
    ) -> Result<Response<ListIssuersResponseProto>, Status> {
        let req = request.into_inner();
        let after = match req.after.as_deref() {
            Some(raw) => {
                Some(parse_issuer_id(raw).map_err(|e| HandlerError::from(e).into_status())?)
            }
            None => None,
        };
        let limit = if req.limit == 0 {
            DEFAULT_LIST_LIMIT
        } else {
            req.limit.min(MAX_LIST_LIMIT)
        };

        let issuers: Vec<IssuerProto> = self
            .run_read("issuer.list", move |repos| {
                repos
                    .issuers
                    .list_paged(after, limit, LoadMode::Lazy)
                    .map(|rows| rows.iter().map(issuer_to_proto).collect())
                    .map_err(HandlerError::from)
            })
            .await?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "issuer.list",
            count = issuers.len(),
            limit,
            "list registered issuers (paged)"
        );

        Ok(Response::new(ListIssuersResponseProto { issuers }))
    }

    async fn patch(
        &self,
        request: Request<v1::PatchIssuerRequestProto>,
    ) -> Result<Response<PatchIssuerResponseProto>, Status> {
        let req = request.into_inner();
        let cmd = patch_request_to_domain(&req).map_err(|e| HandlerError::from(e).into_status())?;
        let dry_run = cmd.dry_run;

        let outcome = self
            .run_write("issuer.patch", dry_run, move |repos| {
                repos
                    .issuers
                    .apply_patch(&cmd.id, cmd.expected_version, cmd.patch.clone())
                    .map_err(HandlerError::from)
                    .map(write_outcome_to_proto)
            })
            .await?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "issuer.patch",
            id = %req.id,
            dry_run = req.dry_run,
            "patched issuer"
        );

        Ok(Response::new(PatchIssuerResponseProto {
            outcome: Some(outcome),
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteIssuerRequestProto>,
    ) -> Result<Response<DeleteIssuerResponseProto>, Status> {
        let req = request.into_inner();
        let id = parse_issuer_id(&req.id).map_err(|e| HandlerError::from(e).into_status())?;
        let expected_version = req.expected_version;
        let dry_run = req.dry_run;

        let outcome = self
            .run_write("issuer.delete", dry_run, move |repos| {
                repos
                    .issuers
                    .delete(&id, expected_version)
                    .map_err(HandlerError::from)
                    .map(write_outcome_to_proto)
            })
            .await?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "issuer.delete",
            id = %req.id,
            dry_run,
            "deleted issuer"
        );

        Ok(Response::new(DeleteIssuerResponseProto {
            outcome: Some(outcome),
        }))
    }
}
