#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_used
    )
)]

mod error;

use std::path::{Path, PathBuf};
use std::time::Duration;

use tonic::transport::{Channel, Endpoint, Uri};
use valqeron_core::{Issuer, IssuerId, IssuerPatch, Versioned, WriteOutcome};
use valqeron_proto::v1::rpc_admin_service_client::RpcAdminServiceClient;
use valqeron_proto::v1::rpc_issuer_service_client::RpcIssuerServiceClient;
use valqeron_proto::{
    PROTOCOL_VERSION, issuer_from_proto, issuer_to_register_request, resolve_socket_path, v1,
    write_outcome_from_proto,
};

pub use error::ClientError;
use v1::{
    DeleteIssuerRequestProto, GetIssuerRequestProto, HealthRequestProto, ListIssuersRequestProto,
    PatchIssuerRequestProto, StatusRequestProto,
};

/// Default bound on establishing the connection.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Default bound on any single RPC.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Engine identity captured by the connect-time handshake.
#[derive(Debug, Clone)]
pub struct EngineInfo {
    pub engine_version: String,
    pub protocol_version: u32,
}

/// Engine diagnostics reported by `AdminService::Status`.
#[derive(Debug, Clone)]
pub struct EngineStatus {
    pub engine_version: String,
    pub protocol_version: u32,
    pub db_path: String,
    pub uptime_secs: u64,
    pub pid: u32,
}

/// Connection options. `Default` resolves the socket via
/// `VALQERON_SOCKET` / the platform convention and uses the bounded default
/// timeouts.
#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    /// Explicit socket path; `None` resolves the shared convention.
    pub socket: Option<PathBuf>,
    /// Bound on establishing the connection (`None` = 2s default).
    pub connect_timeout: Option<Duration>,
    /// Bound on each RPC (`None` = 30s default).
    pub request_timeout: Option<Duration>,
}

/// Blocking handle to a running engine.
pub struct Client {
    runtime: tokio::runtime::Runtime,
    channel: Channel,
    socket: PathBuf,
    engine: EngineInfo,
}

impl Client {
    /// Resolve the socket, connect with a bounded timeout, and run the
    /// version handshake.
    pub fn connect(options: ClientOptions) -> Result<Self, ClientError> {
        let socket = resolve_socket_path(options.socket.clone())
            .map_err(|e| ClientError::Config(e.to_string()))?;

        // A missing socket file is the cheapest, clearest "not running"
        // signal — fail before building a runtime.
        if !socket.exists() {
            return Err(ClientError::NotRunning { socket });
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ClientError::Runtime(e.to_string()))?;

        let connect_timeout = options.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        let request_timeout = options.request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT);

        let channel = runtime
            .block_on(async {
                // The URI is a placeholder required by the HTTP/2 layer; the
                // connector below dials the Unix socket instead.
                let endpoint = Endpoint::try_from("http://valqeron.engine")
                    .map_err(|e| ClientError::Config(e.to_string()))?
                    .connect_timeout(connect_timeout)
                    .timeout(request_timeout);

                let path = socket.clone();
                let connector = tower::service_fn(move |_uri: Uri| {
                    let path = path.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                });

                endpoint
                    .connect_with_connector(connector)
                    .await
                    .map_err(|e| classify_connect_error(&socket, &e))
            })
            .map_err(|e| match e {
                ClientError::Rpc { message, .. } => ClientError::Unreachable {
                    socket: socket.clone(),
                    message,
                },
                other => other,
            })?;

        let mut client = Self {
            runtime,
            channel,
            socket,
            engine: EngineInfo {
                engine_version: String::new(),
                protocol_version: 0,
            },
        };
        client.engine = client.handshake()?;
        Ok(client)
    }

    /// The socket this client is connected to.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Engine identity captured at connect time.
    pub fn engine_info(&self) -> &EngineInfo {
        &self.engine
    }

    fn handshake(&self) -> Result<EngineInfo, ClientError> {
        let health = self.health()?;
        if health.protocol_version != PROTOCOL_VERSION {
            return Err(ClientError::VersionMismatch {
                client_protocol: PROTOCOL_VERSION,
                engine_protocol: health.protocol_version,
                client_version: env!("CARGO_PKG_VERSION").to_string(),
                engine_version: health.engine_version,
            });
        }
        Ok(health)
    }

    /// `AdminService::Health` — cheap liveness + version probe.
    pub fn health(&self) -> Result<EngineInfo, ClientError> {
        let mut admin = RpcAdminServiceClient::new(self.channel.clone());
        let response = self
            .runtime
            .block_on(async move { admin.health(HealthRequestProto {}).await })
            .map_err(|s| self.map_status(s))?
            .into_inner();
        Ok(EngineInfo {
            engine_version: response.engine_version,
            protocol_version: response.protocol_version,
        })
    }

    /// `AdminService::Status` — engine diagnostics.
    pub fn engine_status(&self) -> Result<EngineStatus, ClientError> {
        let mut admin = RpcAdminServiceClient::new(self.channel.clone());
        let response = self
            .runtime
            .block_on(async move { admin.status(StatusRequestProto {}).await })
            .map_err(|s| self.map_status(s))?
            .into_inner();
        Ok(EngineStatus {
            engine_version: response.engine_version,
            protocol_version: response.protocol_version,
            db_path: response.db_path,
            uptime_secs: response.uptime_secs,
            pid: response.pid,
        })
    }

    /// Register a locally validated issuer. Returns the engine's stored
    /// representation (server-generated id and timestamp). Never retried.
    pub fn register_issuer(
        &self,
        issuer: &Issuer,
        dry_run: bool,
    ) -> Result<Versioned<Issuer>, ClientError> {
        let request = issuer_to_register_request(issuer, dry_run);
        let mut issuers = RpcIssuerServiceClient::new(self.channel.clone());
        let response = self
            .runtime
            .block_on(async move { issuers.register(request).await })
            .map_err(|s| self.map_status(s))?
            .into_inner();
        let wire = response
            .issuer
            .ok_or_else(|| ClientError::InvalidResponse("register returned no issuer".into()))?;
        issuer_from_proto(&wire).map_err(|e| ClientError::InvalidResponse(e.to_string()))
    }

    /// Fetch one issuer; `None` when the id is unknown.
    pub fn get_issuer(&self, id: &IssuerId) -> Result<Option<Versioned<Issuer>>, ClientError> {
        let request = GetIssuerRequestProto { id: id.value() };
        let mut issuers = RpcIssuerServiceClient::new(self.channel.clone());
        let response = self
            .runtime
            .block_on(async move { issuers.get(request).await })
            .map_err(|s| self.map_status(s))?
            .into_inner();
        response
            .issuer
            .as_ref()
            .map(issuer_from_proto)
            .transpose()
            .map_err(|e| ClientError::InvalidResponse(e.to_string()))
    }

    /// Keyset-paged listing: issuers whose id sorts after `after`.
    pub fn list_issuers(
        &self,
        after: Option<&IssuerId>,
        limit: u32,
    ) -> Result<Vec<Versioned<Issuer>>, ClientError> {
        let request = ListIssuersRequestProto {
            after: after.map(IssuerId::value),
            limit,
        };
        let mut issuers = RpcIssuerServiceClient::new(self.channel.clone());
        let response = self
            .runtime
            .block_on(async move { issuers.list(request).await })
            .map_err(|s| self.map_status(s))?
            .into_inner();
        response
            .issuers
            .iter()
            .map(issuer_from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ClientError::InvalidResponse(e.to_string()))
    }

    /// Conditionally patch an issuer. Never retried.
    pub fn patch_issuer(
        &self,
        id: &IssuerId,
        expected_version: u32,
        patch: &IssuerPatch,
        dry_run: bool,
    ) -> Result<WriteOutcome, ClientError> {
        let request = PatchIssuerRequestProto {
            id: id.value(),
            expected_version,
            name: patch.name().map(|n| n.as_str().to_string()),
            status: patch.status().map(String::from),
            cnpj: patch.cnpj().map(|c| c.as_str().to_string()),
            lei: patch.lei().map(|l| l.as_str().to_string()),
            country_code: patch.country_code().map(|c| c.as_str().to_string()),
            dry_run,
        };
        let mut issuers = RpcIssuerServiceClient::new(self.channel.clone());
        let response = self
            .runtime
            .block_on(async move { issuers.patch(request).await })
            .map_err(|s| self.map_status(s))?
            .into_inner();
        let outcome = response
            .outcome
            .ok_or_else(|| ClientError::InvalidResponse("patch returned no outcome".into()))?;
        write_outcome_from_proto(&outcome).map_err(|e| ClientError::InvalidResponse(e.to_string()))
    }

    /// Conditionally delete an issuer. Never retried.
    pub fn delete_issuer(
        &self,
        id: &IssuerId,
        expected_version: u32,
        dry_run: bool,
    ) -> Result<WriteOutcome, ClientError> {
        let request = DeleteIssuerRequestProto {
            id: id.value(),
            expected_version,
            dry_run,
        };
        let mut issuers = RpcIssuerServiceClient::new(self.channel.clone());
        let response = self
            .runtime
            .block_on(async move { issuers.delete(request).await })
            .map_err(|s| self.map_status(s))?
            .into_inner();
        let outcome = response
            .outcome
            .ok_or_else(|| ClientError::InvalidResponse("delete returned no outcome".into()))?;
        write_outcome_from_proto(&outcome).map_err(|e| ClientError::InvalidResponse(e.to_string()))
    }

    /// gRPC status → typed error: transport-level unavailability keeps its
    /// dedicated variant; everything else surfaces as the engine's own
    /// code + message.
    fn map_status(&self, status: tonic::Status) -> ClientError {
        match status.code() {
            tonic::Code::Unavailable => ClientError::Unreachable {
                socket: self.socket.clone(),
                message: status.message().to_string(),
            },
            code => ClientError::Rpc {
                code: format!("{code:?}"),
                message: status.message().to_string(),
            },
        }
    }
}

/// Classify a connect-time transport error: connection refused / missing
/// socket means "not running"; anything else is "unreachable".
fn classify_connect_error(socket: &Path, err: &tonic::transport::Error) -> ClientError {
    use std::error::Error as _;
    let mut source: Option<&(dyn std::error::Error + 'static)> = err.source();
    while let Some(current) = source {
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            if matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) {
                return ClientError::NotRunning {
                    socket: socket.to_path_buf(),
                };
            }
            break;
        }
        source = current.source();
    }
    ClientError::Unreachable {
        socket: socket.to_path_buf(),
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_socket_is_a_distinct_not_running_error() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("absent.sock");
        let result = Client::connect(ClientOptions {
            socket: Some(socket.clone()),
            ..ClientOptions::default()
        });
        let Err(err) = result else {
            panic!("connect must fail on a missing socket");
        };
        match err {
            ClientError::NotRunning { socket: reported } => assert_eq!(reported, socket),
            other => panic!("expected NotRunning, got {other:?}"),
        }
    }

    #[test]
    fn stale_socket_file_maps_to_not_running() {
        // A socket file nobody listens on: connect is refused.
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("stale.sock");
        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
        // The listener is closed but the file remains.
        assert!(socket.exists());

        let result = Client::connect(ClientOptions {
            socket: Some(socket),
            connect_timeout: Some(Duration::from_millis(500)),
            ..ClientOptions::default()
        });
        match result {
            Err(e) => assert!(
                e.is_not_running() || matches!(e, ClientError::Unreachable { .. }),
                "expected NotRunning/Unreachable, got {e:?}"
            ),
            Ok(_) => panic!("connect must fail on a dead socket"),
        }
    }

    #[test]
    fn socket_owned_by_an_unrelated_process_fails_the_handshake_or_transport() {
        // Something accepts connections but speaks no gRPC: the client must
        // fail with a typed error, never hang.
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("imposter.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let accept_thread = std::thread::spawn(move || {
            // Accept one connection and drop it immediately.
            let _ = listener.accept();
        });

        let result = Client::connect(ClientOptions {
            socket: Some(socket),
            connect_timeout: Some(Duration::from_millis(500)),
            request_timeout: Some(Duration::from_millis(500)),
        });
        assert!(result.is_err(), "an imposter socket must not handshake");
        let _ = accept_thread.join();
    }

    #[test]
    fn error_display_names_both_protocol_versions() {
        let err = ClientError::VersionMismatch {
            client_protocol: 1,
            engine_protocol: 2,
            client_version: "0.1.0".into(),
            engine_version: "0.9.0".into(),
        };
        let text = err.to_string();
        assert!(text.contains("protocol v2"), "{text}");
        assert!(text.contains("protocol v1"), "{text}");
        assert!(text.contains("0.9.0"), "{text}");
    }
}
