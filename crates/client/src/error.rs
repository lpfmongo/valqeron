use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(
        "no engine is running on {socket}; install and start the engine service \
         (during development: `just engine-install`)"
    )]
    NotRunning { socket: PathBuf },

    #[error("engine unreachable on {socket}: {message}")]
    Unreachable { socket: PathBuf, message: String },

    #[error(
        "engine {engine_version} speaks protocol v{engine_protocol}, but this client \
         (v{client_version}) requires protocol v{client_protocol}; upgrade the older side"
    )]
    VersionMismatch {
        client_protocol: u32,
        engine_protocol: u32,
        client_version: String,
        engine_version: String,
    },

    /// The engine rejected the call: a gRPC code plus the engine error's own
    /// human-readable message (the whole error contract since protocol v2).
    #[error("rpc failed ({code}): {message}")]
    Rpc { code: String, message: String },

    #[error("client configuration error: {0}")]
    Config(String),

    #[error("invalid response from engine: {0}")]
    InvalidResponse(String),

    #[error("failed to start the client runtime: {0}")]
    Runtime(String),
}

impl ClientError {
    pub fn is_not_running(&self) -> bool {
        matches!(self, ClientError::NotRunning { .. })
    }
}
