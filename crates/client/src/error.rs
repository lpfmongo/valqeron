use std::path::PathBuf;
use v1::ProblemDetailProto;
use valqeron_proto::v1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineProblem {
    pub problem_type: String,
    pub title: String,
    pub status: u32,
    pub detail: String,
    pub extensions_json: String,
    pub causes: Vec<String>,
}

impl From<ProblemDetailProto> for EngineProblem {
    fn from(p: ProblemDetailProto) -> Self {
        Self {
            problem_type: p.r#type,
            title: p.title,
            status: p.status,
            detail: p.detail,
            extensions_json: p.extensions_json,
            causes: p.causes,
        }
    }
}

impl std::fmt::Display for EngineProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.detail.is_empty() {
            f.write_str(&self.title)
        } else {
            write!(f, "{}: {}", self.title, self.detail)
        }
    }
}

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

    #[error("{0}")]
    Problem(Box<EngineProblem>),

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
    pub fn problem(&self) -> Option<&EngineProblem> {
        match self {
            ClientError::Problem(p) => Some(p.as_ref()),
            _ => None,
        }
    }

    pub fn is_not_running(&self) -> bool {
        matches!(self, ClientError::NotRunning { .. })
    }
}
