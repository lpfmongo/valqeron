use clap::{Args, Subcommand};
use serde_json::{Value, json};
use valqeron_client::Client;

use crate::commands::Command;
use crate::context::AppContext;

#[derive(Subcommand, Debug)]
pub enum EngineCommand {
    /// Check the engine's health (version handshake round-trip).
    Ping(PingArgs),

    /// Run engine diagnostics and return its status.
    Status(StatusArgs),
}

impl EngineCommand {
    pub fn as_command(&self) -> &dyn Command {
        match self {
            EngineCommand::Ping(args) => args,
            EngineCommand::Status(args) => args,
        }
    }
}

#[derive(Args, Debug)]
pub struct PingArgs {}

impl Command for PingArgs {
    fn execute(&self, client: &Client, _ctx: &AppContext) -> anyhow::Result<Value> {
        let info = client.health()?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "engine.ping",
            engine_version = %info.engine_version,
            "engine ping"
        );

        Ok(json!({
            "engine_version": info.engine_version,
            "protocol_version": info.protocol_version,
            "socket": client.socket().display().to_string(),
        }))
    }
}

#[derive(Args, Debug)]
pub struct StatusArgs {}

impl Command for StatusArgs {
    fn execute(&self, client: &Client, _ctx: &AppContext) -> anyhow::Result<Value> {
        let status = client.engine_status()?;

        tracing::info!(
            target: "valqeron::audit",
            operation = "engine.status",
            engine_version = %status.engine_version,
            uptime_secs = status.uptime_secs,
            "engine status"
        );

        Ok(json!({
            "engine_version": status.engine_version,
            "protocol_version": status.protocol_version,
            "db_path": status.db_path,
            "uptime_secs": status.uptime_secs,
            "pid": status.pid,
            "socket": client.socket().display().to_string(),
        }))
    }
}
