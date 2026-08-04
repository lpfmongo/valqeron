use std::io::IsTerminal;
use std::path::Path;

use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};
use valqeron_config::ENGINE_APP;

/// Dual-layer tracing, mirroring the CLI's setup: compact human output on
/// stderr (the service manager's log stream) plus a JSON file layer.
///
/// Unlike the CLI, the `valqeron::audit` target stays **enabled** on stderr:
/// for a daemon, stderr *is* the operator surface (journald / launchd log
/// files), and audit events belong there.
pub fn init(
    level: Level,
    log_file: Option<&Path>,
) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let stderr_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.to_string()));

    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .compact()
        .with_filter(stderr_filter);

    let file = log_file.and_then(|path| match open_log_file(path) {
        Ok(handle) => Some(handle),
        Err(e) => {
            eprintln!("warning: file logging disabled: {e}");
            None
        }
    });

    match file {
        Some((non_blocking, guard)) => {
            let file_filter = EnvFilter::new(valqeron_config::file_log_level(&ENGINE_APP));

            let file_layer = fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .json()
                .with_filter(file_filter);

            tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer)
                .init();
            Some(guard)
        }
        None => {
            tracing_subscriber::registry().with(stderr_layer).init();
            None
        }
    }
}

fn open_log_file(
    path: &Path,
) -> std::io::Result<(
    tracing_appender::non_blocking::NonBlocking,
    tracing_appender::non_blocking::WorkerGuard,
)> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    Ok(tracing_appender::non_blocking(file))
}
