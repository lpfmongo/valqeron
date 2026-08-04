use std::path::{Path, PathBuf};

use valqeron_config::CLI_APP;
use valqeron_infrastructure::Synchronous;

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone)]
pub struct ValqeronConfig {
    db_path: PathBuf,
    log_file: Option<PathBuf>,
    reader_pool_size: usize,
    durable: bool,
}

impl ValqeronConfig {
    pub fn resolve(
        db_path_flag: Option<PathBuf>,
        log_file_flag: Option<Option<PathBuf>>,
        no_log_file: bool,
        reader_pool_size: usize,
        durable: bool,
    ) -> AppResult<Self> {
        let db_path = valqeron_config::resolve_db_path(db_path_flag).map_err(config_err)?;
        let log_file = valqeron_config::resolve_log_file(&CLI_APP, log_file_flag, no_log_file)
            .map_err(config_err)?;
        Ok(Self {
            db_path,
            log_file,
            reader_pool_size,
            durable,
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn log_file(&self) -> Option<&Path> {
        self.log_file.as_deref()
    }

    pub fn file_log_level(&self) -> String {
        valqeron_config::file_log_level(&CLI_APP)
    }

    pub fn reader_pool_size(&self) -> usize {
        self.reader_pool_size
    }

    pub fn synchronous(&self) -> Synchronous {
        if self.durable {
            Synchronous::Full
        } else {
            Synchronous::Normal
        }
    }

    pub fn durability_label(&self) -> &'static str {
        if self.durable { "strict" } else { "relaxed" }
    }

    pub fn ensure_db_parent(&self) -> AppResult<()> {
        if let Some(parent) = self.db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| AppError::Io(format!("creating {}: {e}", parent.display())))?;
        }
        Ok(())
    }
}

fn config_err(err: valqeron_config::ConfigError) -> AppError {
    AppError::Config(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_db_flag_wins() {
        let cfg = ValqeronConfig::resolve(Some(PathBuf::from("/tmp/x.db")), None, false, 4, false)
            .unwrap();
        assert_eq!(cfg.db_path(), Path::new("/tmp/x.db"));
        assert_eq!(cfg.reader_pool_size(), 4);
        assert_eq!(cfg.synchronous(), Synchronous::Normal);
    }

    #[test]
    fn explicit_log_file_flag_wins() {
        let cfg = ValqeronConfig::resolve(
            Some(PathBuf::from("/tmp/x.db")),
            Some(Some(PathBuf::from("/tmp/out.log"))),
            false,
            2,
            false,
        )
        .unwrap();
        assert_eq!(cfg.log_file(), Some(Path::new("/tmp/out.log")));
    }

    #[test]
    fn log_file_flag_without_path_uses_default_location() {
        let cfg = ValqeronConfig::resolve(
            Some(PathBuf::from("/tmp/x.db")),
            Some(None),
            false,
            4,
            false,
        )
        .unwrap();
        let log = cfg.log_file().expect("default log path");
        assert!(log.ends_with(CLI_APP.log_file_name));
    }

    #[test]
    fn file_logging_is_on_by_default_when_flag_absent() {
        // Absent flag (and no opt-out) resolves to the default log location.
        let cfg = ValqeronConfig::resolve(Some(PathBuf::from("/tmp/x.db")), None, false, 4, false)
            .unwrap();
        let log = cfg.log_file().expect("default log path when flag absent");
        assert!(log.ends_with(CLI_APP.log_file_name));
    }

    #[test]
    fn no_log_file_flag_disables_file_logging() {
        // `--no-log-file` wins even over an explicit `--log-file PATH`.
        let cfg = ValqeronConfig::resolve(
            Some(PathBuf::from("/tmp/x.db")),
            Some(Some(PathBuf::from("/tmp/out.log"))),
            true,
            4,
            false,
        )
        .unwrap();
        assert!(cfg.log_file().is_none());
    }

    #[test]
    fn durable_flag_selects_strict_durability() {
        let cfg = ValqeronConfig::resolve(Some(PathBuf::from("/tmp/x.db")), None, false, 4, true)
            .unwrap();
        assert_eq!(cfg.synchronous(), Synchronous::Full);
    }
}
