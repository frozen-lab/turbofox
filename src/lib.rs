mod cfg;
mod error;
mod logger;
mod utils;

pub use cfg::{TurboConfig, TurboConfigValue, TurboLogLevel};
pub use error::{TurboError, TurboResult};

pub struct TurboFox;

impl TurboFox {
    pub fn new<P: AsRef<std::path::PathBuf>>(dirpath: P, cfg: TurboConfig) -> TurboResult<()> {
        let logger = crate::logger::Logger::new(cfg.logging, cfg.log_level);
        crate::utils::prep_directory(dirpath.as_ref(), &logger)?;

        Ok(())
    }
}
