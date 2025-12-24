#![allow(unsafe_op_in_unsafe_fn)]

mod cfg;
mod core;
mod error;
mod logger;
mod utils;

#[allow(unused)]
mod engine;

#[cfg(target_os = "linux")]
mod linux;

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
