use crate::{
    errors::{InternalError, InternalResult, TurboError, TurboResult},
    logger::Logger,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct TurboConfig {
    /// Initial capacity for [TurboFox]
    pub(crate) init_cap: usize,

    /// Directory path for [TurboFox] for persistence
    pub(crate) dirpath: Arc<PathBuf>,

    /// For interal as well as external logging
    pub(crate) logger: Arc<Logger>,

    /// Buffer size used for paged data persistance
    pub(crate) page_size: usize,
}

impl TurboConfig {
    /// Configurations for [TurboFox] database
    ///
    /// The directory at `dirpath` is created on the fly, if not already created.
    ///
    /// **NOTE**: Always make sure the directory is empty to avoid unintended behaviours.
    ///
    /// ## Default Configs
    ///
    /// - Initial capacity is `1024`
    /// - Maximum allowed klen is `128`
    /// - Logging is `disabled`
    /// - Memory overhead is about `~ 12 KiB`
    /// - Disk overhead is about `~ 268 KiB`
    pub fn new<P: AsRef<Path>>(dirpath: P) -> TurboResult<Self> {
        let logger: Arc<Logger> = Arc::new(Logger::default());
        let dirpath: Arc<PathBuf> = Arc::new(dirpath.as_ref().into());

        // we create the dir, if not aleady created
        Self::create_if_missing(&dirpath)
            .inspect(|_| logger.trace("(TurboConfig) [new] Created new directory"))
            .map_err(|e| {
                logger.error(format!(
                    "(TurboConfig) [new] Unable to create directory at dirpath({:?}) due to error: {e}",
                    dirpath.as_path(),
                ));
                e
            })?;

        Ok(Self {
            logger,
            dirpath,
            init_cap: crate::burrow::DEFAULT_INIT_CAP,
            page_size: crate::burrow::DEFAULT_PAGE_SIZE,
        })
    }

    /// Update initial capcity for [TurboFox] database.
    ///
    /// **NOTE**: Capacity must be greater then or equal to 128, while also being power of 2
    pub fn init_cap(mut self, cap: usize) -> TurboResult<Self> {
        // sanity checks
        (cap >= 0x80)
            .then_some(())
            .ok_or_else(|| TurboError::InvalidConfig("init_cap must be >= 128".into()))?;
        Self::is_power_of_two(cap)
            .then_some(())
            .ok_or_else(|| TurboError::InvalidConfig("init_cap must be power of 2".into()))?;

        self.init_cap = cap;
        Ok(self)
    }

    /// Enable or Disable logging for [TurboFox] database.
    ///
    /// **NOTE**: By default only _error_ logs are shown. You can change this by
    /// updating [LoggerLevel] on [TurboConfig]
    pub fn log(mut self, enabled: bool) -> Self {
        Arc::make_mut(&mut self.logger).enabled = enabled;
        self
    }

    pub fn log_lvl(mut self) -> Self {
        todo!()
    }

    pub fn memory_overhead_bytes(&self) -> usize {
        todo!()
    }

    pub fn disk_overhead_bytes(&self) -> usize {
        todo!()
    }

    #[inline]
    const fn is_power_of_two(n: usize) -> bool {
        (n & (n - 0x01)) == 0x00
    }

    /// Create directory if missing
    fn create_if_missing(dirpath: &PathBuf) -> InternalResult<()> {
        if !dirpath.as_path().exists() {
            std::fs::create_dir_all(&dirpath.as_path())?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_cfg(target: &'static str) -> (Self, tempfile::TempDir) {
        let tempdir = tempfile::TempDir::new().expect("New temp directory");
        let dirpath: Arc<PathBuf> = Arc::new(tempdir.path().into());
        let logger: Arc<Logger> = Arc::new(Logger::test_logger(target));

        // we create the dir, if not aleady created
        Self::create_if_missing(&dirpath)
            .inspect(|_| logger.trace("(TurboConfig) [new] Created new directory"))
            .map_err(|e| {
                logger.error(format!(
                    "(TurboConfig) [new] Unable to create directory at dirpath({:?}) due to error: {e}",
                    dirpath.as_path(),
                ));
            })
            .expect("Should create a new dir");

        let cfg = Self {
            logger,
            dirpath,
            init_cap: crate::burrow::DEFAULT_INIT_CAP,
            page_size: crate::burrow::DEFAULT_PAGE_SIZE,
        };

        (cfg, tempdir)
    }
}
