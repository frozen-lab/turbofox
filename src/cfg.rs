use crate::{
    core::is_pow_of_2,
    error::{InternalError, InternalResult},
    logger::{LogCtx, Logger, TurboLogLevel},
    TurboResult,
};
use std::{
    fmt::format,
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) const DEFAULT_INIT_CAP: usize = 0x400;
pub(crate) const DEFAULT_PAGE_SIZE: usize = 0x40;

/// Configurations for `[TurboFox]`
///
/// `TurboConfig` defines all the tuneable behaviour for the `[TurboFox]` database.
///
/// ## Defaults
///
/// - `init_cap`: *1024 entries*
/// - `page_size`: *64 bytes*  
/// - `logging`: *disabled*
///
/// ## Directory
///
/// The directory at `dirpath` **is created if not already**. When it already exists, it is reused.
///
/// **⚠️ WARN:** The directory *must be empty* to avoid accidental damage of pre-existent files.
///
/// ## Example
///
/// ```
/// use turbofox::{TurboConfig, TurboLogLevel};
///
/// let cfg = TurboConfig::new("/tmp/test_data/turbodb")
///     .init_cap(0x400)
///     .page_size(0x80)
///     .log_level(TurboLogLevel::INFO)
///     .enable_logging()
///     .build()
///     .expect("valid config for TurboFox");
/// ```
#[derive(Debug, Clone)]
pub struct TurboConfig {
    pub(crate) init_cap: usize,
    pub(crate) page_size: usize,
    pub(crate) logger: Arc<Logger>,
    pub(crate) dirpath: Arc<PathBuf>,
}

impl TurboConfig {
    /// Build a new [`TurboConfig`] using the provided directory path.
    ///
    /// Returns a [`TurboConfigBuilder`] which exposes the builder API for config tuning.
    ///
    /// ## Example
    ///
    /// ```
    /// use turbofox::TurboConfig;
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb").build().expect("valid config");
    /// ```
    pub fn new<P: AsRef<Path>>(dirpath: P) -> TurboConfigBuilder {
        TurboConfigBuilder {
            init_cap: DEFAULT_INIT_CAP,
            page_size: DEFAULT_PAGE_SIZE,
            dirpath: Arc::new(dirpath.as_ref().into()),
            logger: Arc::new(Logger::default()),
            error: None,
        }
    }
}

/// Builder for constructing a [`TurboConfig`].
///
/// ## Example
///
/// ```
/// use turbofox::{TurboConfig, TurboLogLevel};
///
/// let cfg = TurboConfig::new("/tmp/test_data/turbodb")
///     .init_cap(0x400)
///     .page_size(0x80)
///     .log_level(TurboLogLevel::INFO)
///     .enable_logging()
///     .build();
/// assert!(cfg.is_ok());
/// ```
#[derive(Debug)]
pub struct TurboConfigBuilder {
    init_cap: usize,
    page_size: usize,
    dirpath: Arc<PathBuf>,
    logger: Arc<Logger>,
    error: Option<InternalError>,
}

impl TurboConfigBuilder {
    fn push_err(&mut self, err: InternalError) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    /// Sets the initial capacity of entries for `[TurboFox]`
    ///
    /// **WARN:** The capacity **must be a power of two**, as required by the storage layout.
    ///
    /// ### Choosing initial capacity
    ///
    /// - Use **128-1024 entries** for predominantly small datasets
    /// - Use **1024-4096 entries** for larger datasets
    ///
    /// **NOTE:** Using appropriate initial capacity will reduce the need for growing and rehashing, hence
    /// improving overall perf.
    ///
    /// ### Errors
    ///
    /// A non power of two capacity is recorded as a configuration error and will cause
    /// [`build`](TurboConfigBuilder::build) to fail.
    ///
    /// ## Example
    ///
    /// ```
    /// use turbofox::TurboConfig;
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb").init_cap(0x400).build();
    /// assert!(cfg.is_ok());
    /// ```
    pub fn init_cap(mut self, cap: usize) -> Self {
        if !is_pow_of_2(cap) {
            self.push_err(InternalError::InvalidConfig("init_cap must be power of 2".into()));
        } else {
            self.init_cap = cap;
        }

        self
    }

    /// Sets the on-disk page size used by `[TurboFox]` for storing key–value pairs.
    ///
    /// Each `key + value` pair is written into fixed size pages.
    /// Larger `page_size` reduces fragmentation for large values but may waste space for small ones.
    ///
    /// **WARN:** The page size **must be a power of two**, as required by the storage layout.
    ///
    /// ### Choosing a page size
    ///
    /// - Use **64–128 bytes** for predominantly small keys/values  
    /// - Use **256–1024 bytes** if values are typically larger  
    ///
    /// **NOTE:** Larger pages improve sequential IO throughput but will increase on-disk space overhead
    ///
    /// ### Errors
    ///
    /// A non power of two page size is recorded as a configuration error and will cause
    /// [`build`](TurboConfigBuilder::build) to fail.
    ///
    /// ## Example
    ///
    /// ```
    /// use turbofox::TurboConfig;
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb").page_size(0x80).build();
    /// assert!(cfg.is_ok());
    /// ```
    pub fn page_size(mut self, size: usize) -> Self {
        if !is_pow_of_2(size) {
            self.push_err(InternalError::InvalidConfig("page_size must be power of 2".into()));
        } else {
            self.page_size = size;
        }

        self
    }

    /// Enables logging for `[TurboFox]`
    ///
    /// Default verbosity level is [`TurboLogLevel::Error`]
    ///
    /// Use [`log_level`](TurboConfigBuilder::log_level) to control verbosity.
    ///
    /// ## Example
    ///
    /// ```
    /// use turbofox::TurboConfig;
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb").enable_logging().build();
    /// assert!(cfg.is_ok());
    /// ```
    pub fn enable_logging(mut self) -> Self {
        Arc::make_mut(&mut self.logger).enable();
        self
    }

    /// Disables logging for `[TurboFox]`
    ///
    /// **NOTE:** Logging is disabled by default. This method can be used to modify existing configs.
    /// Which will help for copy/clone methods
    ///
    /// ## Example
    ///
    /// ```
    /// use turbofox::TurboConfig;
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb").disable_logging().build();
    /// assert!(cfg.is_ok());
    /// ```
    pub fn disable_logging(mut self) -> Self {
        Arc::make_mut(&mut self.logger).disable();
        self
    }

    /// Sets the `[TurboLogLevel]` for `[TurboFox]`
    ///
    /// ## Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboLogLevel};
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb")
    ///     .enable_logging()
    ///     .log_level(TurboLogLevel::INFO)
    ///     .build();
    /// assert!(cfg.is_ok());
    /// ```
    pub fn log_level(mut self, level: TurboLogLevel) -> Self {
        Arc::make_mut(&mut self.logger).set_level(level);
        self
    }

    /// Finilizes config and builds the `[TurboConfig]`
    ///
    /// ## Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboLogLevel};
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb").build();
    /// assert!(cfg.is_ok());
    /// ```
    pub fn build(self) -> TurboResult<TurboConfig> {
        if let Some(err) = self.error {
            return Err(err.into());
        }
        create_dir_if_missing(&self.dirpath, &self.logger)?;

        Ok(TurboConfig {
            init_cap: self.init_cap,
            page_size: self.page_size,
            dirpath: self.dirpath,
            logger: self.logger,
        })
    }
}

fn create_dir_if_missing(dirpath: &PathBuf, logger: &Logger) -> InternalResult<()> {
    if !dirpath.exists() {
        std::fs::create_dir_all(&dirpath)
            .inspect(|_| {
                logger.info(LogCtx::Cfg, format!("Created new directory, path={:?}", dirpath));
            })
            .map_err(|e| {
                logger.error(LogCtx::Cfg, format!("Failed to create new directory due to error: {e}"));
                e
            })?;
    }

    let metadata = std::fs::metadata(&dirpath).map_err(|e| {
        logger.error(LogCtx::Cfg, format!("Failed to read metadata due to error: {e}"));
        e
    })?;

    // NOTE: dirpath must be a valid directory
    if !metadata.is_dir() {
        let err = InternalError::InvalidPath(format!("Path({:?}) is not a valid directory", dirpath));
        logger.error(LogCtx::Cfg, format!("TurboConfig contains invalid path: {err}"));
        return Err(err);
    }

    // NOTE: We must have read permission to the directory
    std::fs::read_dir(&dirpath).map_err(|_| {
        let e = InternalError::PermissionDenied("Read permission denied for dirpath".into());
        logger.error(LogCtx::Cfg, format!("Failed to read from directory due to error: {e}"));
        e
    })?;

    // NOTE: we must have write permission to the directory
    let test_file = dirpath.join(".turbofox_perm_test");
    match std::fs::File::create(&test_file) {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
        }
        Err(_) => {
            let e = InternalError::PermissionDenied("Write permission denied for dirpath".into());
            logger.error(LogCtx::Cfg, format!("Failed to write to directory due to error: {e}"));
            return Err(e);
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(unused)]
pub(crate) fn test_cfg(target: &'static str) -> (TurboConfig, tempfile::TempDir) {
    let tempdir = tempfile::TempDir::new().expect("New temp directory");
    let dirpath: Arc<PathBuf> = Arc::new(tempdir.path().into());
    let logger: Arc<Logger> = Arc::new(crate::logger::test_logger(target));

    // we create the dir, if not aleady created
    create_dir_if_missing(&dirpath, &logger).expect("Should create a new dir");

    let cfg = TurboConfig {
        logger,
        dirpath,
        init_cap: DEFAULT_INIT_CAP,
        page_size: DEFAULT_PAGE_SIZE,
    };

    (cfg, tempdir)
}
