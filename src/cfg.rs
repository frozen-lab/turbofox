use crate::{
    error::{TurboError, TurboResult},
    logger::Logger,
    utils::{is_pow_of_2, prep_directory, unlikely},
};
use std::path::{Path, PathBuf};

const DEFAULT_INIT_CAP: usize = 0x400;
const DEFAULT_GROWTH_FACTOR: usize = 2;

pub(crate) const DEFAULT_BUF_SIZE: usize = 0x40;
pub(crate) const MAX_KEY_SIZE: usize = 0x40;

/// Logging verbosity levels for [TurboConfig]
///
/// Log levels are **ordered by verbosity**, where higher levels include
/// all logs from lower levels
///
/// ## Order
///
/// ```txt
/// ERROR < WARN < INFO < TRACE
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum TurboLogLevel {
    /// Allows **only critical errors** to be logged
    ERROR = 0x00,

    /// Allows **warnings and errors** to be logged
    WARN = 0x01,

    /// Allows **informational messages, warnings and errors** to be logged
    INFO = 0x02,

    /// Allows **all events** to be logged
    ///
    /// ## NOTE
    ///
    /// This level emits logs for nearly every database action and is intended
    /// **strictly for debugging and development**
    ///
    /// ## WARNING
    ///
    /// Using this level in production can _significantly impact performance and generate
    /// large log volumes_
    TRACE = 0x03,
}

/// Configurations for [TurboFox] database
///
/// `TurboConfig` defines all the tuneable behaviour for the `TurboFox`
///
/// ## Default Config
///
/// - `logging`: *disabled*
/// - `init_cap`: *1024 entries*
/// - `growth_factor`: *2x*
///
/// ## Directory
///
/// The directory at provided `dirpath` **is created if not already**
///
/// In case it already exists, it is reused
///
/// **⚠️ WARN:** The directory *must be empty* to avoid accidental damage of pre-existing files
#[derive(Debug, Clone)]
pub struct TurboConfig {
    dirpath: PathBuf,
    logger: Logger,
    init_cap: usize,
    growth_factor: usize,
}

// sanity check
const _: () = assert!(std::mem::size_of::<TurboConfig>() == 0x40);

impl TurboConfig {
    /// Builds a new [TurboConfig] using provided `dirpath`
    ///
    /// ## Directory
    ///
    /// The directory at provided `dirpath` **is created if not already**
    ///
    /// In case it already exists, it is reused
    ///
    /// **⚠️ WARN:** The directory *must be empty* to avoid accidental damage of pre-existing files
    ///
    /// ## Default Config
    ///
    /// - `logging`: *disabled*
    /// - `init_cap`: *1024 entries*
    /// - `buf_size`: *64 bytes*
    /// - `max_key_len`: *64 bytes*
    /// - `growth_factor`: *2x*
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::TurboConfig;
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb");
    /// assert!(cfg.is_ok());
    /// ```
    pub fn new<P: AsRef<Path>>(dirpath: P) -> TurboResult<Self> {
        let logger = Logger::default();
        let dirpath = dirpath.as_ref().to_path_buf();

        prep_directory(&dirpath, &logger)?;

        Ok(Self {
            logger,
            dirpath,
            growth_factor: DEFAULT_GROWTH_FACTOR,
            init_cap: DEFAULT_INIT_CAP,
        })
    }

    /// Configure _initial capacity_ for `TurboFox`
    ///
    /// This value represents the number of entries the database holds before any
    /// growth is triggered
    ///
    /// ## Constraints
    ///
    /// - Must be **power of 2**
    ///
    /// ## Tips
    ///
    /// - For larger work loads, use higher `init_cap`, e.g. _8192_ to avoid growth and rehash cost
    /// - For smaller work loads, use lower `init_cap`, e.g. _32_, to reduce memory usage
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboError};
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb/")?.init_cap(0x400)?;
    /// # Ok::<(), TurboError>(())
    /// ```
    pub fn init_cap(mut self, cap: usize) -> TurboResult<Self> {
        if unlikely(cap == 0) {
            return Err(TurboError::InvalidConfig("init_cap should never be zero".into()));
        }

        if !is_pow_of_2(cap as usize) {
            return Err(TurboError::InvalidConfig("init_cap must be power of 2".into()));
        }

        self.init_cap = cap;
        Ok(self)
    }

    /// Configure _logging visibility_ for `TurboFox`
    ///
    /// Default verbosity level is [`TurboLogLevel::Error`]
    ///
    /// Use [log_level](TurboConfig::log_level) to control verbosity.
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboError};
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb/")?.logging(true);
    /// # Ok::<(), TurboError>(())
    /// ```
    pub fn logging(mut self, enable: bool) -> Self {
        self.logger.enable(enable);
        self
    }

    /// Configure _logging verbosity level_ for `TurboFox`
    ///
    /// Use `TurboLogLevel` to control verbosity.
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboError, TurboLogLevel};
    ///
    /// let cfg = TurboConfig::new("/tmp/test_data/turbodb/")?.log_level(TurboLogLevel::INFO);
    /// # Ok::<(), TurboError>(())
    /// ```
    pub fn log_level(mut self, level: TurboLogLevel) -> Self {
        self.logger.set_level(level);
        self
    }
}
