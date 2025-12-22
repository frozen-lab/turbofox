const DEFAULT_GROWTH_FACTOR: u64 = 2;
const DEFAULT_BUF_SIZE: TurboConfigValue = TurboConfigValue::N64;
const DEFAULT_INIT_CAP: TurboConfigValue = TurboConfigValue::N128;
const DEFAULT_MAX_KEY_LEN: TurboConfigValue = TurboConfigValue::N64;

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
/// - `buf_size`: *64 bytes*
/// - `max_key_len`: *64 bytes*
/// - `growth_factor`: *2x*
/// - `initial_capacity`: *128 entries*
///
/// ## Memory Overhead
///
/// TODO: fill in after finalizing
///
/// ## Disk Overhead
///
/// TODO: fill in after finalizing
///
/// # Example
///
/// ```
/// use turbofox::{TurboConfig, TurboLogLevel, TurboConfigValue};
///
/// let cfg = TurboConfig {
///   logging: true,
///   growth_factor: 4,
///   log_level: TurboLogLevel::INFO,
///   initial_capacity: TurboConfigValue::N1024,
///   max_key_len: TurboConfigValue::N128,
///   buf_size: TurboConfigValue::N128,
/// };
/// ```
#[derive(Debug)]
pub struct TurboConfig {
    /// Controls _logging visibility_ for [TurboFox]
    ///
    /// By default logging is disabled
    ///
    /// When enabled, default verbosity level is `TurboLogLevel::ERROR`
    ///
    /// Use [log_level](TurboConfig::log_level) to control verbosity.
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::TurboConfig;
    ///
    /// let cfg = TurboConfig {
    ///   logging: true,
    ///   ..Default::default()
    /// };
    /// ```
    pub logging: bool,

    /// Controls _growth factor_ for [TurboFox]
    ///
    /// Default growth factor is `2x`
    ///
    /// e.g. For `growth_facotr = 2`, when [TurboFox] reaches _~80%_ of its current cap,
    /// database is grown to have `new_cap = current_cap * growth_factor`, i.e. 2x of the
    /// current cap
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::TurboConfig;
    ///
    /// let cfg = TurboConfig {
    ///   growth_factor: 4,
    ///   ..Default::default()
    /// };
    /// ```
    pub growth_factor: u64,

    /// Controls _logging verbosity level_ for [TurboFox]
    ///
    /// Default verbosity level is `TurboLogLevel::Error`
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboLogLevel};
    ///
    /// let cfg = TurboConfig {
    ///   logging: true,
    ///   log_level: TurboLogLevel::INFO,
    ///   ..Default::default()
    /// };
    /// ```
    pub log_level: TurboLogLevel,

    /// Controls _internal buffer size_ used for storing raw Key-Value bytes in [TurboFox]
    ///
    /// Default _buf size_ is `TurboConfigValue::N64`
    ///
    /// On disk storage has a buffered layout, where each buffer is of _buf_size_, e.g. key-value
    /// buffer is of 200 bytes, and `buf_size = 64`, the raw bytes would be stored across *4 buffers*
    ///
    /// For smaller key-value buffers, use smaller _buf_size_, and larger for larger buffers, for
    /// optimal performance and storage overheads
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboConfigValue};
    ///
    /// let cfg = TurboConfig {
    ///   buf_size: TurboConfigValue::N128,
    ///   ..Default::default()
    /// };
    /// ```
    pub buf_size: TurboConfigValue,

    /// Controls _maximum allowed key size_ for [TurboFox]
    ///
    /// Default _max key len_ is `TurboConfigValue::N64`
    ///
    /// **NOTE:** Make sure `max_key_len == buf_size` for optimal performance and resurce usage
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboConfigValue};
    ///
    /// let cfg = TurboConfig {
    ///   buf_size: TurboConfigValue::N128,
    ///   max_key_len: TurboConfigValue::N128,
    ///   ..Default::default()
    /// };
    /// ```
    pub max_key_len: TurboConfigValue,

    /// Controls _initial database capacity_ for [TurboFox]
    ///
    /// Default _initial cap_ is `TurboConfigValue::N128`
    ///
    /// This value represents the number of entries the database holds before any
    /// growth is triggered
    ///
    /// ## Tips
    ///
    /// - For larger work loads, use higher `init_cap`, e.g. 8192 to avoid the growth cost
    /// - For smaller work loads, use lower `init_cap`, e.g. 32 to reduce memory usage
    ///
    /// # Example
    ///
    /// ```
    /// use turbofox::{TurboConfig, TurboConfigValue};
    ///
    /// let cfg = TurboConfig {
    ///   initial_capacity: TurboConfigValue::N4096,
    ///   ..Default::default()
    /// };
    /// ```
    pub initial_capacity: TurboConfigValue,
}

impl Default for TurboConfig {
    fn default() -> Self {
        Self {
            logging: false,
            buf_size: DEFAULT_BUF_SIZE,
            log_level: TurboLogLevel::ERROR,
            max_key_len: DEFAULT_MAX_KEY_LEN,
            initial_capacity: DEFAULT_INIT_CAP,
            growth_factor: DEFAULT_GROWTH_FACTOR,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum TurboConfigValue {
    N32,
    N64,
    N128,
    N256,
    N512,
    N1024,
    N2048,
    N4096,
    N8192,
    N16384,
}

impl TurboConfigValue {
    #[inline]
    pub(crate) const fn to_u64(&self) -> u64 {
        match self {
            Self::N32 => 0x20,
            Self::N64 => 0x40,
            Self::N128 => 0x80,
            Self::N256 => 0x100,
            Self::N512 => 0x200,
            Self::N1024 => 0x400,
            Self::N2048 => 0x800,
            Self::N4096 => 0x1000,
            Self::N8192 => 0x2000,
            Self::N16384 => 0x4000,
        }
    }
}
