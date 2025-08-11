/// Configurations for [TurboCache] struct
#[derive(Clone)]
pub(crate) struct InternalConfig {
    pub(crate) dirpath: std::path::PathBuf,
    pub(crate) initial_capacity: usize,
}

/// A custom result type
pub type TurboResult<T> = Result<T, TurboError>;

/// A custom set of errors thrown by [TurboCache]
#[derive(Debug)]
pub enum TurboError {
    /// Represents an I/O error
    Io(std::io::Error),

    /// Lock was poisoned because another thread panicked while holding it
    LockPoisoned(String),

    /// File offset in Bucket reached u32::Max (upper bound)
    OffsetOverflow(usize),

    /// Key size is greater then u16::Max
    KeyTooLarge(usize),

    /// Value size is greater then u16::Max
    ValueTooLarge(usize),

    /// An unknown error occoured, generally means DB is in contention
    Unknown,
}

impl std::fmt::Display for TurboError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurboError::Io(err) => write!(f, "I/O error: {}", err),
            TurboError::KeyTooLarge(size) => write!(f, "Key size ({}) is too large", size),
            TurboError::ValueTooLarge(size) => write!(f, "Value size ({}) is too large", size),
            TurboError::OffsetOverflow(size) => write!(f, "File offset overflowed at {}", size),
            TurboError::LockPoisoned(e) => write!(f, "Lock poisoned due to an error, [e]: {e}"),
            TurboError::Unknown => write!(f, "Some unknown error occurred"),
        }
    }
}

impl From<std::io::Error> for TurboError {
    fn from(err: std::io::Error) -> Self {
        TurboError::Io(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for TurboError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        TurboError::LockPoisoned(e.to_string())
    }
}

impl From<InternalError> for TurboError {
    fn from(err: InternalError) -> Self {
        match err {
            InternalError::Io(e) => TurboError::Io(e),
            InternalError::LockPoisoned(e) => TurboError::LockPoisoned(e),
            InternalError::OffsetOverflow(n) => TurboError::OffsetOverflow(n),
            InternalError::KeyTooLarge(n) => TurboError::KeyTooLarge(n),
            InternalError::ValueTooLarge(n) => TurboError::ValueTooLarge(n),
            InternalError::InvalidFile => TurboError::Unknown,
            InternalError::BucketFull => TurboError::Unknown,
        }
    }
}

/// A custom result type
pub(crate) type InternalResult<T> = Result<T, InternalError>;

/// Set of errors propogated internally inside the crate
#[derive(Debug)]
pub(crate) enum InternalError {
    /// Represents an I/O error
    Io(std::io::Error),

    /// Lock was poisoned because another thread panicked while holding it
    LockPoisoned(String),

    /// File offset in Bucket reached u32::Max (upper bound)
    OffsetOverflow(usize),

    /// Key size is greater then u16::Max
    KeyTooLarge(usize),

    /// Value size is greater then u16::Max
    ValueTooLarge(usize),

    /// Invalid or outdated file
    ///
    /// NOTE: For internal use only
    InvalidFile,

    /// Implies that the underlying bucket is full
    ///
    /// NOTE: For internal use only
    BucketFull,
}

impl From<std::io::Error> for InternalError {
    fn from(err: std::io::Error) -> Self {
        InternalError::Io(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for InternalError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        InternalError::LockPoisoned(e.to_string())
    }
}
