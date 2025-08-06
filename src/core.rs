use std::path::PathBuf;
use std::sync::PoisonError;

pub(crate) const VERSION: u32 = 0;
pub(crate) const MAGIC: [u8; 4] = *b"TCv0";

pub(crate) const BUCKET_NAME: &str = "default_bucket";
pub(crate) const STAGING_BUCKET_NAME: &str = "staging_bucket";
pub(crate) const INDEX_NAME: &str = "tc_index";

/// A custom type for Key-Value pair object
pub(crate) type KVPair = (Vec<u8>, Vec<u8>);

/// Custom `Result` type returned by TurboCache and its op's
pub type TurboResult<T> = Result<T, TurboError>;

pub(crate) type InternalResult<T> = Result<T, InternalError>;

/// Configurations for `TurboCache`
#[derive(Clone)]
pub(crate) struct TurboConfig {
    pub dirpath: PathBuf,
    pub initial_capacity: usize,
}

#[derive(Debug)]
pub enum TurboError {
    /// Key size out of range
    KeyTooLarge(usize),

    /// Value size out of range
    ValueTooLarge(usize),

    /// An I/O error occurred.
    Io(std::io::Error),

    /// Lock was poisoned because another thread panicked while holding it.
    LockPoisoned(String),

    /// DB under contention, must be clear before any "set" operations
    Contention,

    /// Unknown error
    Unknown,
}

impl std::fmt::Display for TurboError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurboError::Io(err) => write!(f, "I/O error: {}", err),
            TurboError::KeyTooLarge(size) => write!(f, "Key size ({}) is too large", size),
            TurboError::ValueTooLarge(size) => write!(f, "Value size ({}) is too large", size),
            TurboError::LockPoisoned(e) => write!(f, "Lock poisoned due to an error, [e]: {e}"),
            TurboError::Contention => write!(f, "TurboCache has reached contention :("),
            TurboError::Unknown => write!(f, "Some unknown error occurred"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum InternalError {
    /// An I/O error occurred.
    Io(std::io::Error),

    /// Lock was poisoned because another thread panicked while holding it.
    LockPoisoned(String),

    /// Invalid buffer or shard file
    InvalidFile,

    /// Implies that the underlying bucket is FULL
    BucketFull,
}

impl From<std::io::Error> for InternalError {
    fn from(err: std::io::Error) -> Self {
        InternalError::Io(err)
    }
}

impl<T> From<PoisonError<T>> for InternalError {
    fn from(e: PoisonError<T>) -> Self {
        InternalError::LockPoisoned(e.to_string())
    }
}

impl std::error::Error for InternalError {}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternalError::Io(_) => write!(f, ""),
            InternalError::LockPoisoned(_) => write!(f, ""),
            InternalError::InvalidFile => write!(f, ""),
            InternalError::BucketFull => write!(f, ""),
        }
    }
}

impl From<InternalError> for TurboError {
    fn from(err: InternalError) -> Self {
        match err {
            InternalError::Io(e) => TurboError::Io(e),
            InternalError::LockPoisoned(e) => TurboError::LockPoisoned(e),
            InternalError::BucketFull => TurboError::Contention,
            InternalError::InvalidFile => TurboError::Contention,
            // _ => TurboError::Unknown,
        }
    }
}
