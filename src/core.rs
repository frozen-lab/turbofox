use std::path::Path;
use std::sync::PoisonError;

pub(crate) const VERSION: u32 = 0;
pub(crate) const MAGIC: [u8; 4] = *b"TCv0";

pub(crate) const BUCKET_NAME: &str = "default_bucket";
pub(crate) const STAGING_BUCKET_NAME: &str = "staging_bucket";
pub(crate) const INDEX_NAME: &str = "tc_index";

/// A custom type for Key-Value pair object
pub(crate) type KVPair = (Vec<u8>, Vec<u8>);

/// Custom `Result` type returned by TurboCache and its op's
pub type TurboResult<T> = Result<T, InternalError>;

/// Configurations for `TurboCache`
pub(crate) struct TurboConfig<P: AsRef<Path>> {
    pub dirpath: P,
    pub initial_capacity: usize,
}

#[derive(Debug)]
pub enum InternalError {
    /// An I/O error occurred.
    Io(std::io::Error),

    /// Key size out of range
    KeyTooLarge(usize),

    /// Value size out of range
    ValueTooLarge(usize),

    /// Lock was poisoned because another thread panicked while holding it.
    LockPoisoned(String),

    /// Invalid buffer or shard file
    ///
    /// NOTE: Only for internal use
    InvalidFile,
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
            InternalError::Io(err) => write!(f, "I/O error: {}", err),
            InternalError::KeyTooLarge(size) => write!(f, "Key size ({}) is too large", size),
            InternalError::ValueTooLarge(size) => write!(f, "Value size ({}) is too large", size),
            InternalError::LockPoisoned(e) => write!(f, "Lock poisoned due to error: {e}"),
            // NOTE: this is never exposed to outside
            InternalError::InvalidFile => write!(f, ""),
        }
    }
}
