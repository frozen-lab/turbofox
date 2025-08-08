/// Configurations for [TurboCache] struct
#[derive(Clone)]
pub(crate) struct InternalConfig {
    pub dirpath: std::path::PathBuf,
    pub initial_capacity: usize,
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

    /// Invalid or outdated file
    InvalidFile,

    /// Implies that the underlying bucket is full
    BucketFull,

    /// File offset in Bucket reached u32::Max (upper bound)
    OffsetOverflow,

    /// Key size is greater then u16::Max
    KeyTooLarge,

    /// Value size is greater then u16::Max
    ValueTooLarge,
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
