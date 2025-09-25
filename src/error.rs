/// A specialized `Result` type for operations in [TurboCache]
pub type TurboResult<T> = Result<T, TurboError>;

/// A custom set of errors thrown by [TurboCache] public API's
///
/// ### Note
///
/// These represent only the *surface-level* errors. Internal details
/// are abstracted away and mapped into these variants.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TurboError {
    /// Represents an underlying I/O error (file system, OS, etc.)
    Io(String),

    /// The bucket has reached its maximum size and cannot grow further.
    ///
    /// The associated `usize` indicates the capacity at which it failed.
    BucketOverflow(usize),

    /// A fallback for unexpected or uncategorized errors.
    Unknown(String),
}

impl From<InternalError> for TurboError {
    fn from(err: InternalError) -> Self {
        match err {
            InternalError::Io(e) => TurboError::Io(e),
            InternalError::BucketOverflow(n) => TurboError::BucketOverflow(n),
            _ => TurboError::Unknown("Unknown error has occurred".into()),
        }
    }
}

impl std::fmt::Display for TurboError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurboError::Io(err) => write!(f, "I/O error: {err}"),
            TurboError::Unknown(err) => write!(f, "Unknown error: {err}"),
            TurboError::BucketOverflow(n) => write!(
                f,
                "Overflow: Bucket is full and can not grow beyound {n} pairs"
            ),
        }
    }
}

/// A custom result type for internal error handleing
pub(crate) type InternalResult<T> = Result<T, InternalError>;

/// Set of errors propogated internally inside the crate
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) enum InternalError {
    /// Represents an I/O error
    Io(String),

    /// Lock poisoned
    ///
    /// NOTE: This can be because another thread panicked while holding it.
    ///
    /// **Must be handled internally**
    LockPoisoned(String),

    /// Invalid or outdated file
    ///
    /// NOTE: This can be because the file does not match the [Magic] or
    /// it has old (outdated) version
    ///
    /// **Must be handled internally**
    InvalidFile,

    /// Implies that the underlying bucket is full
    ///
    /// NOTE: This used more like a trigger then error, when this is thrown
    /// we spawn staging bucket
    ///
    /// **Must be handled internally**
    BucketFull(usize),

    /// The bucket contains an invalid entry
    ///
    /// ## Reasons
    ///
    /// - Invalid [Namespace] value
    ///
    /// **Must be handled internally**
    InvalidEntry(String),

    /// The bucket has reached its max capacity and can not be grown further.
    ///
    /// ## Reasons
    ///
    /// - The insert count has reached its max cap of `u32::Max`
    /// - The insertion offset has reached its max cap of `2^40 - 1` i.e `u40::Max`
    ///
    /// NOTE: This is rare, but acts as a guard rail to prevent crash.
    BucketOverflow(usize),
}

impl From<std::io::Error> for InternalError {
    fn from(e: std::io::Error) -> Self {
        InternalError::Io(e.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for InternalError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        InternalError::LockPoisoned(e.to_string())
    }
}
