use crate::kosh::KoshConfig;

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
    BucketOverflow(String),

    /// A fallback for unexpected or uncategorized errors.
    Unknown,
}

impl From<InternalError> for TurboError {
    fn from(err: InternalError) -> Self {
        match err {
            InternalError::Io(e) => TurboError::Io(e),
            InternalError::BucketOverflow(config) => {
                let bkt = {
                    if let Some(cfg) = config {
                        cfg.name
                    } else {
                        "unknown bucket"
                    }
                };

                TurboError::BucketOverflow(bkt.to_string())
            }
            _ => TurboError::Unknown,
        }
    }
}

impl From<std::io::Error> for TurboError {
    fn from(e: std::io::Error) -> Self {
        TurboError::Io(e.to_string())
    }
}

impl std::fmt::Display for TurboError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurboError::Io(err) => write!(f, "[ERROR]: {}\n{err}", IO_ERROR),
            TurboError::Unknown => write!(f, "[ERROR]: {}", UNK_ERROR),
            TurboError::BucketOverflow(bkt) => {
                write!(
                    f,
                    "[ERROR]: {}\nOverflow for {bkt} Bucket",
                    BKT_OVERFLOW_ERROR
                )
            }
        }
    }
}

const IO_ERROR: &'static str = "An I/O releated error has occurred ;[";
const UNK_ERROR: &'static str = "Some unknown error has occurred ¬_¬";
const BKT_OVERFLOW_ERROR: &'static str =
    "Bucket has reached it's max capacity and can't be grown further ;_;";

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
    InvalidFile(Option<KoshConfig>),

    /// Implies that the underlying bucket is full
    ///
    /// NOTE: This used more like a trigger then error, when this is thrown
    /// we spawn staging bucket
    ///
    /// **Must be handled internally**
    BucketFull(Option<KoshConfig>),

    /// The bucket contains an invalid entry
    ///
    /// ## Reasons
    ///
    /// - Invalid [Namespace] value
    ///
    /// **Must be handled internally**
    InvalidEntry(Option<KoshConfig>),

    /// The bucket has reached its max capacity and can not be grown further.
    ///
    /// ## Reasons
    ///
    /// - The insert count has reached its max cap of `u32::Max`
    /// - The insertion offset has reached its max cap of `2^40 - 1` i.e `u40::Max`
    ///
    /// NOTE: This is rare, but acts as a guard rail to prevent crash.
    BucketOverflow(Option<KoshConfig>),
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
