/// Current version of TurboCache shards
pub(crate) const VERSION: u64 = 0;

/// Versioned MAGIC value to help identify shards specific to TurboCache
pub(crate) const MAGIC: [u8; 8] = *b"TURBOCv0";

/// The number of rows in the shard's index.
pub(crate) const ROWS_NUM: usize = 512;

/// The number of slots in each row of the index.
pub(crate) const ROWS_WIDTH: usize = 32;

/// Maximum allowed length for the key
pub(crate) const MAX_KEY_SIZE: usize = 16;

#[derive(Debug, Clone)]
#[allow(unused)]
pub(crate) struct TurboConfig {
    /// Path of the storage directory
    pub dirpath: std::path::PathBuf,
}

/// A specialized `Result` type for shard operations.
pub type TResult<T> = Result<T, TError>;

/// Errors that can occur during shard operations.
#[derive(Debug)]
pub enum TError {
    /// An I/O error occurred.
    Io(std::io::Error),

    /// A row in the shard's index is full.
    RowFull(usize),

    /// File offset is out of bounds of `2^22` allowed range
    OffsetOob(usize),

    /// The shard selector is out of the range handled by this shard.
    ShardOutOfRange(u32),

    /// Key is too large
    KeyTooLarge(usize),

    /// Value is too large
    ValTooLarge(usize),

    /// Key is too small
    KeyTooSmall,
}

impl From<std::io::Error> for TError {
    fn from(err: std::io::Error) -> Self {
        TError::Io(err)
    }
}

impl std::fmt::Display for TError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TError::Io(err) => write!(f, "I/O error: {}", err),
            TError::RowFull(row) => write!(f, "row {} is full", row),
            TError::OffsetOob(offset) => write!(f, "offset {} is out of bounds", offset),
            TError::ShardOutOfRange(shard) => write!(f, "out of range of {}", shard),
            TError::KeyTooLarge(size) => {
                write!(f, "key size {size} should be lower then {MAX_KEY_SIZE}")
            }
            TError::ValTooLarge(size) => {
                write!(f, "value size {size} should be lower then `2^10` (1024)")
            }
            TError::KeyTooSmall => write!(f, "Key buffered must not be of zeroed bytes"),
        }
    }
}

impl std::error::Error for TError {}
