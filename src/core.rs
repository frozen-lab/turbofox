#![allow(dead_code)]

use std::path::Path;

pub(crate) const VERSION: u32 = 0;
pub(crate) const MAGIC: [u8; 4] = *b"TCv0";

pub(crate) const BUCKET_NAME: &str = "default_bucket";
pub(crate) const STAGING_BUCKET_NAME: &str = "staging_bucket";
pub(crate) const INDEX_NAME: &str = "tc_index";

/// A custom type for Key-Value pair object
pub(crate) type KVPair = (Vec<u8>, Vec<u8>);

/// Custom `Result` type returned by TurboCache and its op's
pub type TurboResult<T> = Result<T, TurboError>;

/// Configurations for `TurboCache`
pub(crate) struct TurboConfig<P: AsRef<Path>> {
    pub dirpath: P,
    pub initial_capacity: usize,
}

#[derive(Debug)]
pub enum TurboError {
    /// An I/O error occurred.
    Io(std::io::Error),

    /// Key size out of range
    KeyTooLarge(usize),

    /// Value size out of range
    ValueTooLarge(usize),

    /// Invalid buffer or shard file
    InvalidFile,

    BucketFull,
}

impl From<std::io::Error> for TurboError {
    fn from(err: std::io::Error) -> Self {
        TurboError::Io(err)
    }
}

impl std::error::Error for TurboError {}

impl std::fmt::Display for TurboError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurboError::Io(err) => write!(f, "I/O error: {}", err),
            TurboError::KeyTooLarge(size) => write!(f, "Key size ({}) is too large", size),
            TurboError::ValueTooLarge(size) => write!(f, "Value size ({}) is too large", size),
            TurboError::InvalidFile => write!(f, "Invalid shard/buffer file"),
            TurboError::BucketFull => write!(f, "Bucket is full"),
        }
    }
}
