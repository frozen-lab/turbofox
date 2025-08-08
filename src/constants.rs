/// Format version for on disk files
pub(crate) const VERSION: u32 = 1;

/// ID to be used for all [TurboCache] files
pub(crate) const MAGIC: [u8; 4] = *b"TCv0";

/// Default [Bucket] file name
pub(crate) const DEFAULT_BUCKET_NAME: &str = "default_bucket";

/// Staging [Bucket] file name
pub(crate) const STAGING_BUCKET_NAME: &str = "staging_bucket";

/// [Index] file name
pub(crate) const INDEX_NAME: &str = "index";

/// A custom type for Key-Value pair object
pub(crate) type KVPair = (Vec<u8>, Vec<u8>);

/// A custom type for Key object
pub(crate) type Key = Vec<u8>;
