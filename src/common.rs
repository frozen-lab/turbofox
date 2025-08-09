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

#[cfg(test)]
pub(crate) fn gen_dataset(size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    const KEY_LEN: usize = 32;
    const VAL_LEN: usize = 128;
    const SEED: u64 = 42;

    let mut rng = StdRng::seed_from_u64(SEED);

    (0..size)
        .map(|_| {
            let key = (0..KEY_LEN).map(|_| rng.random()).collect();
            let val = (0..VAL_LEN).map(|_| rng.random()).collect();

            (key, val)
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn create_temp_dir() -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().expect("tempdir");

    tmp
}
