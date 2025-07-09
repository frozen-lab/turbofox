//! This module provides the `Router`, the central component for directing database
//! operations to the correct shard.
//!
//! The `Router` is responsible for managing a collection of `Shard` instances, each
//! representing a partition of the keyspace. When a `get`, `set`, or `remove`
//! operation is requested, the router uses the key's hash to identify the
//! appropriate shard and forwards the request to it.
//!
//! The `Router` also handles the loading of existing shards from disk and the
//! creation of new shards when the database is first opened.

use crate::{
    hasher::TurboHasher,
    shard::{Error, Shard, TResult},
};
use std::path::PathBuf;

/// The `Router` manages a collection of shards and routes database operations
/// to the correct one based on the key's hash.
///
/// It acts as the primary entry point for interacting with the database,
/// abstracting away the underlying sharding mechanism.
pub(crate) struct Router {
    shards: Vec<Shard>,
}

impl Router {
    pub(crate) const END_OF_SHARDS: u32 = 1u32 << 16;

    /// Opens the database at the specified directory, loading existing shards or
    /// creating a new one if none are found.
    ///
    /// This function will scan the directory for shard files, clean up any temporary
    /// or merge-related files, and initialize the `Router` with the loaded shards.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    /// use turbocache::router::Router;
    ///
    /// let dir = PathBuf::from("/tmp/turbocache_docs_open");
    /// std::fs::create_dir_all(&dir).unwrap();
    /// let router = Router::open(&dir).unwrap();
    /// ```
    pub fn open(dirpath: &PathBuf) -> TResult<Self> {
        let shards = Self::load(dirpath)?;

        if shards.len() == 0 {
            let shard = Shard::open(&dirpath, 0..(u16::MAX as u32 + 1), true)?;

            return Ok(Self {
                shards: vec![shard],
            });
        }

        Ok(Self { shards })
    }

    /// Loads all valid shards from the specified directory.
    ///
    /// This function scans the directory for files with the `shard_` prefix,
    /// parses the shard's range from the filename, and initializes a `Shard`
    /// for each valid file. It also cleans up any leftover temporary files.
    fn load(dirpath: &PathBuf) -> TResult<Vec<Shard>> {
        let mut found_shards: Vec<Shard> = vec![];

        for res in std::fs::read_dir(&dirpath)? {
            let entry = res?;
            let filename = entry.file_name();

            let Some(filename) = filename.to_str() else {
                continue;
            };

            let Ok(filetype) = entry.file_type() else {
                continue;
            };

            if !filetype.is_file() {
                continue;
            }

            if filename.starts_with("bottom_")
                || filename.starts_with("top_")
                || filename.starts_with("merge_")
            {
                std::fs::remove_file(entry.path())?;

                continue;
            } else if !filename.starts_with("shard_") {
                continue;
            }

            let Some((_, span)) = filename.split_once("_") else {
                continue;
            };

            let Some((start, end)) = span.split_once("-") else {
                continue;
            };

            let start = u32::from_str_radix(start, 16).expect(filename);
            let end = u32::from_str_radix(end, 16).expect(filename);
            let range = start..end;

            if start >= end || end > Self::END_OF_SHARDS {
                // NOTE: Invalid shard
                continue;
            }

            found_shards.push(Shard::open(&dirpath, range, false)?);
        }

        Ok(found_shards)
    }

    /// Sets a key-value pair in the appropriate shard.
    ///
    /// The shard is determined by the `shard_selector` of the provided `hash`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    /// use turbocache::router::Router;
    /// use turbocache::hasher::TurboHasher;
    ///
    /// let dir = PathBuf::from("/tmp/turbocache_docs_set");
    /// std::fs::create_dir_all(&dir).unwrap();
    /// let router = Router::open(&dir).unwrap();
    ///
    /// let key = b"hello";
    /// let value = b"world";
    /// let hash = TurboHasher::new(key);
    ///
    /// router.set((key, value), hash).unwrap();
    /// ```
    pub fn set(&self, buf: (&[u8], &[u8]), hash: TurboHasher) -> TResult<()> {
        let s = hash.shard_selector();

        for shard in &self.shards {
            if shard.span.contains(&s) {
                return shard.set(buf, hash);
            }
        }

        // if we ran out of room in this row
        Err(Error::ShardOutOfRange(s))
    }

    /// Retrieves a value by its key from the appropriate shard.
    ///
    /// The shard is determined by the `shard_selector` of the provided `hash`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    /// use turbocache::router::Router;
    /// use turbocache::hasher::TurboHasher;
    ///
    /// let dir = PathBuf::from("/tmp/turbocache_docs_get");
    /// std::fs::create_dir_all(&dir).unwrap();
    /// let router = Router::open(&dir).unwrap();
    ///
    /// let key = b"hello";
    /// let value = b"world";
    /// let hash = TurboHasher::new(key);
    ///
    /// router.set((key, value), hash).unwrap();
    /// let retrieved = router.get(key, hash).unwrap();
    ///
    /// assert_eq!(retrieved, Some(value.to_vec()));
    /// ```
    pub fn get(&self, buf: &[u8], hash: TurboHasher) -> TResult<Option<Vec<u8>>> {
        let s = hash.shard_selector();

        for shard in &self.shards {
            if shard.span.contains(&s) {
                return shard.get(buf, hash);
            }
        }

        // if we ran out of room in this row
        Err(Error::ShardOutOfRange(s))
    }

    /// Removes a key-value pair from the appropriate shard.
    ///
    /// The shard is determined by the `shard_selector` of the provided `hash`.
    /// Returns `true` if the key was found and removed, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    /// use turbocache::router::Router;
    /// use turbocache::hasher::TurboHasher;
    ///
    /// let dir = PathBuf::from("/tmp/turbocache_docs_remove");
    /// std::fs::create_dir_all(&dir).unwrap();
    /// let router = Router::open(&dir).unwrap();
    ///
    /// let key = b"hello";
    /// let value = b"world";
    /// let hash = TurboHasher::new(key);
    ///
    /// router.set((key, value), hash).unwrap();
    /// let was_removed = router.remove(key, hash).unwrap();
    ///
    /// assert!(was_removed);
    ///
    /// let retrieved = router.get(key, hash).unwrap();
    /// assert_eq!(retrieved, None);
    /// ```
    pub fn remove(&self, buf: &[u8], hash: TurboHasher) -> TResult<bool> {
        let s = hash.shard_selector();

        for shard in &self.shards {
            if shard.span.contains(&s) {
                return shard.remove(buf, hash);
            }
        }

        // if we ran out of room in this row
        Err(Error::ShardOutOfRange(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn new_router() -> TResult<(Router, TempDir)> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;
        let router = Router::open(&dir)?;

        Ok((router, tmp))
    }

    #[test]
    fn set_get_remove_via_router() {
        let (router, _tmp) = new_router().unwrap();
        let key = b"key1";
        let val = b"val1".to_vec();
        let h = TurboHasher::new(key);

        // not present yet
        assert_eq!(router.get(key, h).unwrap(), None);

        // set and get
        router.set((key, &val), h).unwrap();
        assert_eq!(router.get(key, h).unwrap(), Some(val));

        // remove and gone
        assert!(router.remove(key, h).unwrap());
        assert_eq!(router.get(key, h).unwrap(), None);

        // removing again returns false
        assert!(!router.remove(key, h).unwrap());
    }

    #[test]
    fn persistence_across_reopen() {
        // first open, insert a key
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir).unwrap();

        {
            let router = Router::open(&dir).unwrap();
            let key = b"persist";
            let val = b"data".to_vec();
            let h = TurboHasher::new(key);

            router.set((key, &val), h).unwrap();
        }

        let router2 = Router::open(&dir).unwrap();
        let key = b"persist";
        let h2 = TurboHasher::new(key);

        assert_eq!(router2.get(key, h2).unwrap(), Some(b"data".to_vec()));
    }

    #[test]
    fn out_of_range_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&dir).unwrap();

        let s = Shard::open(&dir, 0..1, true).unwrap();
        let router = Router { shards: vec![s] };

        let key = b"x";
        let fake = TurboHasher::new(key);

        let res = router.get(key, fake);

        assert!(matches!(res, Err(Error::ShardOutOfRange(_))));
    }
}
