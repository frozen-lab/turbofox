//! # TurboCache
//!
//! TurboCache is a high-performance, embedded key-value store for Rust
//!
//! ### Example Usage
//!
//! ```rust
//! use turbocache::TurboCache;
//! use std::path::PathBuf;
//!
//! // Create a new TurboCache instance in a temporary directory.
//! let dir = PathBuf::from("/tmp/turbocache_docs");
//! std::fs::create_dir_all(&dir).unwrap();
//! let cache = TurboCache::new(dir).unwrap();
//!
//! // Set a key-value pair.
//! cache.set(b"hello", b"world").unwrap();
//!
//! // Get the value for a key.
//! let value = cache.get(b"hello").unwrap();
//! assert_eq!(value, Some(b"world".to_vec()));
//!
//! // Remove a key.
//! let was_removed = cache.remove(b"hello").unwrap();
//! assert_eq!(was_removed, Some(b"world".to_vec()));
//!
//! // The key is now gone.
//! let value = cache.get(b"hello").unwrap();
//! assert_eq!(value, None);
//! ```
mod core;
mod hasher;
mod router;
mod shard;

pub use crate::core::{TError, TResult};
use hasher::TurboHasher;
use router::Router;
use std::path::PathBuf;

/// The main interface to the TurboCache database.
pub struct TurboCache {
    router: Router,
}

impl TurboCache {
    /// Creates a new `TurboCache` instance at the specified directory.
    ///
    /// **NOTE:** If the directory does not exist, it will be created. If it already contains
    /// a TurboCache database, it will be loaded.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use turbocache::TurboCache;
    /// use std::path::PathBuf;
    ///
    /// let dir = PathBuf::from("/tmp/turbocache_docs_new");
    /// std::fs::create_dir_all(&dir).unwrap();
    /// let cache = TurboCache::new(dir).unwrap();
    /// ```
    pub fn new(dirpath: PathBuf) -> TResult<Self> {
        Ok(Self {
            router: Router::open(&dirpath)?,
        })
    }

    /// Sets a key-value pair in the cache.
    ///
    /// **Note:** If the key already exists, its value will be overwritten.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use turbocache::TurboCache;
    /// use std::path::PathBuf;
    ///
    /// let dir = PathBuf::from("/tmp/turbocache_docs_set");
    /// std::fs::create_dir_all(&dir).unwrap();
    /// let cache = TurboCache::new(dir).unwrap();
    ///
    /// cache.set(b"my_key", b"my_value").unwrap();
    /// ```
    pub fn set(&self, kbuf: &[u8], vbuf: &[u8]) -> TResult<()> {
        let hash = TurboHasher::new(kbuf);

        self.router.set((kbuf, vbuf), hash)
    }

    /// Retrieves a value from the cache by its key.
    ///
    /// Returns `Ok(Some(value))` if the key is found, `Ok(None)` if not.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use turbocache::TurboCache;
    /// use std::path::PathBuf;
    ///
    /// let dir = PathBuf::from("/tmp/turbocache_docs_get");
    /// std::fs::create_dir_all(&dir).unwrap();
    /// let cache = TurboCache::new(dir).unwrap();
    ///
    /// cache.set(b"another_key", b"another_value").unwrap();
    /// let value = cache.get(b"another_key").unwrap();
    ///
    /// assert_eq!(value, Some(b"another_value".to_vec()));
    /// ```
    pub fn get(&self, kbuf: &[u8]) -> TResult<Option<Vec<u8>>> {
        let hash = TurboHasher::new(kbuf);

        self.router.get(kbuf, hash)
    }

    /// Removes a key-value pair from the cache.
    ///
    /// Returns `Ok(true)` if the key was found and removed, `Ok(false)` if not.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use turbocache::TurboCache;
    /// use std::path::PathBuf;
    ///
    /// let dir = PathBuf::from("/tmp/turbocache_docs_remove");
    /// std::fs::create_dir_all(&dir).unwrap();
    ///
    /// let cache = TurboCache::new(dir).unwrap();
    ///
    /// cache.set(b"to_be_removed", b"data").unwrap();
    /// let value = cache.remove(b"to_be_removed").unwrap();
    ///
    /// assert_eq!(value, Some(b"data".to_vec()));
    /// ```
    pub fn remove(&self, kbuf: &[u8]) -> TResult<Option<Vec<u8>>> {
        let hash = TurboHasher::new(kbuf);

        self.router.remove(kbuf, hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache() -> (TurboCache, TempDir) {
        let tmp = TempDir::new().expect("create tempdir");
        let cache = TurboCache::new(tmp.path().to_path_buf()).expect("init cache");

        (cache, tmp)
    }

    #[test]
    fn set_and_get_returns_correct_value() -> TResult<()> {
        let (cache, _) = create_cache();

        let key = b"foo";
        let val = b"bar";

        cache.set(key, val)?;
        let fetched = cache.get(key)?;

        assert_eq!(fetched, Some(val.to_vec()));

        Ok(())
    }

    #[test]
    fn get_returns_none_for_missing_key() -> TResult<()> {
        let (cache, _) = create_cache();
        let key = b"no_such_key";
        let fetched = cache.get(key)?;

        assert_eq!(fetched, None);

        Ok(())
    }

    #[test]
    fn overwrite_value_for_key() -> TResult<()> {
        let (cache, _) = create_cache();

        let key = b"hello";
        let val1 = b"world";
        let val2 = b"world_2";

        cache.set(key, val1)?;
        assert_eq!(cache.get(key)?, Some(val1.to_vec()));

        cache.set(key, val2)?;
        assert_eq!(cache.get(key)?, Some(val2.to_vec()));

        Ok(())
    }

    #[test]
    fn remove_existing_key_returns_true() -> TResult<()> {
        let (cache, _) = create_cache();

        let key = b"remove-me";
        let val = b"soon";

        cache.set(key, val)?;
        assert_eq!(cache.get(key)?, Some(val.to_vec()));

        let removed = cache.remove(key)?;
        assert_ne!(removed, None);

        let get_again = cache.get(key)?;
        assert_eq!(get_again, None);

        Ok(())
    }

    #[test]
    fn remove_nonexistent_key_returns_false() -> TResult<()> {
        let (cache, _) = create_cache();
        let removed = cache.remove(b"nope")?;

        assert_eq!(removed, None);
        Ok(())
    }

    #[test]
    fn data_persists_between_instances() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        {
            let cache = TurboCache::new(dir.clone())?;
            cache.set(b"persistent", b"data")?;
        }

        {
            let new_cache = TurboCache::new(dir.clone())?;
            let value = new_cache.get(b"persistent")?;

            assert_eq!(value, Some(b"data".to_vec()));
        }

        Ok(())
    }

    #[test]
    fn multiple_keys_can_be_stored() -> TResult<()> {
        let (cache, _) = create_cache();

        for i in 0..100 {
            let key = format!("key_{i}").into_bytes();
            let val = format!("val_{i}").into_bytes();

            cache.set(&key, &val)?;
        }

        for i in 0..100 {
            let key = format!("key_{i}").into_bytes();
            let expected = format!("val_{i}").into_bytes();
            let fetched = cache.get(&key)?;

            assert_eq!(fetched, Some(expected));
        }

        Ok(())
    }
}
