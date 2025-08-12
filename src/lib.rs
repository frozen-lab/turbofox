//!
//! TurboCache is a persistent and efficient embedded KV database
//!

use router::{Router, RouterIterator};
use std::{
    path::Path,
    sync::{Arc, RwLock},
};
use types::InternalConfig;

mod bucket;
mod common;
mod hasher;
mod index;
mod router;
mod types;

pub use crate::types::{TurboError, TurboResult};

/// [TurboCache] is a persistent and efficient embedded KV database
///
/// ## Example
///
/// ```rs
/// use turbocache::TurboCache;
///
/// fn main() {
///     let path = std::env::temp_dir().join("cache-dir");
///     let cache = TurboCache::new(path, 1024).unwrap();
///
///     for i in 0..5 {
///         cache.set(&vec![i], &vec![i * 10]).unwrap();
///     }
///
///     assert_eq!(cache.get(&vec![3]).unwrap(), Some(vec![30]));
///     assert_eq!(cache.del(&vec![3]).unwrap(), Some(vec![30]));
/// }
/// ```
#[derive(Clone)]
pub struct TurboCache {
    router: Arc<RwLock<Router>>,
}

impl TurboCache {
    /// Open or Create a new [`TurboCache`] instance
    ///
    /// ## Parameters
    ///
    /// - `dirpath`: [Path] to a directory on disk
    /// - `initial_capacity`: Initial capacity (no. of KV pairs stored) before auto scaling happens
    ///
    /// > NOTE: `initial_capacity` is only used to create new instances, it makes no effect otherwise
    ///
    /// ## Errors
    ///
    /// An instance of [TurboError] is returned
    ///
    /// ## Example
    ///
    /// ```rs
    /// use turbocache::TurboCache;
    ///
    /// let path = std::env::temp_dir().join("cache-dir");
    /// let cache = TurboCache::new(path, 1024).unwrap();
    ///
    /// assert_eq!(cache.total_count().unwrap(), 0);
    /// ```
    pub fn new<P: AsRef<Path>>(dirpath: P, initial_capacity: usize) -> TurboResult<Self> {
        let internal_config = InternalConfig {
            initial_capacity,
            dirpath: dirpath.as_ref().to_path_buf(),
        };

        let router = Router::new(internal_config)?;

        Ok(Self {
            router: Arc::new(RwLock::new(router)),
        })
    }

    /// Inserts or update a key–value pair
    ///
    /// > NOTE: If the key already exists, its value will be overwritten.
    ///
    /// ## Errors
    ///
    /// An instance of [TurboError] is returned
    ///
    /// ## Example
    ///
    /// ```rs
    /// use turbocache::TurboCache;
    ///
    /// let path = std::env::temp_dir().join("cache-dir");
    /// let cache = TurboCache::new(path, 1024).unwrap();
    ///
    /// cache.set(b"hello", b"world").unwrap();
    /// assert_eq!(cache.get(b"hello").unwrap(), Some(b"world".to_vec()));
    /// ```
    pub fn set(&self, key: &[u8], value: &[u8]) -> TurboResult<()> {
        let mut write_lock = self.write_lock()?;

        let k = key.to_vec();
        let v = value.to_vec();

        write_lock.set((k, v))?;

        Ok(())
    }

    /// Retrive a value for a given key
    ///
    /// > NOTE: If the key already exists, its value will be overwritten.
    ///
    /// ## Returns
    ///
    /// - `Ok(Some(Vec<u8>))` if the key exists.
    /// - `Ok(None)` if the key is not found.
    ///
    /// ## Errors
    ///
    /// An instance of [TurboError] is returned
    ///
    /// ## Example
    ///
    /// ```rs
    /// use turbocache::TurboCache;
    ///
    /// let path = std::env::temp_dir().join("cache-dir");
    /// let cache = TurboCache::new(path, 1024).unwrap();
    ///
    /// cache.set(b"a", b"1").unwrap();
    ///
    /// assert_eq!(cache.get(b"a").unwrap(), Some(b"1".to_vec()));
    /// assert_eq!(cache.get(b"missing").unwrap(), None);
    /// ```
    pub fn get(&self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let lock = self.read_lock()?;
        let res = lock.get(key.to_vec())?;

        Ok(res)
    }

    /// Delete a key–value pair
    ///
    /// ## Errors
    ///
    /// An instance of [TurboError] is returned
    ///
    /// ## Returns
    ///
    /// - `Ok(Some(Vec<u8>))` containing the removed value if the key existed
    /// - `Ok(None)` if the key was not found
    ///
    /// ## Example
    ///
    /// ```rs
    /// use turbocache::TurboCache;
    ///
    /// let path = std::env::temp_dir().join("cache-dir");
    /// let cache = TurboCache::new(path, 1024).unwrap();
    ///
    /// cache.set(b"x", b"y").unwrap();
    ///
    /// assert_eq!(cache.del(b"x").unwrap(), Some(b"y".to_vec()));
    /// assert_eq!(cache.get(b"x").unwrap(), None);
    /// ```    
    pub fn del(&self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let mut write_lock = self.write_lock()?;
        let res = write_lock.del(key.to_vec())?;

        Ok(res)
    }

    /// Returns an iterator over all key–value pairs
    ///
    /// ## Returns
    ///
    /// - An [`Iterator`] over `TurboResult<(Vec<u8>, Vec<u8>)>`.  
    /// - Each `Ok` item contains a `(key, value)` pair.
    ///
    /// ## Example
    ///
    /// ```rs
    /// use turbocache::TurboCache;
    ///
    /// let path = std::env::temp_dir().join("cache-dir");
    /// let cache = TurboCache::new(path, 1024).unwrap();
    ///
    /// cache.set(b"a", b"1").unwrap();
    /// cache.set(b"b", b"2").unwrap();
    ///
    /// let mut keys = Vec::new();
    ///
    /// for res in cache.iter().unwrap() {
    ///     let (k, v) = res.unwrap();
    ///     keys.push(k);
    /// }
    ///
    /// assert!(keys.contains(&b"a".to_vec()));
    /// assert!(keys.contains(&b"b".to_vec()));
    /// ```
    pub fn iter(&self) -> TurboResult<TurboCacheIter<'_>> {
        let router_guard = self.read_lock()?;
        let router_iter = router_guard.iter()?;

        Ok(TurboCacheIter {
            _guard: router_guard,
            iter: router_iter,
        })
    }

    /// Returns the total number of key–value pairs stored
    ///
    /// ## Example
    ///
    /// ```rs
    /// use turbocache::TurboCache;
    ///
    /// let path = std::env::temp_dir().join("cache-dir");
    /// let cache = TurboCache::new(path, 1024).unwrap();
    ///
    /// cache.set(b"k1", b"v1").unwrap();
    /// cache.set(b"k2", b"v2").unwrap();
    ///
    /// assert_eq!(cache.total_count().unwrap(), 2);
    /// ```
    pub fn total_count(&self) -> TurboResult<usize> {
        let lock = self.read_lock()?;
        let count = lock.get_insert_count()?;

        Ok(count)
    }

    // Acquire the read lock while mapping a lock poison error into [TurboError]
    fn read_lock(&self) -> Result<std::sync::RwLockReadGuard<'_, Router>, TurboError> {
        Ok(self.router.read()?)
    }

    // Acquire the write lock while mapping a lock poison error into [TurboError]
    fn write_lock(&self) -> Result<std::sync::RwLockWriteGuard<'_, Router>, TurboError> {
        Ok(self.router.write()?)
    }
}

pub struct TurboCacheIter<'a> {
    _guard: std::sync::RwLockReadGuard<'a, Router>,
    iter: RouterIterator,
}

impl<'a> Iterator for TurboCacheIter<'a> {
    type Item = TurboResult<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(pair)) => return Some(Ok(pair)),

            Some(Err(e)) => {
                let err = TurboError::from(e);

                return Some(Err(err));
            }

            None => None,
        }
    }
}

#[cfg(test)]
mod turbocache_tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache(capacity: usize) -> TurboCache {
        let tmp = TempDir::new().unwrap();

        TurboCache::new(tmp.path().to_path_buf(), capacity).unwrap()
    }

    #[test]
    fn insert_and_get() {
        let cache = create_cache(10);
        let key = b"foo".to_vec();
        let value = b"bar".to_vec();

        cache.set(&key, &value).unwrap();
        assert_eq!(cache.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn overwrite_value() {
        let cache = create_cache(10);
        let key = b"k1".to_vec();

        cache.set(&key, b"v1").unwrap();
        cache.set(&key, b"v2").unwrap();

        assert_eq!(cache.get(&key).unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_and_exists() {
        let cache = create_cache(10);
        let key = b"hello".to_vec();
        cache.set(&key, b"world").unwrap();

        assert!(cache.get(&key).unwrap().is_some());

        cache.del(&key).unwrap();

        assert!(cache.get(&key).unwrap().is_none());
        assert_eq!(cache.get(&key).unwrap(), None);
    }

    #[test]
    fn total_count_reflects_inserts_and_deletes() {
        let cache = create_cache(10);

        for i in 0..5 {
            cache.set(&[i], &[i]).unwrap();
        }

        assert_eq!(cache.total_count().unwrap(), 5);

        cache.del(&[0]).unwrap();
        cache.del(&[1]).unwrap();

        assert_eq!(cache.total_count().unwrap(), 3);
    }

    #[test]
    fn get_non_existent_key_returns_none() {
        let cache = create_cache(10);

        assert_eq!(cache.get(b"nope").unwrap(), None);
    }

    #[test]
    fn iterate_over_all_items() {
        let cache = create_cache(10);

        let items = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        for (k, v) in &items {
            cache.set(k, v).unwrap();
        }

        let mut iterated: Vec<_> = cache.iter().unwrap().map(|res| res.unwrap()).collect();

        iterated.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected = items.clone();
        expected.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(iterated, expected);
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn concurrent_sets() {
        let mut threads = vec![];
        let tmp = TempDir::new().unwrap();
        let num_threads = 10;
        let ops_per_thread = 100;

        let cache = TurboCache::new(tmp.path().to_path_buf(), 4096).unwrap();

        for i in 0..num_threads {
            let cache_clone = cache.clone();

            let handle = std::thread::spawn(move || {
                for j in 0..ops_per_thread {
                    let key_val = (i * ops_per_thread + j) as u32;

                    let key = key_val.to_be_bytes().to_vec();
                    let value = key.clone();

                    cache_clone.set(&key, &value).unwrap();
                }
            });

            threads.push(handle);
        }

        for handle in threads {
            handle.join().unwrap();
        }

        // Verify that all the data is present
        let mut total_keys = 0;

        for i in 0..num_threads {
            for j in 0..ops_per_thread {
                let key_val = (i * ops_per_thread + j) as u32;

                let key = key_val.to_be_bytes().to_vec();
                let expected_value = key.clone();

                assert_eq!(cache.get(&key).unwrap(), Some(expected_value));
                total_keys += 1;
            }
        }

        assert_eq!(cache.total_count().unwrap(), total_keys);
    }

    #[test]
    fn concurrent_deletes() {
        let tmp = TempDir::new().unwrap();
        let num_threads = 10;
        let items_per_thread = 100;
        let total_items = num_threads * items_per_thread;

        let cache = TurboCache::new(tmp.path().to_path_buf(), 4096).unwrap();

        // Pre-populate
        for i in 0..total_items {
            let key = (i as u32).to_be_bytes().to_vec();

            cache.set(&key, &key).unwrap();
        }

        assert_eq!(cache.total_count().unwrap(), total_items);

        let mut threads = vec![];

        for i in 0..num_threads {
            let cache_clone = cache.clone();

            let handle = std::thread::spawn(move || {
                for j in 0..items_per_thread {
                    let key_val = i * items_per_thread + j;

                    let key = (key_val as u32).to_be_bytes().to_vec();
                    let deleted = cache_clone.del(&key).unwrap();

                    assert!(deleted.is_some(), "Key should exist before delete");
                }
            });

            threads.push(handle);
        }

        for handle in threads {
            handle.join().unwrap();
        }

        // Verify that all the pairs are deleted
        assert_eq!(cache.total_count().unwrap(), 0);

        for i in 0..total_items {
            let key = (i as u32).to_be_bytes().to_vec();

            assert!(cache.get(&key).unwrap().is_none());
        }
    }

    #[test]
    fn concurrent_reads_and_writes() {
        let tmp = TempDir::new().unwrap();
        let cache = TurboCache::new(tmp.path().to_path_buf(), 128).unwrap();

        // Pre-populate some keys
        for i in 0..10u8 {
            cache.set(&i.to_be_bytes(), &vec![0]).unwrap();
        }

        let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut threads = vec![];

        // Writer threads
        for i in 0..4u8 {
            let cache_clone = cache.clone();
            let stop_signal_clone = stop_signal.clone();

            let handle = std::thread::spawn(move || {
                let mut counter = 1u8;

                while !stop_signal_clone.load(std::sync::atomic::Ordering::SeqCst) {
                    let key_idx = i; // each writer gets its own key

                    cache_clone
                        .set(&key_idx.to_be_bytes(), &vec![counter])
                        .unwrap();

                    counter = counter.wrapping_add(1);

                    if counter == 0 {
                        counter = 1;
                    }

                    std::thread::sleep(Duration::from_millis(5));
                }
            });

            threads.push(handle);
        }

        // Reader threads
        for _ in 0..4 {
            let cache_clone = cache.clone();
            let stop_signal_clone = stop_signal.clone();

            let handle = std::thread::spawn(move || {
                while !stop_signal_clone.load(std::sync::atomic::Ordering::SeqCst) {
                    for i in 0..10u8 {
                        if let Some(value) = cache_clone.get(&i.to_be_bytes()).unwrap() {
                            assert!(!value.is_empty());
                        }
                    }

                    std::thread::sleep(Duration::from_millis(1));
                }
            });

            threads.push(handle);
        }

        std::thread::sleep(Duration::from_secs(1));
        stop_signal.store(true, std::sync::atomic::Ordering::SeqCst);

        for handle in threads {
            handle.join().unwrap();
        }

        // All keys should still exist
        for i in 0..10u8 {
            let val = cache.get(&i.to_be_bytes()).unwrap();

            assert!(val.is_some());
            assert!(!val.unwrap().is_empty());
        }
    }

    #[test]
    fn high_contention_on_single_key() {
        let tmp = TempDir::new().unwrap();
        let cache = TurboCache::new(tmp.path().to_path_buf(), 128).unwrap();

        let key = b"the_one_key".to_vec();
        cache.set(&key, &vec![0]).unwrap();

        let mut threads = vec![];

        for i in 0..8u8 {
            let cache_clone = cache.clone();
            let key_clone = key.clone();

            let handle = std::thread::spawn(move || {
                for j in 1..=10u8 {
                    let value = vec![i * 10 + j];

                    // Half the threads will set, half will delete then set
                    if i % 2 == 0 {
                        cache_clone.set(&key_clone, &value).unwrap();
                    } else {
                        if cache_clone.del(&key_clone).unwrap().is_some() {
                            cache_clone.set(&key_clone, &value).unwrap();
                        }
                    }

                    std::thread::sleep(Duration::from_millis(1));
                }
            });

            threads.push(handle);
        }

        for handle in threads {
            handle.join().unwrap();
        }

        // The final value is unpredictable, but it must exist and not be the initial value.
        let final_value = cache.get(&key).unwrap();

        assert!(final_value.is_some());
        assert_ne!(final_value.unwrap(), vec![0]);
        assert_eq!(cache.total_count().unwrap(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_operations_during_swap() {
        let tmp = TempDir::new().unwrap();
        let mut threads = vec![];

        // Small enough to trigger swaps, big enough to avoid thrashing every single insert.
        let cache = Arc::new(TurboCache::new(tmp.path().to_path_buf(), 8).unwrap());

        for i in 0..4 {
            let cache_clone = cache.clone();

            let handle = std::thread::spawn(move || {
                for j in 0..20 {
                    let key_val = (i * 20 + j) as u16;
                    let key = key_val.to_be_bytes().to_vec();

                    let mut attempt = 0;

                    loop {
                        match cache_clone.set(&key, &vec![i as u8]) {
                            Ok(_) => break,

                            Err(e) => {
                                attempt += 1;

                                eprintln!(
                                    "Thread {} failed to set key {:?} on attempt {}: {:?}",
                                    i, key_val, attempt, e
                                );

                                if attempt >= 5 {
                                    panic!("Failed to insert key {:?} after retries", key_val);
                                }

                                std::thread::sleep(Duration::from_millis(2));
                            }
                        }
                    }

                    std::thread::sleep(Duration::from_millis(1));
                }
            });

            threads.push(handle);
        }

        for handle in threads {
            handle.join().unwrap();
        }

        // Give any pending swap a chance to finalize.
        std::thread::sleep(Duration::from_millis(50));

        let total_count = cache.total_count().unwrap();
        assert_eq!(total_count, 80, "Expected 80 keys, found {}", total_count);

        for i in 0..4 {
            for j in 0..20 {
                let key_val = (i * 20 + j) as u16;
                let key = key_val.to_be_bytes().to_vec();

                let mut attempt = 0;
                let mut _got = None;

                loop {
                    match cache.get(&key) {
                        Ok(val) => {
                            _got = val;
                            break;
                        }

                        Err(e) => {
                            attempt += 1;

                            eprintln!(
                                "Get failed for key {:?} on attempt {}: {:?}",
                                key_val, attempt, e
                            );

                            if attempt >= 5 {
                                panic!("Failed to get key {:?} after retries", key_val);
                            }

                            std::thread::sleep(Duration::from_millis(2));
                        }
                    }
                }

                assert_eq!(
                    _got,
                    Some(vec![i as u8]),
                    "Key {:?} had wrong value: {:?}",
                    key_val,
                    _got
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_iteration_with_modification() {
        let tmp = TempDir::new().unwrap();
        let cache = TurboCache::new(tmp.path().to_path_buf(), 256).unwrap();

        // Pre-populate with known keys
        for i in 0..10u8 {
            cache.set(&vec![i], &vec![i]).unwrap();
        }

        let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut threads = vec![];

        // Modifier thread (adds and removes)
        let modifier_cache = cache.clone();
        let stop_signal_clone = stop_signal.clone();

        let modifier_handle = std::thread::spawn(move || {
            let mut key_gen = 10u8;

            while !stop_signal_clone.load(std::sync::atomic::Ordering::SeqCst) {
                modifier_cache.set(&vec![key_gen], &vec![key_gen]).unwrap();

                // occasionally delete a pre-populated key
                if key_gen % 5 == 0 {
                    modifier_cache.del(&vec![key_gen % 10]).unwrap();
                }

                key_gen = key_gen.wrapping_add(1);

                if key_gen < 10 {
                    key_gen = 10;
                } // stay out of the initial key space

                std::thread::sleep(Duration::from_millis(2));
            }
        });

        threads.push(modifier_handle);

        // Iterator thread
        let iterator_cache = cache.clone();
        let stop_signal_clone = stop_signal.clone();

        let iterator_handle = std::thread::spawn(move || {
            while !stop_signal_clone.load(std::sync::atomic::Ordering::SeqCst) {
                // The iterator should run to completion without panicking.
                // The exact contents are non-deterministic, but it must be a valid state.
                let items: Vec<_> = iterator_cache.iter().unwrap().collect();

                assert!(!items.is_empty()); // Should always have some items

                for item in items {
                    assert!(item.is_ok()); // Each item should be valid
                }

                std::thread::sleep(Duration::from_millis(5));
            }
        });

        threads.push(iterator_handle);

        std::thread::sleep(Duration::from_secs(1));
        stop_signal.store(true, std::sync::atomic::Ordering::SeqCst);

        for handle in threads {
            handle.join().unwrap();
        }
    }
}
