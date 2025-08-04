//! TurboCache is a persistent and efficient embedded KV database.
//!
//! ### Example
//!
//! ```rust
//! use turbocache::{TurboCache, TurboResult};
//!
//! const CACHE_INITIAL_CAP: usize = 1024;
//!
//! fn main() -> TurboResult<()> {
//!     let path = std::env::temp_dir().join("cache-dir");
//!     let mut cache = TurboCache::new(path, CACHE_INITIAL_CAP).unwrap();
//!
//!     for i in 0..5 {
//!         cache.set(vec![i], vec![i * 10]).unwrap();
//!     }
//!
//!     assert_eq!(cache.get(vec![3]).unwrap(), Some(vec![30]));
//!     assert_eq!(cache.del(vec![3]).unwrap(), Some(vec![30]));
//!
//!     Ok(())
//! }
//! ```

mod bucket;
mod core;
mod hash;
mod queue;
mod router;

use core::{InternalError, InternalResult, TurboConfig};
use router::Router;
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

pub use core::{TurboError, TurboResult};

/// TurboCache is a persistent and efficient embedded KV database.
///
/// ### Example
///
/// ```rust
/// use turbocache::{TurboCache, TurboResult};
///
/// const CACHE_INITIAL_CAP: usize = 1024;
///
/// fn main() -> TurboResult<()> {
///     let path = std::env::temp_dir().join("cache-dir");
///     let mut cache = TurboCache::new(path, CACHE_INITIAL_CAP).unwrap();
///
///     for i in 0..5 {
///         cache.set(vec![i], vec![i * 10]).unwrap();
///     }
///
///     assert_eq!(cache.get(vec![3]).unwrap(), Some(vec![30]));
///     assert_eq!(cache.del(vec![3]).unwrap(), Some(vec![30]));
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct TurboCache {
    router: Arc<RwLock<Router>>,
}

impl TurboCache {
    /// Open (or create) a cache rooted at `dirpath`, with the given initial capacity.
    ///
    /// ### Errors
    ///
    /// Returns a `TurboError::Io` if the directory cannot be created or files opened,
    /// or other errors if the on‑disk files are corrupt.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use tempfile::TempDir;
    /// use turbocache::TurboCache;
    ///
    /// let tmp = TempDir::new().unwrap();
    /// let cache = TurboCache::new(tmp.path().to_path_buf(), 32).unwrap();
    /// ```
    pub fn new(dirpath: PathBuf, initial_capacity: usize) -> TurboResult<Self> {
        let config = TurboConfig {
            initial_capacity,
            dirpath,
        };

        let router = Router::new(config)?;

        Ok(Self {
            router: Arc::new(RwLock::new(router)),
        })
    }

    /// Insert or update the given key-value pair.
    ///
    /// **NOTE:** If the key already exists, its old value is overwritten.
    ///
    /// ### Error
    ///
    /// Returns `TurboError` if any error occurs!
    ///
    /// ### Example
    ///
    /// ```rust
    /// use tempfile::TempDir;
    /// use turbocache::TurboCache;
    ///
    /// let tmp = TempDir::new().unwrap();
    /// let mut cache = TurboCache::new(tmp.path().to_path_buf(), 8).unwrap();
    ///
    /// cache.set(b"apple".to_vec(), b"red".to_vec()).unwrap();
    /// assert_eq!(cache.get(b"apple".to_vec()).unwrap(), Some(b"red".to_vec()));
    /// ```
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> TurboResult<()> {
        let mut write_lock = self.write_lock()?;

        Ok(write_lock.set((key, value))?)
    }

    /// Retrieve the current value for `key`, or `None` if it’s not present.
    ///
    /// ### Errors
    ///
    /// Returns `TurboError` if any error occurs!
    ///
    /// ### Example
    ///
    /// ```rust
    /// use tempfile::TempDir;
    /// use turbocache::TurboCache;
    ///
    /// let tmp = TempDir::new().unwrap();
    /// let mut cache = TurboCache::new(tmp.path().to_path_buf(), 8).unwrap();
    ///
    /// cache.set(b"k".to_vec(), b"v".to_vec()).unwrap();
    ///
    /// assert_eq!(cache.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    /// assert!(cache.get(b"missing".to_vec()).unwrap().is_none());
    /// ```
    pub fn get(&self, key: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        let read_lock = self.read_lock()?;

        Ok(read_lock.get(key)?)
    }

    /// Delete and return the value for `key`, or `None` if it wasn’t present.
    ///
    /// ### Errors
    ///
    /// Returns `TurboError` if any error occurs!
    ///
    /// ### Example
    ///
    /// ```rust
    /// use tempfile::TempDir;
    /// use turbocache::TurboCache;
    ///
    /// let tmp = TempDir::new().unwrap();
    /// let mut cache = TurboCache::new(tmp.path().to_path_buf(), 8).unwrap();
    ///
    /// cache.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
    /// let old = cache.del(b"foo".to_vec()).unwrap();
    ///
    /// assert_eq!(old, Some(b"bar".to_vec()));
    /// assert!(cache.get(b"foo".to_vec()).unwrap().is_none());
    /// ```
    pub fn del(&mut self, key: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        let mut write_lock = self.write_lock()?;

        Ok(write_lock.del(key)?)
    }

    /// Iterate over **all** stored key/value pairs in the cache—first the live bucket,
    /// then any in the staging bucket—yielding them one by one.
    ///
    /// The iteration order matches the underlying bucket‑slot scan order and is **not**
    /// guaranteed to correspond to insertion order; if you need a stable order, collect
    /// into a vector and sort it yourself.
    ///
    /// ### Errors
    ///
    /// Returns `TurboError` if any error occurs!
    ///
    /// ### Example
    ///
    /// ```rust
    /// use tempfile::TempDir;
    /// use turbocache::TurboCache;
    /// use std::collections::HashSet;
    ///
    /// let tmp = TempDir::new().unwrap();
    /// let mut cache = TurboCache::new(tmp.path().to_path_buf(), 3).unwrap();
    ///
    /// cache.set(b"x".to_vec(), b"1".to_vec()).unwrap();
    /// cache.set(b"y".to_vec(), b"2".to_vec()).unwrap();
    ///
    /// let got: HashSet<_> = cache.iter().unwrap().map(|r| r.unwrap()).collect();
    /// let want: HashSet<_> = vec![ (b"x".to_vec(), b"1".to_vec()), (b"y".to_vec(), b"2".to_vec()) ]
    ///     .into_iter()
    ///     .collect();
    ///
    /// assert_eq!(got, want);
    /// ```
    pub fn iter(&self) -> TurboResult<impl Iterator<Item = TurboResult<(Vec<u8>, Vec<u8>)>> + '_> {
        let read_lock = self.read_lock()?;
        let collected: Vec<_> = read_lock.iter()?.collect();

        Ok(collected.into_iter().map(|item| item.map_err(|e| e.into())))
    }

    /// Get totale number of items in the db at the given state
    ///
    /// ### Errors
    ///
    /// Returns `TurboError` if any error occurs!
    ///
    /// ### Example
    ///
    /// ```rust
    /// use tempfile::TempDir;
    /// use turbocache::TurboCache;
    ///
    /// let tmp = TempDir::new().unwrap();
    /// let mut cache = TurboCache::new(tmp.path().to_path_buf(), 3).unwrap();
    ///
    /// cache.set(b"x".to_vec(), b"1".to_vec()).unwrap();
    /// cache.set(b"y".to_vec(), b"2".to_vec()).unwrap();
    ///
    /// assert_eq!(cache.get_inserts().unwrap(), 2);
    /// ```
    pub fn get_inserts(&self) -> TurboResult<usize> {
        let read_lock = self.read_lock()?;

        Ok(read_lock.get_inserts()?)
    }

    // Acquire the read lock for [Router] while mapping a poison error into [TurboError]
    fn read_lock(&self) -> InternalResult<std::sync::RwLockReadGuard<'_, Router>> {
        Ok(self
            .router
            .read()
            .map_err(|e| InternalError::LockPoisoned(e.to_string()))?)
    }

    // Acquire the write lock for [Router] while mapping a poison error into [TurboError]
    fn write_lock(&self) -> InternalResult<std::sync::RwLockWriteGuard<'_, Router>> {
        Ok(self
            .router
            .write()
            .map_err(|e| InternalError::LockPoisoned(e.to_string()))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn collect_pairs(cache: &TurboCache) -> HashSet<(Vec<u8>, Vec<u8>)> {
        cache
            .iter()
            .unwrap()
            .map(|res| res.expect("iter error"))
            .collect()
    }

    #[test]
    fn basic_set_get_del() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path().to_path_buf(), 16).unwrap();

        assert!(cache.get(b"foo".to_vec()).unwrap().is_none());

        cache.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert_eq!(cache.get(b"foo".to_vec()).unwrap(), Some(b"bar".to_vec()));

        let deleted = cache.del(b"foo".to_vec()).unwrap();

        assert_eq!(deleted, Some(b"bar".to_vec()));
        assert!(cache.get(b"foo".to_vec()).unwrap().is_none());
    }

    #[test]
    fn iter_on_empty_cache() {
        let tmp = TempDir::new().unwrap();
        let cache = TurboCache::new(tmp.path().to_path_buf(), 8).unwrap();

        // empty cache → iter yields nothing
        assert!(cache.iter().unwrap().next().is_none());
    }

    #[test]
    fn iter_after_simple_inserts() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path().to_path_buf(), 100).unwrap();

        let inputs = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        for pair in &inputs {
            cache.set(pair.0.clone(), pair.1.clone()).unwrap();
        }

        let got: HashSet<_> = collect_pairs(&cache);
        let want: HashSet<_> = inputs.into_iter().collect();

        assert_eq!(got, want);
    }

    #[test]
    fn iter_through_staging_and_swap() {
        let tmp = TempDir::new().unwrap();

        // small capacity so that `threshold = (cap * 4) / 5` floors to 1
        let mut cache = TurboCache::new(tmp.path().to_path_buf(), 2).unwrap();

        // Insert 5 items → forces staging, partial migrations, and at least one final swap
        let all: Vec<_> = (0..5).map(|i| (vec![i], vec![i + 100])).collect();

        for (k, v) in &all {
            cache.set(k.clone(), v.clone()).unwrap();
        }

        // confirm get() sees everything
        for (k, v) in &all {
            assert_eq!(cache.get(k.clone()).unwrap(), Some(v.clone()));
        }

        // confirm iter() sees everything (order-agnostic)
        let got = collect_pairs(&cache);
        let want: HashSet<_> = all.into_iter().collect();

        assert_eq!(got, want);
    }

    #[test]
    fn delete_via_public_api_and_iter() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path().to_path_buf(), 4).unwrap();

        cache.set(b"x".to_vec(), b"10".to_vec()).unwrap();
        cache.set(b"y".to_vec(), b"20".to_vec()).unwrap();
        cache.set(b"z".to_vec(), b"30".to_vec()).unwrap();

        let deleted = cache.del(b"y".to_vec()).unwrap().unwrap();
        assert_eq!(deleted, b"20".to_vec());

        let got = collect_pairs(&cache);
        let want: HashSet<_> = vec![
            (b"x".to_vec(), b"10".to_vec()),
            (b"z".to_vec(), b"30".to_vec()),
        ]
        .into_iter()
        .collect();

        assert_eq!(got, want);
    }

    // #[test]
    // fn persistence_across_reopen() {
    //     let tmp = TempDir::new().unwrap();
    //     let path = tmp.path().to_path_buf();

    //     {
    //         let mut cache1 = TurboCache::new(&path, 8).unwrap();

    //         cache1.set(b"keep".to_vec(), b"alive".to_vec()).unwrap();
    //         cache1.set(b"swap".to_vec(), b"me".to_vec()).unwrap();

    //         for i in 0..10 {
    //             cache1.set(vec![i], vec![i]).unwrap();
    //         }
    //     }

    //     let cache2 = TurboCache::new(&path, 8).unwrap();

    //     assert_eq!(
    //         cache2.get(b"keep".to_vec()).unwrap(),
    //         Some(b"alive".to_vec())
    //     );
    //     assert_eq!(cache2.get(b"swap".to_vec()).unwrap(), Some(b"me".to_vec()));

    //     let got = collect_pairs(&cache2);

    //     assert!(got.contains(&(b"keep".to_vec(), b"alive".to_vec())));
    //     assert!(got.contains(&(b"swap".to_vec(), b"me".to_vec())));
    // }
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
            let mut cache_clone = cache.clone();

            let handle = std::thread::spawn(move || {
                for j in 0..ops_per_thread {
                    let key_val = (i * ops_per_thread + j) as u32;

                    let key = key_val.to_be_bytes().to_vec();
                    let value = key.clone();

                    cache_clone.set(key, value).unwrap();
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

                assert_eq!(cache.get(key).unwrap(), Some(expected_value));
                total_keys += 1;
            }
        }

        assert_eq!(cache.get_inserts().unwrap(), total_keys);
    }

    #[test]
    fn concurrent_deletes() {
        let tmp = TempDir::new().unwrap();
        let num_threads = 10;
        let items_per_thread = 100;
        let total_items = num_threads * items_per_thread;

        let mut cache = TurboCache::new(tmp.path().to_path_buf(), 4096).unwrap();

        // Pre-populate
        for i in 0..total_items {
            let key = (i as u32).to_be_bytes().to_vec();

            cache.set(key.clone(), key.clone()).unwrap();
        }

        assert_eq!(cache.get_inserts().unwrap(), total_items);

        let mut threads = vec![];

        for i in 0..num_threads {
            let mut cache_clone = cache.clone();

            let handle = std::thread::spawn(move || {
                for j in 0..items_per_thread {
                    let key_val = i * items_per_thread + j;

                    let key = (key_val as u32).to_be_bytes().to_vec();
                    let deleted = cache_clone.del(key.clone()).unwrap();

                    assert!(deleted.is_some(), "Key should exist before delete");
                }
            });

            threads.push(handle);
        }

        for handle in threads {
            handle.join().unwrap();
        }

        // Verify that all the pairs are deleted
        assert_eq!(cache.get_inserts().unwrap(), 0);

        for i in 0..total_items {
            let key = (i as u32).to_be_bytes().to_vec();

            assert!(cache.get(key).unwrap().is_none());
        }
    }

    #[test]
    fn concurrent_reads_and_writes() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path().to_path_buf(), 128).unwrap();

        // Pre-populate some keys
        for i in 0..10u8 {
            cache.set(i.to_be_bytes().to_vec(), vec![0]).unwrap();
        }

        let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut threads = vec![];

        // Writer threads
        for i in 0..4u8 {
            let mut cache_clone = cache.clone();
            let stop_signal_clone = stop_signal.clone();

            let handle = std::thread::spawn(move || {
                let mut counter = 1u8;

                while !stop_signal_clone.load(std::sync::atomic::Ordering::SeqCst) {
                    let key_idx = i; // each writer gets its own key

                    cache_clone
                        .set(key_idx.to_be_bytes().to_vec(), vec![counter])
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
                        if let Some(value) = cache_clone.get(i.to_be_bytes().to_vec()).unwrap() {
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
            let val = cache.get(i.to_be_bytes().to_vec()).unwrap();

            assert!(val.is_some());
            assert!(!val.unwrap().is_empty());
        }
    }

    #[test]
    fn high_contention_on_single_key() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path().to_path_buf(), 128).unwrap();

        let key = b"the_one_key".to_vec();
        cache.set(key.clone(), vec![0]).unwrap();

        let mut threads = vec![];

        for i in 0..8u8 {
            let mut cache_clone = cache.clone();
            let key_clone = key.clone();

            let handle = std::thread::spawn(move || {
                for j in 1..=10u8 {
                    let value = vec![i * 10 + j];

                    // Half the threads will set, half will delete then set
                    if i % 2 == 0 {
                        cache_clone.set(key_clone.clone(), value).unwrap();
                    } else {
                        if cache_clone.del(key_clone.clone()).unwrap().is_some() {
                            cache_clone.set(key_clone.clone(), value).unwrap();
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
        let final_value = cache.get(key.clone()).unwrap();

        assert!(final_value.is_some());
        assert_ne!(final_value.unwrap(), vec![0]);
        assert_eq!(cache.get_inserts().unwrap(), 1);
    }

    // #[test]
    // fn concurrent_operations_during_swap() {
    //     let tmp = TempDir::new().unwrap();
    //     let mut threads = vec![];

    //     // A very small capacity to guarantee swaps happen quickly.
    //     // Threshold for staging is (2 * 4) / 5 = 1. So, the 2nd item goes to staging.
    //     let cache = TurboCache::new(tmp.path().to_path_buf(), 2).unwrap();

    //     for i in 0..4 {
    //         let mut cache_clone = cache.clone();

    //         let handle = std::thread::spawn(move || {
    //             for j in 0..20 {
    //                 let key_val = (i * 20 + j) as u16;
    //                 let key = key_val.to_be_bytes().to_vec();

    //                 cache_clone.set(key, vec![i as u8]).unwrap();

    //                 std::thread::sleep(Duration::from_millis(1));
    //             }
    //         });

    //         threads.push(handle);
    //     }

    //     for handle in threads {
    //         handle.join().unwrap();
    //     }

    //     // Verify all keys were inserted correctly despite the chaos of swapping.
    //     assert_eq!(cache.get_inserts().unwrap(), 80);

    //     for i in 0..4 {
    //         for j in 0..20 {
    //             let key_val = (i * 20 + j) as u16;
    //             let key = key_val.to_be_bytes().to_vec();

    //             assert_eq!(cache.get(key).unwrap(), Some(vec![i as u8]));
    //         }
    //     }
    // }

    #[test]
    fn concurrent_iteration_with_modification() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path().to_path_buf(), 256).unwrap();

        // Pre-populate with known keys
        for i in 0..10u8 {
            cache.set(vec![i], vec![i]).unwrap();
        }

        let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut threads = vec![];

        // Modifier thread (adds and removes)
        let mut modifier_cache = cache.clone();
        let stop_signal_clone = stop_signal.clone();

        let modifier_handle = std::thread::spawn(move || {
            let mut key_gen = 10u8;

            while !stop_signal_clone.load(std::sync::atomic::Ordering::SeqCst) {
                modifier_cache.set(vec![key_gen], vec![key_gen]).unwrap();

                // occasionally delete a pre-populated key
                if key_gen % 5 == 0 {
                    modifier_cache.del(vec![key_gen % 10]).unwrap();
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
