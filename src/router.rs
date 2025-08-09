use crate::{
    bucket::Bucket,
    constants::{KVPair, Key, DEFAULT_BUCKET_NAME, INDEX_NAME, STAGING_BUCKET_NAME},
    index::Index,
    types::{InternalConfig, InternalError, InternalResult},
};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
    thread::{self, JoinHandle},
};

pub(crate) struct Router {
    config: InternalConfig,
    index: Arc<RwLock<Index>>,
    live_bucket: Arc<RwLock<Bucket>>,
    staging_bucket: Option<Arc<RwLock<Bucket>>>,
    mgr: MgrManager,
}

impl Router {
    pub fn new(config: InternalConfig) -> InternalResult<Self> {
        // make sure the dir exists
        std::fs::create_dir_all(&config.dirpath)?;

        let index_path = config.dirpath.join(INDEX_NAME);
        let index = Index::open(&index_path, config.initial_capacity)?;

        let bucket_path = config.dirpath.join(DEFAULT_BUCKET_NAME);
        let live_bucket = Bucket::new(&bucket_path, index.get_capacity())?;

        let num_entries = live_bucket.get_inserted_count()?;
        let threshold = live_bucket.get_threshold()?;

        let staging_bucket: Option<Arc<RwLock<Bucket>>> = if num_entries >= threshold {
            let bucket_path = config.dirpath.join(STAGING_BUCKET_NAME);
            let bucket = Bucket::new(&bucket_path, index.get_staging_capacity())?;

            Some(Arc::new(RwLock::new(bucket)))
        } else {
            None
        };

        Ok(Self {
            config,
            staging_bucket,
            index: Arc::new(RwLock::new(index)),
            live_bucket: Arc::new(RwLock::new(live_bucket)),
            mgr: MgrManager::new(),
        })
    }

    /// Get count of all the pairs from [TurboCache]
    pub fn get_insert_count(&self) -> InternalResult<usize> {
        let mut count = self.read_lock(&self.live_bucket)?.get_inserted_count()?;

        if let Some(staging) = &self.staging_bucket {
            count += self.read_lock(&staging)?.get_inserted_count()?;
        }

        Ok(count)
    }

    /// Insert a new [KVPair]
    ///
    /// NOTE: This operation is blocked if either of migration
    /// or bucket swap is in progress, this happens w/o any
    /// CPU burn
    pub fn set(&mut self, pair: KVPair) -> InternalResult<()> {
        // blcoking for migration
        let mgr_guard = self.mgr.mutex.lock()?;
        let mg = self
            .mgr
            .cvar
            .wait_while(mgr_guard, |_| self.mgr.flag.load(Ordering::Acquire))?;
        drop(mg);

        // If `live_bucket` is empty, we need to swap the buckts
        //
        // NOTE: This is a blocking operation and will block current operation
        {
            let lock = self.live_bucket.write()?;
            let count = lock.get_inserted_count()?;
            drop(lock);

            if count == 0 {
                self.perform_bucket_swap()?;
            }
        }

        // README: If staging is available, insert directly into it
        if let Some(staging) = &self.staging_bucket {
            let res = match Self::internal_set(staging, &pair)? {
                true => Ok(()),
                // NOTE: In theory this should never happen
                false => Err(InternalError::BucketFull),
            };

            let live_bucket = Arc::clone(&self.live_bucket);
            let staging_bucket = Arc::clone(staging);
            self.mgr.spawn(live_bucket, staging_bucket)?;

            return res;
        }

        let try_insert = Self::internal_set(&self.live_bucket, &pair)?;

        // this states that, underlying bucket has reached its cap,
        // so we must create staging bucket
        if !try_insert {
            //
            // Create new staging bucket
            //

            let index_lock = self.write_lock(&self.index)?;
            let current_cap = index_lock.get_capacity();
            let (staging, new_cap) =
                Self::create_staging_bucket(&self.config.dirpath, current_cap)?;
            let staging_bucket = Arc::new(RwLock::new(staging));

            //
            // update index
            //

            index_lock.set_staging_capacity(new_cap);
            drop(index_lock);

            //
            // set rejected pair into staging
            //

            // NOTE: In theory this will never happen, but I think
            // it's good to state it for easier control flow understanding
            if !Self::internal_set(&staging_bucket, &pair)? {
                return Err(InternalError::BucketFull);
            }

            self.staging_bucket = Some(staging_bucket);
        }

        Ok(())
    }

    /// Fetch a value from [TurboCache]
    ///
    /// NOTE: This operation is only blocked for migration to take place
    pub fn get(&self, key: Key) -> InternalResult<Option<Vec<u8>>> {
        // blcoking for migration
        let mgr_guard = self.mgr.mutex.lock()?;
        let mg = self
            .mgr
            .cvar
            .wait_while(mgr_guard, |_| self.mgr.flag.load(Ordering::Acquire))?;
        drop(mg);

        if let Some(staging) = &self.staging_bucket {
            let read_lock = self.read_lock(staging)?;

            return read_lock.get(key);
        }

        self.read_lock(&self.live_bucket)?.get(key)
    }

    /// Delete a [KvPair] from [TurboCache]
    ///
    /// NOTE: This operation is blocked for both the migration thread and
    /// bucket swapping
    pub fn del(&mut self, key: Key) -> InternalResult<Option<Vec<u8>>> {
        // blcoking for migration
        let mgr_guard = self.mgr.mutex.lock()?;
        let mg = self
            .mgr
            .cvar
            .wait_while(mgr_guard, |_| self.mgr.flag.load(Ordering::Acquire))?;
        drop(mg);

        if let Some(staging) = &self.staging_bucket {
            let write_lock = self.write_lock(staging)?;

            return write_lock.del(key);
        }

        let write_lock = self.write_lock(&self.live_bucket)?;
        let del_val = write_lock.del(key)?;
        let live_count = write_lock.get_inserted_count()?;

        drop(write_lock);

        // If `live_bucket` is empty, we need to swap the buckts
        //
        // NOTE: This is a blocking operation and will block current operation
        if live_count == 0 {
            self.perform_bucket_swap()?;
        }

        Ok(del_val)
    }

    fn internal_set(bucket: &Arc<RwLock<Bucket>>, pair: &KVPair) -> InternalResult<bool> {
        let write_lock = bucket.write()?;

        return write_lock.set(pair);
    }

    fn create_staging_bucket<P: AsRef<Path>>(
        dirpath: P,
        cap: usize,
    ) -> InternalResult<(Bucket, usize)> {
        let path = dirpath.as_ref().join(STAGING_BUCKET_NAME);
        let new_cap = Index::calc_new_cap(cap);
        let bucket = Bucket::new(path, new_cap)?;

        Ok((bucket, new_cap))
    }

    fn perform_bucket_swap(&mut self) -> InternalResult<()> {
        // Wait until any migration finishes
        let mgr_guard = self.mgr.mutex.lock()?;
        let mg = self
            .mgr
            .cvar
            .wait_while(mgr_guard, |_| self.mgr.flag.load(Ordering::Acquire))?;
        drop(mg);

        // sanity check
        if self.staging_bucket.is_none() {
            return Ok(());
        }

        let bucket_path = self.config.dirpath.join(DEFAULT_BUCKET_NAME);
        let staging_path = self.config.dirpath.join(STAGING_BUCKET_NAME);
        let staging_bucket = self.staging_bucket.take().unwrap();

        // Acquire a write lock to flush both the bucket's data to disk, ensuring
        // that all pending writes are durable before we rename the file.
        //
        // NOTE: A write lock is required because `Bucket::flush` requires write lock to the
        // underlying [Bucket].
        self.write_lock(&staging_bucket)?.flush()?;
        self.write_lock(&self.live_bucket)?.flush()?;

        let old_bucket = std::mem::replace(&mut self.live_bucket, staging_bucket);

        // On Windows, a memory-mapped file generally cannot be deleted or renamed
        // while it is mapped. We must drop the `old_bucket` to unmap its file
        // before proceeding with filesystem operations. The `swap_in_progress`
        // flag prevents other threads from accessing the inconsistent state.
        drop(old_bucket);

        std::fs::remove_file(&bucket_path)?;
        std::fs::rename(&staging_path, &bucket_path)?;

        // Lock the index to safely update metadata.
        let mut index_lock = self.write_lock(&self.index)?;

        let new_cap = index_lock.get_staging_capacity();
        let new_bucket = Bucket::new(&bucket_path, new_cap)?;
        let meta = index_lock.metadata_mut();

        // update metadata
        meta.capacity = AtomicUsize::new(new_cap);
        meta.staging_capacity = AtomicUsize::new(0);

        // Flush the updated index metadata to disk.
        index_lock.flush()?;
        drop(index_lock);

        self.live_bucket = Arc::new(RwLock::new(new_bucket));

        Ok(())
    }

    /// Acquire a read‑lock on `Arc<RwLock<T>>`, mapping poison error => InternalError.
    fn read_lock<'a, T>(
        &'a self,
        lk: &'a Arc<RwLock<T>>,
    ) -> InternalResult<RwLockReadGuard<'a, T>> {
        Ok(lk.read()?)
    }

    /// Acquire a write‑lock on `Arc<RwLock<T>>`, mapping poison error => InternalError.
    fn write_lock<'a, T>(
        &'a self,
        lk: &'a Arc<RwLock<T>>,
    ) -> InternalResult<RwLockWriteGuard<'a, T>> {
        Ok(lk.write()?)
    }
}

struct MgrManager {
    mutex: Mutex<()>,
    cvar: Arc<Condvar>,
    flag: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MgrManager {
    fn new() -> Self {
        Self {
            mutex: Mutex::new(()),
            cvar: Arc::new(Condvar::new()),
            flag: Arc::new(AtomicBool::new(false)),
            thread: None,
        }
    }

    fn spawn(
        &mut self,
        live_bucket: Arc<RwLock<Bucket>>,
        staging_bucket: Arc<RwLock<Bucket>>,
    ) -> InternalResult<()> {
        // a custom mechanism to set the flag when this
        // is dropped w/ solidarity or upon error
        struct MgrGuard(Arc<AtomicBool>, Arc<Condvar>);

        impl Drop for MgrGuard {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
                self.1.notify_all();
            }
        }

        // Acquire the mutex to synchronize with `Router::set()` waiters.
        // This ensures we flip the flag before any further `Router::set()`
        // calls proceed
        let guard = self.mutex.lock()?;

        // mark migration in-progress so other callers block at the top
        // of `Router::set()`.
        self.flag.store(true, Ordering::Release);

        let flag = Arc::clone(&self.flag);
        let cvar = Arc::clone(&self.cvar);

        // thread handle
        let handle = thread::Builder::new()
            .name("tc-batch-migrator".into())
            .spawn(move || {
                let _guard = MgrGuard(flag, cvar);

                // migrate pairs from live -> staging
                if let Ok(live) = live_bucket.write() {
                    loop {
                        match live.iter_del() {
                            Ok(Some(pair)) => {
                                let _ = Router::internal_set(&staging_bucket, &pair);
                                continue;
                            }
                            // migration is done
                            Ok(None) => break,
                            // README: Even if this fails, we can retry migration
                            // at next `Router::set()` operation
                            Err(_) => break,
                        }
                    }
                }
            })?;

        // release the mutex now, which allows waiting `Router::set()`
        // calls to see `flag == true`
        drop(guard);

        // store the handler to join w/ main thread for graceful shutdown
        // of the system
        self.thread = Some(handle);

        Ok(())
    }
}

impl Drop for MgrManager {
    fn drop(&mut self) {
        if let Some(tx) = self.thread.take() {
            let _ = tx.join();
        }
    }
}

#[cfg(test)]
mod router_tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::TempDir;

    const KEY_LEN: usize = 32;
    const VAL_LEN: usize = 128;
    const SEED: u64 = 42;
    const CAP: usize = 1024;

    fn gen_dataset(size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rng = StdRng::seed_from_u64(SEED);

        (0..size)
            .map(|_| {
                let key = (0..KEY_LEN).map(|_| rng.random()).collect();
                let val = (0..VAL_LEN).map(|_| rng.random()).collect();

                (key, val)
            })
            .collect()
    }

    fn create_router(cap: usize) -> (Router, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let dir = tmp.path().to_path_buf();
        let config = InternalConfig {
            dirpath: dir,
            initial_capacity: cap,
        };

        let router = Router::new(config).expect("Router::new");

        (router, tmp)
    }

    #[test]
    fn test_large_set_operation() {
        let db_count = CAP * 5;
        let dataset = gen_dataset(db_count);
        let (mut router, _dir) = create_router(CAP);

        // set all items
        for pair in dataset {
            router.set(pair).unwrap();
        }

        assert_eq!(db_count, router.get_insert_count().unwrap());
    }

    #[test]
    fn test_concurrency_of_set_operation() {
        let mut threads = vec![];
        let num_threads = 10;
        let ops_per_thread = 100;

        let (router, _dir) = create_router(CAP);
        let router_arc = Arc::new(RwLock::new(router));

        for i in 0..num_threads {
            let router_clone = Arc::clone(&router_arc);

            let handle = std::thread::spawn(move || {
                for j in 0..ops_per_thread {
                    let key_val = (i * ops_per_thread + j) as u32;

                    let key = key_val.to_be_bytes().to_vec();
                    let value = key.clone();

                    match router_clone.write().unwrap().set((key, value)) {
                        Ok(_) => {}
                        Err(e) => panic!("Error {:?}", e),
                    }
                }
            });

            threads.push(handle);
        }

        for handle in threads {
            handle.join().unwrap();
        }

        let pairs_count = router_arc.write().unwrap().get_insert_count().unwrap();
        let total_pairs = ops_per_thread * num_threads;

        assert_eq!(pairs_count, total_pairs);
    }
}
