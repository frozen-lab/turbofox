use crate::{
    bucket::Bucket,
    core::{
        InternalResult, KVPair, TurboConfig, BUCKET_NAME, INDEX_NAME, MAGIC, STAGING_BUCKET_NAME,
        VERSION,
    },
    hash::TurboHasher,
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

#[repr(C, align(8))]
struct Metadata {
    version: u32,
    magic: [u8; 4],
    capacity: AtomicUsize,
    staging_capacity: AtomicUsize,
    threshold: AtomicUsize,
}

struct Index {
    mmap: MmapMut,
}

impl Index {
    const META_SIZE: u64 = size_of::<Metadata>() as u64;

    pub fn new<P: AsRef<Path>>(path: P, cap: usize) -> InternalResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        if file.metadata()?.len() < Self::META_SIZE {
            return Self::create(file, cap);
        }

        let header_mmap = unsafe {
            MmapOptions::new()
                .len(Self::META_SIZE as usize)
                .map_mut(&file)?
        };

        let index = Self { mmap: header_mmap };
        let meta = index.metadata();

        // re-init on invalid file
        if meta.version != VERSION || meta.magic != MAGIC {
            return Self::create(file, cap);
        }

        Ok(index)
    }

    fn create(file: File, cap: usize) -> InternalResult<Self> {
        file.set_len(Self::META_SIZE)?;

        let mut header_mmap = unsafe {
            MmapOptions::new()
                .len(Self::META_SIZE as usize)
                .map_mut(&file)?
        };

        // zeroed values
        header_mmap[..].fill(0u8);

        let index = Self { mmap: header_mmap };

        let meta = index.metadata_mut();

        meta.capacity = AtomicUsize::new(cap);
        meta.staging_capacity = AtomicUsize::new(0);
        meta.threshold = AtomicUsize::new(Index::calc_threshold(cap));

        meta.version = VERSION;
        meta.magic = MAGIC;

        // make sure meta is stored
        index.mmap.flush()?;

        Ok(index)
    }

    /// Returns a mutable reference to [Metadata]
    #[inline(always)]
    fn metadata_mut(&self) -> &mut Metadata {
        unsafe { &mut *(self.mmap.as_ptr() as *mut Metadata) }
    }

    /// Returns an immutable reference to [Metadata]
    #[inline(always)]
    fn metadata(&self) -> &Metadata {
        unsafe { &*(self.mmap.as_ptr() as *const Metadata) }
    }

    #[inline]
    fn get_capacity(&self) -> usize {
        self.metadata().capacity.load(Ordering::Acquire)
    }

    #[inline]
    fn get_staging_capacity(&self) -> usize {
        self.metadata().staging_capacity.load(Ordering::Acquire)
    }

    #[inline]
    fn set_staging_capacity(&self, new_cap: usize) {
        self.metadata_mut()
            .staging_capacity
            .store(new_cap, Ordering::Release);
    }

    #[inline]
    fn get_threshold(&self) -> usize {
        self.metadata().threshold.load(Ordering::Acquire)
    }

    #[inline]
    fn calc_threshold(cap: usize) -> usize {
        cap.saturating_mul(4) / 5
    }

    #[inline]
    fn calc_new_cap(cap: usize) -> usize {
        cap.saturating_mul(2)
    }
}

pub(crate) struct Router {
    config: TurboConfig,
    index: Arc<RwLock<Index>>,
    bucket: Arc<RwLock<Bucket>>,
    staging_bucket: Option<Arc<RwLock<Bucket>>>,

    // Swapping
    swap_mutex: Mutex<()>,
    swap_cvar: Arc<Condvar>,
    swap_flag: Arc<AtomicBool>,
    swap_thread: Option<JoinHandle<()>>,

    // Migration
    mgr_index: Arc<RwLock<AtomicUsize>>,
    mgr_mutex: Mutex<()>,
    mgr_cvar: Arc<Condvar>,
    mgr_flag: Arc<AtomicBool>,
    mgr_thread: Option<JoinHandle<()>>,
}

impl Router {
    pub fn new(config: TurboConfig) -> InternalResult<Self> {
        // make sure the dir exists
        std::fs::create_dir_all(&config.dirpath)?;

        let index_path = config.dirpath.join(INDEX_NAME);
        let index = Index::new(&index_path, config.initial_capacity)?;

        let bucket_path = config.dirpath.join(BUCKET_NAME);
        let bucket = Bucket::new(&bucket_path, index.get_capacity())?;

        let num_entries = bucket.get_inserts()?;
        let threshold = index.get_threshold();

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
            mgr_thread: None,
            swap_thread: None,
            swap_mutex: Mutex::new(()),
            mgr_mutex: Mutex::new(()),
            mgr_cvar: Arc::new(Condvar::new()),
            swap_cvar: Arc::new(Condvar::new()),
            index: Arc::new(RwLock::new(index)),
            bucket: Arc::new(RwLock::new(bucket)),
            mgr_index: Arc::new(RwLock::new(AtomicUsize::new(0))),
            mgr_flag: Arc::new(AtomicBool::new(false)),
            swap_flag: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn set(&mut self, pair: KVPair) -> InternalResult<()> {
        // NOTE: This operation is blocked if either of migration
        // or bucket swap is in progress, this happens w/o any
        // CPU burn

        // blcoking for migration
        let mgr_guard = self.mgr_mutex.lock()?;
        let mg = self
            .mgr_cvar
            .wait_while(mgr_guard, |_| self.mgr_flag.load(Ordering::Acquire))?;
        drop(mg);

        // blcoking for bucket swap
        let swap_guard = self.swap_mutex.lock()?;
        let sg = self
            .swap_cvar
            .wait_while(swap_guard, |_| self.swap_flag.load(Ordering::Acquire))?;
        drop(sg);

        self.internal_set(pair)
    }

    fn internal_set(&mut self, pair: KVPair) -> InternalResult<()> {
        let sign = TurboHasher::new(&pair.0).0;

        // if staging is available, write directly to it
        if let Some(staging) = &self.staging_bucket {
            let live_lock = self.read_lock(&self.bucket)?;
            let staging_lock = self.write_lock(staging)?;

            let live_inserts = live_lock.get_inserts()?;
            let staging_inserts = staging_lock.get_inserts()?;
            let staging_cap = self.read_lock(&self.index)?.get_staging_capacity();

            let staging_threshold = {
                let idx_lock = self.read_lock(&self.index)?;

                Index::calc_threshold(idx_lock.get_staging_capacity())
            };

            // HACK: If for some reasons, we are unable to migrate from live
            // to staging bucket before staging is full, we must wait indefinitely
            // till the migration is DONE!
            //
            // ISSUE: If at any state staging is full, the whole system will be under
            // contention
            //
            // NOTE: This is a blocking operation and waits for "migration thread" to
            // be completely executed to avoid any contention
            if live_inserts + staging_inserts + 1 >= staging_threshold {
                // println!("[live]:{live_inserts}, [staging]: {staging_inserts}, [th]: {staging_threshold}, [cap]: {staging_cap}");

                // spawn therad to migrate pairs batch from live to staging bucket
                // let tx = Self::spawn_migration_thread(
                //     Arc::clone(&self.index),
                //     Arc::clone(&self.bucket),
                //     Arc::clone(self.staging_bucket.as_ref().unwrap()),
                //     Arc::clone(&self.mgr_flag),
                //     Arc::clone(&self.mgr_cvar),
                //     Arc::clone(&self.mgr_index),
                // )?;

                // // block the main thread till the thread is completely executed
                // let _ = tx.join();
            }

            staging_lock.set(pair, sign)?;
            drop(staging_lock);
            drop(live_lock);

            // check if live bucket is empty, if true, trigger the swap
            // otherwise, spawn the migration thread
            if let Ok(b) = &self.bucket.try_read() {
                if b.get_inserts()? == 0 {
                    // &mut self.swap_with_staging()?;
                } else {
                    // spawn therad to migrate pairs batch from live to staging bucket
                    let tx = Self::spawn_migration_thread(
                        Arc::clone(&self.index),
                        Arc::clone(&self.bucket),
                        Arc::clone(self.staging_bucket.as_ref().unwrap()),
                        Arc::clone(&self.mgr_flag),
                        Arc::clone(&self.mgr_cvar),
                        Arc::clone(&self.mgr_index),
                    )?;

                    // store the handle for graceful shutdown
                    self.mgr_thread = Some(tx);
                }
            }

            return Ok(());
        }

        // live bucket insert (only when staging is not available)
        {
            let index_lock = self.write_lock(&self.index)?;
            let bucket_lock = self.write_lock(&self.bucket)?;

            bucket_lock.set(pair, sign)?;

            // NOTE: If we've reached the [threshold], create the staging bucket

            let inserts = bucket_lock.get_inserts()?;
            let threshold = index_lock.get_threshold();

            if inserts >= threshold && self.staging_bucket.is_none() {
                let current_cap = index_lock.get_capacity();
                let new_cap = Index::calc_new_cap(current_cap);

                let staging = Bucket::new(&self.config.dirpath.join(STAGING_BUCKET_NAME), new_cap)?;
                index_lock.set_staging_capacity(new_cap);

                drop(bucket_lock);
                drop(index_lock);

                let staging_arc = Arc::new(RwLock::new(staging));
                self.staging_bucket = Some(staging_arc.clone());
            }

            Ok(())
        }
    }

    pub fn get(&self, kbuf: Vec<u8>) -> InternalResult<Option<Vec<u8>>> {
        // NOTE: This operation is blocked if either of migration
        // or bucket swap is in progress, this happens w/o any
        // CPU burn

        // blcoking for migration
        let mgr_guard = self.mgr_mutex.lock()?;
        let mg = self
            .mgr_cvar
            .wait_while(mgr_guard, |_| self.mgr_flag.load(Ordering::Acquire))?;
        drop(mg);

        // blcoking for bucket swap
        let swap_guard = self.swap_mutex.lock()?;
        let sg = self
            .swap_cvar
            .wait_while(swap_guard, |_| self.swap_flag.load(Ordering::Acquire))?;
        drop(sg);

        //
        // perform the operation
        //

        let sign = TurboHasher::new(&kbuf).0;

        if let Some(bucket) = &self.staging_bucket {
            // spawn therad to migrate pairs batch from live to staging bucket
            //
            // HACK: As we can not mutate the `Self` here, we can not store the
            // thread-handle for graceful closure! Even on graceful shutdown,
            // it'll be inturepted
            let _ = Self::spawn_migration_thread(
                Arc::clone(&self.index),
                Arc::clone(&self.bucket),
                Arc::clone(self.staging_bucket.as_ref().unwrap()),
                Arc::clone(&self.mgr_flag),
                Arc::clone(&self.mgr_cvar),
                Arc::clone(&self.mgr_index),
            )?;

            let bucket_lock = self.read_lock(bucket)?;

            if let Some(val) = bucket_lock.get(kbuf.clone(), sign)? {
                return Ok(Some(val));
            }
        }

        let bucket_lock = self.read_lock(&self.bucket)?;

        bucket_lock.get(kbuf, sign)
    }

    pub fn del(&mut self, kbuf: Vec<u8>) -> InternalResult<Option<Vec<u8>>> {
        // NOTE: This operation is blocked if either of migration
        // or bucket swap is in progress, this happens w/o any
        // CPU burn

        // blcoking for migration
        let mgr_guard = self.mgr_mutex.lock()?;
        let mg = self
            .mgr_cvar
            .wait_while(mgr_guard, |_| self.mgr_flag.load(Ordering::Acquire))?;
        drop(mg);

        // blcoking for bucket swap
        let swap_guard = self.swap_mutex.lock()?;
        let sg = self
            .swap_cvar
            .wait_while(swap_guard, |_| self.swap_flag.load(Ordering::Acquire))?;
        drop(sg);

        //
        // perform the operation
        //

        let sign = TurboHasher::new(&kbuf).0;

        if let Some(bucket_arc) = &self.staging_bucket {
            {
                let bucket_lock = self.write_lock(bucket_arc)?;

                if let Some(val) = bucket_lock.del(kbuf.clone(), sign)? {
                    // if staging has no items left,
                    // ▶ remove the staging bucket
                    if bucket_lock.get_inserts()? == 0 {
                        drop(bucket_lock);
                        self.staging_bucket = None;

                        // close the migration handle
                        if let Some(tx) = self.mgr_thread.take() {
                            let _ = tx.join();
                        }

                        let index_lock = self.write_lock(&self.index)?;
                        let meta = index_lock.metadata_mut();

                        meta.staging_capacity = AtomicUsize::new(0);
                    } else {
                        drop(bucket_lock);

                        // spawn therad to migrate pairs batch from live to staging bucket
                        let tx = Self::spawn_migration_thread(
                            Arc::clone(&self.index),
                            Arc::clone(&self.bucket),
                            Arc::clone(self.staging_bucket.as_ref().unwrap()),
                            Arc::clone(&self.mgr_flag),
                            Arc::clone(&self.mgr_cvar),
                            Arc::clone(&self.mgr_index),
                        )?;

                        // store the handle for graceful shutdown
                        self.mgr_thread = Some(tx);
                    }

                    return Ok(Some(val));
                }
            }
        }

        {
            let bucket_lock = self.write_lock(&self.bucket)?;
            let val = bucket_lock.del(kbuf, sign)?;

            let staging_inserts = if let Some(staging_bucket) = &self.staging_bucket {
                self.read_lock(staging_bucket)?.get_inserts()?
            } else {
                0
            };

            if bucket_lock.get_inserts()? == 0 {
                if staging_inserts > 0 {
                    drop(bucket_lock);

                    // spawn therad to migrate pairs batch from live to staging bucket
                    let tx = Self::spawn_migration_thread(
                        Arc::clone(&self.index),
                        Arc::clone(&self.bucket),
                        Arc::clone(self.staging_bucket.as_ref().unwrap()),
                        Arc::clone(&self.mgr_flag),
                        Arc::clone(&self.mgr_cvar),
                        Arc::clone(&self.mgr_index),
                    )?;

                    // store the handle for graceful shutdown
                    self.mgr_thread = Some(tx);
                } else if self.staging_bucket.is_some() {
                    drop(bucket_lock);

                    // As both live and staging are empty,
                    // ▶ remove the staging bucket
                    self.staging_bucket = None;

                    // close the migration handle
                    if let Some(tx) = self.mgr_thread.take() {
                        let _ = tx.join();
                    }

                    let index_lock = self.write_lock(&self.index)?;
                    let meta = index_lock.metadata_mut();

                    meta.staging_capacity = AtomicUsize::new(0);
                }
            }

            return Ok(val);
        }
    }

    pub fn get_inserts(&self) -> InternalResult<usize> {
        let lock = self.read_lock(&self.bucket)?;
        let mut inserts = lock.get_inserts()?;

        if let Some(bucket) = &self.staging_bucket {
            let lock = self.read_lock(bucket)?;
            inserts += lock.get_inserts()?;
        }

        Ok(inserts)
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

    fn swap_with_staging(&mut self) -> InternalResult<()> {
        struct SwapGuard(Arc<AtomicBool>, Arc<Condvar>);

        impl Drop for SwapGuard {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
                self.1.notify_all();
            }
        }
        // update the swap flag
        self.swap_flag.store(true, Ordering::Release);
        let _guard = SwapGuard(Arc::clone(&self.swap_flag), Arc::clone(&self.swap_cvar));

        let bucket_path = self.config.dirpath.join(BUCKET_NAME);
        let staging_path = self.config.dirpath.join(STAGING_BUCKET_NAME);

        let staging_bucket = self
            .staging_bucket
            .take()
            .expect("swap_in_staging called with no staging_bucket");

        // Acquire a write lock to flush both the bucket's data to disk, ensuring
        // that all pending writes are durable before we rename the file.
        //
        // NOTE: A write lock is required because `Bucket::flush` requires write lock to the
        // underlying [Bucket].
        self.write_lock(&staging_bucket)?.flush()?;
        self.write_lock(&self.bucket)?.flush()?;

        let old_bucket = std::mem::replace(&mut self.bucket, staging_bucket);

        // On Windows, a memory-mapped file generally cannot be deleted or renamed
        // while it is mapped. We must drop the `old_bucket` to unmap its file
        // before proceeding with filesystem operations. The `swap_in_progress`
        // flag prevents other threads from accessing the inconsistent state.
        drop(old_bucket);

        std::fs::remove_file(&bucket_path)?;
        std::fs::rename(&staging_path, &bucket_path)?;

        // Lock the index to safely update metadata.
        let index_lock = self.write_lock(&self.index)?;

        let new_cap = index_lock.get_staging_capacity();
        let new_bucket = Bucket::new(&bucket_path, new_cap)?;
        let meta = index_lock.metadata_mut();

        meta.capacity = AtomicUsize::new(new_cap);
        meta.staging_capacity = AtomicUsize::new(0);
        meta.threshold = AtomicUsize::new(Index::calc_threshold(new_cap));

        // Flush the updated index metadata to disk.
        index_lock.mmap.flush()?;

        drop(index_lock);
        self.bucket = Arc::new(RwLock::new(new_bucket));

        Ok(())
    }

    /// fsync the given file path (must exist).
    fn fsync_file(path: &Path) -> InternalResult<()> {
        let file = File::open(path)?;
        file.sync_all()?;

        Ok(())
    }

    /// fsync the directory containing `path`, so that creations/renames within it persist.
    fn fsync_parent_dir(path: &Path) -> InternalResult<()> {
        let parent = path
            .parent()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no parent"))?;

        let dirf = std::fs::OpenOptions::new().read(true).open(parent)?;
        dirf.sync_all()?;

        Ok(())
    }

    /// Atomically replace `dest` with `src` via a rename, with full fsyncs for crash safety.
    ///
    /// After this returns, `dest` exists with the new contents, or an error is returned
    /// and `dest` is left untouched.
    fn atomic_rename(src: &Path, dest: &Path) -> InternalResult<()> {
        Self::fsync_file(src)?;
        Self::fsync_parent_dir(src)?;

        std::fs::rename(src, dest)?;
        Self::fsync_parent_dir(dest)?;

        Ok(())
    }

    fn spawn_migration_thread(
        index: Arc<RwLock<Index>>,
        live_bucket: Arc<RwLock<Bucket>>,
        staging_bucket: Arc<RwLock<Bucket>>,
        mgr_flag: Arc<AtomicBool>,
        mgr_cvar: Arc<Condvar>,
        mgr_index: Arc<RwLock<AtomicUsize>>,
    ) -> InternalResult<JoinHandle<()>> {
        // a custom mechanism to set the flag when this
        // is dropped w/ solidarity or upon error
        struct MgrGuard(Arc<AtomicBool>, Arc<Condvar>);

        impl Drop for MgrGuard {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
                self.1.notify_all();
            }
        }

        // thread handle
        let handle = thread::Builder::new()
            .name("tc-batch-migrator".into())
            .spawn(move || {
                // update the migration flag
                mgr_flag.store(true, Ordering::Release);
                let _guard = MgrGuard(mgr_flag, mgr_cvar);

                // Compute batch size = `max(1, 25% of capacity)`
                //
                // NOTE: If [full_migration] is requested, then batch is the entire cap
                let _batch_size = match index.read() {
                    Ok(idx) => (idx.get_capacity() / 4).max(1),
                    // unable to obtain the lock
                    Err(_) => return,
                };

                // Try to acquire read locks to both buckets,
                // but back off if contention or after fixed tries
                for _ in 0..5 {
                    match (
                        live_bucket.try_write(),
                        staging_bucket.try_write(),
                        mgr_index.try_write(),
                    ) {
                        (Ok(live), Ok(staged), Ok(idx)) => {
                            let mut cursor = idx.load(Ordering::Acquire);

                            println!("In the MGR");

                            loop {
                                match live.iter_del(&mut cursor) {
                                    Ok(Some((k, v))) => {
                                        let sig = TurboHasher::new(&k).0;

                                        // absorb error if any!
                                        //
                                        // FIXME/HACK: Here we end up skipping the pair,
                                        // so there is potential data loss
                                        let _ = staged.set((k, v), sig);

                                        idx.fetch_add(1, Ordering::SeqCst);
                                    }
                                    // bucket is empty, migration is done
                                    _ => break,
                                }
                            }

                            // done with this batch
                            break;
                        }

                        // couldn’t get the locks, sleep & retry
                        _ => {
                            thread::sleep(Duration::from_millis(20));
                        }
                    }
                }
            })?;

        Ok(handle)
    }
}

impl Drop for Router {
    // graceful shutdown for [Router]
    fn drop(&mut self) {
        if let Some(tx) = self.mgr_thread.take() {
            let _ = tx.join();
        }

        if let Some(tx) = self.swap_thread.take() {
            let _ = tx.join();
        }
    }
}

pub(crate) struct RouterIter<'a> {
    live_guard: RwLockReadGuard<'a, Bucket>,
    staging_guard: Option<RwLockReadGuard<'a, Bucket>>,
    live_remaining: usize,
    staging_remaining: usize,
    live_idx: usize,
    staging_idx: usize,
    state: IterState,
}

enum IterState {
    Live,
    Staging,
    Done,
}

impl super::Router {
    pub fn iter(&self) -> InternalResult<RouterIter<'_>> {
        while self.swap_flag.load(Ordering::Acquire) {
            std::thread::yield_now();
        }

        let live_guard = self.read_lock(&self.bucket)?;
        let staging_guard = match &self.staging_bucket {
            Some(arc) => Some(self.read_lock(arc)?),
            None => None,
        };

        let live_remaining = live_guard.get_inserts()?;
        let staging_remaining = match staging_guard.as_ref() {
            Some(g) => g.get_inserts()?,
            None => 0, // only when there is no staging bucket
        };

        let state = if live_remaining > 0 {
            IterState::Live
        } else if staging_remaining > 0 {
            IterState::Staging
        } else {
            IterState::Done
        };

        Ok(RouterIter {
            live_guard,
            staging_guard,
            live_remaining,
            staging_remaining,
            live_idx: 0,
            staging_idx: 0,
            state,
        })
    }
}

impl<'a> Iterator for RouterIter<'a> {
    type Item = InternalResult<KVPair>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.state {
                IterState::Live => {
                    if self.live_remaining == 0 {
                        self.state = if self.staging_remaining > 0 {
                            IterState::Staging
                        } else {
                            IterState::Done
                        };

                        continue;
                    }

                    match self.live_guard.iter(&mut self.live_idx) {
                        Ok(Some(pair)) => {
                            self.live_remaining -= 1;
                            return Some(Ok(pair));
                        }
                        Ok(None) => {
                            // exhausted early? skip ahead to staging
                            self.live_remaining = 0;

                            continue;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterState::Staging => {
                    let bucket = match &self.staging_guard {
                        Some(b) => b,
                        None => {
                            self.state = IterState::Done;
                            continue;
                        }
                    };

                    if self.staging_remaining == 0 {
                        self.state = IterState::Done;
                        continue;
                    }

                    match bucket.iter(&mut self.staging_idx) {
                        Ok(Some(pair)) => {
                            self.staging_remaining -= 1;
                            return Some(Ok(pair));
                        }
                        Ok(None) => {
                            self.staging_remaining = 0;
                            continue;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterState::Done => return None,
            }
        }
    }
}

#[cfg(test)]
mod iter_tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn make_router(cap: usize) -> (TempDir, Router) {
        let tmp = TempDir::new().expect("tempdir");
        let dir = tmp.path().to_path_buf();

        let config = TurboConfig {
            dirpath: dir,
            initial_capacity: cap,
        };

        let router = Router::new(config).expect("Router::new");

        (tmp, router)
    }

    /// collect all kv pairs from router.iter() into a HashSet
    fn collect_pairs(router: &Router) -> HashSet<(Vec<u8>, Vec<u8>)> {
        router
            .iter()
            .unwrap()
            .map(|res| res.expect("iter error"))
            .collect::<HashSet<_>>()
    }

    #[test]
    fn iter_empty_db_yields_none() {
        let (_tmp, router) = {
            let (tmp, r) = make_router(4);
            (tmp, r)
        };

        // empty => iter().next() == None
        assert!(router.iter().unwrap().next().is_none());
    }

    #[test]
    fn iter_only_live_entries() {
        let (_tmp, mut router) = make_router(4);
        let inputs = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        for pair in &inputs {
            router.set(pair.clone()).unwrap();
        }

        let (_tmp2, mut router2) = make_router(10);

        for pair in &inputs {
            router2.set(pair.clone()).unwrap();
        }

        let got: HashSet<_> = collect_pairs(&router2);
        let want: HashSet<_> = inputs.into_iter().collect();

        assert_eq!(got, want);
    }

    #[test]
    fn iter_live_and_staging_entries() {
        let (_tmp, mut router) = make_router(4);

        // threshold = (4 * 4) / 5 = 3
        // insert 3 => staging appears on the 3rd insert
        let live_pairs = vec![
            (b"x".to_vec(), b"10".to_vec()),
            (b"y".to_vec(), b"11".to_vec()),
            (b"z".to_vec(), b"12".to_vec()),
        ];

        for p in live_pairs.iter().take(2) {
            router.set(p.clone()).unwrap();
        }

        // 3rd insert => staging created
        router.set(live_pairs[2].clone()).unwrap();

        let stag_pairs = vec![
            (b"a".to_vec(), b"20".to_vec()),
            (b"b".to_vec(), b"21".to_vec()),
        ];

        for p in &stag_pairs {
            router.set(p.clone()).unwrap();
        }

        let got = collect_pairs(&router);
        let want: HashSet<_> = live_pairs
            .into_iter()
            .chain(stag_pairs.into_iter())
            .collect();

        assert_eq!(got, want);
    }

    #[test]
    fn iter_after_swap_contains_everything_in_new_live() {
        let (_tmp, mut router) = make_router(3);

        // threshold = (3 * 4) / 5 = 2
        // insert 2 => staging appears on 2nd
        let all_pairs: Vec<_> = (0..5).map(|i| (vec![i], vec![i + 100])).collect();

        for p in all_pairs.iter() {
            router.set(p.clone()).unwrap();
        }

        let got = collect_pairs(&router);
        let want: HashSet<_> = all_pairs.into_iter().collect();

        assert_eq!(router.get_inserts().unwrap(), 5);
        assert_eq!(got, want);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_router(cap: usize) -> (TempDir, Router) {
        let tmp = TempDir::new().expect("tempdir");
        let dir = tmp.path().to_path_buf();

        let config = TurboConfig {
            dirpath: dir,
            initial_capacity: cap,
        };

        let router = Router::new(config).expect("Router::new");

        (tmp, router)
    }

    #[test]
    fn basic_set_get_del() {
        let (_tmp, mut router) = make_router(16);

        let key = b"hello".to_vec();
        let val = b"world".to_vec();

        router.set((key.clone(), val.clone())).unwrap();
        let got = router.get(key.clone()).unwrap().unwrap();
        let deleted = router.del(key.clone()).unwrap().unwrap();

        assert_eq!(got, val);
        assert_eq!(deleted, val);
        assert!(router.get(key).unwrap().is_none());
    }

    // #[test]
    // fn triggers_staging_and_swaps() {
    //     // capacity=4 → threshold = 3
    //     let (_tmp, mut router) = make_router(4);
    //     let inputs: Vec<_> = (0..6).map(|i| (vec![i], vec![i + 100])).collect();

    //     let threshold = router.read_lock(&router.index).unwrap().get_threshold();
    //     assert_eq!(threshold, 3);

    //     for i in 0..(threshold - 1) {
    //         router.set(inputs[i].clone()).unwrap();

    //         assert!(
    //             router.staging_bucket.is_none(),
    //             "#{} should not have staging",
    //             i
    //         );
    //     }

    //     // hitting threshold: staging must appear
    //     router.set(inputs[threshold - 1].clone()).unwrap();
    //     assert!(
    //         router.staging_bucket.is_some(),
    //         "staging must exist once inserts == threshold"
    //     );

    //     let (cap_before, stag_cap) = {
    //         let index_lock = router.read_lock(&router.index).unwrap();
    //         (index_lock.get_capacity(), index_lock.get_staging_capacity())
    //     };

    //     assert_eq!(stag_cap, cap_before * 2, "staging_capacity doubled");

    //     // keep inserting to force migration & final swap
    //     for p in inputs.iter().skip(threshold) {
    //         router.set(p.clone()).unwrap();
    //     }

    //     // now all items (6) should be in the new live bucket
    //     for (k, v) in inputs.into_iter() {
    //         let got = router.get(k.clone()).unwrap().expect("found");

    //         assert_eq!(got, v);
    //     }

    //     assert!(
    //         router.staging_bucket.is_none(),
    //         "staging should be None after swap"
    //     );
    //     assert_eq!(
    //         router.read_lock(&router.index).unwrap().get_capacity(),
    //         stag_cap
    //     );
    // }

    // #[test]
    // fn delete_triggers_swap_when_live_empty() {
    //     // capacity=2, threshold=1 → staging immediately
    //     let (_tmp, mut router) = make_router(2);

    //     // insert 1 → staging
    //     router.set((b"a".to_vec(), b"1".to_vec())).unwrap();
    //     assert!(router.staging_bucket.is_some());

    //     // insert second into staging then delete both
    //     router.set((b"b".to_vec(), b"2".to_vec())).unwrap();
    //     router.del(b"a".to_vec()).unwrap();

    //     // just one entry, under the threshold
    //     assert!(router.staging_bucket.is_none());

    //     // after draining, staging_bucket should be None, capacity reset
    //     let _ = router.del(b"b".to_vec()).unwrap();
    //     assert!(router.staging_bucket.is_none());

    //     // and get returns None
    //     assert!(router.get(b"a".to_vec()).unwrap().is_none());
    //     assert!(router.get(b"b".to_vec()).unwrap().is_none());
    // }

    // #[test]
    // fn persistence_of_index_and_bucket() {
    //     let tmp = TempDir::new().unwrap();
    //     let path = tmp.path().to_path_buf();

    //     {
    //         let config = TurboConfig {
    //             dirpath: path.clone(),
    //             initial_capacity: 8,
    //         };
    //         let mut router = Router::new(config).unwrap();

    //         router.set((b"x".to_vec(), b"100".to_vec())).unwrap();

    //         // force staging
    //         for i in 0..10 {
    //             router.set((vec![i], vec![i + 1])).unwrap();
    //         }

    //         // record the updated capacity
    //         let cap_after = router.read_lock(&router.index).unwrap().get_capacity();

    //         assert!(cap_after > 8);
    //     }

    //     let config = TurboConfig {
    //         dirpath: path,
    //         initial_capacity: 8,
    //     };
    //     let router2 = Router::new(config).unwrap();

    //     // capacity must persist
    //     let cap_persisted = router2.read_lock(&router2.index).unwrap().get_capacity();
    //     assert!(cap_persisted > 8);

    //     // data must still be there
    //     let got = router2.get(b"x".to_vec()).unwrap().unwrap();
    //     assert_eq!(got, b"100".to_vec());
    // }

    #[test]
    fn get_and_del_nonexistent() {
        let (_tmp, mut router) = make_router(8);

        // never inserted
        assert!(router.get(b"nope".to_vec()).unwrap().is_none());
        assert!(router.del(b"nope".to_vec()).unwrap().is_none());

        // still nothing, no staging should appear
        assert!(router.staging_bucket.is_none());
    }

    #[test]
    fn threshold_boundaries() {
        let (_tmp, mut router) = make_router(5);
        let thr = router.read_lock(&router.index).unwrap().get_threshold();

        // (5 * 4) / 5 = 4
        assert_eq!(thr, 4);

        // insert 0..(thr-1), no staging
        for i in 0..(thr - 1) {
            router.set((vec![i as u8], vec![i as u8])).unwrap();

            assert!(
                router.staging_bucket.is_none(),
                "no staging at insert {}",
                i + 1
            );
        }

        // Now do the 4th (thr - 1 = 3) insert, reaching `inserts == threshold`
        router.set((vec![(thr - 1) as u8], vec![0])).unwrap();
        assert!(
            router.staging_bucket.is_some(),
            "staging must appear at insert == threshold"
        );

        // insert more ⇒ staging should still be Some, not re‑created
        let cap_before = router
            .read_lock(&router.index)
            .unwrap()
            .get_staging_capacity();
        router.set((vec![100], vec![100])).unwrap();

        assert_eq!(
            router
                .read_lock(&router.index)
                .unwrap()
                .get_staging_capacity(),
            cap_before
        );
    }

    #[test]
    fn reinit_on_bad_magic() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // first, create a normal router & shut down
        {
            let config = TurboConfig {
                dirpath: path.clone(),
                initial_capacity: 8,
            };
            let mut r = Router::new(config).unwrap();

            r.set((b"x".to_vec(), b"y".to_vec())).unwrap();
        }

        // now overwrite the index file with garbage
        let idx = path.join(INDEX_NAME);
        std::fs::write(&idx, &[0u8; size_of::<Metadata>()]).unwrap();

        // reopening should not panic, and get("x") should now be None

        let config = TurboConfig {
            dirpath: path,
            initial_capacity: 16,
        };
        let r2 = Router::new(config).unwrap();

        assert!(r2.get(b"x".to_vec()).unwrap().is_none());
        assert_eq!(r2.read_lock(&r2.index).unwrap().get_capacity(), 16); // should have the new cap
    }

    #[test]
    fn rapid_delete_insert_cycle() {
        let (_tmp, mut router) = make_router(2);

        // force staging by inserting two, which should also trigger a swap
        router.set((b"a".to_vec(), b"1".to_vec())).unwrap();
        router.set((b"b".to_vec(), b"2".to_vec())).unwrap();
        router.set((b"c".to_vec(), b"3".to_vec())).unwrap();

        // Even After the swap, the staging bucket should be there
        assert!(router.staging_bucket.is_some());

        // delete 3 keys => back to no staging
        router.del(b"a".to_vec()).unwrap();
        router.del(b"b".to_vec()).unwrap();
        router.del(b"c".to_vec()).unwrap();

        assert!(router.staging_bucket.is_none());

        // The new capacity is 4, so the threshold is 3.
        // Insert again => staging should re-appear at threshold
        router.set((b"d".to_vec(), b"4".to_vec())).unwrap();
        router.set((b"e".to_vec(), b"5".to_vec())).unwrap();
        router.set((b"f".to_vec(), b"6".to_vec())).unwrap();

        assert!(router.staging_bucket.is_some());
        assert_eq!(router.get_inserts().unwrap(), 3);
    }

    #[test]
    fn delete_cycle_capacity_is_either_initial_or_doubled_and_invariants_hold() {
        let (_tmp, mut router) = make_router(3);

        let init_cap = router.read_lock(&router.index).unwrap().get_capacity();
        let doubled_cap = crate::router::Index::calc_new_cap(init_cap);

        // force staging into existence:
        // threshold = init_cap * 4/5 (floor), so inserting >= threshold will create staging
        let threshold = router.read_lock(&router.index).unwrap().get_threshold();
        for i in 0..(threshold + 1) {
            let key = vec![i as u8];
            let val = vec![i as u8];

            router.set((key, val)).unwrap();
        }

        assert!(router.staging_bucket.is_some());

        let total_keys = router.get_inserts().unwrap();

        // delete all available keys
        for i in 0..total_keys {
            let key = vec![i as u8];

            router.del(key).unwrap();
        }

        // staging must be gone
        assert!(router.staging_bucket.is_none());

        // final capacity must be either init_cap or doubled_cap
        let final_cap = router.read_lock(&router.index).unwrap().get_capacity();
        assert!(
            final_cap == init_cap || final_cap == doubled_cap,
            "final capacity {} must be one of initial {} or doubled {}",
            final_cap,
            init_cap,
            doubled_cap
        );

        // metadata invariants
        let index_lock = router.read_lock(&router.index).unwrap();
        let meta = index_lock.metadata();

        assert_eq!(meta.staging_capacity.load(Ordering::Acquire), 0);
        assert_eq!(
            meta.threshold.load(Ordering::Acquire),
            crate::router::Index::calc_threshold(final_cap)
        );

        // no keys should remain
        for i in 0..total_keys {
            let key = vec![i as u8];

            assert!(router.get(key).unwrap().is_none());
        }
    }
}
