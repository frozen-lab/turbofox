use crate::{
    bucket::Bucket,
    common::{KVPair, Key, DEFAULT_BUCKET_NAME, INDEX_NAME, STAGING_BUCKET_NAME},
    index::Index,
    types::{InternalConfig, InternalError, InternalResult},
};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
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

        // crash recovery for atomic bucket swap operation
        SwapManager::startup_recover(&config.dirpath)?;

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
        self.mgr.wait_for_migration()?;

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
                SwapManager::perform_bucket_swap(self)?;
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
        self.mgr.wait_for_migration()?;

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
        self.mgr.wait_for_migration()?;

        // do not return early if key is not found in staging
        if let Some(staging) = &self.staging_bucket {
            {
                let write_lock = self.write_lock(staging)?;

                if let Some(val) = write_lock.del(key.clone())? {
                    return Ok(Some(val));
                }
            }
        }

        let write_lock = self.write_lock(&self.live_bucket)?;
        let del_val = write_lock.del(key)?;
        let live_count = write_lock.get_inserted_count()?;

        drop(write_lock);

        // If `live_bucket` is empty, we need to swap the buckts
        //
        // NOTE: This is a blocking operation and will block current operation
        if live_count == 0 {
            SwapManager::perform_bucket_swap(self)?;
        }

        Ok(del_val)
    }

    /// Iterator for [TurboCache]
    ///
    /// NOTE: This operation is blocked by "Migration Thread"
    pub fn iter(&self) -> InternalResult<RouterIterator> {
        self.mgr.wait_for_migration()?;

        let live_bucket = Arc::clone(&self.live_bucket);
        let staging_bucket = match &self.staging_bucket {
            Some(staging) => Some(Arc::clone(staging)),
            None => None,
        };

        let live_remaining = self.read_lock(&self.live_bucket)?.get_inserted_count()?;
        let staging_remaining = match &self.staging_bucket {
            Some(staging) => staging.read()?.get_inserted_count()?,
            None => 0,
        };

        let state = if live_remaining > 0 {
            IterState::Live
        } else if staging_remaining > 0 {
            IterState::Staging
        } else {
            IterState::Done
        };

        Ok(RouterIterator {
            live_bucket,
            staging_bucket,
            live_idx: 0,
            staging_idx: 0,
            state,
        })
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

enum IterState {
    Live,
    Staging,
    Done,
}

pub(crate) struct RouterIterator {
    live_bucket: Arc<RwLock<Bucket>>,
    staging_bucket: Option<Arc<RwLock<Bucket>>>,
    live_idx: usize,
    staging_idx: usize,
    state: IterState,
}

impl Iterator for RouterIterator {
    type Item = InternalResult<KVPair>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.state {
                IterState::Live => match self.live_bucket.read() {
                    Err(e) => return Some(Err(InternalError::LockPoisoned(e.to_string()))),

                    Ok(lock) => match lock.iter(&mut self.live_idx) {
                        Ok(Some(pair)) => return Some(Ok(pair)),

                        Ok(None) => {
                            if self.staging_bucket.is_some() {
                                self.state = IterState::Staging;
                                continue;
                            }

                            self.state = IterState::Done;
                            return None;
                        }

                        Err(e) => return Some(Err(e)),
                    },
                },

                IterState::Staging => match &self.staging_bucket {
                    Some(stg) => match stg.read() {
                        Err(e) => return Some(Err(InternalError::LockPoisoned(e.to_string()))),

                        Ok(lock) => match lock.iter(&mut self.staging_idx) {
                            Ok(Some(pair)) => return Some(Ok(pair)),

                            Ok(None) => {
                                self.state = IterState::Done;
                                return None;
                            }

                            Err(e) => return Some(Err(e)),
                        },
                    },

                    None => {
                        self.state = IterState::Done;
                        return None;
                    }
                },

                IterState::Done => return None,
            }
        }
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

    #[inline]
    fn calc_batch_size(free_spots: usize) -> usize {
        const TIME_PER_OP: usize = 1;
        const MAX_BATCH_SIZE: usize = 200;

        std::cmp::min(free_spots / 2, MAX_BATCH_SIZE / TIME_PER_OP)
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
                    let live_cap = live.get_capacity().unwrap_or(0);
                    let live_inserts = live.get_inserted_count().unwrap_or(0);
                    let free_spots = live_cap - live_inserts;
                    let mut batch_size = Self::calc_batch_size(free_spots);

                    while batch_size > 0 {
                        match live.iter_del() {
                            Ok(Some(pair)) => {
                                let _ = Router::internal_set(&staging_bucket, &pair);
                                batch_size -= 1;
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

    pub fn wait_for_migration(&self) -> InternalResult<()> {
        let guard = self.mutex.lock()?;
        let _sg = self
            .cvar
            .wait_while(guard, |_| self.flag.load(Ordering::Acquire))?;

        Ok(())
    }

    pub fn join(&mut self) {
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for MgrManager {
    fn drop(&mut self) {
        self.join();
    }
}

struct SwapManager;

impl SwapManager {
    fn fsync_dir<P: AsRef<Path>>(dir: &P) -> InternalResult<()> {
        let dir_file = OpenOptions::new().read(true).open(dir)?;
        dir_file.sync_all()?;

        return Ok(());
    }

    fn write_swap_journal<P: AsRef<Path>>(journal_path: &P, contents: &str) -> InternalResult<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(journal_path)?;

        f.write_all(contents.as_bytes())?;
        f.sync_all()?;

        Ok(())
    }

    fn atomic_rename_with_backup<P: AsRef<Path>>(
        dir: &P,
        live: &P,
        staging: &P,
        backup: &std::path::Path,
    ) -> InternalResult<()> {
        // If a previous backup exists, remove it first
        //
        // NOTE: We must also ensure dir entry removal is durable
        // before proceeding
        if backup.exists() {
            fs::remove_file(backup)?;
            Self::fsync_dir(dir)?;
        }

        // [POSIX] Atomic renamem, `live => backup`
        fs::rename(live, backup)?;

        // [POSIX] Atomic renamem, `staging => live`
        fs::rename(staging, live)?;

        Self::fsync_dir(dir)?;
        Ok(())
    }

    fn perform_bucket_swap(router: &mut Router) -> InternalResult<()> {
        // sanity check
        if router.staging_bucket.is_none() {
            return Ok(());
        }

        router.mgr.wait_for_migration()?;

        let bucket_path = router.config.dirpath.join(DEFAULT_BUCKET_NAME);
        let staging_path = router.config.dirpath.join(STAGING_BUCKET_NAME);

        // NOTE: We create a timestamped backup name to reduce risk of accidental
        // overwrites at times of crashes
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let backup_name = format!("{}.bak.{}", DEFAULT_BUCKET_NAME, ts);
        let backup_path = router.config.dirpath.join(backup_name);

        // NOTE: We Take the staging bucket out cause we must swap it w/
        // (in-memory) `live_bucket`
        let staging_bucket = router.staging_bucket.take().unwrap();

        // README: We obtain 'write-locks' and flush/sync both in-mem buckets to disk
        {
            router.write_lock(&staging_bucket)?.flush()?;
            router.write_lock(&router.live_bucket)?.flush()?;
        }

        // HACK: Write a swap journal so startup can recover if we crash mid-swap.
        let journal_path = router.config.dirpath.join("swap_journal");
        let journal_contents = format!(
            r#"{{"op":"swap","live":"{}","staging":"{}","backup":"{}","ts":{}}}"#,
            bucket_path.display(),
            staging_path.display(),
            backup_path.display(),
            ts
        );
        Self::write_swap_journal(&journal_path, &journal_contents)?;

        let old_bucket = std::mem::replace(&mut router.live_bucket, staging_bucket);

        // FIX: For WIN32, a memory-mapped file generally cannot be deleted or renamed while it is
        // mapped. We must drop the `old_bucket` to unmap its file before proceeding with filesystem
        // operations. Read more at -> `https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-deletefile`
        drop(old_bucket);

        // Perform atomic renames on disk,
        //     live => backup, staging => live
        //
        // NOTE: This sequence is safer cause onn failure, we still have
        // the journal describing intent.
        //
        // FIXME: If rename fails, we need an attempt to restore the in-memory state
        // of the [Router] to avoid the inconsistent state.
        Self::atomic_rename_with_backup(
            &router.config.dirpath,
            &bucket_path,
            &staging_path,
            &backup_path,
        )?;

        {
            let live_cap = {
                let live_read = router.read_lock(&router.live_bucket)?;

                live_read.get_capacity()?
            };

            let mut index_lock = router.write_lock(&router.index)?;
            index_lock.set_capacity(live_cap);
            index_lock.set_staging_capacity(0);

            index_lock.flush()?;
        }

        // NOTE: We must remove swap journal and remove the backup after successful swap
        {
            if backup_path.exists() {
                let _ = fs::remove_file(&backup_path);
            }

            if journal_path.exists() {
                let _ = fs::remove_file(&journal_path);
            }
        }

        let _ = Self::fsync_dir(&router.config.dirpath);

        Ok(())
    }

    fn startup_recover<P: AsRef<Path>>(dir: &P) -> InternalResult<()> {
        let journal_path = dir.as_ref().join("swap_journal");
        let live_path = dir.as_ref().join(DEFAULT_BUCKET_NAME);
        let staging_path = dir.as_ref().join(STAGING_BUCKET_NAME);

        // helper function for cleanup
        let cleanup = |journal: &Path, maybe_backup: Option<&Path>| -> InternalResult<()> {
            if let Some(b) = maybe_backup {
                if b.exists() {
                    let _ = fs::remove_file(b);
                }
            }

            if journal.exists() {
                let _ = fs::remove_file(journal);
            }

            let _ = Self::fsync_dir(dir);
            Ok(())
        };

        // NOTE: If journal exists, we use it as the primary source of truth
        if journal_path.exists() {
            let s = fs::read_to_string(&journal_path)?;
            let v: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::Value::Null);

            let j_live = v.get("live").and_then(|x| x.as_str()).map(PathBuf::from);
            let j_staging = v.get("staging").and_then(|x| x.as_str()).map(PathBuf::from);
            let j_backup = v.get("backup").and_then(|x| x.as_str()).map(PathBuf::from);

            let live = j_live.unwrap_or_else(|| live_path.clone());
            let staging = j_staging.unwrap_or_else(|| staging_path.clone());
            let backup = j_backup.as_ref().map(|p| p.clone());

            // ▶ Case 1: Staging missing but live exists
            //
            // This indicates that swap probably completed! So just perform cleanup
            // by removoving `backup + journal`
            if !staging.exists() && live.exists() {
                return cleanup(&journal_path, backup.as_deref());
            }

            // ▶ Case 2: Staging exists but live missing
            //
            // So we try to finish swap by renaming `staging => live`
            if staging.exists() && !live.exists() {
                fs::rename(&staging, &live)?;
                Self::fsync_dir(dir)?;

                // cleanup everything
                return cleanup(&journal_path, backup.as_deref());
            }

            // ▶ Case 3: Both staging & live exist
            if staging.exists() && live.exists() {
                let s_meta = staging.metadata();
                let l_meta = live.metadata();

                if let (Ok(s_meta), Ok(l_meta)) = (s_meta, l_meta) {
                    let s_m = s_meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    let l_m = l_meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);

                    if s_m > l_m {
                        // NOTE: We must prefer staging!
                        //
                        // So we rotate live to backup and put staging as live

                        let ts = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let recovery_backup = dir
                            .as_ref()
                            .join(format!("{}.bak.recover.{}", DEFAULT_BUCKET_NAME, ts));

                        // live => backup
                        fs::rename(&live, &recovery_backup)?;

                        // staging => live
                        fs::rename(&staging, &live)?;

                        // sync + cleanup
                        Self::fsync_dir(dir)?;
                        return cleanup(&journal_path, Some(&recovery_backup));
                    } else {
                        // staging is older, remove stale staging
                        let _ = fs::remove_file(&staging);
                        let _ = Self::fsync_dir(dir);

                        return cleanup(&journal_path, backup.as_deref());
                    }
                }
            }

            // ▶ Case 4: Neither staging nor live exist, but only backup exists
            if !staging.exists() && !live.exists() {
                if let Some(bk) = &backup {
                    if bk.exists() {
                        // restore `backup => live`
                        fs::rename(bk, &live)?;
                        Self::fsync_dir(dir)?;

                        return cleanup(&journal_path, None);
                    }
                }

                // Fallback: Let's try to find newest backup file in directory
                if let Some(newest) = Self::find_newest_backup(dir) {
                    fs::rename(newest, &live)?;
                    Self::fsync_dir(dir)?;

                    return cleanup(&journal_path, None);
                }

                // Nothing to recover — maybe DB was empty, indicates new init.
                // So just cleanup and exit.
                return cleanup(&journal_path, None);
            }

            // ▶ Case 5: This is a generic fallback
            //
            // HACK: Just remove journal and return
            let _ = fs::remove_file(&journal_path);
            let _ = Self::fsync_dir(dir);

            return Ok(());
        }

        // README: So no journal is present.
        //
        // Need to handle some remaining cases conservatively.

        // ▶ Case 6: If live-bucket is missing but there's a backup
        //
        // We restore newest backup => live
        if !live_path.exists() {
            if let Some(newest) = Self::find_newest_backup(dir) {
                fs::rename(&newest, &live_path)?;
                Self::fsync_dir(dir)?;

                return Ok(());
            }

            // NOTE: If staging exists and live missing,
            // we promote staging => live
            if staging_path.exists() {
                fs::rename(&staging_path, &live_path)?;
                Self::fsync_dir(dir)?;
                return Ok(());
            }
        }

        // ▶ Case 7: If both live & staging exist but no journal
        //
        // We choose newer (mtime heuristic)
        if live_path.exists() && staging_path.exists() {
            let s_meta = staging_path.metadata();
            let l_meta = live_path.metadata();

            if let (Ok(s_meta), Ok(l_meta)) = (s_meta, l_meta) {
                let s_m = s_meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                let l_m = l_meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);

                if s_m > l_m {
                    // move `live => backup`, `staging => live`
                    let ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let recovery_backup = dir
                        .as_ref()
                        .join(format!("{}.bak.recover.{}", DEFAULT_BUCKET_NAME, ts));

                    fs::rename(&live_path, &recovery_backup)?;
                    fs::rename(&staging_path, &live_path)?;

                    Self::fsync_dir(dir)?;
                } else {
                    let _ = fs::remove_file(&staging_path);
                    let _ = Self::fsync_dir(dir);
                }
            }
        }

        Ok(())
    }

    fn find_newest_backup<P: AsRef<Path>>(dir: &P) -> Option<PathBuf> {
        let prefix = format!("{}{}", DEFAULT_BUCKET_NAME, ".bak.");
        let mut newest: Option<(PathBuf, SystemTime)> = None;

        if let Ok(rd) = fs::read_dir(dir) {
            for ent in rd.flatten() {
                let p = ent.path();

                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if name.starts_with(&prefix) {
                        if let Ok(meta) = p.metadata() {
                            if let Ok(m) = meta.modified() {
                                match &newest {
                                    Some((_, ref cur_m)) if m > *cur_m => {
                                        newest = Some((p.clone(), m))
                                    }

                                    None => newest = Some((p.clone(), m)),

                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        newest.map(|(p, _)| p)
    }
}

#[cfg(test)]
mod iter_tests {
    use super::*;
    use crate::common::create_temp_dir;
    use std::collections::HashSet;

    fn create_router(cap: usize) -> (Router, tempfile::TempDir) {
        let tmp = create_temp_dir();
        let dir = tmp.path().to_path_buf();
        let config = InternalConfig {
            dirpath: dir,
            initial_capacity: cap,
        };

        let router = Router::new(config).expect("Router::new");

        (router, tmp)
    }

    fn collect_pairs(router: &Router) -> HashSet<(Vec<u8>, Vec<u8>)> {
        router
            .iter()
            .unwrap()
            .map(|res| res.expect("iter error"))
            .collect::<HashSet<_>>()
    }

    #[test]
    fn iter_empty_db_yields_none() {
        let (router, _tmp) = create_router(4);

        assert!(router.iter().unwrap().next().is_none());
    }

    #[test]
    fn iter_only_live_entries() {
        let (mut router, _tmp) = create_router(4);
        let inputs = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        for pair in &inputs {
            router.set(pair.clone()).unwrap();
        }

        let (mut router2, _tmp2) = create_router(10);

        for pair in &inputs {
            router2.set(pair.clone()).unwrap();
        }

        let got: HashSet<_> = collect_pairs(&router2);
        let want: HashSet<_> = inputs.into_iter().collect();

        assert_eq!(got, want);
    }

    #[test]
    fn iter_live_and_staging_entries() {
        let (mut router, _tmp) = create_router(4);

        let live_pairs = vec![
            (b"x".to_vec(), b"10".to_vec()),
            (b"y".to_vec(), b"11".to_vec()),
            (b"z".to_vec(), b"12".to_vec()),
        ];

        for p in live_pairs.iter().take(2) {
            router.set(p.clone()).unwrap();
        }

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
}

#[cfg(test)]
mod router_tests {
    use super::*;
    use crate::common::{create_temp_dir, gen_dataset};

    const CAP: usize = 1024;

    fn create_router(cap: usize) -> (Router, tempfile::TempDir) {
        let tmp = create_temp_dir();
        let dir = tmp.path().to_path_buf();
        let config = InternalConfig {
            dirpath: dir,
            initial_capacity: cap,
        };

        let router = Router::new(config).expect("Router::new");

        (router, tmp)
    }

    #[cfg(unix)]
    #[test]
    fn test_large_load() {
        let db_count = CAP * 5;
        let dataset = gen_dataset(db_count);
        let (mut router, _dir) = create_router(CAP);

        // set all items
        for pair in &dataset {
            router.set(pair.clone()).unwrap();
        }

        assert_eq!(db_count, router.get_insert_count().unwrap());

        // get all items
        for (k, v) in &dataset {
            let val = router.get(k.clone()).unwrap();

            assert_eq!(Some(v.clone()), val);
        }

        // delete all items
        for (k, v) in dataset {
            let val = router.del(k).unwrap();

            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_set_get_del_cycle() {
        let (mut router, _dir) = create_router(16);

        let key = b"hello".to_vec();
        let value = b"world".to_vec();
        let pair = (key.clone(), value.clone());

        router.set(pair).expect("set should succeed");
        let got = router
            .get(key.clone())
            .expect("get result")
            .expect("value present");

        assert_eq!(got, value);
        assert_eq!(router.get_insert_count().unwrap(), 1);

        let deleted = router
            .del(key.clone())
            .expect("del result")
            .expect("deleted value");

        assert_eq!(deleted, b"world".to_vec());
        assert!(router.get(key).unwrap().is_none());
        assert_eq!(router.get_insert_count().unwrap(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn test_bulk_set_and_spot_checks() {
        let db_count = 500usize;
        let dataset = gen_dataset(db_count);
        let (mut router, _dir) = create_router(128);

        for pair in &dataset {
            router.set(pair.clone()).unwrap();
        }

        assert_eq!(db_count, router.get_insert_count().unwrap());

        for i in [0, db_count / 3, db_count - 1].iter() {
            let (k, v) = &dataset[*i];
            let got = router.get(k.clone()).unwrap().expect("value present");

            assert_eq!(&got, v);
        }
    }
    #[test]
    fn test_staging_created_when_live_full_and_rejected_pair_placed_in_staging() {
        let cap = 4usize;
        let dataset = gen_dataset(cap * 2);
        let (mut router, _dir) = create_router(cap);

        for pair in &dataset {
            let _ = router.set(pair.clone());
        }

        let total = router.get_insert_count().expect("get_insert_count");

        assert!(total > 0, "total inserts should be > 0");
        assert!(
            router.staging_bucket.is_some(),
            "staging bucket should be created"
        );
    }

    #[test]
    fn test_get_insert_count_counts_live_and_staging_explicitly() {
        let (mut router, _dir) = create_router(16);
        let dataset_live = gen_dataset(3);

        for p in &dataset_live {
            router.set(p.clone()).unwrap();
        }

        let (staging_bucket, _) =
            Router::create_staging_bucket(&_dir.path().to_path_buf(), 32).unwrap();

        let staging_arc = Arc::new(RwLock::new(staging_bucket));
        {
            let s = staging_arc.clone();
            let pairs = gen_dataset(2);

            for p in &pairs {
                let ok = Router::internal_set(&s, p).expect("internal_set");

                assert!(ok, "staging insert should succeed");
            }
        }

        router.staging_bucket = Some(staging_arc);
        let count = router.get_insert_count().unwrap();

        assert_eq!(count, 3 + 2);
    }

    #[cfg(unix)]
    #[test]
    fn test_perform_bucket_swap_updates_index_and_replaces_live() {
        let (mut router, tmpdir) = create_router(8);
        let (staging_bucket, staging_cap) =
            Router::create_staging_bucket(&tmpdir.path().to_path_buf(), 8).unwrap();
        let staging_arc = Arc::new(RwLock::new(staging_bucket));

        router.staging_bucket = Some(staging_arc);

        {
            let idx_r = router.read_lock(&router.index).unwrap();
            let old_cap = idx_r.get_capacity();

            assert_eq!(old_cap, 8usize);
        }

        SwapManager::perform_bucket_swap(&mut router).expect("perform_bucket_swap");

        assert!(
            router.staging_bucket.is_none(),
            "staging cleared after swap"
        );

        {
            let idx_r = router.read_lock(&router.index).unwrap();
            let n_cap = idx_r.get_capacity();
            let s_cap = idx_r.get_staging_capacity();

            assert_eq!(n_cap, staging_cap);
            assert_eq!(s_cap, 0usize);
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_del_triggers_swap_when_live_becomes_empty_and_staging_exists() {
        let (mut router, tmpdir) = create_router(8);
        let single = gen_dataset(1).into_iter().next().unwrap();
        router.set(single.clone()).unwrap();

        assert_eq!(router.get_insert_count().unwrap(), 1);

        let (staging_bucket, staging_cap) =
            Router::create_staging_bucket(&tmpdir.path().to_path_buf(), 8).unwrap();
        router.staging_bucket = Some(Arc::new(RwLock::new(staging_bucket)));

        let deleted = router.del(single.0.clone()).unwrap();

        assert!(deleted.is_some(), "value should be returned by del");
        assert!(
            router.staging_bucket.is_none(),
            "staging cleared after swap"
        );

        {
            let idx_r = router.read_lock(&router.index).unwrap();

            assert_eq!(idx_r.get_staging_capacity(), 0usize);
            assert_eq!(idx_r.get_capacity(), staging_cap);
        }
    }

    #[test]
    fn test_internal_set_respects_bucket_full_and_returns_false() {
        let (mut router, _dir) = create_router(4);
        let dataset = gen_dataset(4);

        for p in &dataset {
            let ok = router.set(p.clone());

            assert!(ok.is_ok());
        }

        let extra = gen_dataset(1).into_iter().next().unwrap();
        let ok = Router::internal_set(&router.live_bucket, &extra).expect("internal_set result Ok");

        assert!(!ok, "internal_set should return false when bucket is full");
    }

    #[test]
    fn test_create_staging_bucket_has_expected_new_capacity() {
        let (router, tmpdir) = create_router(16);

        let cur_cap = {
            let idx_r = router.read_lock(&router.index).unwrap();
            idx_r.get_capacity()
        };

        let (_staging, new_cap) =
            Router::create_staging_bucket(&tmpdir.path().to_path_buf(), cur_cap).unwrap();

        let expected = Index::calc_new_cap(cur_cap);

        assert_eq!(new_cap, expected);
    }
}

#[cfg(test)]
mod router_concurrency_tests {
    use super::*;
    use crate::common::{create_temp_dir, gen_dataset};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    const CAP: usize = 1024;

    fn create_router(cap: usize) -> (Arc<RwLock<Router>>, tempfile::TempDir) {
        let tmp = create_temp_dir();
        let dir = tmp.path().to_path_buf();
        let config = InternalConfig {
            dirpath: dir,
            initial_capacity: cap,
        };

        let router = {
            let r = Router::new(config).expect("Router::new");

            Arc::new(RwLock::new(r))
        };

        (router, tmp)
    }

    #[cfg(unix)]
    #[test]
    fn test_concurrent_writes_and_reads() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 256;
        let total = THREADS * PER_THREAD;

        let (router, _tmp) = create_router(CAP);
        let router = Arc::clone(&router);

        let mut handles = Vec::new();

        // NOTE: Many writers concurrently inserting non-overlapping keys while multiple readers
        // concurrently probe for presence. After all writers finish we assert total count.

        // Writers: each thread writes PER_THREAD unique pairs
        for t in 0..THREADS {
            let router = Arc::clone(&router);
            let base = t * PER_THREAD;

            let pairs: Vec<_> = (0..PER_THREAD)
                .map(|i| {
                    let (_k, v) = gen_dataset(1).into_iter().next().unwrap();

                    (format!("t{}_{}", t, base + i).into_bytes(), v)
                })
                .collect();

            handles.push(thread::spawn(move || {
                for chunk in pairs.chunks(16) {
                    let mut r = router.write().expect("router write");

                    for pair in chunk {
                        r.set(pair.clone()).expect("set");
                    }

                    drop(r);
                }
            }));
        }

        let stop = Arc::new(AtomicUsize::new(0));
        let mut reader_handles = Vec::new();

        for _ in 0..4 {
            let router = Arc::clone(&router);
            let stop = Arc::clone(&stop);

            reader_handles.push(thread::spawn(move || {
                let start = Instant::now();

                while start.elapsed() < Duration::from_millis(1000)
                    && stop.load(Ordering::Acquire) == 0
                {
                    for t in 0..THREADS {
                        let key = format!("t{}_{}", t, 0).into_bytes();
                        let r = router.read().expect("router read");
                        let _ = r.get(key).expect("get should not error");

                        drop(r);
                    }

                    thread::sleep(Duration::from_millis(10));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        stop.store(1, Ordering::Release);

        for h in reader_handles {
            h.join().unwrap();
        }

        let r = router.read().expect("router read");
        let cnt = r.get_insert_count().expect("get_insert_count");

        assert_eq!(cnt, total, "expected {} entries, got {}", total, cnt);
    }

    #[cfg(unix)]
    #[test]
    fn test_concurrent_set_triggers_migration_and_no_data_loss() {
        let small_cap = 128usize;
        let (router, _tmp) = create_router(small_cap);
        let router = Arc::clone(&router);

        const THREADS: usize = 4;
        const PER_THREAD: usize = 200;
        let total = THREADS * PER_THREAD;

        // NOTE: Concurrent insertions that cause staging creation and migration.
        // Multiple threads fill the DB past threshold; after joins we wait until all items are visible.

        let mut handles = Vec::new();

        for t in 0..THREADS {
            let router = Arc::clone(&router);

            handles.push(thread::spawn(move || {
                let mut pairs = Vec::with_capacity(PER_THREAD);

                for i in 0..PER_THREAD {
                    let key = format!("thr{}_{}", t, i).into_bytes();
                    let val = format!("val{}_{}", t, i).into_bytes();

                    pairs.push((key, val));
                }

                for chunk in pairs.chunks(8) {
                    let mut r = router.write().expect("router write");

                    for p in chunk {
                        r.set(p.clone()).expect("set");
                    }

                    drop(r);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // wait up to 2s for the system to finish background migration and reach stable count
        let deadline = Instant::now() + Duration::from_millis(2000);

        loop {
            {
                let r = router.read().expect("router read");
                let cnt = r.get_insert_count().expect("get_insert_count");

                if cnt == total {
                    break;
                }
            }

            if Instant::now() > deadline {
                panic!(
                    "timeout waiting for all items to appear (expected {}, partial visible)",
                    total
                );
            }

            thread::sleep(Duration::from_millis(20));
        }

        let r = router.read().expect("router read");

        for t in 0..THREADS {
            let k = format!("thr{}_{}", t, 0).into_bytes();

            assert!(r.get(k).expect("get ok").is_some());
        }
    }

    #[test]
    fn test_concurrent_set_and_delete_mixed_workload() {
        let (router, _tmp) = create_router(512);
        let router = Arc::clone(&router);

        // NOTE: Mixed concurrent sets and deletes: multiple writers are inserting while
        // other threads deleting some keys.
        // Ensures no panics and final cardinality is as expected.

        {
            let mut r = router.write().unwrap();
            let base = gen_dataset(200);

            for p in base {
                r.set(p).unwrap();
            }
        }

        let w_router = Arc::clone(&router);

        let writer = thread::spawn(move || {
            for i in 0..200 {
                let pair = (
                    format!("w_{}", i).into_bytes(),
                    format!("wv_{}", i).into_bytes(),
                );
                let mut r = w_router.write().unwrap();

                r.set(pair).unwrap();
            }
        });

        let mut deleters = Vec::new();

        for i in 0..4 {
            let d_router = Arc::clone(&router);

            deleters.push(thread::spawn(move || {
                for j in 0..50 {
                    let key = format!("d{}_{}", i, j).into_bytes();
                    let mut r = d_router.write().unwrap();
                    let _ = r.del(key);
                }
            }));
        }

        writer.join().unwrap();

        for d in deleters {
            d.join().unwrap();
        }

        let r = router.read().unwrap();
        let cnt = r.get_insert_count().unwrap();

        assert!(cnt <= 400, "count should be <= 400; got {}", cnt);
    }
}
