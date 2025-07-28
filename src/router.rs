#![allow(dead_code)]

use crate::{
    bucket::Bucket,
    core::{KVPair, TurboResult, BUCKET_NAME, INDEX_NAME, MAGIC, STAGING_BUCKET_NAME, VERSION},
    hash::TurboHasher,
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

#[repr(C, align(8))]
struct Metadata {
    version: u32,
    magic: [u8; 4],
    capacity: AtomicUsize,
    staging_capacity: AtomicUsize,
    staged_entries: AtomicUsize,
    threshold: AtomicUsize,
}

struct Index {
    mmap: MmapMut,
}

impl Index {
    const META_SIZE: u64 = size_of::<Metadata>() as u64;

    pub fn new<P: AsRef<Path>>(path: P, cap: usize) -> TurboResult<Self> {
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

    fn create(file: File, cap: usize) -> TurboResult<Self> {
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
        meta.staged_entries = AtomicUsize::new(0);
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
    fn get_staged_entries(&self) -> usize {
        self.metadata().staged_entries.load(Ordering::Acquire)
    }

    #[inline]
    fn update_staged_entries(&self, value: usize) -> usize {
        self.metadata()
            .staged_entries
            .fetch_add(value, Ordering::Release)
    }

    #[inline]
    fn get_staging_capacity(&self) -> usize {
        self.metadata().staging_capacity.load(Ordering::Acquire)
    }

    #[inline]
    fn set_capacity(&self, new_cap: usize) {
        self.metadata_mut()
            .capacity
            .store(new_cap, Ordering::Release);
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
    fn set_threshold(&self, new_cap: usize) {
        let t = Self::calc_threshold(new_cap);

        self.metadata_mut().threshold.store(t, Ordering::Release);
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

pub(crate) struct Router<P: AsRef<Path>> {
    dirpath: P,
    index: Index,
    bucket: Bucket,
    staging_bucket: Option<Bucket>,
}

impl<P: AsRef<Path>> Router<P> {
    pub fn new(dirpath: P, cap: usize) -> TurboResult<Self> {
        // make sure the dir exists
        std::fs::create_dir_all(dirpath.as_ref())?;

        let index_path = dirpath.as_ref().join(INDEX_NAME);
        let index = Index::new(&index_path, cap)?;

        let bucket_path = dirpath.as_ref().join(BUCKET_NAME);
        let bucket = Bucket::new(&bucket_path, index.get_capacity())?;

        let num_entries = bucket.get_inserts();
        let threshold = index.get_threshold();

        let staging_bucket: Option<Bucket> = if num_entries >= threshold {
            let bucket_path = dirpath.as_ref().join(STAGING_BUCKET_NAME);
            let bucket = Bucket::new(&bucket_path, index.get_staging_capacity())?;

            Some(bucket)
        } else {
            None
        };

        Ok(Self {
            dirpath,
            index,
            bucket,
            staging_bucket,
        })
    }

    pub fn set(&mut self, pair: KVPair) -> TurboResult<()> {
        let sign = TurboHasher::new(&pair.0).0;

        if let Some(bucket) = &mut self.staging_bucket {
            bucket.set(pair, sign)?;

            // incremental migration from bucket to staging bucket
            let mut staged_items: usize = 0;
            let mut start = self.index.get_staged_entries();

            let current_cap = self.index.get_capacity();
            let current_staged = self.index.get_staged_entries();
            let current_inserts = self.bucket.get_inserts();

            for _ in 0..(current_cap / 4) {
                if let Some(item) = self.bucket.iter_del(&mut start)? {
                    let new_sign = TurboHasher::new(&item.0).0;
                    bucket.set(item, new_sign)?;

                    staged_items += 1;
                    continue;
                }

                if current_staged + staged_items >= current_inserts {
                    break;
                }
            }

            // update staged items len
            self.index.update_staged_entries(staged_items);

            if self.bucket.get_inserts() == 0 {
                self.swap_with_staging()?;
            }

            return Ok(());
        }

        self.bucket.set(pair, sign)?;

        let inserts = self.bucket.get_inserts();
        let threshold = self.index.get_threshold();

        // if we reach [threshold], then create a staging bucket
        if inserts >= threshold {
            let new_cap = Index::calc_new_cap(self.index.get_capacity());

            let bucket_path = self.dirpath.as_ref().join(STAGING_BUCKET_NAME);
            let bucket = Bucket::new(&bucket_path, new_cap)?;

            self.staging_bucket = Some(bucket);
            self.index.set_staging_capacity(new_cap);
        }

        Ok(())
    }

    pub fn get(&self, kbuf: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        let sign = TurboHasher::new(&kbuf).0;

        if let Some(bucket) = &self.staging_bucket {
            if let Some(val) = bucket.get(kbuf.clone(), sign)? {
                return Ok(Some(val));
            }
        }

        self.bucket.get(kbuf, sign)
    }

    pub fn del(&mut self, kbuf: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        let sign = TurboHasher::new(&kbuf).0;

        if let Some(bucket) = &mut self.staging_bucket {
            if let Some(val) = bucket.del(kbuf.clone(), sign)? {
                // remove staging bucket if No item is remaining
                if bucket.get_inserts() == 0 {
                    self.staging_bucket = None;

                    let meta = self.index.metadata_mut();

                    meta.staging_capacity = AtomicUsize::new(0);
                    meta.staged_entries = AtomicUsize::new(0);
                }

                return Ok(Some(val));
            }
        }

        let val = self.bucket.del(kbuf, sign);

        if self.bucket.get_inserts() == 0 && self.staging_bucket.is_some() {
            self.swap_with_staging()?;
        }

        return val;
    }

    fn swap_with_staging(&mut self) -> TurboResult<()> {
        let bucket_path = self.dirpath.as_ref().join(BUCKET_NAME);
        let staging_path = self.dirpath.as_ref().join(STAGING_BUCKET_NAME);

        let staging_bucket = self
            .staging_bucket
            .take()
            .expect("swap_in_staging called with no staging_bucket");

        let old_bucket = std::mem::replace(&mut self.bucket, staging_bucket);
        drop(old_bucket);

        std::fs::remove_file(&bucket_path)?;
        std::fs::rename(&staging_path, &bucket_path)?;

        let new_cap = self.index.get_staging_capacity();
        let new_bucket = Bucket::new(&bucket_path, new_cap)?;
        let meta = self.index.metadata_mut();

        self.bucket = new_bucket;

        meta.staged_entries = AtomicUsize::new(0);
        meta.capacity = AtomicUsize::new(new_cap);
        meta.staging_capacity = AtomicUsize::new(0);
        meta.threshold = AtomicUsize::new(Index::calc_threshold(new_cap));

        self.index.mmap.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_router(cap: usize) -> (TempDir, Router<std::path::PathBuf>) {
        let tmp = TempDir::new().expect("tempdir");
        let dir = tmp.path().to_path_buf();
        let router = Router::new(dir, cap).expect("Router::new");

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

    #[test]
    fn triggers_staging_and_swaps() {
        // capacity=4 → threshold = 3
        let (_tmp, mut router) = make_router(4);
        let inputs: Vec<_> = (0..6).map(|i| (vec![i], vec![i + 100])).collect();

        let threshold = router.index.get_threshold();
        assert_eq!(threshold, 3);

        for i in 0..(threshold - 1) {
            router.set(inputs[i].clone()).unwrap();

            assert!(
                router.staging_bucket.is_none(),
                "#{} should not have staging",
                i
            );
        }

        // hitting threshold: staging must appear
        router.set(inputs[threshold - 1].clone()).unwrap();
        assert!(
            router.staging_bucket.is_some(),
            "staging must exist once inserts == threshold"
        );

        let cap_before = router.index.get_capacity();
        let stag_cap = router.index.get_staging_capacity();

        assert_eq!(stag_cap, cap_before * 2, "staging_capacity doubled");

        // keep inserting to force migration & final swap
        for p in inputs.iter().skip(threshold) {
            router.set(p.clone()).unwrap();
        }

        // now all items (6) should be in the new live bucket
        for (k, v) in inputs.into_iter() {
            let got = router.get(k.clone()).unwrap().expect("found");

            assert_eq!(got, v);
        }

        assert!(
            router.staging_bucket.is_none(),
            "staging should be None after swap"
        );
        assert_eq!(router.index.get_capacity(), stag_cap);
    }

    #[test]
    fn delete_triggers_swap_when_live_empty() {
        // capacity=2, threshold=1 → staging immediately
        let (_tmp, mut router) = make_router(2);

        // insert 1 → staging
        router.set((b"a".to_vec(), b"1".to_vec())).unwrap();
        assert!(router.staging_bucket.is_some());

        // insert second into staging then delete both
        router.set((b"b".to_vec(), b"2".to_vec())).unwrap();
        router.del(b"a".to_vec()).unwrap();

        // just one entry, under the threshold
        assert!(router.staging_bucket.is_none());

        // after draining, staging_bucket should be None, capacity reset
        let _ = router.del(b"b".to_vec()).unwrap();
        assert!(router.staging_bucket.is_none());

        // and get returns None
        assert!(router.get(b"a".to_vec()).unwrap().is_none());
        assert!(router.get(b"b".to_vec()).unwrap().is_none());
    }

    #[test]
    fn persistence_of_index_and_bucket() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let mut router = Router::new(&path, 8).unwrap();

            router.set((b"x".to_vec(), b"100".to_vec())).unwrap();

            // force staging
            for i in 0..10 {
                router.set((vec![i], vec![i + 1])).unwrap();
            }

            // record the updated capacity
            let cap_after = router.index.get_capacity();

            assert!(cap_after > 8);
        }

        let router2 = Router::new(&path, 8).unwrap();

        // capacity must persist
        let cap_persisted = router2.index.get_capacity();
        assert!(cap_persisted > 8);

        // data must still be there
        let got = router2.get(b"x".to_vec()).unwrap().unwrap();
        assert_eq!(got, b"100".to_vec());
    }
}
