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
    fn update_staged_entries(&self, value: usize) {
        self.metadata()
            .staged_entries
            .fetch_add(value, Ordering::Release);
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

            loop {
                if let Some(item) = self.bucket.iter(&mut start)? {
                    let new_sign = TurboHasher::new(&item.0).0;
                    bucket.set(item, new_sign)?;

                    staged_items += 1;
                    continue;
                }

                if staged_items >= 8 {
                    break;
                }
            }

            // update staged items len
            self.index.update_staged_entries(staged_items);

            if self.bucket.get_inserts() == 0 {
                // need to swap the buckets, del the bucket and assign staging
                // bucket to it, after a rename
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
        }

        Ok(())
    }

    pub fn get(&self, kbuf: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        let sign = TurboHasher::new(&kbuf).0;

        if let Some(bucket) = &self.staging_bucket {
            return bucket.get(kbuf, sign);
        }

        self.bucket.get(kbuf, sign)
    }

    pub fn del(&mut self, kbuf: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        let sign = TurboHasher::new(&kbuf).0;

        if let Some(bucket) = &mut self.staging_bucket {
            let val = bucket.del(kbuf, sign);

            // remove staging bucket if No item is remaining
            if bucket.get_inserts() == 0 {
                self.staging_bucket = None;

                let meta = self.index.metadata_mut();

                meta.staging_capacity = AtomicUsize::new(0);
                meta.staged_entries = AtomicUsize::new(0);
            }

            return val;
        }

        let val = self.bucket.del(kbuf, sign);

        if self.bucket.get_inserts() == 0 {
            // need to swap the buckets, del the bucket and assign staging
            // bucket to it, after a rename
        }

        return val;
    }
}
