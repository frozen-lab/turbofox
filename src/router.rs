#![allow(dead_code)]

use crate::{
    bucket::Bucket,
    core::{
        TurboResult, BUCKET_NAME, INDEX_NAME, INITIAL_BUFFER_CAP, MAGIC, STAGING_BUCKET_NAME,
        VERSION,
    },
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
    sync::atomic::AtomicUsize,
};

#[repr(C)]
struct Metadata {
    version: u32,
    magic: [u8; 4],
    capacity: AtomicUsize,
    staging_capacity: AtomicUsize,
    staged_entries: AtomicUsize,
}

struct Index {
    mmap: MmapMut,
}

impl Index {
    const META_SIZE: u64 = size_of::<Metadata>() as u64;

    pub fn new(path: &PathBuf) -> TurboResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        if file.metadata()?.len() < Self::META_SIZE {
            return Self::create(file);
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
            return Self::create(file);
        }

        Ok(index)
    }

    fn create(file: File) -> TurboResult<Self> {
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

        meta.capacity = AtomicUsize::new(INITIAL_BUFFER_CAP);
        meta.staging_capacity = AtomicUsize::new(0);
        meta.staged_entries = AtomicUsize::new(0);
        meta.version = VERSION;
        meta.magic = MAGIC;

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

    fn get_capacity(&self) -> usize {
        self.metadata()
            .capacity
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[inline]
    fn get_threshold(&self) -> usize {
        (self.get_capacity() as f64 * 0.8) as usize
    }
}

pub(crate) struct Router {
    index: Index,
    bucket: Bucket,
    staging_bucket: Option<Bucket>,
}

impl Router {
    pub fn new(dirpath: &PathBuf) -> TurboResult<Self> {
        // make sure the dir exists
        std::fs::create_dir_all(dirpath)?;

        let index_path = dirpath.join(INDEX_NAME);
        let index = Index::new(&index_path)?;

        let bucket_path = dirpath.join(BUCKET_NAME);
        let bucket = Bucket::new(&bucket_path, index.get_capacity())?;

        let num_entries = bucket.get_insertes();
        let threshold = index.get_threshold();

        let staging_bucket: Option<Bucket> = if num_entries >= threshold {
            let bucket_path = dirpath.join(STAGING_BUCKET_NAME);
            let bucket = Bucket::new(&bucket_path, index.get_capacity())?;

            Some(bucket)
        } else {
            None
        };

        Ok(Self {
            index,
            bucket,
            staging_bucket,
        })
    }
}
