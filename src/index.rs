use crate::{
    constants::{MAGIC, VERSION},
    types::InternalResult,
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::OpenOptions,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

#[repr(C, align(8))]
pub(crate) struct Metadata {
    version: u32,
    magic: [u8; 4],
    capacity: AtomicUsize,
    staging_capacity: AtomicUsize,
}

pub(crate) struct Index {
    mmap: MmapMut,
}

impl Index {
    const META_SIZE: u64 = size_of::<Metadata>() as u64;

    pub fn open<P: AsRef<Path>>(path: P, cap: usize) -> InternalResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let is_new = file.metadata()?.len() < Self::META_SIZE;

        if is_new {
            file.set_len(Self::META_SIZE)?;
        }

        let mut header_mmap = unsafe {
            MmapOptions::new()
                .len(Self::META_SIZE as usize)
                .map_mut(&file)?
        };

        if is_new {
            // zeroed values
            header_mmap[..].fill(0u8);
        }

        let index = Self { mmap: header_mmap };
        let meta = index.metadata_mut();

        if is_new {
            meta.version = VERSION;
            meta.magic = MAGIC;

            meta.capacity = AtomicUsize::new(cap);
            meta.staging_capacity = AtomicUsize::new(0);
        }

        // TODO: Take action if [MAGIC] or [VERSION] does not match

        Ok(index)
    }

    /// Returns a mutable reference to [Metadata]
    #[inline(always)]
    pub fn metadata_mut(&self) -> &mut Metadata {
        unsafe { &mut *(self.mmap.as_ptr() as *mut Metadata) }
    }

    /// Returns an immutable reference to [Metadata]
    #[inline(always)]
    pub fn metadata(&self) -> &Metadata {
        unsafe { &*(self.mmap.as_ptr() as *const Metadata) }
    }

    #[inline]
    pub fn get_capacity(&self) -> usize {
        self.metadata().capacity.load(Ordering::Acquire)
    }

    #[inline]
    pub fn get_staging_capacity(&self) -> usize {
        self.metadata().staging_capacity.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_staging_capacity(&self, new_cap: usize) {
        self.metadata_mut()
            .staging_capacity
            .store(new_cap, Ordering::Release);
    }

    #[inline]
    pub fn calc_new_cap(cap: usize) -> usize {
        cap.saturating_mul(2)
    }
}

#[cfg(test)]
mod index_tests {
    use super::*;
    use tempfile::NamedTempFile;

    const TEST_CAP: usize = 16;

    fn create_index_file() -> (NamedTempFile, Index) {
        let tmp = NamedTempFile::new().expect("failed to create temp file");
        let idx = Index::open(tmp.path(), TEST_CAP).expect("failed to open index");

        (tmp, idx)
    }

    #[test]
    fn new_file_sets_metadata_correctly() {
        let (_tmp, idx) = create_index_file();
        let meta = idx.metadata();

        assert_eq!(meta.version, VERSION, "version should be set");
        assert_eq!(meta.magic, MAGIC, "magic should be set");
        assert_eq!(
            meta.capacity.load(Ordering::Relaxed),
            TEST_CAP,
            "capacity should match initial"
        );
        assert_eq!(
            meta.staging_capacity.load(Ordering::Relaxed),
            0,
            "staging capacity should start at 0"
        );
    }

    #[test]
    fn get_and_set_staging_capacity() {
        let (_tmp, idx) = create_index_file();

        assert_eq!(idx.get_staging_capacity(), 0);

        idx.set_staging_capacity(42);
        assert_eq!(idx.get_staging_capacity(), 42);

        idx.set_staging_capacity(100);
        assert_eq!(idx.get_staging_capacity(), 100);
    }

    #[test]
    fn get_capacity_returns_initial_value() {
        let (_tmp, idx) = create_index_file();

        assert_eq!(idx.get_capacity(), TEST_CAP);
    }

    #[test]
    fn calc_new_cap_doubles_value() {
        assert_eq!(Index::calc_new_cap(4), 8);
        assert_eq!(Index::calc_new_cap(0), 0); // saturating_mul should keep it 0
    }

    #[test]
    fn existing_file_is_not_reinitialized() {
        let tmp = NamedTempFile::new().expect("create temp file");

        {
            let idx = Index::open(tmp.path(), TEST_CAP).expect("open");
            idx.set_staging_capacity(77);

            assert_eq!(idx.get_staging_capacity(), 77);
        }

        let reopened = Index::open(tmp.path(), TEST_CAP).expect("reopen");
        assert_eq!(reopened.get_staging_capacity(), 77);
    }

    #[test]
    fn metadata_persists_to_disk() {
        let tmp = NamedTempFile::new().expect("create temp file");

        {
            let idx = Index::open(tmp.path(), TEST_CAP).expect("open");
            idx.set_staging_capacity(999);
        }

        let reopened = Index::open(tmp.path(), TEST_CAP).expect("reopen");

        assert_eq!(
            reopened.get_staging_capacity(),
            999,
            "staging capacity should persist after reopen"
        );
    }
}
