//! [Patra] (पत्र) is an on disk, append only, custom I/O layer for [Kosh].
//!
//! ## File Contents
//!
//! ▶ Meta => File metadata (version, magic, stats, etc.)
//! ▶ Signs => Fixed sized space to store (u32) signatures of key's of pairs stored
//! ▶ PairBytes => Fixed sized space (u8 * 10) to store Pair offsets (klen, etc.)
//! ▶ Data => Append only space to store raw KV pairs
//!
//! ## On-Disk Layout
//!
//! [ 0 <==> size_of::<Meta>() )
//!    File metadata (signature, version, etc.)
//!
//! [ meta_region <==> meta_region + size_of::<Sign>() * capacity )
//!     Signatures array (4 bytes per slot)
//!     ├─ slot0_sign : Sign
//!     ├─ ...
//!     └─ slot(capacity-1)_sign : Sign
//!     // Values can be EMPTY_SIGN, TOMBSTONE_SIGN, or Sign
//!
//! [ sign_region_end <==> sign_region_end + size_of::<PairBytes>() * capacity )
//!     Pair offsets array (10 bytes per slot)
//!     ├─ slot0_pair : PairBytes  // packed { namespace: 8b | position: 40b | klen: 16b | vlen: 16b }
//!     ├─ ...
//!     └─ slot(capacity-1)_pair : PairBytes
//!
//! [ header_size <==> EOF )
//!     Data region (variable-length)
//!     ├─ Entry0: [ key bytes (klen) ][ value bytes (vlen) ]
//!     ├─ ...
//!     └─ appended sequentially as write_offset grows
//!

use crate::{
    error::{InternalError, InternalResult},
    kosh::{
        meta::{Meta, PairBytes},
        simd::ISA,
    },
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    usize,
};

pub(crate) type Sign = u32;

#[derive(Debug)]
pub(crate) struct Patra {
    meta: Meta,
    mmap: MmapMut,
    file: File,
    isa: ISA,
    stats: Stats,
}

#[derive(Debug)]
struct Stats {
    header_size: usize,
    capacity: usize,
    sign_offset: usize,
    pair_offset: usize,
    threshold: usize,
}

impl Patra {
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        // sanity check
        debug_assert!(capacity % 16 == 0, "Capacity must be multiple of 16");

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let sign_offset = size_of::<Meta>();
        let pair_offset = sign_offset + capacity * size_of::<Sign>();

        let header_size = Self::calc_header_size(capacity);
        let threshold = Self::calc_threshold(capacity);

        // zero-init the file
        file.set_len(header_size as u64)?;

        let mut mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;
        let meta = Meta::new(&mut mmap);

        let isa = ISA::detect_isa();
        let stats = Stats {
            header_size,
            capacity,
            sign_offset,
            pair_offset,
            threshold,
        };

        Ok(Self {
            file,
            mmap,
            meta,
            isa,
            stats,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        // sanity check
        debug_assert!(capacity % 16 == 0, "Capacity must be multiple of 16");

        let file = OpenOptions::new()
            // Create the file just in case to avoid crash
            //
            // NOTE: If we throw IO error for non-existing file it'll be propogated
            // to the users, so we create the file and throw an invalid file error
            // as if the file is new, its not a valid [BucketFile] yet!
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        let sign_offset = size_of::<Meta>();
        let pair_offset = sign_offset + capacity * size_of::<Sign>();

        let header_size = Self::calc_header_size(capacity);
        let threshold = Self::calc_threshold(capacity);

        let file_len = file.metadata()?.len();

        // NOTE: If `file.len()` is smaller then `header_size`, it's a sign of
        // invalid initilization or the file was tampered with! In this scenerio,
        // we delete the file and create it again!
        if file_len < header_size as u64 {
            return Err(InternalError::InvalidFile);
        }

        let mut mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;
        let meta = Meta::new(&mut mmap);

        // NOTE: while validating version and magic of the file, if not matched,
        // we should simply delete the file, as we do not have any earlier
        // versions to support.
        if !meta.is_current_version() {
            return Err(InternalError::InvalidFile);
        }

        // safeguard for the write pointer
        if meta.get_write_pointer() > file_len {
            return Err(InternalError::InvalidFile);
        }

        // safeguard for the insert count
        if meta.get_insert_count() > capacity {
            return Err(InternalError::InvalidFile);
        }

        let isa = ISA::detect_isa();
        let stats = Stats {
            header_size,
            capacity,
            sign_offset,
            pair_offset,
            threshold,
        };

        Ok(Self {
            file,
            mmap,
            meta,
            isa,
            stats,
        })
    }

    /// Calculate the size of header based on given capacity for [Bucket]
    ///
    /// ### Size Calculation
    ///
    /// `sizeof(Meta) + (sizeof(Sign) * CAP) + (sizeof(PairBytes) * CAP)`
    #[inline(always)]
    const fn calc_header_size(capacity: usize) -> usize {
        size_of::<Meta>() + (size_of::<Sign>() * capacity) + (size_of::<PairBytes>() * capacity)
    }

    /// Calculate threshold w/ given capacity for [Bucket]
    ///
    /// NOTE: It's 80% of given capacity
    #[inline(always)]
    const fn calc_threshold(cap: usize) -> usize {
        cap.saturating_mul(4) / 5
    }

    #[inline(always)]
    fn read_pair(&self, idx: usize) -> PairBytes {
        debug_assert!(
            idx < self.stats.capacity,
            "Index must not be bigger then the capacity"
        );

        unsafe {
            let ptr = (self.mmap.as_ptr().add(self.stats.pair_offset) as *const PairBytes).add(idx);
            std::ptr::read(ptr)
        }
    }

    #[inline(always)]
    fn insert_pair(&mut self, idx: usize, pair: PairBytes) {
        debug_assert!(
            idx < self.stats.capacity,
            "Index must not be bigger then the capacity"
        );

        unsafe {
            let ptr =
                (self.mmap.as_mut_ptr().add(self.stats.pair_offset) as *mut PairBytes).add(idx);
            std::ptr::write(ptr, pair);
        }
    }
}
