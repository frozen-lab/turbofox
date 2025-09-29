use crate::{
    error::InternalResult,
    kosh::{
        meta::{Meta, PairBytes, Sign},
        simd::ISA,
    },
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
};

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
    fn new<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let header_size = Self::calc_header_size(capacity);
        let sign_offset = size_of::<Meta>();
        let pair_offset = sign_offset + capacity * size_of::<Sign>();
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

    /// Calculate the size of header based on given capacity for [Bucket]
    ///
    /// ### Size Calculation
    ///
    /// `sizeof(Meta) + (sizeof(Sign) * CAP) + (sizeof(PairRaw) * CAP)`
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
}
