use crate::MODULE_ID;
use frozen_core::{error, fmmap};
use std::{path, time};

pub(crate) const ITEMS_PER_ROW: usize = 0x100;

#[repr(C)]
#[derive(Debug)]
struct Page {
    hash_row: [u64; ITEMS_PER_ROW],
    meta_row: [Metadata; ITEMS_PER_ROW],
}

#[repr(C)]
#[derive(Debug)]
struct Metadata {
    storage_id: u64,
    key: [u8; 0x10],
}

#[derive(Debug)]
pub(crate) struct Index {
    mmap: fmmap::FrozenMMap<Page>,
}

impl Index {
    pub(crate) fn new<P: AsRef<path::Path>>(
        path: P,
        init_pages: usize,
        flush_duration: time::Duration,
    ) -> error::FrozenResult<Self> {
        let cfg = fmmap::FrozenMMapCfg {
            flush_duration,
            module_id: MODULE_ID,
            initial_count: init_pages,
            immediate_durability: false,
        };

        let mmap = fmmap::FrozenMMap::<Page>::new(path, cfg)?;
        Ok(Self { mmap })
    }
}
