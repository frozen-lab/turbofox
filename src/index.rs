use crate::MODULE_ID;
use frozen_core::{error, fmmap};
use std::{path, time};

pub(crate) type Key = [u8; 0x10];

const SEED: u64 = 0xDEADC0DEDEADC0DE;
const EMPTY: u64 = 0;
const TOMBSTONE: u64 = 1;

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

    #[inline(always)]
    pub(crate) fn write(&self, key: Key, storage_id: u64) -> error::FrozenResult<()> {
        let hash = hash(&key);

        let total = self.mmap.total_slots();
        let start = (hash as usize) % total;

        for probe in 0..total {
            let page_idx = (start + probe) % total;

            let mut inserted = false;
            let mut first_tombstone = None;

            unsafe {
                self.mmap.write(page_idx, |raw_page| {
                    let page = &mut *raw_page;

                    for i in 0..ITEMS_PER_ROW {
                        match page.hash_row[i] {
                            EMPTY => {
                                let slot = first_tombstone.unwrap_or(i);

                                page.hash_row[slot] = hash;
                                page.meta_row[slot] = Metadata { storage_id, key };

                                inserted = true;
                                return;
                            }

                            TOMBSTONE => {
                                if first_tombstone.is_none() {
                                    first_tombstone = Some(i);
                                }
                            }

                            h if h == hash && page.meta_row[i].key == key => {
                                page.meta_row[i].storage_id = storage_id;
                                inserted = true;
                                return;
                            }

                            _ => {}
                        }
                    }

                    if let Some(slot) = first_tombstone.take() {
                        page.hash_row[slot] = hash;
                        page.meta_row[slot] = Metadata { storage_id, key };
                        inserted = true;
                    }
                })?;
            }

            if inserted {
                return Ok(());
            }
        }

        panic!("capacity exhausted");
    }

    #[inline(always)]
    pub(crate) fn read(&self, key: Key) -> error::FrozenResult<Option<u64>> {
        let hash = hash(&key);

        let total = self.mmap.total_slots();
        let start = (hash as usize) % total;

        for probe in 0..total {
            let page_idx = (start + probe) % total;
            let mut result = None;

            unsafe {
                self.mmap.read(page_idx, |raw_page| {
                    let page = &*raw_page;

                    for i in 0..ITEMS_PER_ROW {
                        match page.hash_row[i] {
                            EMPTY => return,

                            TOMBSTONE => continue,

                            h if h == hash && page.meta_row[i].key == key => {
                                result = Some(page.meta_row[i].storage_id);
                                return;
                            }

                            _ => {}
                        }
                    }
                });
            }

            if result.is_some() {
                return Ok(result);
            }
        }

        Ok(None)
    }

    #[inline(always)]
    pub(crate) fn delete(&self, key: Key) -> error::FrozenResult<()> {
        let hash = hash(&key);

        let total = self.mmap.total_slots();
        let start = (hash as usize) % total;

        for probe in 0..total {
            let page_idx = (start + probe) % total;
            let mut deleted = false;

            unsafe {
                self.mmap.write(page_idx, |raw_page| {
                    let page = &mut *raw_page;

                    for i in 0..ITEMS_PER_ROW {
                        match page.hash_row[i] {
                            EMPTY => return,

                            TOMBSTONE => continue,

                            h if h == hash && page.meta_row[i].key == key => {
                                page.hash_row[i] = TOMBSTONE;
                                deleted = true;
                                return;
                            }

                            _ => {}
                        }
                    }
                })?;
            }

            if deleted {
                return Ok(());
            }
        }

        Ok(())
    }
}

#[inline(always)]
fn hash(key: &Key) -> u64 {
    let hash = twox_hash::XxHash64::oneshot(SEED, key);

    match hash {
        EMPTY | TOMBSTONE => 2,
        hash => hash,
    }
}
