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
    n_buffers: u64,
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
    pub(crate) fn write(&self, key: Key, storage_id: u64, n_buffers: u64) -> error::FrozenResult<()> {
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
                                page.meta_row[slot] = Metadata {
                                    storage_id,
                                    key,
                                    n_buffers,
                                };

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
                        page.meta_row[slot] = Metadata {
                            storage_id,
                            key,
                            n_buffers,
                        };
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
    pub(crate) fn read(&self, key: Key) -> error::FrozenResult<Option<(u64, u64)>> {
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
                                let row = &page.meta_row[i];
                                result = Some((row.storage_id, row.n_buffers));
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
    pub(crate) fn delete(&self, key: Key) -> error::FrozenResult<Option<(u64, u64)>> {
        let hash = hash(&key);

        let total = self.mmap.total_slots();
        let start = (hash as usize) % total;

        for probe in 0..total {
            let mut deleted_meta = None;
            let page_idx = (start + probe) % total;

            unsafe {
                self.mmap.write(page_idx, |raw_page| {
                    let page = &mut *raw_page;

                    for i in 0..ITEMS_PER_ROW {
                        match page.hash_row[i] {
                            EMPTY => return,

                            TOMBSTONE => continue,

                            h if h == hash && page.meta_row[i].key == key => {
                                page.hash_row[i] = TOMBSTONE;

                                let meta_row = &page.meta_row[i];
                                deleted_meta = Some((meta_row.storage_id, meta_row.n_buffers));
                                return;
                            }

                            _ => {}
                        }
                    }
                })?;
            }

            if deleted_meta.is_some() {
                return Ok(deleted_meta);
            }
        }

        Ok(None)
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

#[cfg(test)]
mod tests {
    use super::*;

    const INIT_PAGES: usize = 4;
    const FLUSH_DURATION: time::Duration = time::Duration::from_secs(1);

    fn init() -> (tempfile::TempDir, Index) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("index");
        let index = Index::new(path, INIT_PAGES, FLUSH_DURATION).expect("create index");

        (dir, index)
    }

    fn key(id: u8) -> Key {
        [id; 16]
    }

    mod write_read {
        use super::*;

        #[test]
        fn ok_single_entry() {
            let (_dir, index) = init();

            index.write(key(1), 42, 5).unwrap();

            assert_eq!(index.read(key(1)).unwrap(), Some((42, 5)));
        }

        #[test]
        fn ok_multiple_entries() {
            let (_dir, index) = init();

            for i in 0..200u8 {
                index.write(key(i), i as u64, (i % 10) as u64).unwrap();
            }

            for i in 0..200u8 {
                assert_eq!(index.read(key(i)).unwrap(), Some((i as u64, (i % 10) as u64)));
            }
        }

        #[test]
        fn ok_missing_key() {
            let (_dir, index) = init();

            assert_eq!(index.read(key(7)).unwrap(), None);
        }

        #[test]
        fn ok_overwrite_existing() {
            let (_dir, index) = init();

            index.write(key(1), 10, 2).unwrap();
            index.write(key(1), 20, 8).unwrap();

            assert_eq!(index.read(key(1)).unwrap(), Some((20, 8)));
        }
    }

    mod delete {
        use super::*;

        #[test]
        fn ok_delete_existing() {
            let (_dir, index) = init();

            index.write(key(1), 99, 1).unwrap();

            assert_eq!(index.read(key(1)).unwrap(), Some((99, 1)));

            index.delete(key(1)).unwrap();

            assert_eq!(index.read(key(1)).unwrap(), None);
        }

        #[test]
        fn ok_delete_missing() {
            let (_dir, index) = init();

            index.delete(key(1)).unwrap();
            index.delete(key(1)).unwrap();

            assert_eq!(index.read(key(1)).unwrap(), None);
        }

        #[test]
        fn ok_delete_one_preserves_others() {
            let (_dir, index) = init();

            for i in 0..100u8 {
                index.write(key(i), i as u64, 3).unwrap();
            }

            index.delete(key(50)).unwrap();

            for i in 0..100u8 {
                if i == 50 {
                    assert_eq!(index.read(key(i)).unwrap(), None);
                } else {
                    assert_eq!(index.read(key(i)).unwrap(), Some((i as u64, 3)));
                }
            }
        }
    }

    mod tombstones {
        use super::*;

        #[test]
        fn ok_reinsert_deleted_key() {
            let (_dir, index) = init();

            index.write(key(1), 10, 2).unwrap();
            index.delete(key(1)).unwrap();

            assert_eq!(index.read(key(1)).unwrap(), None);

            index.write(key(1), 77, 4).unwrap();

            assert_eq!(index.read(key(1)).unwrap(), Some((77, 4)));
        }

        #[test]
        fn ok_many_delete_reinsert() {
            let (_dir, index) = init();

            for i in 0..100u8 {
                index.write(key(i), i as u64, 1).unwrap();
            }

            for i in 0..100u8 {
                index.delete(key(i)).unwrap();
            }

            for i in 0..100u8 {
                index.write(key(i), (i as u64) + 1000, 5).unwrap();
            }

            for i in 0..100u8 {
                assert_eq!(index.read(key(i)).unwrap(), Some(((i as u64) + 1000, 5)));
            }
        }
    }

    mod stress {
        use super::*;

        #[test]
        fn ok_random_crud() {
            let (_dir, index) = init();

            let mut rng = 0xDEADBEEFCAFEBABEu64;

            #[inline(always)]
            fn rand(state: &mut u64) -> u64 {
                *state ^= *state << 13;
                *state ^= *state >> 7;
                *state ^= *state << 17;
                *state
            }

            let mut expected = std::collections::HashMap::new();

            for _ in 0..10_000 {
                let id = (rand(&mut rng) % 128) as u8;

                match rand(&mut rng) % 3 {
                    0 => {
                        let value = rand(&mut rng);
                        let n_bufs = rand(&mut rng) % 100; // Generate a random buffer count

                        index.write(key(id), value, n_bufs).unwrap();
                        expected.insert(id, (value, n_bufs));
                    }

                    1 => {
                        index.delete(key(id)).unwrap();
                        expected.remove(&id);
                    }

                    _ => {
                        assert_eq!(index.read(key(id)).unwrap(), expected.get(&id).copied());
                    }
                }
            }
        }
    }

    #[test]
    #[should_panic(expected = "capacity exhausted")]
    fn err_capacity_exhausted() {
        let (_dir, index) = init();

        let capacity = INIT_PAGES * ITEMS_PER_ROW;

        for i in 0..capacity {
            let mut k = [0u8; 16];
            k[..8].copy_from_slice(&(i as u64).to_le_bytes());

            index.write(k, i as u64, 1).unwrap();
        }

        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&(capacity as u64).to_le_bytes());

        index.write(k, 0, 0).unwrap();
    }
}
