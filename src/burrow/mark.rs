use crate::{
    errors::InternalResult,
    linux::{file::File, mmap::MMap},
    TurboConfig,
};

const VERSION: u32 = 0x01;
const MAGIC: [u8; 0x04] = *b"mrk1";
const PATH: &'static str = "mark";

//
// Meta
//

#[derive(Debug, Copy, Clone)]
#[repr(C, align(0x20))]
struct Meta {
    magic: [u8; 0x04],
    version: u32,
    num_rows: u32,
    _padd_one: [u8; 0x04],
    num_items: u64,
    _padd_two: [u8; 0x08],
}

impl Meta {
    #[inline]
    const fn new(num_rows: u32, num_items: u64) -> Self {
        Self {
            num_rows,
            num_items,
            magic: MAGIC,
            version: VERSION,
            _padd_one: [0x00; 0x04],
            _padd_two: [0x00; 0x08],
        }
    }

    #[inline]
    const fn incr_num_rows(&mut self, added_count: usize) {
        self.num_rows += added_count as u32;
    }

    #[inline]
    const fn get_num_rows(&self) -> usize {
        self.num_rows as usize
    }
}

const META_SIZE: usize = std::mem::size_of::<Meta>();

// sanity checks
const _: () = assert!(META_SIZE == 0x20, "META must be of 32 bytes!");

//
// Rows
//

const ITEMS_PER_ROW: usize = 0x10;

#[repr(C)]
struct Offsets {
    trail_idx: u32,
    vbuf_slots: u16,
    klen: u16,
    vlen: u16,
    flag: u8,
    _padd: u8,
}

#[repr(C, align(0x40))]
struct Row {
    signs: [u32; ITEMS_PER_ROW],
    offsets: [Offsets; ITEMS_PER_ROW],
}

const ROW_SIZE: usize = std::mem::size_of::<Row>();

// Sanity checks
const _: () = assert!(ROW_SIZE == 0x100, "Row must be of 256 bytes");
const _: () = assert!(std::mem::size_of::<Offsets>() == 0x0C);
const _: () = assert!(std::mem::size_of::<Row>() % (0x04 + 0x0C) == 0x00);

//
// Mark
//

#[derive(Debug)]
pub(super) struct Mark {
    file: File,
    mmap: MMap,
    cfg: TurboConfig,
    rows_ptr: *mut Row,
    meta_ptr: *mut Meta,
}

impl Mark {}
