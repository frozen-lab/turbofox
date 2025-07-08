#![allow(dead_code)]

use memmap::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    io,
    mem::size_of,
    path::PathBuf,
    sync::atomic::{AtomicU16, AtomicU32, Ordering},
};

/// Current version of TurboCache shards
pub(crate) const VERSION: u8 = 0;

/// Versioned MAGIC value to help identify shards specific to TurboCache
pub(crate) const MAGIC: [u8; 8] = *b"TURBOv0\0";

pub(crate) const TABLE_SIZE: usize = 512 * 64;
pub(crate) const TABLE_THRESHOLD: usize = (TABLE_SIZE as f64 * 0.9) as usize;
pub(crate) const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;

pub type TResult<T> = io::Result<T>;

#[repr(C, align(4096))]
struct PageAligned<T>(T);

#[derive(Clone, Copy)]
#[repr(C)]
struct IndexSlot {
    offset: u32,
    klen: u16,
    vlen: u16,
}

impl Default for IndexSlot {
    fn default() -> Self {
        Self {
            offset: 0,
            vlen: 0,
            klen: 0,
        }
    }
}

#[repr(C)]
struct ShardMeta {
    magic: [u8; 8],
    version: u8,
}

#[repr(C)]
struct ShardStats {
    n_occupied: AtomicU16,
    n_deleted: AtomicU16,
    write_offset: AtomicU32,
}

#[repr(C)]
struct ShardHeader {
    meta: ShardMeta,
    stats: ShardStats,
    fp: PageAligned<[u16; TABLE_SIZE]>,
    slots: PageAligned<[IndexSlot; TABLE_SIZE]>,
}

impl Default for ShardHeader {
    fn default() -> Self {
        Self {
            meta: ShardMeta {
                magic: MAGIC,
                version: VERSION,
            },
            stats: ShardStats {
                n_occupied: AtomicU16::new(0),
                n_deleted: AtomicU16::new(0),
                write_offset: AtomicU32::new(0),
            },
            fp: PageAligned([0u16; TABLE_SIZE]),
            slots: PageAligned([IndexSlot::default(); TABLE_SIZE]),
        }
    }
}

struct ShardFile {
    file: File,
    mmap: MmapMut,
}

impl ShardFile {
    fn open(file: File) -> TResult<Self> {
        let mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;

        Ok(Self { file, mmap })
    }

    fn new(path: PathBuf) -> TResult<Self> {
        let file = Self::file(path, true)?;
        file.set_len(HEADER_SIZE)?;

        Self::open(file)
    }

    fn file(path: PathBuf, truncate: bool) -> TResult<File> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(truncate)
            .open(path)?;

        Ok(file)
    }

    #[inline(always)]
    fn header(&self) -> &ShardHeader {
        unsafe { &*(self.mmap.as_ptr() as *const ShardHeader) }
    }

    #[inline(always)]
    fn header_mut(&self) -> &mut ShardHeader {
        unsafe { &mut *(self.mmap.as_ptr() as *mut ShardHeader) }
    }

    fn write_slot(&self, kbuf: &[u8], vbuf: &[u8]) -> TResult<IndexSlot> {
        let vlen = vbuf.len();
        let klen = kbuf.len();
        let blen = klen + vlen;

        let write_offset: u32 = self
            .header()
            .stats
            .write_offset
            .fetch_add(blen as u32, Ordering::SeqCst) as u32;

        Self::write_all_at(&self.file, vbuf, write_offset as u64 + HEADER_SIZE)?;

        Ok(IndexSlot {
            offset: write_offset,
            vlen: vlen as u16,
            klen: klen as u16,
        })
    }

    fn read_slot(&self, slot: &IndexSlot) -> TResult<(Vec<u8>, Vec<u8>)> {
        let vlen = slot.vlen as usize;
        let klen = slot.klen as usize;
        let mut buf = vec![0u8; vlen + klen];

        Self::read_exact_at(&self.file, &mut buf, slot.offset as u64)?;

        let vbuf = buf[klen..klen + vlen].to_owned();
        buf.truncate(klen);

        Ok((buf, vbuf))
    }

    fn read_kbuf(&self, slot: &IndexSlot) -> TResult<Vec<u8>> {
        let klen = slot.klen as usize;
        let mut buf = vec![0u8; klen];

        Self::read_exact_at(&self.file, &mut buf, slot.offset as u64)?;

        Ok(buf)
    }

    fn read_vbuf(&self, slot: &IndexSlot) -> TResult<Vec<u8>> {
        let klen = slot.klen as usize;
        let vlen = slot.vlen as usize;

        let mut buf = vec![0u8; vlen];

        Self::read_exact_at(&self.file, &mut buf, (slot.offset + klen as u32) as u64)?;

        Ok(buf)
    }

    #[cfg(unix)]
    fn read_exact_at(f: &File, buf: &mut [u8], offset: u64) -> TResult<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }

    #[cfg(unix)]
    fn write_all_at(f: &File, buf: &[u8], offset: u64) -> TResult<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
    }
}
