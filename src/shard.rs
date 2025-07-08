#![allow(dead_code)]

use crate::hasher::{TurboHasher, INVALID_FP};
use memmap::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    io,
    mem::size_of,
    ops::Range,
    path::PathBuf,
    sync::atomic::{AtomicU16, AtomicU32, Ordering},
};

/// Current version of TurboCache shards
pub(crate) const VERSION: u8 = 0;

/// Versioned MAGIC value to help identify shards specific to TurboCache
pub(crate) const MAGIC: [u8; 8] = *b"TURBOv0\0";

pub(crate) const ROWS_NUM: usize = 1024;
pub(crate) const ROWS_WIDTH: usize = 32;
pub(crate) const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;

pub type TResult<T> = io::Result<T>;

#[repr(C, align(4096))]
struct PageAligned<T>(T);

#[derive(Clone, Copy)]
#[repr(C)]
struct ShardSlot {
    offset: u32,
    klen: u16,
    vlen: u16,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct IndexRow {
    fp: [u32; ROWS_WIDTH],
    slots: [ShardSlot; ROWS_WIDTH],
}

impl Default for IndexRow {
    fn default() -> Self {
        Self {
            fp: [0u32; ROWS_WIDTH],
            slots: ShardSlot {
                offset: 0,
                klen: 0,
                vlen: 0,
            },
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
    index: PageAligned<[IndexRow; ROWS_NUM]>,
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
            index: PageAligned([IndexRow::default(); ROWS_NUM]),
        }
    }
}

struct ShardFile {
    file: File,
    mmap: MmapMut,
}

impl ShardFile {
    fn open(path: PathBuf, truncate: bool) -> TResult<Self> {
        let file = {
            if truncate {
                Self::new(path)?
            } else {
                Self::file(path, truncate)?
            }
        };

        let mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;

        Ok(Self { file, mmap })
    }

    fn new(path: PathBuf) -> TResult<File> {
        let file = Self::file(path, true)?;
        file.set_len(HEADER_SIZE)?;

        Ok(file)
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

    #[inline(always)]
    fn row(&self, row_idx: usize) -> &IndexRow {
        &self.header().index.0[row_idx]
    }

    #[inline(always)]
    fn row_mut(&self, row_idx: usize) -> &mut IndexRow {
        &mut self.header_mut().index.0[row_idx]
    }

    fn write_slot(&self, kbuf: &[u8], vbuf: &[u8]) -> TResult<ShardSlot> {
        let vlen = vbuf.len();
        let klen = kbuf.len();
        let blen = klen + vlen;

        let write_offset: u32 = self
            .header()
            .stats
            .write_offset
            .fetch_add(blen as u32, Ordering::SeqCst) as u32;

        Self::write_all_at(&self.file, vbuf, write_offset as u64 + HEADER_SIZE)?;

        Ok(ShardSlot {
            offset: write_offset,
            vlen: vlen as u16,
            klen: klen as u16,
        })
    }

    fn read_slot(&self, slot: &ShardSlot) -> TResult<(Vec<u8>, Vec<u8>)> {
        let vlen = slot.vlen as usize;
        let klen = slot.klen as usize;
        let mut buf = vec![0u8; vlen + klen];

        Self::read_exact_at(&self.file, &mut buf, slot.offset as u64)?;

        let vbuf = buf[klen..klen + vlen].to_owned();
        buf.truncate(klen);

        Ok((buf, vbuf))
    }

    fn read_kbuf(&self, slot: &ShardSlot) -> TResult<Vec<u8>> {
        let klen = slot.klen as usize;
        let mut buf = vec![0u8; klen];

        Self::read_exact_at(&self.file, &mut buf, slot.offset as u64)?;

        Ok(buf)
    }

    fn read_vbuf(&self, slot: &ShardSlot) -> TResult<Vec<u8>> {
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

pub(crate) struct Shard {
    pub(crate) span: Range<u32>,
    dirpath: PathBuf,
    file: ShardFile,
}

impl Shard {
    pub fn open(dirpath: PathBuf, span: Range<u32>, truncate: bool) -> TResult<Self> {
        let filepath = dirpath.join(format!("shard_{:04x}-{:04x}", span.start, span.end));

        let file = ShardFile::open(filepath, truncate)?;

        Ok(Self {
            span,
            dirpath,
            file,
        })
    }

    pub fn insert(&self, buf: (&[u8], &[u8]), hash: TurboHasher) -> TResult<()> {
        let (kbuf, vbuf) = buf;
        let row_idx = hash.row_selector() as usize;
        let fp = hash.fingerprint();

        let row = self.file.row_mut(row_idx);

        // incase the fingerprint already exists
        for idx in 0..ROWS_WIDTH {
            if row.fp[idx] == fp {
                let slot = row.slots[idx];
                let key = self.file.read_kbuf(&slot)?;

                if key == kbuf {
                    let new_slot = self.file.write_slot(kbuf, vbuf)?;
                    row.slots[idx] = new_slot;
                }

                return Ok(());
            }
        }

        // otherwise find the first free slot (fp == 0) to insert
        for idx in 0..ROWS_WIDTH {
            if row.fp[idx] == INVALID_FP {
                let slot = self.file.write_slot(kbuf, vbuf)?;

                row.fp[idx] = fp;
                row.slots[idx] = slot;

                let header = self.file.header_mut();
                header.stats.n_occupied.fetch_add(1, Ordering::SeqCst);

                return Ok(());
            }
        }

        Ok(())
    }
}
