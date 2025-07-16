#![allow(dead_code)]

use crate::core::{TurboError, TurboResult};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    mem::size_of,
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};

const BUF_CAP: usize = 8 * 64;
const VERSION: u32 = 0;
const MAGIC: [u8; 4] = *b"TCv0";
const BUF_FILE_NAME: &str = "buffer";
const HEADER_SIZE: u64 = size_of::<Header>() as u64;

#[repr(C)]
struct Meta {
    magic: [u8; 4],
    version: u32,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
        }
    }
}

#[repr(C)]
struct Stats {
    occupied: AtomicU32,
    offset: AtomicU32,
}

#[repr(C)]
struct Offset {
    offset: u32,
    vlen: u16,
    klen: u16,
}

#[repr(C)]
struct Header {
    meta: Meta,
    stats: Stats,
    signs: [u32; BUF_CAP],
    offset: [Offset; BUF_CAP],
}

struct BufFile {
    file: File,
    mmap: MmapMut,
}

impl BufFile {
    pub fn open(dirpath: &PathBuf) -> TurboResult<Self> {
        let file_path = dirpath.join(BUF_FILE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(file_path)?;

        let file_meta = file.metadata()?;

        // if new file is created
        if file_meta.len() < HEADER_SIZE {
            return Self::create(file);
        }

        let mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;
        let buffer = Self { file, mmap };
        let head = buffer.header();

        // if file is invalid (invalid meta, or else)
        if head.meta.magic != MAGIC && head.meta.version != VERSION {
            return Err(TurboError::InvalidFile);
        }

        Ok(buffer)
    }

    fn create(file: File) -> TurboResult<Self> {
        file.set_len(0)?;
        file.set_len(HEADER_SIZE)?;

        let mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;
        let buffer = Self { file, mmap };

        let head = buffer.header_mut();

        // set metadata
        head.meta.magic = MAGIC;
        head.meta.version = VERSION;

        // set stats
        head.stats.occupied = AtomicU32::new(0);
        head.stats.offset = AtomicU32::new(0);

        Ok(buffer)
    }

    fn write_slot(&self, buf: (&[u8], &[u8])) -> TurboResult<Offset> {
        let klen = buf.0.len();
        let vlen = buf.1.len();
        let blen = klen + vlen;

        let mut wbuf = vec![0u8; blen];

        wbuf[..klen].copy_from_slice(buf.0);
        wbuf[klen..].copy_from_slice(buf.1);

        let offset = self
            .header()
            .stats
            .offset
            .fetch_add(blen as u32, Ordering::SeqCst);

        Self::write_all_at(&self.file, &wbuf, offset)?;

        Ok(Offset {
            offset,
            vlen: vlen as u16,
            klen: klen as u16,
        })
    }

    fn read_slot(&self, offset: Offset) -> TurboResult<(Vec<u8>, Vec<u8>)> {
        let klen = offset.klen as usize;
        let vlen = offset.vlen as usize;

        let mut rbuf = vec![0u8; klen + vlen];
        let slot_offset = self.header().stats.offset.load(Ordering::SeqCst);

        Self::read_exact_at(&self.file, &mut rbuf, slot_offset)?;

        let vbuf = rbuf[klen..(klen + vlen)].to_owned();
        rbuf.truncate(klen);

        Ok((rbuf, vbuf))
    }

    /// Returns an immutable reference to the header
    #[inline(always)]
    fn header(&self) -> &Header {
        unsafe { &*(self.mmap.as_ptr() as *const Header) }
    }

    /// Returns a mutable reference to the header
    #[inline(always)]
    fn header_mut(&self) -> &mut Header {
        unsafe { &mut *(self.mmap.as_ptr() as *mut Header) }
    }

    /// Reads the exact number of bytes required to fill `buf` from a given offset.
    #[cfg(unix)]
    #[inline]
    fn read_exact_at(f: &File, buf: &mut [u8], offset: u32) -> TurboResult<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset as u64)?;

        Ok(())
    }

    /// Writes a buffer to a file at a given offset.
    #[cfg(unix)]
    #[inline]
    fn write_all_at(f: &File, buf: &[u8], offset: u32) -> TurboResult<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset as u64)?;

        Ok(())
    }
}

pub(crate) struct Buffer {}
