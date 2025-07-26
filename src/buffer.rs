#![allow(dead_code)]

use crate::{
    core::{KVPair, TurboResult, MAGIC, VERSION},
    TurboError,
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    mem::size_of,
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};

#[repr(C)]
struct Meta {
    version: u32,
    magic: [u8; 4],
}

#[repr(C)]
struct Stats {
    n_pairs: AtomicU32,
    file_offset: AtomicU32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct PairOffset {
    offset: u32,
    klen: u16,
    vlen: u16,
}

#[repr(C, align(4096))]
struct PageAligned<T>(T);

#[repr(C)]
struct Header {
    meta: Meta,
    stats: Stats,
    signuatures: PageAligned<Box<[u32]>>,
    offsets: PageAligned<Box<[PairOffset]>>,
}

struct BucketFile {
    mmap: MmapMut,
    file: File,
    header_size: usize,
}

impl BucketFile {
    fn open(bucket_path: &PathBuf, capacity: usize) -> TurboResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(bucket_path)?;

        let file_meta = file.metadata()?;
        let header_size = Self::get_header_size(capacity);

        if file_meta.len() < header_size as u64 {
            return Err(TurboError::InvalidFile);
        }

        let mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;
        let buffer = Self {
            file,
            mmap,
            header_size,
        };
        let head = buffer.header();

        // if file is invalid (invalid meta, or else)
        if head.meta.magic != MAGIC && head.meta.version != VERSION {
            return Err(TurboError::InvalidFile);
        }

        Ok(buffer)
    }

    /// Create a new buffer w/ default state
    fn create(file: File, header_size: usize) -> TurboResult<Self> {
        file.set_len(header_size as u64)?;

        let mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;
        let buffer = Self {
            file,
            mmap,
            header_size,
        };

        // set metadata
        let head = buffer.header_mut();

        head.meta.magic = MAGIC;
        head.meta.version = VERSION;

        Ok(buffer)
    }

    /// Write a [KVPair] to the bucket and get [PairOffset]
    fn write_slot(&self, pair: KVPair) -> TurboResult<PairOffset> {
        let klen = pair.0.len();
        let vlen = pair.1.len();
        let blen = klen + vlen;

        let mut buf = vec![0u8; blen];

        buf[..klen].copy_from_slice(&pair.0);
        buf[klen..].copy_from_slice(&pair.1);

        let offset = self
            .header()
            .stats
            .file_offset
            .fetch_add(blen as u32, Ordering::SeqCst);

        Self::write_all_at(&self.file, &buf, offset as u64)?;

        Ok(PairOffset {
            klen: klen as u16,
            vlen: vlen as u16,
            offset,
        })
    }

    /// Read a [KVPair] from a given [PairOffset]
    fn read_slot(&self, pair: &PairOffset) -> TurboResult<KVPair> {
        let klen = pair.klen as usize;
        let vlen = pair.vlen as usize;
        let mut buf = vec![0u8; klen + vlen];

        Self::read_exact_at(&self.file, &mut buf, pair.offset as u64)?;

        let vbuf = buf[klen..(klen + vlen)].to_owned();
        buf.truncate(klen);

        Ok((buf, vbuf))
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

    /// Reads the exact number of bytes at a given offset (`pread`)
    #[cfg(unix)]
    fn read_exact_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }

    /// Writes a buffer to a file at a given offset (`pwrite`)
    #[cfg(unix)]
    fn write_all_at(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
    }

    /// Reads the exact number of bytes at a given offset (`pread`)
    #[cfg(windows)]
    fn read_exact_at(f: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
        while !buf.is_empty() {
            match std::os::windows::fs::FileExt::seek_read(f, buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
        } else {
            Ok(())
        }
    }

    /// Writes a buffer to a file at a given offset (`pwrite`)
    #[cfg(windows)]
    fn write_all_at(f: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
        while !buf.is_empty() {
            match std::os::windows::fs::FileExt::seek_write(f, buf, offset) {
                Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Calculate the size of header based on the capacity of the Buffer
    ///
    /// > Size is calculated as below,
    ///
    /// SIZE = sizeof(META) + sizeof(Stats) + (sizeof(PairOffset) * N) + (sizeof(u32) * N)
    ///
    /// Where,
    ///   N = Capacity of Buffer
    /// ```
    const fn get_header_size(capacity: usize) -> usize {
        let mut n = size_of::<Meta>() + size_of::<Stats>();

        n += size_of::<PairOffset>() * capacity;
        n += 4 * capacity;

        n
    }
}
