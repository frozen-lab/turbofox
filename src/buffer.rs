#![allow(dead_code)]

use crate::{
    core::{TurboConfig, TurboResult, DEFAULT_BUF_FILE_NAME, MAGIC, VERSION},
    TurboError,
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    mem::size_of,
};

#[repr(C)]
struct Meta {
    version: u32,
    magic: [u8; 4],
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
    signuatures: PageAligned<Box<[u32]>>,
    offsets: PageAligned<Box<[PairOffset]>>,
}

struct BufFile {
    mmap: MmapMut,
    file: File,
    header_size: usize,
}

impl BufFile {
    fn open(config: &TurboConfig) -> TurboResult<Self> {
        let file_path = config.dirpath.join(DEFAULT_BUF_FILE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(file_path)?;

        let file_meta = file.metadata()?;
        let header_size = Self::get_header_size(config.buf_cap);

        if file_meta.len() < header_size as u64 {
            return Self::create(file, header_size);
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
        file.set_len(0)?;
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
    /// ```txt
    /// size = sizeof(META) + (sizeof(PairOffset) * N) + (sizeof(u32) * N)
    ///
    /// where,
    ///   N = Capacity of Buffer
    /// ```
    const fn get_header_size(capacity: usize) -> usize {
        let mut n = size_of::<Meta>();

        n += size_of::<PairOffset>() * capacity;
        n += 4 * capacity;

        n
    }
}
