#![allow(dead_code)]

use crate::{
    core::{KVPair, TurboResult, MAGIC, VERSION},
    hash::{EMPTY_SIGN, TOMBSTONE_SIGN},
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

impl Header {
    fn lookup_set(&self, start_idx: &mut usize, capacity: usize, sign: u32) -> Option<()> {
        loop {
            let existing_sign = self.signuatures.0[*start_idx];

            if existing_sign == TOMBSTONE_SIGN || existing_sign == EMPTY_SIGN {
                return None;
            }

            if existing_sign == sign {
                return Some(());
            }

            *start_idx = (*start_idx + 1) % capacity;
        }
    }

    fn lookup_get(&self, start_idx: &mut usize, capacity: usize, sign: u32) -> Option<()> {
        loop {
            let existing_sign = self.signuatures.0[*start_idx];

            if existing_sign == EMPTY_SIGN {
                return None;
            }

            if existing_sign == sign {
                return Some(());
            }

            *start_idx = (*start_idx + 1) % capacity;
        }
    }
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
        if head.meta.magic != MAGIC || head.meta.version != VERSION {
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

pub struct Bucket {
    file: BucketFile,
    capacity: usize,
}

impl Bucket {
    pub fn new(path: &PathBuf, capacity: usize) -> TurboResult<Self> {
        let file = BucketFile::open(path, capacity)?;

        Ok(Self { file, capacity })
    }

    pub fn set(&self, pair: KVPair, sign: u32) -> TurboResult<()> {
        let header = self.file.header_mut();
        let mut idx = sign as usize % self.capacity;

        let is_update = header.lookup_set(&mut idx, self.capacity, sign).is_some();

        if !is_update {
            header.stats.n_pairs.fetch_add(1, Ordering::SeqCst);
        }

        let p_offset = self.file.write_slot(pair)?;

        header.signuatures.0[idx] = sign;
        header.offsets.0[idx] = p_offset;

        Ok(())
    }

    pub fn get(&self, kbuf: Vec<u8>, sign: u32) -> TurboResult<Option<Vec<u8>>> {
        let header = self.file.header();
        let mut idx = sign as usize % self.capacity;

        loop {
            if let Some(_) = header.lookup_get(&mut idx, self.capacity, sign) {
                let pair_offset = header.offsets.0[idx];
                let pair = self.file.read_slot(&pair_offset)?;

                if pair.0 == kbuf {
                    return Ok(Some(pair.1));
                }
            } else {
                return Ok(None);
            }
        }
    }

    pub fn del(&self, kbuf: Vec<u8>, sign: u32) -> TurboResult<Option<Vec<u8>>> {
        let header = self.file.header_mut();
        let mut idx = sign as usize % self.capacity;

        loop {
            if let Some(_) = header.lookup_get(&mut idx, self.capacity, sign) {
                let pair_offset = header.offsets.0[idx];
                let pair = self.file.read_slot(&pair_offset)?;

                if pair.0 == kbuf {
                    header.signuatures.0[idx] = TOMBSTONE_SIGN;
                    header.stats.n_pairs.fetch_sub(1, Ordering::SeqCst);

                    return Ok(Some(pair.1));
                }
            } else {
                return Ok(None);
            }
        }
    }

    pub fn iter(&self, start: &mut usize) -> TurboResult<Option<KVPair>> {
        let header = self.file.header();

        while *start < self.capacity {
            let idx = *start;
            *start += 1;

            let sign = header.signuatures.0[idx];

            if sign != EMPTY_SIGN && sign != TOMBSTONE_SIGN {
                let p_offset = header.offsets.0[idx];
                let pair = self.file.read_slot(&p_offset)?;

                return Ok(Some(pair));
            }
        }

        Ok(None)
    }
}
