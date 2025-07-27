#![allow(dead_code)]

use crate::{
    core::{KVPair, TurboResult, MAGIC, VERSION},
    hash::{EMPTY_SIGN, TOMBSTONE_SIGN},
    TurboError,
};
use core::slice;
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
    inserts: AtomicU32,
    file_offset: AtomicU32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct PairOffset {
    offset: u32,
    klen: u16,
    vlen: u16,
}

struct BucketFile {
    mmap: MmapMut,
    file: File,
    capacity: usize,
    header_size: usize,
    sign_offset: usize,
    po_offset: usize,
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
            return Self::create(file, header_size, capacity);
        }

        let sign_offset = size_of::<Meta>();
        let po_offset = sign_offset + capacity * size_of::<u32>();
        let mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;

        let bucket = Self {
            file,
            mmap,
            capacity,
            header_size,
            sign_offset,
            po_offset,
        };

        let meta = bucket.metadata();

        // check if file is invalid (invalid magic, or else)
        if meta.magic != MAGIC || meta.version != VERSION {
            return Err(TurboError::InvalidFile);
        }

        Ok(bucket)
    }

    /// Create a new buffer w/ default state
    fn create(file: File, header_size: usize, capacity: usize) -> TurboResult<Self> {
        file.set_len(header_size as u64)?;

        let sign_offset = size_of::<Meta>();
        let po_offset = sign_offset + capacity * size_of::<u32>();
        let mut mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;

        // init w/ zeroed values
        mmap[..].fill(0);

        let bucket = Self {
            file,
            mmap,
            capacity,
            header_size,
            sign_offset,
            po_offset,
        };

        // set metadata
        let meta = bucket.metadata_mut();

        meta.magic = MAGIC;
        meta.version = VERSION;

        Ok(bucket)
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
            .metadata()
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

    fn lookup_insert_slot(&self, start_idx: &mut usize, signs: &[u32], sign: u32) -> bool {
        loop {
            match signs[*start_idx] {
                s if s == sign => {
                    return false;
                }
                EMPTY_SIGN | TOMBSTONE_SIGN => {
                    return true;
                }
                _ => {
                    *start_idx = (*start_idx + 1) % self.capacity;
                }
            }
        }
    }

    fn lookup_slot(&self, start_idx: &mut usize, signs: &[u32], sign: u32) -> bool {
        loop {
            match signs[*start_idx] {
                s if s == sign => {
                    return false;
                }
                EMPTY_SIGN => {
                    return true;
                }
                _ => {
                    *start_idx = (*start_idx + 1) % self.capacity;
                }
            }
        }
    }
    /// Returns an immutable reference to [Metadata]
    #[inline(always)]
    fn metadata(&self) -> &Meta {
        unsafe { &*(self.mmap.as_ptr() as *const Meta) }
    }

    /// Returns a mutable reference to [Metadata]
    #[inline(always)]
    fn metadata_mut(&self) -> &mut Meta {
        unsafe { &mut *(self.mmap.as_ptr() as *mut Meta) }
    }

    /// Returns an immutable reference to signatures slice
    #[inline(always)]
    fn get_signatures(&self) -> &[u32] {
        unsafe {
            let ptr = self.mmap.as_ptr().add(self.sign_offset) as *const u32;
            slice::from_raw_parts(ptr, self.capacity)
        }
    }

    /// Write a new signature into slot `idx`
    #[inline]
    pub fn set_signature(&mut self, idx: usize, sign: u32) {
        assert!(idx < self.capacity);

        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.sign_offset) as *mut u32;
            *ptr.add(idx) = sign;
        }
    }

    /// Read a single [PairOffset] by index, directly from the mmap
    #[inline(always)]
    pub fn get_pair_offset(&self, idx: usize) -> PairOffset {
        debug_assert!(idx < self.capacity);
        unsafe {
            let ptr = self.mmap.as_ptr().add(self.po_offset) as *const PairOffset;
            *ptr.add(idx)
        }
    }

    /// Write a new item into [PairOffset] slice
    #[inline]
    pub fn set_pair_offset(&mut self, idx: usize, po: PairOffset) {
        assert!(idx < self.capacity);

        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.po_offset) as *mut PairOffset;
            *ptr.add(idx) = po;
        }
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
        let mut n = size_of::<Meta>();

        n += size_of::<u32>() * capacity;
        n += size_of::<PairOffset>() * capacity;

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

    pub fn set(&mut self, pair: KVPair, sign: u32) -> TurboResult<()> {
        let meta = self.file.metadata_mut();
        let signs = self.file.get_signatures();

        let mut start_idx = sign as usize % self.capacity;

        // find the exact slot and know if it's a new insert
        let was_empty = self.file.lookup_insert_slot(&mut start_idx, signs, sign);

        if was_empty {
            meta.inserts.fetch_add(1, Ordering::SeqCst);
        }

        let po = self.file.write_slot(pair)?;

        self.file.set_signature(start_idx, sign);
        self.file.set_pair_offset(start_idx, po);

        Ok(())
    }

    pub fn get(&self, kbuf: Vec<u8>, sign: u32) -> TurboResult<Option<Vec<u8>>> {
        let signs = self.file.get_signatures();

        let mut start_idx = sign as usize % self.capacity;

        loop {
            let was_empty = self.file.lookup_slot(&mut start_idx, signs, sign);

            if was_empty {
                return Ok(None);
            }

            let po = self.file.get_pair_offset(start_idx);
            let (k, v) = self.file.read_slot(&po)?;

            if kbuf == k {
                return Ok(Some(v));
            }
        }
    }

    pub fn del(&mut self, kbuf: Vec<u8>, sign: u32) -> TurboResult<Option<Vec<u8>>> {
        let meta = self.file.metadata_mut();
        let signs = self.file.get_signatures();

        let mut start_idx = sign as usize % self.capacity;

        loop {
            let was_empty = self.file.lookup_slot(&mut start_idx, signs, sign);

            if was_empty {
                return Ok(None);
            }

            let po = self.file.get_pair_offset(start_idx);
            let (k, v) = self.file.read_slot(&po)?;

            if kbuf == k {
                meta.inserts.fetch_sub(1, Ordering::SeqCst);
                self.file.set_signature(start_idx, TOMBSTONE_SIGN);

                return Ok(Some(v));
            }
        }
    }

    pub fn iter(&self, start: &mut usize) -> TurboResult<Option<KVPair>> {
        let signs = self.file.get_signatures();

        while *start < self.capacity {
            let idx = *start;
            *start += 1;

            if signs[idx] != EMPTY_SIGN && signs[idx] != TOMBSTONE_SIGN {
                let p_offset = self.file.get_pair_offset(idx);
                let pair = self.file.read_slot(&p_offset)?;

                return Ok(Some(pair));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::TurboHasher;

    #[test]
    fn test_set_and_get() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("bucket.dat");

        let mut bucket = Bucket::new(&path, 16).unwrap();

        let key = b"hello".to_vec();
        let val = b"world".to_vec();
        let sig = TurboHasher::new(&key).0;

        // initially missing
        assert_eq!(bucket.get(key.clone(), sig).unwrap(), None);

        // insert and retrieve
        bucket.set((key.clone(), val.clone()), sig).unwrap();
        let got = bucket.get(key.clone(), sig).unwrap();

        assert_eq!(got, Some(val.clone()));
    }
}
