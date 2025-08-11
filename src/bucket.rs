use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    mem::size_of,
    path::Path,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use crate::{
    common::{KVPair, Key, MAGIC, VERSION},
    hasher::{Hasher, EMPTY_SIGN, TOMBSTONE_SIGN},
    types::{InternalError, InternalResult},
};

#[repr(C)]
struct Meta {
    magic: [u8; 4],
    version: u32,
    inserts: AtomicU32,
    iter_idx: AtomicU32,
    insert_offset: AtomicU64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct Pair(u64);

impl Pair {
    fn from_offset(offset: Offset) -> InternalResult<Self> {
        if offset.position > (1u64 << 40) {
            return Err(InternalError::OffsetOverflow(offset.position as usize));
        }

        if offset.klen > (1u16 << 12) {
            return Err(InternalError::KeyTooLarge(offset.klen as usize));
        }

        if offset.vlen > (1u16 << 12) {
            return Err(InternalError::ValueTooLarge(offset.vlen as usize));
        }

        let off_bits = offset.position & ((1u64 << 40) - 1);
        let klen_bits = (offset.klen as u64 & ((1u64 << 12) - 1)) << 40;
        let vlen_bits = (offset.vlen as u64 & ((1u64 << 12) - 1)) << 52;

        Ok(Self(off_bits | klen_bits | vlen_bits))
    }

    fn to_offset(&self) -> Offset {
        let position = self.0 & ((1u64 << 40) - 1);
        let klen = (self.0 >> 40) & ((1u64 << 12) - 1);
        let vlen = (self.0 >> 52) & ((1u64 << 12) - 1);

        Offset {
            position,
            klen: klen as u16,
            vlen: vlen as u16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
struct Offset {
    position: u64,
    klen: u16,
    vlen: u16,
}

#[cfg(test)]
mod pair_tests {
    use super::{Offset, Pair};

    #[test]
    fn test_basic_round_trip() {
        let o = Offset {
            position: 123456789,
            klen: 100,
            vlen: 200,
        };

        let encoded = Pair::from_offset(o).unwrap();
        let decoded = encoded.to_offset();

        assert_eq!(o.position, decoded.position);
        assert_eq!(o.klen, decoded.klen);
        assert_eq!(o.vlen, decoded.vlen);
    }

    #[test]
    fn test_boundaries() {
        let o = Offset {
            position: 0,
            klen: 0,
            vlen: 0,
        };

        let encoded = Pair::from_offset(o).unwrap();
        let decoded = encoded.to_offset();

        assert_eq!(o, decoded);

        let max_pos = (1u64 << 40) - 1;
        let max_klen = (1u16 << 12) - 1;
        let max_vlen = (1u16 << 12) - 1;

        let o = Offset {
            position: max_pos,
            klen: max_klen,
            vlen: max_vlen,
        };

        let encoded = Pair::from_offset(o).unwrap();
        let decoded = encoded.to_offset();

        assert_eq!(o, decoded);
    }

    #[test]
    fn test_randomized_values() {
        for i in 0..1000 {
            let position = (i * 1234567) as u64 & ((1u64 << 40) - 1);
            let klen = (i * 37 % (1 << 12)) as u16;
            let vlen = (i * 91 % (1 << 12)) as u16;

            let o = Offset {
                position,
                klen,
                vlen,
            };
            let encoded = Pair::from_offset(o).unwrap();
            let decoded = encoded.to_offset();

            assert_eq!(o, decoded, "Failed at iteration {i}");
        }
    }
}

struct BucketFile {
    mmap: MmapMut,
    file: File,
    capacity: usize,
    header_size: usize,
    sign_offsets: usize,
    pair_offsets: usize,
    threshold: usize,
}

impl BucketFile {
    fn open<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        let file_meta = file.metadata()?;
        let header_size = Self::calc_header_size(capacity);

        // check if its a new file
        //
        // If it's a new file then we must create a zero bytes Header
        //
        // NOTE: If sizeof(meta) is smaller then `header_size`, we treat
        // it as an uninitialized or new file
        let is_new = file_meta.len() < header_size as u64;

        // set min required len
        if is_new {
            file.set_len(header_size as u64)?;
        }

        let mut mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;
        let sign_offsets = size_of::<Meta>();
        let pair_offsets = sign_offsets + capacity * size_of::<u32>();
        let threshold = Self::calc_threshold(capacity);

        if is_new {
            mmap[..].fill(0u8);
        }

        let bucket = Self {
            file,
            mmap,
            capacity,
            header_size,
            sign_offsets,
            pair_offsets,
            threshold,
        };

        if is_new {
            let meta = bucket.metadata_mut();

            meta.magic = MAGIC;
            meta.version = VERSION;
        } else {
            // validate the file
            let meta = bucket.metadata();

            if meta.version != VERSION || meta.magic != MAGIC {
                return Err(InternalError::InvalidFile);
            }
        }

        Ok(bucket)
    }

    /// Returns an immutable reference to [Meta]
    #[inline(always)]
    fn metadata(&self) -> &Meta {
        unsafe { &*(self.mmap.as_ptr() as *const Meta) }
    }

    /// Returns a mutable reference to [Meta]
    #[inline(always)]
    fn metadata_mut(&self) -> &mut Meta {
        unsafe { &mut *(self.mmap.as_ptr() as *mut Meta) }
    }

    fn lookup_slot(
        &self,
        mut idx: usize,
        signs: &[u32],
        sign: u32,
        kbuf: &[u8],
    ) -> InternalResult<(usize, bool)> {
        for _ in 0..self.capacity {
            match signs[idx] {
                EMPTY_SIGN | TOMBSTONE_SIGN => return Ok((idx, true)),
                s if s == sign => {
                    let po = self.get_po(idx);
                    let (existing_key, _) = self.read_slot(&po)?;

                    if existing_key == kbuf {
                        return Ok((idx, false));
                    }
                }
                _ => {}
            }
            idx = (idx + 1) % self.capacity;
        }

        // NOTE: This should never be reached
        Err(InternalError::BucketFull)
    }

    /// Read a [KVPair] from a given [Pair]
    fn read_slot(&self, pair: &Pair) -> InternalResult<KVPair> {
        let offset = pair.to_offset();
        let klen = offset.klen as usize;
        let vlen = offset.vlen as usize;

        let mut buf = vec![0u8; klen + vlen];

        Self::read_exact_at(
            &self.file,
            &mut buf,
            self.header_size as u64 + offset.position as u64,
        )?;

        let vbuf = buf[klen..(klen + vlen)].to_owned();
        buf.truncate(klen);

        Ok((buf, vbuf))
    }

    /// Write a [KVPair] to the bucket and get [Pair]
    fn write_slot(&self, pair: &KVPair) -> InternalResult<Pair> {
        let klen = pair.0.len();
        let vlen = pair.1.len();
        let blen = klen + vlen;

        let mut buf = vec![0u8; blen];

        buf[..klen].copy_from_slice(&pair.0);
        buf[klen..].copy_from_slice(&pair.1);

        let position = self
            .metadata()
            .insert_offset
            .fetch_add(blen as u64, Ordering::SeqCst);

        Self::write_all_at(&self.file, &buf, self.header_size as u64 + position)?;

        let offset = Offset {
            klen: klen as u16,
            vlen: vlen as u16,
            position,
        };

        Ok(Pair::from_offset(offset)?)
    }

    #[inline]
    fn get_inserted_count(&self) -> usize {
        self.metadata().inserts.load(Ordering::SeqCst) as usize
    }

    #[inline]
    fn get_iter_idx(&self) -> usize {
        self.metadata().iter_idx.load(Ordering::SeqCst) as usize
    }

    #[inline]
    fn increment_iter_idx(&self) {
        self.metadata_mut().iter_idx.fetch_add(1, Ordering::SeqCst);
    }

    #[inline]
    fn calc_threshold(cap: usize) -> usize {
        cap.saturating_mul(4) / 5
    }

    /// Read a single [Pair] at index, directly from the mmap
    #[inline(always)]
    fn get_po(&self, idx: usize) -> Pair {
        // sanity check
        debug_assert!(idx < self.capacity);

        unsafe {
            let ptr = self.mmap.as_ptr().add(self.pair_offsets) as *const Pair;

            *ptr.add(idx)
        }
    }

    /// Write a new [PairOffset] at given index
    #[inline]
    fn set_po(&mut self, idx: usize, po: Pair) {
        // sanity check
        debug_assert!(idx < self.capacity);

        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.pair_offsets) as *mut Pair;
            *ptr.add(idx) = po;
        }
    }

    /// Returns an immutable reference to signatures slice
    #[inline(always)]
    fn get_signatures_slice(&self) -> &[u32] {
        unsafe {
            let ptr = self.mmap.as_ptr().add(self.sign_offsets) as *const u32;

            core::slice::from_raw_parts(ptr, self.capacity)
        }
    }

    /// Write a new signature at `idx` into signatures slice
    #[inline]
    fn set_signature(&mut self, idx: usize, sign: u32) {
        assert!(idx < self.capacity);

        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.sign_offsets) as *mut u32;

            *ptr.add(idx) = sign;
        }
    }

    /// Calculate the size of header based on the capacity of the Buffer
    ///
    /// Size is calculated as below,
    ///
    /// `SIZE = sizeof(Meta) + (sizeof(u32) * CAP) + (sizeof(PairOffset) * CAP)`
    #[inline]
    const fn calc_header_size(capacity: usize) -> usize {
        let mut n = size_of::<Meta>();

        n += size_of::<u32>() * capacity;
        n += size_of::<Pair>() * capacity;

        n
    }

    /// Read given numof bytes from a given offset uing `pread`
    #[cfg(unix)]
    fn read_exact_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }

    /// Write a buffer to a file at a given offset using `pwrite`
    #[cfg(unix)]
    fn write_all_at(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
    }

    /// Read given numof bytes from a given offset uing `pread`
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

    /// Write a buffer to a file at a given offset using `pwrite`
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
}

#[cfg(test)]
mod bucket_file_tests {
    use super::*;
    use tempfile::NamedTempFile;

    const TEST_CAP: usize = 16;

    fn open_bucket() -> BucketFile {
        let tmp = NamedTempFile::new().expect("create temp file");
        BucketFile::open(tmp.path(), TEST_CAP).expect("open bucket")
    }

    #[test]
    fn test_new_file_initialization() {
        let bucket = open_bucket();
        let meta = bucket.metadata();

        // MAGIC and VERSION should be set
        assert_eq!(meta.magic, MAGIC);
        assert_eq!(meta.version, VERSION);

        // inserts and iter idx start at zero
        assert_eq!(meta.inserts.load(Ordering::SeqCst), 0);
        assert_eq!(meta.iter_idx.load(Ordering::SeqCst), 0);

        // signatures and pair-offset regions should be zero
        let signs = bucket.get_signatures_slice();
        assert!(signs.iter().all(|&s| s == 0));

        for idx in 0..bucket.capacity {
            let po = bucket.get_po(idx);
            let offset = po.to_offset();

            assert_eq!(offset.klen, 0);
            assert_eq!(offset.vlen, 0);
            assert_eq!(offset.position, 0);
        }
    }

    #[test]
    fn test_reopen_existing_file() {
        // create and write one entry
        let tmp = NamedTempFile::new().unwrap();

        {
            let mut bucket = BucketFile::open(tmp.path(), TEST_CAP).unwrap();
            let kv = (b"foo".to_vec(), b"bar".to_vec());
            let po = bucket.write_slot(&kv).unwrap();

            bucket.set_po(0, po);
            bucket.set_signature(0, 12345);

            // metadata updates
            bucket.metadata_mut().inserts.fetch_add(1, Ordering::SeqCst);
        }

        // at re-open, it shouldn’t be re-zero'ed
        let bucket2 = BucketFile::open(tmp.path(), TEST_CAP).unwrap();
        let signs = bucket2.get_signatures_slice();
        assert_eq!(signs[0], 12345);

        let po2 = bucket2.get_po(0);
        let kv2 = bucket2.read_slot(&po2).unwrap();

        assert_eq!(kv2.0, b"foo".to_vec());
        assert_eq!(kv2.1, b"bar".to_vec());
    }

    #[test]
    fn test_write_and_read_roundtrip() {
        let mut bucket = open_bucket();

        let kv = (b"key1".to_vec(), b"value1".to_vec());
        let sign = 0xDEADBEEF;
        let po = bucket.write_slot(&kv).unwrap();

        bucket.set_po(3, po);
        bucket.set_signature(3, sign);
        bucket.metadata_mut().inserts.fetch_add(1, Ordering::SeqCst);

        let signs = bucket.get_signatures_slice();
        let (found_idx, is_new) = bucket.lookup_slot(3, signs, sign, &kv.0).unwrap();

        assert_eq!(found_idx, 3);
        assert!(!is_new, "existing key should report is_new=false");

        let po_back = bucket.get_po(3);
        let kv_back = bucket.read_slot(&po_back).unwrap();

        assert_eq!(kv_back, kv);
    }

    #[test]
    fn test_lookup_empty_and_tombstone() {
        let mut bucket = open_bucket();
        let signs = bucket.get_signatures_slice();
        let (idx, is_new) = bucket.lookup_slot(5, signs, 42, b"whatever").unwrap();

        assert_eq!(idx, 5);
        assert!(is_new);

        bucket.set_signature(5, TOMBSTONE_SIGN);
        let signs2 = bucket.get_signatures_slice();
        let (idx2, is_new2) = bucket.lookup_slot(5, signs2, 42, b"new").unwrap();

        assert_eq!(idx2, 5);
        assert!(is_new2);
    }

    #[test]
    fn test_po_and_signature_mutation() {
        let mut bucket = open_bucket();

        // set a fake PairOffset at idx 7
        let fake_po = Offset {
            klen: 1,
            vlen: 2,
            position: 99,
        };

        let pair = Pair::from_offset(fake_po).unwrap();
        bucket.set_po(7, pair);

        let got_po = bucket.get_po(7);
        let got_off = got_po.to_offset();

        assert_eq!(got_off.klen, 1);
        assert_eq!(got_off.vlen, 2);
        assert_eq!(got_off.position, 99);

        // set and read a signature
        bucket.set_signature(7, 0xABC);
        let signs = bucket.get_signatures_slice();

        assert_eq!(signs[7], 0xABC);
    }

    #[test]
    fn test_calc_threshold() {
        // w/ respect to this threshold = capacity * 4 / 5
        assert_eq!(BucketFile::calc_threshold(0), 0);
        assert_eq!(BucketFile::calc_threshold(5), 4);
        assert_eq!(BucketFile::calc_threshold(10), 8);
    }
}

pub struct Bucket {
    file: Arc<RwLock<BucketFile>>,
}

impl Bucket {
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let file = Self::open_bucket(path, capacity)?;

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
        })
    }

    fn open_bucket<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<BucketFile> {
        let file = match BucketFile::open(&path, capacity) {
            Ok(f) => f,

            Err(InternalError::InvalidFile) => {
                // returns IO error if something goes wrong
                std::fs::remove_file(&path)?;

                // try to reopen the file
                Self::open_bucket(path, capacity)?
            }

            Err(e) => return Err(e),
        };

        Ok(file)
    }

    pub fn set(&self, pair: &KVPair) -> InternalResult<bool> {
        let sign = Hasher::new(&pair.0).0;
        let mut file = self.write_lock()?;
        let meta = file.metadata_mut();

        // threshold has reached, so pair can not be inserted
        if file.get_inserted_count() >= file.threshold {
            return Ok(false);
        }

        let signs = file.get_signatures_slice();
        let start_idx = sign as usize % file.capacity;
        let (idx, is_new) = file.lookup_slot(start_idx, signs, sign, &pair.0)?;

        if is_new {
            meta.inserts.fetch_add(1, Ordering::SeqCst);
        }

        let po = file.write_slot(pair)?;
        file.set_signature(idx, sign);
        file.set_po(idx, po);

        return Ok(true);
    }

    pub fn get(&self, key: Key) -> InternalResult<Option<Vec<u8>>> {
        let sign = Hasher::new(&key).0;
        let file = self.read_lock()?;
        let signs = file.get_signatures_slice();
        let mut idx = sign as usize % file.capacity;

        for _ in 0..file.capacity {
            match signs[idx] {
                EMPTY_SIGN => return Ok(None),

                s if s == sign => {
                    let po = file.get_po(idx);
                    let (k, v) = file.read_slot(&po)?;

                    if key == k {
                        return Ok(Some(v));
                    }
                }

                _ => {}
            }

            idx = (idx + 1) % file.capacity;
        }

        Ok(None)
    }

    pub fn del(&self, key: Key) -> InternalResult<Option<Vec<u8>>> {
        let sign = Hasher::new(&key).0;
        let mut file = self.write_lock()?;

        let meta = file.metadata_mut();
        let signs = file.get_signatures_slice();
        let mut idx = sign as usize % file.capacity;

        for _ in 0..file.capacity {
            match signs[idx] {
                EMPTY_SIGN => return Ok(None),

                s if s == sign => {
                    let po = file.get_po(idx);
                    let (k, v) = file.read_slot(&po)?;

                    // found the key
                    if key == k {
                        // update meta and header
                        meta.inserts.fetch_sub(1, Ordering::SeqCst);
                        file.set_signature(idx, TOMBSTONE_SIGN);

                        return Ok(Some(v));
                    }
                }

                _ => {}
            }

            idx = (idx + 1) % file.capacity;
        }

        Ok(None)
    }

    pub fn iter(&self, start: &mut usize) -> InternalResult<Option<KVPair>> {
        let file = self.read_lock()?;
        let signs = file.get_signatures_slice();
        let cap = file.capacity;

        while *start < cap {
            let idx = *start;
            *start += 1;

            match signs[idx] {
                EMPTY_SIGN | TOMBSTONE_SIGN => {
                    continue;
                }

                _ => {
                    let p_offset = file.get_po(idx);
                    let pair = file.read_slot(&p_offset)?;

                    return Ok(Some(pair));
                }
            }
        }

        Ok(None)
    }

    pub fn iter_del(&self) -> InternalResult<Option<KVPair>> {
        let mut file = self.write_lock()?;
        let mut idx = file.get_iter_idx();

        let meta = file.metadata_mut();
        let signs = file.get_signatures_slice();
        let cap = file.capacity;

        while idx < cap {
            let cur_idx = idx;

            // increment for the next iteration
            file.increment_iter_idx();
            idx += 1;

            match signs[cur_idx] {
                EMPTY_SIGN | TOMBSTONE_SIGN => continue,

                _ => {
                    let p_offset = file.get_po(cur_idx);
                    let pair = file.read_slot(&p_offset)?;

                    meta.inserts.fetch_sub(1, Ordering::SeqCst);
                    file.set_signature(cur_idx, TOMBSTONE_SIGN);

                    return Ok(Some(pair));
                }
            }
        }

        Ok(None)
    }

    pub fn get_inserted_count(&self) -> InternalResult<usize> {
        let file = self.read_lock()?;
        let count = file.get_inserted_count();

        Ok(count)
    }

    pub fn get_threshold(&self) -> InternalResult<usize> {
        let file = self.read_lock()?;

        Ok(file.threshold)
    }

    pub fn get_capacity(&self) -> InternalResult<usize> {
        let file = self.read_lock()?;

        Ok(file.capacity)
    }

    /// Flush buckets data to disk
    ///
    /// NOTE: Requires write lock to the bucket
    pub fn flush(&self) -> InternalResult<()> {
        let mut write_lock = self.write_lock()?;

        write_lock.mmap.flush()?;
        write_lock.file.flush()?;

        Ok(())
    }

    // Acquire the read lock while mapping a poison error into [InternalError]
    fn read_lock(&self) -> Result<std::sync::RwLockReadGuard<'_, BucketFile>, InternalError> {
        Ok(self.file.read()?)
    }

    // Acquire the write lock while mapping a poison error into [InternalError]
    fn write_lock(&self) -> Result<std::sync::RwLockWriteGuard<'_, BucketFile>, InternalError> {
        Ok(self.file.write()?)
    }
}

impl Drop for Bucket {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[cfg(test)]
mod bucket_concurrency_tests {
    use super::*;
    use crate::common::{create_temp_dir, gen_dataset};

    fn create_bucket_with_cap(cap: usize) -> Bucket {
        let dir = create_temp_dir();
        let path = dir.path().join("bucket_conc.temp");

        Bucket::new(&path, cap).expect("New bucket instance")
    }

    #[test]
    fn concurrent_gets() {
        let count = 400usize;
        let cap = 2048usize;
        let dataset = gen_dataset(count);
        let bucket = create_bucket_with_cap(cap);

        for p in &dataset {
            assert!(bucket.set(p).unwrap(), "pre-insert should succeed");
        }

        let shared = std::sync::Arc::new(bucket);
        let num_threads = 12usize;
        let reads_per_thread = 300usize;
        let mut handles = Vec::with_capacity(num_threads);

        for t in 0..num_threads {
            let b = std::sync::Arc::clone(&shared);
            let keys = dataset.clone();

            let handle = std::thread::spawn(move || {
                for i in 0..reads_per_thread {
                    let idx = (t * reads_per_thread + i) % keys.len();
                    let (k, v) = &keys[idx];
                    let got = b.get(k.clone()).expect("get ok");
                    assert_eq!(got.expect("value present"), *v);
                }
            });

            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(shared.get_inserted_count().unwrap(), count);
    }

    #[test]
    fn concurrent_deletes_partitioned() {
        let count = 600usize;
        let cap = 4096usize;
        let dataset = gen_dataset(count);
        let bucket = create_bucket_with_cap(cap);

        for p in &dataset {
            assert!(bucket.set(p).unwrap());
        }

        let shared = std::sync::Arc::new(bucket);
        let num_threads = 8usize;
        let chunk = (count + num_threads - 1) / num_threads;
        let mut handles = Vec::with_capacity(num_threads);

        for t in 0..num_threads {
            let b = std::sync::Arc::clone(&shared);

            let slice: Vec<Vec<u8>> = dataset
                .iter()
                .skip(t * chunk)
                .take(chunk)
                .map(|(k, _)| k.clone())
                .collect();

            let handle = std::thread::spawn(move || {
                for key in slice {
                    let _ = b.del(key).expect("del ok");
                }
            });

            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(shared.get_inserted_count().unwrap(), 0);
    }

    #[test]
    fn concurrent_iter_del_workers() {
        let count = 500usize;
        let cap = 2048usize;
        let dataset = gen_dataset(count);
        let bucket = create_bucket_with_cap(cap);

        for p in &dataset {
            assert!(bucket.set(p).unwrap());
        }

        let shared = std::sync::Arc::new(bucket);
        let num_workers = 6usize;
        let mut handles = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let b = std::sync::Arc::clone(&shared);

            let handle = std::thread::spawn(move || {
                loop {
                    match b.iter_del() {
                        Ok(Some(_pair)) => continue, // removed one, keep going
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }
            });

            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(shared.get_inserted_count().unwrap(), 0);
    }

    #[test]
    fn mixed_set_get_del_workload() {
        let initial_count = 300usize;
        let new_count = 150usize;
        let cap = 4096usize;

        let initial_dataset = gen_dataset(initial_count);
        let bucket = create_bucket_with_cap(cap);

        for p in &initial_dataset {
            assert!(bucket.set(p).unwrap());
        }

        let shared = std::sync::Arc::new(bucket);
        let num_getters = 10usize;
        let getter_iters = 400usize;

        let initial_keys: Vec<Vec<u8>> = initial_dataset.iter().map(|(k, _)| k.clone()).collect();
        let mut handles = vec![];

        for g in 0..num_getters {
            let b = std::sync::Arc::clone(&shared);
            let keys = initial_keys.clone();

            let handle = std::thread::spawn(move || {
                for i in 0..getter_iters {
                    let idx = (g * getter_iters + i) % keys.len();

                    let _ = b.get(keys[idx].clone());
                }
            });

            handles.push(handle);
        }

        let num_del_threads = 6usize;
        let del_chunk = (initial_count + num_del_threads - 1) / num_del_threads;

        for t in 0..num_del_threads {
            let b = std::sync::Arc::clone(&shared);
            let slice: Vec<Vec<u8>> = initial_dataset
                .iter()
                .skip(t * del_chunk)
                .take(del_chunk)
                .map(|(k, _)| k.clone())
                .collect();

            let handle = std::thread::spawn(move || {
                for k in slice {
                    let _ = b.del(k).unwrap();
                }
            });

            handles.push(handle);
        }

        let num_set_threads = 4usize;
        let per_thread = (new_count + num_set_threads - 1) / num_set_threads;

        for t in 0..num_set_threads {
            let b = std::sync::Arc::clone(&shared);

            let start = t * per_thread;
            let end = ((t + 1) * per_thread).min(new_count);
            let mut pairs = Vec::with_capacity(end - start);

            for i in start..end {
                let key_val = (10_000_000usize + i) as u32; // offset far away from initial keys
                let key = key_val.to_be_bytes().to_vec();
                let v = key.clone();

                pairs.push((key, v));
            }

            let handle = std::thread::spawn(move || {
                for p in pairs {
                    let _ = b.set(&p).unwrap();
                }
            });

            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(shared.get_inserted_count().unwrap(), new_count);
    }

    #[test]
    fn concurrent_set_upserts_do_not_double_count() {
        let unique_keys = 50usize;
        let upserts_per_thread = 500usize;
        let threads = 8usize;
        let cap = 4096usize;

        let mut pairs = Vec::with_capacity(unique_keys);

        for i in 0..unique_keys {
            let key_val = (3000 + i) as u32;
            let key = key_val.to_be_bytes().to_vec();
            let val = key.clone();

            pairs.push((key, val));
        }

        let bucket = create_bucket_with_cap(cap);
        let shared = std::sync::Arc::new(bucket);
        let mut handles = Vec::new();

        for _ in 0..threads {
            let b = std::sync::Arc::clone(&shared);
            let p_clone = pairs.clone();

            let handle = std::thread::spawn(move || {
                for _ in 0..upserts_per_thread {
                    for p in &p_clone {
                        let _ = b.set(p).unwrap();
                    }
                }
            });

            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(shared.get_inserted_count().unwrap(), unique_keys);
    }
}

#[cfg(test)]
mod bucket_tests {
    use super::*;
    use crate::common::create_temp_dir;
    use std::io::{Seek, SeekFrom, Write};

    const CAP: usize = 8;

    fn create_bucket() -> Bucket {
        let dir = create_temp_dir();
        let path = dir.path().join("bucket.temp");

        Bucket::new(&path, CAP).expect("New bucket instance")
    }

    #[test]
    fn test_set_and_get() {
        let bucket = create_bucket();

        let key = b"foo".to_vec();
        let val = b"bar".to_vec();

        // empty => None
        assert_eq!(bucket.get(key.clone()).unwrap(), None);

        // insert and get
        bucket.set(&(key.clone(), val.clone())).unwrap();
        assert_eq!(bucket.get(key.clone()).unwrap(), Some(val.clone()));
    }

    #[test]
    fn test_delete_and_tombstone() {
        let bucket = create_bucket();

        let key = b"alpha".to_vec();
        let val = b"beta".to_vec();

        bucket.set(&(key.clone(), val.clone())).unwrap();
        let got = bucket.del(key.clone()).unwrap();

        // check returned value and try fetching again
        assert_eq!(got, Some(val.clone()));
        assert_eq!(bucket.get(key.clone()).unwrap(), None);

        // re-insert same key again
        let newval = b"gamma".to_vec();
        bucket.set(&(key.clone(), newval.clone())).unwrap();

        assert_eq!(bucket.get(key.clone()).unwrap(), Some(newval));
    }

    #[test]
    fn test_iter_only_live_entries() {
        let bucket = create_bucket();
        let mut inserted = Vec::new();

        // insert 3 keys
        for &s in &["one", "two", "three"] {
            let key = s.as_bytes().to_vec();
            let val = (s.to_uppercase()).as_bytes().to_vec();

            bucket.set(&(key.clone(), val.clone())).unwrap();
            inserted.push((key, val));
        }

        // delete "two"
        let key_to_delete = b"two".to_vec();

        let _ = bucket.del(key_to_delete.clone()).unwrap();

        // collect via iter
        let mut out = Vec::new();
        let mut idx = 0;

        while let Some((k, v)) = bucket.iter(&mut idx).unwrap() {
            out.push((k, v));
        }

        // should contain only "one" and "three"
        let mut expected = inserted;
        expected.retain(|(k, _)| k != &key_to_delete);

        assert_eq!(out.len(), expected.len());

        for pair in expected {
            assert!(out.contains(&pair));
        }
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = create_temp_dir();
        let path = dir.path().join("bucket.temp");

        let key = b"k".to_vec();
        let val = b"v".to_vec();

        // first session
        {
            let bucket = Bucket::new(&path, CAP).unwrap();

            bucket.set(&(key.clone(), val.clone())).unwrap();
        }

        // second session
        {
            let bucket2 = Bucket::new(&path, CAP).unwrap();
            let got = bucket2.get(key.clone()).unwrap();

            assert_eq!(got, Some(b"v".to_vec()));
        }
    }

    #[test]
    fn test_invalid_magic_or_version() {
        let dir = create_temp_dir();
        let path = dir.path().join("bucket.dat");

        // create a bucket normally
        {
            let _ = Bucket::new(&path, CAP).unwrap();
        }

        // Corrupt the header: change magic
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff, 0xff, 0xff, 0xff]).unwrap();
        file.flush().unwrap();

        // reopening should not throw an error
        match Bucket::new(&path, CAP) {
            Ok(_) => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_upsert_does_not_increment_and_overwrites() {
        let bucket = create_bucket();

        let key = b"dup".to_vec();

        bucket.set(&(key.clone(), b"one".to_vec())).unwrap();
        bucket.set(&(key.clone(), b"two".to_vec())).unwrap();

        assert_eq!(bucket.get(key.clone()).unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn test_zero_length_keys_and_values() {
        let bucket = create_bucket();

        // empty key, non-empty value
        let k1 = vec![];

        bucket.set(&(k1.clone(), b"V".to_vec())).unwrap();
        assert_eq!(bucket.get(k1.clone()).unwrap(), Some(b"V".to_vec()));

        // non-empty key, empty value
        let k2 = b"K".to_vec();

        bucket.set(&(k2.clone(), vec![])).unwrap();
        assert_eq!(bucket.get(k2.clone()).unwrap(), Some(vec![]));

        // both empty
        let k3 = vec![];

        bucket.set(&(k3.clone(), vec![])).unwrap();
        assert_eq!(bucket.get(k3.clone()).unwrap(), Some(vec![]));
    }

    #[test]
    fn test_del_on_nonexistent_returns_none() {
        let bucket = create_bucket();

        let key = b"ghost".to_vec();

        assert_eq!(bucket.del(key.clone()).unwrap(), None);
        assert_eq!(bucket.get(key).unwrap(), None);
    }

    #[test]
    fn test_file_offset_continues_after_reopen() {
        let dir = create_temp_dir();
        let path = dir.path().join("offset.temp");

        // first session: write 3 bytes
        {
            let bucket = Bucket::new(&path, CAP).unwrap();

            let key1 = b"A".to_vec(); // len = 1
            let val1 = b"111".to_vec(); // len = 3

            bucket.set(&(key1, val1)).unwrap();

            // NOTE: Bucket is dropped! So mmap will be flushed on drop
        }

        // second session: reopen, write 4 more bytes
        {
            let bucket = Bucket::new(&path, CAP).unwrap();

            let key2 = b"B".to_vec();
            let val2 = b"2222".to_vec(); // length = 4

            bucket.set(&(key2.clone(), val2.clone())).unwrap();

            let got = bucket.get(key2).unwrap();
            assert_eq!(got, Some(val2));
        }
    }

    #[test]
    fn test_random_inserts_and_gets() {
        const CAP: usize = 50;
        const NUM: usize = 40; // 80% of CAP

        let dir = create_temp_dir();
        let path = dir.path().join("random.temp");
        let bucket = Bucket::new(&path, CAP).unwrap();

        // very simple LCG for reproducible “random” bytes
        let mut seed: u32 = 0x1234_5678;

        fn next(seed: &mut u32) -> u32 {
            *seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            *seed
        }

        // generate, insert, and remember all pairs
        let mut entries = Vec::with_capacity(NUM);

        for _ in 0..NUM {
            let knum = next(&mut seed);
            let vnum = next(&mut seed);

            let key = knum.to_be_bytes().to_vec();
            let val = vnum.to_be_bytes().to_vec();

            bucket.set(&(key.clone(), val.clone())).unwrap();
            entries.push((key, val));
        }

        for (key, val) in &entries {
            let got = bucket.get(key.clone()).unwrap();

            assert_eq!(
                got.as_ref(),
                Some(val),
                "for key={:02X?} expected value={:02X?}, got={:?}",
                key,
                val,
                got
            );
        }

        // a key we never inserted should be absent
        let absent_key = next(&mut seed).to_be_bytes().to_vec();

        assert_eq!(
            bucket.get(absent_key).unwrap(),
            None,
            "unexpectedly found a value for a non‐existent key"
        );
    }
}
