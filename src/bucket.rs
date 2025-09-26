//! A `Bucket` is an on-disk, immutable, append-only HashTable to store the
//! KeyValue pairs. To reduce I/O and achieve performance it uses a
//! fix sized, memory-mapped Header.

use crate::error::{InternalError, InternalResult};
use hasher::{Hasher, EMPTY_SIGN, TOMBSTONE_SIGN};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

/// ----------------------------------------
/// Constants and Types
/// ----------------------------------------

const VERSION: u32 = 2;
const MAGIC: [u8; 4] = *b"TCv2";

type Sign = u32;
pub(crate) type KeyValue = (Vec<u8>, Vec<u8>);
pub(crate) type Key = Vec<u8>;

/// ----------------------------------------
/// Namespaces
/// ----------------------------------------

/// This acts as an id for an item stored in [Bucket]
///
/// NOTE: The *index* must start from `0` cause the 0th item,
/// [Base] in here, acts as an default in the [BucketFile].
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Namespace {
    Base = 0,
    List = 1,
    ListItem = 2,
    Queue = 3,
    QueueItem = 4,
    Set = 5,
    SetItem = 6,
}

impl From<Namespace> for u8 {
    fn from(ns: Namespace) -> u8 {
        ns as u8
    }
}

impl TryFrom<u8> for Namespace {
    type Error = InternalError;

    fn try_from(value: u8) -> InternalResult<Namespace> {
        match value {
            0 => Ok(Namespace::Base),
            1 => Ok(Namespace::List),
            2 => Ok(Namespace::ListItem),
            3 => Ok(Namespace::Queue),
            4 => Ok(Namespace::QueueItem),
            5 => Ok(Namespace::Set),
            6 => Ok(Namespace::SetItem),
            err => Err(InternalError::InvalidEntry(
                "Invalid namespace: {err}".into(),
            )),
        }
    }
}

/// ----------------------------------------
/// Bucket File (Pair)
/// ----------------------------------------

#[repr(align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pair {
    ns: Namespace,
    klen: u16,
    vlen: u16,
    offset: u64,
}

type RawPair = [u8; 10];

impl Pair {
    fn to_raw(&self) -> RawPair {
        let mut out = [0u8; 10];

        // namespace
        out[0] = self.ns as u8;

        // klen (LE)
        out[1..3].copy_from_slice(&self.klen.to_le_bytes());

        // vlen (LE)
        out[3..5].copy_from_slice(&self.vlen.to_le_bytes());

        // offset (only 5 bytes, LE)
        let offset_bytes = self.offset.to_le_bytes();
        out[5..10].copy_from_slice(&offset_bytes[..5]);

        out
    }

    fn from_raw(slice: RawPair) -> InternalResult<Pair> {
        let ns = Namespace::try_from(slice[0])?;

        let klen = u16::from_le_bytes([slice[1], slice[2]]);
        let vlen = u16::from_le_bytes([slice[3], slice[4]]);

        let mut offset_bytes = [0u8; 8];
        offset_bytes[..5].copy_from_slice(&slice[5..10]);
        let offset = u64::from_le_bytes(offset_bytes);

        Ok(Pair {
            ns,
            klen,
            vlen,
            offset,
        })
    }
}

#[cfg(test)]
mod pair_tests {
    use super::{Namespace, Pair, RawPair};
    use sphur::Sphur;

    #[test]
    fn test_basic_round_trip() {
        let p = Pair {
            ns: Namespace::Base,
            offset: 123456789,
            klen: 100,
            vlen: 200,
        };

        let encoded = p.to_raw();
        let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

        assert_eq!(p.ns, decoded.ns);
        assert_eq!(p.offset, decoded.offset);
        assert_eq!(p.klen, decoded.klen);
        assert_eq!(p.vlen, decoded.vlen);
    }

    #[test]
    fn test_boundaries() {
        let p = Pair {
            ns: Namespace::Base,
            offset: 0,
            klen: 0,
            vlen: 0,
        };

        let encoded = p.to_raw();
        let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

        assert_eq!(p, decoded);

        let max_off = (1u64 << 40) - 1;
        let max_klen = u16::MAX - 1;
        let max_vlen = u16::MAX - 1;

        let p2 = Pair {
            ns: Namespace::Base,
            offset: max_off,
            klen: max_klen,
            vlen: max_vlen,
        };

        let encoded = p2.to_raw();
        let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

        assert_eq!(p2, decoded);
    }

    #[test]
    fn test_randomized_values() {
        let mut rng = Sphur::new();

        for i in 0..100 {
            let offset = (i * 1234567) as u64 & ((1u64 << 40) - 1);
            let klen = (i * 37 % (1 << 16)) as u16;
            let vlen = (i * 91 % (1 << 16)) as u16;

            let r = rng.gen_range(0..=6) as u8;
            let ns = Namespace::try_from(r).expect("Expected valid [Namespace]");

            let p = Pair {
                ns: ns,
                offset,
                klen,
                vlen,
            };

            let encoded = p.to_raw();
            let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

            assert_eq!(p, decoded, "Failed at iteration {i}");
        }
    }
}

/// ----------------------------------------
/// Bucket File (Meta)
/// ----------------------------------------

#[repr(C)]
struct Meta {
    magic: [u8; 4],
    version: u32,
    inserts: AtomicU64,
    write_pointer: AtomicU64,
}

impl Meta {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            inserts: AtomicU64::new(0),
            write_pointer: AtomicU64::new(0),
        }
    }
}

/// ----------------------------------------
/// Bucket File
/// ----------------------------------------

struct BucketFile {
    mmap: MmapMut,
    file: File,
    capacity: usize,
    header_size: usize,
    sign_offset: usize,
    pair_offset: usize,
    threshold: usize,
}

impl BucketFile {
    fn new<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let header_size = Self::calc_header_size(capacity);
        let sign_offset = size_of::<Meta>();
        let pair_offset = sign_offset + capacity * size_of::<Sign>();
        let threshold = Self::calc_threshold(capacity);

        // zero-init the file
        file.set_len(header_size as u64)?;

        let mut mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;

        // init the meta w/ file version and magic
        unsafe {
            let meta_ptr = mmap.as_mut_ptr() as *mut Meta;
            meta_ptr.write(Meta::default());
        }

        Ok(Self {
            file,
            mmap,
            capacity,
            header_size,
            sign_offset,
            pair_offset,
            threshold,
        })
    }

    fn open<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let file = OpenOptions::new()
            // Create the file just in case to avoid crash
            //
            // NOTE: If we throw IO error for non-existing file it'll be propogated
            // to the users, so we create the file and throw an invalid file error
            // as if the file is new, its not a valid [BucketFile] yet!
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        let header_size = Self::calc_header_size(capacity);
        let sign_offset = size_of::<Meta>();
        let pair_offset = sign_offset + capacity * size_of::<Sign>();
        let threshold = Self::calc_threshold(capacity);

        let file_len = file.metadata()?.len();

        // NOTE: If `file.len()` is smaller then `header_size`, it's a sign of
        // invalid initilization or the file was tampered with! In this scenerio,
        // we delete the file and create it again!
        if file_len < header_size as u64 {
            return Err(InternalError::InvalidFile);
        }

        let mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;
        let meta = unsafe { &*(mmap.as_ptr() as *const Meta) };

        // NOTE: while validating version and magic of the file, if not matched,
        // we should simply delete the file, as we do not have any earlier
        // versions to support.
        if meta.magic != MAGIC || meta.version != VERSION {
            return Err(InternalError::InvalidFile);
        }

        // safeguard for the write pointer
        if meta.write_pointer.load(Ordering::Relaxed) > file_len {
            return Err(InternalError::InvalidFile);
        }

        // safeguard for the insert count
        if meta.inserts.load(Ordering::Relaxed) > capacity as u64 {
            return Err(InternalError::InvalidFile);
        }

        Ok(Self {
            file,
            mmap,
            capacity,
            header_size,
            sign_offset,
            pair_offset,
            threshold,
        })
    }

    /// Calculate the size of header based on given capacity for [Bucket]
    ///
    /// ### Size Calculation
    ///
    /// `sizeof(Meta) + (sizeof(Sign) * CAP) + (sizeof(PairRaw) * CAP)`
    #[inline(always)]
    const fn calc_header_size(capacity: usize) -> usize {
        size_of::<Meta>() + (size_of::<Sign>() * capacity) + (size_of::<RawPair>() * capacity)
    }

    /// Calculate threshold w/ given capacity for [Bucket]
    ///
    /// NOTE: It's 80% of given capacity
    #[inline(always)]
    const fn calc_threshold(cap: usize) -> usize {
        cap.saturating_mul(4) / 5
    }

    /// Returns an immutable reference to [Meta]
    #[inline(always)]
    fn meta(&self) -> &Meta {
        unsafe { &*(self.mmap.as_ptr() as *const Meta) }
    }

    /// Returns a mutable reference to [Meta]
    #[inline(always)]
    fn meta_mut(&self) -> &mut Meta {
        unsafe { &mut *(self.mmap.as_ptr() as *mut Meta) }
    }

    #[inline(always)]
    fn get_inserted_count(&self) -> usize {
        self.meta().inserts.load(Ordering::Acquire) as usize
    }

    #[inline(always)]
    fn incr_inserted_count(&self) {
        self.meta_mut().inserts.fetch_add(1, Ordering::Release);
    }

    #[inline(always)]
    fn decr_inserted_count(&self) {
        self.meta_mut().inserts.fetch_sub(1, Ordering::Release);
    }

    /// Read a single [PairRaw] from an index, directly from the mmap
    #[inline(always)]
    fn get_pair(&self, idx: usize) -> RawPair {
        unsafe {
            let ptr = self.mmap.as_ptr().add(self.pair_offset) as *const RawPair;
            *ptr.add(idx)
        }
    }

    /// Write a new [PairRaw] at given index
    #[inline(always)]
    fn set_pair(&mut self, idx: usize, pair: RawPair) {
        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.pair_offset) as *mut RawPair;
            *ptr.add(idx) = pair;
        }
    }

    /// Returns an immutable reference to signatures slice
    #[inline(always)]
    fn get_signs(&self) -> &[Sign] {
        unsafe {
            let ptr = self.mmap.as_ptr().add(self.sign_offset) as *const Sign;
            core::slice::from_raw_parts(ptr, self.capacity)
        }
    }

    /// Write a new [Sign] at given index
    #[inline(always)]
    fn set_sign(&mut self, idx: usize, sign: Sign) {
        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.sign_offset) as *mut Sign;
            *ptr.add(idx) = sign;
        }
    }

    /// Read a [KeyValue] from a given [PairRaw]
    fn get_slot(&self, raw: RawPair) -> InternalResult<KeyValue> {
        let pair = Pair::from_raw(raw)?;
        let klen = pair.klen as usize;
        let vlen = pair.vlen as usize;

        let mut buf = vec![0u8; klen + vlen];
        Self::read_exact_at(&self.file, &mut buf, self.header_size as u64 + pair.offset)?;

        let vbuf = buf[klen..(klen + vlen)].to_owned();
        buf.truncate(klen);

        Ok((buf, vbuf))
    }

    /// Write a [KeyValue] to the bucket and get [Pair]
    fn set_slot(&self, pair: &KeyValue) -> InternalResult<RawPair> {
        let klen = pair.0.len();
        let vlen = pair.1.len();
        let blen = klen + vlen;

        let mut buf = vec![0u8; blen];

        buf[..klen].copy_from_slice(&pair.0);
        buf[klen..].copy_from_slice(&pair.1);

        let offset = self
            .meta()
            .write_pointer
            .fetch_add(blen as u64, Ordering::Release);

        Self::write_all_at(&self.file, &buf, self.header_size as u64 + offset)?;

        let pair = Pair {
            klen: klen as u16,
            vlen: vlen as u16,
            offset,
            ns: Namespace::Base,
        };

        Ok(pair.to_raw())
    }

    #[allow(unused)]
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
                    let po = self.get_pair(idx);
                    let (existing_key, _) = self.get_slot(po)?;

                    if existing_key == kbuf {
                        return Ok((idx, false));
                    }
                }

                _ => {}
            }

            idx = (idx + 1) % self.capacity;
        }

        Err(InternalError::BucketFull)
    }

    /// Read N bytes at a given offset
    ///
    /// NOTE: this mimics the `pread` syscall on linux
    #[cfg(unix)]
    fn read_exact_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }

    /// Write a buffer to a file at a given offset
    ///
    /// NOTE: this mimic the `pwrite` syscall on linux
    #[cfg(unix)]
    fn write_all_at(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
    }

    /// Read N bytes at a given offset
    ///
    /// NOTE: this mimics the `pread` syscall on linux
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

    /// Write a buffer to a file at a given offset
    ///
    /// NOTE: this mimic the `pwrite` syscall on linux
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
    use tempfile::tempfile;

    const TEST_CAP: usize = 16;

    fn gen_rand() -> u32 {
        let mut rng = sphur::Sphur::new();
        rng.gen_u32()
    }

    fn open_bucket() -> BucketFile {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp
            .path()
            .join(format!("bucket_file_tests_{}", rand.to_string()));

        BucketFile::new(path, TEST_CAP).expect("create bucket")
    }

    #[test]
    fn test_new_file() {
        let bucket = open_bucket();
        let meta = bucket.meta();

        // MAGIC and VERSION should be set
        assert_eq!(meta.magic, MAGIC);
        assert_eq!(meta.version, VERSION);

        // inserts and iter idx start at zero
        assert_eq!(meta.inserts.load(Ordering::SeqCst), 0);

        // signatures and pair-offset regions should be zero
        let signs = bucket.get_signs();
        assert!(signs.iter().all(|&s| s == 0));

        for idx in 0..bucket.capacity {
            let raw = bucket.get_pair(idx);
            let pair = Pair::from_raw(raw).expect("Extract Pair from raw data");

            assert_eq!(pair.klen, 0);
            assert_eq!(pair.vlen, 0);
            assert_eq!(pair.offset, 0);
            assert_eq!(pair.ns, Namespace::Base);
        }
    }

    #[test]
    fn test_file_reopen() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp.path().join(rand.to_string());

        // create & close file
        let init = BucketFile::new(&path, TEST_CAP).expect("create bucket");
        drop(init);

        // reopen and validate
        let reopen = BucketFile::open(path, TEST_CAP).expect("open bucket");
        let meta = reopen.meta();

        assert_eq!(meta.magic, MAGIC);
        assert_eq!(meta.version, VERSION);
    }

    #[test]
    fn test_capacity_mismatch() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp.path().join(rand.to_string());

        {
            let _ = BucketFile::new(&path, TEST_CAP).unwrap();
        }

        // at re-open, cause of the wrong cap, it should throw an error
        match BucketFile::open(path, TEST_CAP * 2) {
            Ok(_) => panic!("Wrong cap should throw an error"),
            Err(_) => {}
        }
    }

    #[test]
    fn test_write_and_read_roundtrip() {
        let mut bucket = open_bucket();

        let kv = (b"key1".to_vec(), b"value1".to_vec());
        let sign = 0xDEADBEEF;
        let pair = bucket.set_slot(&kv).unwrap();

        bucket.set_pair(3, pair);
        bucket.set_sign(3, sign);
        bucket.meta_mut().inserts.fetch_add(1, Ordering::SeqCst);

        let signs = bucket.get_signs();
        let (found_idx, is_new) = bucket.lookup_slot(3, signs, sign, &kv.0).unwrap();

        assert_eq!(found_idx, 3);
        assert!(!is_new, "existing key should report is_new=false");

        let po_back = bucket.get_pair(3);
        let kv_back = bucket.get_slot(po_back).unwrap();

        assert_eq!(kv_back, kv);
    }

    #[test]
    fn test_sign_lookup_empty_and_tombstone() {
        let mut bucket = open_bucket();

        let signs = bucket.get_signs();
        let (idx, is_new) = bucket.lookup_slot(5, signs, 42, b"whatever").unwrap();

        assert_eq!(idx, 5);
        assert!(is_new);

        bucket.set_sign(5, TOMBSTONE_SIGN);

        let signs2 = bucket.get_signs();
        let (idx2, is_new2) = bucket.lookup_slot(5, signs2, 42, b"new").unwrap();

        assert_eq!(idx2, 5);
        assert!(is_new2);
    }

    #[test]
    fn test_pair_and_sign_mutation() {
        let mut bucket = open_bucket();

        // set a fake PairOffset at idx 7
        let fake_pair = Pair {
            klen: 1,
            vlen: 2,
            offset: 99,
            ns: Namespace::Base,
        };

        let pair = fake_pair.to_raw();
        bucket.set_pair(7, pair);

        let got_pair = bucket.get_pair(7);
        let got_off = Pair::from_raw(got_pair).unwrap();

        assert_eq!(got_off.klen, 1);
        assert_eq!(got_off.vlen, 2);
        assert_eq!(got_off.offset, 99);
        assert_eq!(got_off.ns, Namespace::Base);

        // set and read a signature
        bucket.set_sign(7, 0xABC);
        let signs = bucket.get_signs();

        assert_eq!(signs[7], 0xABC);
    }

    #[test]
    fn test_calc_threshold() {
        assert_eq!(BucketFile::calc_threshold(0), 0);
        assert_eq!(BucketFile::calc_threshold(5), 4);
        assert_eq!(BucketFile::calc_threshold(10), 8);
    }
}

/// ----------------------------------------
/// Bucket
/// ----------------------------------------

pub(crate) struct Bucket {
    file: BucketFile,
    iter_idx: usize,
}

impl Bucket {
    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let file = match BucketFile::open(&path, capacity) {
            Ok(f) => f,

            Err(InternalError::InvalidFile) => {
                // returns IO error if something goes wrong
                std::fs::remove_file(&path)?;

                // now we create a new bucket file
                //
                // NOTE: if the same or any error occurs again,
                // we simply throw it out!
                BucketFile::new(&path, capacity)?
            }

            Err(e) => return Err(e),
        };

        Ok(Self { file, iter_idx: 0 })
    }

    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let file = BucketFile::new(path, capacity)?;

        Ok(Self { file, iter_idx: 0 })
    }

    pub fn set(&mut self, kv: &KeyValue) -> InternalResult<bool> {
        let sign = Hasher::new(&kv.0);

        // threshold has reached, so pair can not be inserted
        if self.file.get_inserted_count() >= self.file.threshold {
            return Ok(false);
        }

        let signs = self.file.get_signs();
        let start_idx = sign as usize % self.file.capacity;
        let (idx, is_new) = self.file.lookup_slot(start_idx, signs, sign, &kv.0)?;

        if is_new {
            self.file.incr_inserted_count();
        }

        let pair = self.file.set_slot(kv)?;

        self.file.set_sign(idx, sign);
        self.file.set_pair(idx, pair);

        return Ok(true);
    }

    pub fn get(&self, key: Key) -> InternalResult<Option<Vec<u8>>> {
        let sign = Hasher::new(&key);
        let signs = self.file.get_signs();
        let mut idx = sign as usize % self.file.capacity;

        for _ in 0..self.file.capacity {
            match signs[idx] {
                EMPTY_SIGN => return Ok(None),

                s if s == sign => {
                    let po = self.file.get_pair(idx);
                    let (k, v) = self.file.get_slot(po)?;

                    if key == k {
                        return Ok(Some(v));
                    }
                }

                _ => {}
            }

            idx = (idx + 1) % self.file.capacity;
        }

        Ok(None)
    }

    pub fn del(&mut self, key: Key) -> InternalResult<Option<Vec<u8>>> {
        let sign = Hasher::new(&key);

        let signs = self.file.get_signs();
        let mut idx = sign as usize % self.file.capacity;

        for _ in 0..self.file.capacity {
            match signs[idx] {
                EMPTY_SIGN => return Ok(None),

                s if s == sign => {
                    let po = self.file.get_pair(idx);
                    let (k, v) = self.file.get_slot(po)?;

                    // found the key
                    if key == k {
                        // update meta and header
                        self.file.decr_inserted_count();
                        self.file.set_sign(idx, TOMBSTONE_SIGN);

                        return Ok(Some(v));
                    }
                }

                _ => {}
            }

            idx = (idx + 1) % self.file.capacity;
        }

        Ok(None)
    }

    pub fn iter(&self, start: &mut usize) -> InternalResult<Option<KeyValue>> {
        let file = &self.file;
        let signs = file.get_signs();
        let cap = file.capacity;

        while *start < cap {
            let idx = *start;
            *start += 1;

            match signs[idx] {
                EMPTY_SIGN | TOMBSTONE_SIGN => {
                    continue;
                }

                _ => {
                    let p_offset = file.get_pair(idx);
                    let pair = file.get_slot(p_offset)?;

                    return Ok(Some(pair));
                }
            }
        }

        Ok(None)
    }

    pub fn iter_del(&mut self) -> InternalResult<Option<KeyValue>> {
        let mut file = &mut self.file;

        let signs = file.get_signs();
        let cap = file.capacity;

        while self.iter_idx < cap {
            let cur_idx = self.iter_idx;

            // increment for the next iteration
            self.iter_idx += 1;

            match signs[cur_idx] {
                EMPTY_SIGN | TOMBSTONE_SIGN => continue,

                _ => {
                    let p_offset = file.get_pair(cur_idx);
                    let pair = file.get_slot(p_offset)?;

                    file.decr_inserted_count();
                    file.set_sign(cur_idx, TOMBSTONE_SIGN);

                    return Ok(Some(pair));
                }
            }
        }

        Ok(None)
    }

    pub fn get_inserted_count(&self) -> InternalResult<usize> {
        let count = self.file.get_inserted_count();
        Ok(count)
    }

    pub fn get_threshold(&self) -> InternalResult<usize> {
        Ok(self.file.threshold)
    }

    pub fn get_capacity(&self) -> InternalResult<usize> {
        Ok(self.file.capacity)
    }

    /// Flush buckets data to disk
    ///
    /// NOTE: Requires write lock to the bucket
    pub fn flush(&mut self) -> InternalResult<()> {
        self.file.mmap.flush()?;
        self.file.file.flush()?;

        Ok(())
    }
}

impl Drop for Bucket {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[cfg(test)]
mod bucket_tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};

    const TEST_CAP: usize = 8;

    fn gen_rand() -> u32 {
        let mut rng = sphur::Sphur::new();
        rng.gen_u32()
    }

    fn open_bucket() -> Bucket {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp
            .path()
            .join(format!("bucket_tests_{}", rand.to_string()));

        Bucket::new(path, TEST_CAP).expect("create bucket")
    }

    #[test]
    fn test_set_and_get() {
        let mut bucket = open_bucket();

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
        let mut bucket = open_bucket();

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
        let mut bucket = open_bucket();
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
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp
            .path()
            .join(format!("bucket_tests_{}", rand.to_string()));

        let key = b"k".to_vec();
        let val = b"v".to_vec();

        // first session
        {
            let mut bucket = Bucket::new(&path, TEST_CAP).unwrap();
            bucket.set(&(key.clone(), val.clone())).unwrap();
        }

        // second session
        {
            let mut bucket2 = Bucket::open(&path, TEST_CAP).unwrap();
            let got = bucket2.get(key.clone()).unwrap();

            assert_eq!(got, Some(b"v".to_vec()));
        }
    }

    #[test]
    fn test_reinit_on_capacity_mismatch() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp.path().join(rand.to_string());

        {
            let mut bucket = Bucket::new(&path, TEST_CAP).unwrap();
            bucket.set(&(vec![0], vec![1])).unwrap();
        }

        // at re-open, cause of the wrong cap, it must re-init the file
        let bucket = Bucket::open(path, TEST_CAP * 2).unwrap();

        assert_eq!(bucket.get(vec![0]).unwrap(), None);
        assert_eq!(bucket.get_inserted_count().unwrap(), 0);
    }

    #[test]
    fn test_invalid_magic_or_version() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp
            .path()
            .join(format!("bucket_tests_{}", rand.to_string()));

        // create a bucket normally
        {
            let _ = Bucket::new(&path, TEST_CAP).unwrap();
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
        match Bucket::open(&path, TEST_CAP) {
            Ok(_) => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_upsert_does_not_increment_and_overwrites() {
        let mut bucket = open_bucket();

        let key = b"dup".to_vec();

        bucket.set(&(key.clone(), b"one".to_vec())).unwrap();
        bucket.set(&(key.clone(), b"two".to_vec())).unwrap();

        assert_eq!(bucket.get(key.clone()).unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn test_zero_length_keys_and_values() {
        let mut bucket = open_bucket();

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
        let mut bucket = open_bucket();

        let key = b"ghost".to_vec();

        assert_eq!(bucket.del(key.clone()).unwrap(), None);
        assert_eq!(bucket.get(key).unwrap(), None);
    }

    #[test]
    fn test_file_offset_continues_after_reopen() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp
            .path()
            .join(format!("bucket_tests_{}", rand.to_string()));

        // first session: write 3 bytes
        {
            let mut bucket = Bucket::new(&path, TEST_CAP).expect("create bucket");

            let key1 = b"A".to_vec(); // len = 1
            let val1 = b"111".to_vec(); // len = 3

            bucket.set(&(key1, val1)).unwrap();

            // NOTE: Bucket is dropped! So mmap will be flushed on drop
        }

        // second session: reopen, write 4 more bytes
        {
            let mut bucket = Bucket::open(path, TEST_CAP).expect("create bucket");

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

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp
            .path()
            .join(format!("bucket_tests_{}", rand.to_string()));

        let mut bucket = Bucket::new(path, CAP).expect("create bucket");

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

    #[test]
    fn test_inserted_count_accuracy() {
        let mut bucket = open_bucket();
        assert_eq!(bucket.get_inserted_count().unwrap(), 0);

        bucket.set(&(vec![1], b"one".to_vec())).unwrap();
        bucket.set(&(vec![2], b"two".to_vec())).unwrap();
        assert_eq!(bucket.get_inserted_count().unwrap(), 2);

        bucket.del(vec![1]).unwrap();
        assert_eq!(
            bucket.get_inserted_count().unwrap(),
            1,
            "deletes should reduce live count"
        );

        bucket.set(&(vec![1], b"one-again".to_vec())).unwrap();
        assert_eq!(
            bucket.get_inserted_count().unwrap(),
            2,
            "reinserts should restore count"
        );
    }

    #[test]
    fn test_reopen_preserves_counts() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp
            .path()
            .join(format!("bucket_reopen_counts_{}", rand.to_string()));

        // first session
        {
            let mut bucket = Bucket::new(&path, TEST_CAP).unwrap();

            bucket.set(&(vec![1], b"a".to_vec())).unwrap();
            bucket.set(&(vec![2], b"b".to_vec())).unwrap();
            bucket.del(vec![1]).unwrap();

            // live count should be 1
            assert_eq!(bucket.get_inserted_count().unwrap(), 1);
        }

        // reopen and validate
        {
            let bucket = Bucket::open(&path, TEST_CAP).unwrap();

            assert_eq!(bucket.get_inserted_count().unwrap(), 1);
            assert_eq!(bucket.get(vec![2]).unwrap(), Some(b"b".to_vec()));
            assert_eq!(bucket.get(vec![1]).unwrap(), None);
        }
    }

    #[test]
    fn test_iter_matches_inserted_count() {
        let mut bucket = open_bucket();

        // insert up to threshold (80% of capacity)
        let threshold = bucket.get_threshold().unwrap();

        for i in 0..threshold {
            let key = vec![(i as u8).wrapping_add(1)];
            let val = format!("v{}", i).into_bytes();

            bucket.set(&(key, val)).unwrap();
        }

        // delete a couple of entries
        bucket.del(vec![1]).unwrap();
        bucket.del(vec![3]).unwrap();

        // iterate and count live entries
        let mut idx = 0usize;
        let mut found = Vec::new();

        while let Some((k, v)) = bucket.iter(&mut idx).unwrap() {
            found.push((k, v));
        }

        assert_eq!(found.len(), bucket.get_inserted_count().unwrap());
    }

    #[test]
    fn test_threshold_with_deletes() {
        let mut bucket = open_bucket();
        let threshold = bucket.get_threshold().unwrap();

        // fill until threshold
        for i in 0..threshold {
            let key = vec![(i as u8).wrapping_add(1)];

            bucket.set(&(key, vec![0u8])).unwrap();
        }

        // the next insert should be rejected (returns false)
        assert_eq!(
            bucket.set(&(vec![0xFF], b"overflow".to_vec())).unwrap(),
            false
        );

        // delete one and now insert should succeed
        bucket.del(vec![1]).unwrap();

        assert_eq!(
            bucket.set(&(vec![0xFF], b"overflow".to_vec())).unwrap(),
            true
        );

        // live count should equal threshold again
        assert_eq!(bucket.get_inserted_count().unwrap(), threshold);
    }

    #[test]
    fn test_iter_del_persistence() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp
            .path()
            .join(format!("bucket_iter_del_{}", rand.to_string()));

        // first session: insert two keys and tombstone one via iter_del()
        let deleted_key: Vec<u8>;
        {
            let mut bucket = Bucket::new(&path, TEST_CAP).unwrap();

            let k1 = vec![1u8];
            let k2 = vec![2u8];

            bucket.set(&(k1.clone(), b"foo".to_vec())).unwrap();
            bucket.set(&(k2.clone(), b"bar".to_vec())).unwrap();

            // tombstone one entry via iter_del()
            let deleted = bucket
                .iter_del()
                .unwrap()
                .expect("expected one deleted pair");

            deleted_key = deleted.0.clone();

            // the deleted key must be gone in the same session
            assert_eq!(bucket.get(deleted_key.clone()).unwrap(), None);

            // the other key must still exist
            let other_key = if deleted_key == k1 {
                k2.clone()
            } else {
                k1.clone()
            };

            assert!(bucket.get(other_key).unwrap().is_some());
        }

        // reopen and validate tombstone persisted
        {
            let bucket = Bucket::open(&path, TEST_CAP).unwrap();
            assert_eq!(bucket.get(deleted_key.clone()).unwrap(), None);

            // ensure at least one other key is still present
            let other_key = if deleted_key == vec![1u8] {
                vec![2u8]
            } else {
                vec![1u8]
            };

            assert!(bucket.get(other_key).unwrap().is_some());
        }
    }
}
