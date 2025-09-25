//! A `Bucket` is an on-disk, immutable, append-only HashTable to store the
//! Key-Value pairs. It uses a fix sized, memory-mapped Header.

use crate::error::{InternalError, InternalResult};
use hasher::{EMPTY_SIGN, TOMBSTONE_SIGN};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

/// ----------------------------------------
/// Constants and Types
/// ----------------------------------------

/// File version for [Bucket] file
const VERSION: u32 = 2;

/// Magic Id for [Bucket] file
const MAGIC: [u8; 4] = *b"TCv2";

/// Hash signature of a key
type Sign = u32;

/// A custom type for Key-Value pair object
pub(crate) type KeyValue = (Vec<u8>, Vec<u8>);

/// A custom type for Key object
pub(crate) type Key = Vec<u8>;

/// ----------------------------------------
/// Namespaces
/// ----------------------------------------

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

#[repr(align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pair {
    ns: Namespace,
    klen: u16,
    vlen: u16,
    offset: u64,
}

/// ----------------------------------------
/// Bucket File (Pair)
/// ----------------------------------------

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PairRaw([u8; 10]);

impl PairRaw {
    fn to_raw(pair: Pair) -> Self {
        let mut out = [0u8; 10];

        // namespace
        out[0] = pair.ns as u8;

        // klen (LE)
        out[1..3].copy_from_slice(&pair.klen.to_le_bytes());

        // vlen (LE)
        out[3..5].copy_from_slice(&pair.vlen.to_le_bytes());

        // offset (only 5 bytes, LE)
        let offset_bytes = pair.offset.to_le_bytes();
        out[5..10].copy_from_slice(&offset_bytes[..5]);

        Self(out)
    }

    fn from_raw(&self) -> InternalResult<Pair> {
        let slice = self.0;
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
    use super::{Namespace, Pair, PairRaw};
    use sphur::Sphur;

    #[test]
    fn test_basic_round_trip() {
        let p = Pair {
            ns: Namespace::Base,
            offset: 123456789,
            klen: 100,
            vlen: 200,
        };

        let encoded = PairRaw::to_raw(p);
        let decoded = encoded.from_raw().expect("Decode raw pair");

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

        let encoded = PairRaw::to_raw(p);
        let decoded = encoded.from_raw().expect("Decode raw pair");

        assert_eq!(p, decoded);

        let max_off = (1u64 << 40) - 1;
        let max_klen = (1u16 << 12) - 1;
        let max_vlen = (1u16 << 12) - 1;

        let p2 = Pair {
            ns: Namespace::Base,
            offset: max_off,
            klen: max_klen,
            vlen: max_vlen,
        };

        let encoded = PairRaw::to_raw(p2);
        let decoded = encoded.from_raw().expect("Decode raw pair");

        assert_eq!(p2, decoded);
    }

    #[test]
    fn test_randomized_values() {
        let mut rng = Sphur::new();

        for i in 0..100 {
            let offset = (i * 1234567) as u64 & ((1u64 << 40) - 1);
            let klen = (i * 37 % (1 << 12)) as u16;
            let vlen = (i * 91 % (1 << 12)) as u16;

            let r = rng.gen_range(0..=6) as u8;
            let ns = Namespace::try_from(r).expect("Expected valid [Namespace]");

            let p = Pair {
                ns: ns,
                offset,
                klen,
                vlen,
            };

            let encoded = PairRaw::to_raw(p);
            let decoded = encoded.from_raw().expect("Decode raw pair");

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
    iter_idx: AtomicU64,
    capacity: AtomicU64,
    write_pointer: AtomicU64,
}

impl Meta {
    fn default(capacity: u64) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            inserts: AtomicU64::new(0),
            iter_idx: AtomicU64::new(0),
            capacity: AtomicU64::new(capacity),
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
            .truncate(false)
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
            meta_ptr.write(Meta::default(capacity as u64));
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

        // safeguard for the insert index
        if meta.iter_idx.load(Ordering::Relaxed) > meta.capacity.load(Ordering::Relaxed) {
            return Err(InternalError::InvalidFile);
        }

        // safeguard for the insert count
        if meta.inserts.load(Ordering::Relaxed) > meta.capacity.load(Ordering::Relaxed) {
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
        size_of::<Meta>() + (size_of::<Sign>() * capacity) + (size_of::<PairRaw>() * capacity)
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
    fn increment_inserted_count(&self) {
        self.meta_mut().inserts.store(1, Ordering::Release);
    }

    #[inline(always)]
    fn get_iter_idx(&self) -> usize {
        self.meta().iter_idx.load(Ordering::Acquire) as usize
    }

    #[inline(always)]
    fn get_capacity(&self) -> usize {
        self.meta().capacity.load(Ordering::Acquire) as usize
    }

    #[inline(always)]
    fn increment_iter_idx(&self) {
        self.meta_mut().iter_idx.store(1, Ordering::Release);
    }

    /// Read a single [PairRaw] from an index, directly from the mmap
    #[inline(always)]
    fn get_pair(&self, idx: usize) -> PairRaw {
        unsafe {
            let ptr = self.mmap.as_ptr().add(self.pair_offset) as *const PairRaw;
            *ptr.add(idx)
        }
    }

    /// Write a new [PairRaw] at given index
    #[inline(always)]
    fn set_pair(&mut self, idx: usize, pair: PairRaw) {
        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.pair_offset) as *mut PairRaw;
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
    fn read_slot(&self, raw: &PairRaw) -> InternalResult<KeyValue> {
        let pair = raw.from_raw()?;
        let klen = pair.klen as usize;
        let vlen = pair.vlen as usize;

        let mut buf = vec![0u8; klen + vlen];
        Self::read_exact_at(&self.file, &mut buf, self.header_size as u64 + pair.offset)?;

        let vbuf = buf[klen..(klen + vlen)].to_owned();
        buf.truncate(klen);

        Ok((buf, vbuf))
    }

    /// Write a [KeyValue] to the bucket and get [Pair]
    fn write_slot(&self, pair: &KeyValue) -> InternalResult<PairRaw> {
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

        Ok(PairRaw::to_raw(pair))
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
                    let po = self.get_pair(idx);
                    let (existing_key, _) = self.read_slot(&po)?;

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
        assert_eq!(meta.iter_idx.load(Ordering::SeqCst), 0);

        // signatures and pair-offset regions should be zero
        let signs = bucket.get_signs();
        assert!(signs.iter().all(|&s| s == 0));

        for idx in 0..bucket.capacity {
            let raw = bucket.get_pair(idx);
            let pair = raw.from_raw().expect("Extract Pair from raw data");

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
    fn test_reopen_with_update() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let rand = gen_rand();
        let path = tmp.path().join(rand.to_string());

        {
            let mut bucket = BucketFile::new(&path, TEST_CAP).unwrap();
            let kv = (b"foo".to_vec(), b"bar".to_vec());
            let pair = bucket.write_slot(&kv).unwrap();

            bucket.set_pair(0, pair);
            bucket.set_sign(0, 12345);

            // metadata updates
            bucket.increment_inserted_count();
        }

        // at re-open, it shouldn’t be re-zero'ed
        let bucket2 = BucketFile::open(path, TEST_CAP).unwrap();
        let signs = bucket2.get_signs();
        assert_eq!(signs[0], 12345);

        let po2 = bucket2.get_pair(0);
        let kv2 = bucket2.read_slot(&po2).unwrap();

        assert_eq!(kv2.0, b"foo".to_vec());
        assert_eq!(kv2.1, b"bar".to_vec());
    }

    #[test]
    fn test_write_and_read_roundtrip() {
        let mut bucket = open_bucket();

        let kv = (b"key1".to_vec(), b"value1".to_vec());
        let sign = 0xDEADBEEF;
        let pair = bucket.write_slot(&kv).unwrap();

        bucket.set_pair(3, pair);
        bucket.set_sign(3, sign);
        bucket.meta_mut().inserts.fetch_add(1, Ordering::SeqCst);

        let signs = bucket.get_signs();
        let (found_idx, is_new) = bucket.lookup_slot(3, signs, sign, &kv.0).unwrap();

        assert_eq!(found_idx, 3);
        assert!(!is_new, "existing key should report is_new=false");

        let po_back = bucket.get_pair(3);
        let kv_back = bucket.read_slot(&po_back).unwrap();

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

        let pair = PairRaw::to_raw(fake_pair);
        bucket.set_pair(7, pair);

        let got_pair = bucket.get_pair(7);
        let got_off = got_pair.from_raw().unwrap();

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
