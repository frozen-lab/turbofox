//! [Patra] (पत्र) is an on disk, append only, custom I/O layer for [Kosh].
//!
//! ## File Contents
//!
//! ▶ Meta => File metadata (version, magic, stats, etc.)
//! ▶ Signs => Fixed sized space to store (u32) signatures of key's of pairs stored
//! ▶ PairBytes => Fixed sized space (u8 * 10) to store Pair offsets (klen, etc.)
//! ▶ Data => Append only space to store raw KV pairs
//!
//! ## On-Disk Layout
//!
//! [ 0 <==> size_of::<Meta>() )
//!    File metadata (signature, version, etc.)
//!
//! [ meta_region <==> meta_region + size_of::<Sign>() * capacity )
//!     Signatures array (4 bytes per slot)
//!     ├─ slot0_sign : Sign
//!     ├─ ...
//!     └─ slot(capacity-1)_sign : Sign
//!     // Values can be EMPTY_SIGN, TOMBSTONE_SIGN, or Sign
//!
//! [ sign_region_end <==> sign_region_end + size_of::<PairBytes>() * capacity )
//!     Pair offsets array (10 bytes per slot)
//!     ├─ slot0_pair : PairBytes  // packed { namespace: 8b | position: 40b | klen: 16b | vlen: 16b }
//!     ├─ ...
//!     └─ slot(capacity-1)_pair : PairBytes
//!
//! [ header_size <==> EOF )
//!     Data region (variable-length)
//!     ├─ Entry0: [ key bytes (klen) ][ value bytes (vlen) ]
//!     ├─ ...
//!     └─ appended sequentially as write_offset grows
//!

use crate::{
    error::{InternalError, InternalResult},
    hasher::{EMPTY_SIGN, TOMBSTONE_SIGN},
    kosh::meta::{Meta, Namespace, Pair, PairBytes, EMPTY_PAIR_BYTES},
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    usize,
};

/// ----------------------------------------
/// Constants and Types
/// ----------------------------------------

pub(crate) type Sign = u32;
pub(crate) type Key = Vec<u8>;
pub(crate) type Value = Vec<u8>;
pub(crate) type KeyValue = (Key, Value);

pub(crate) const ROW_SIZE: usize = 16;

#[derive(Debug)]
struct Stats {
    header_size: usize,
    capacity: usize,
    sign_rows: usize,
    sign_offset: usize,
    pair_offset: usize,
    threshold: usize,
}

/// ----------------------------------------
/// Patra (पत्र)
/// ----------------------------------------

#[derive(Debug)]
pub(crate) struct Patra {
    meta: Meta,
    stats: Stats,
    mmap: MmapMut,
    file: File,
}

impl Patra {
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        // sanity check
        debug_assert!(capacity % ROW_SIZE == 0, "Capacity must be multiple of 16");

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        // NOTE: We must make sure, cap is always multiple of [ROW_SIZE]
        let sign_rows = capacity.wrapping_div(ROW_SIZE);

        let sign_offset = Meta::size_of();
        let pair_offset = sign_offset + capacity * size_of::<Sign>();

        let header_size = Self::calc_header_size(capacity);
        let threshold = Self::calc_threshold(capacity);

        // zero-init the file
        file.set_len(header_size as u64)?;

        let mut mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;
        let meta = Meta::new(&mut mmap);

        let stats = Stats {
            header_size,
            capacity,
            sign_rows,
            sign_offset,
            pair_offset,
            threshold,
        };

        Ok(Self {
            file,
            mmap,
            meta,
            stats,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        // sanity check
        debug_assert!(capacity % ROW_SIZE == 0, "Capacity must be multiple of 16");

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

        // NOTE: We must make sure, cap is always multiple of [ROW_SIZE]
        let sign_rows = capacity.wrapping_div(ROW_SIZE);

        let sign_offset = Meta::size_of();
        let pair_offset = sign_offset + capacity * size_of::<Sign>();

        let header_size = Self::calc_header_size(capacity);
        let threshold = Self::calc_threshold(capacity);

        let file_len = file.metadata()?.len();

        // NOTE: If `file.len()` is smaller then `header_size`, it's a sign of
        // invalid initilization or the file was tampered with! In this scenerio,
        // we delete the file and create it again!
        if file_len < header_size as u64 {
            return Err(InternalError::InvalidFile);
        }

        let mut mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;
        let meta = Meta::open(&mut mmap);

        // NOTE: while validating version and magic of the file, if not matched,
        // we should simply delete the file, as we do not have any earlier
        // versions to support.
        if !meta.is_current_version() {
            return Err(InternalError::InvalidFile);
        }

        // safeguard for the write pointer
        if meta.get_write_pointer() > file_len {
            return Err(InternalError::InvalidFile);
        }

        // safeguard for the insert count
        if meta.get_insert_count() > capacity {
            return Err(InternalError::InvalidFile);
        }

        let stats = Stats {
            header_size,
            capacity,
            sign_rows,
            sign_offset,
            pair_offset,
            threshold,
        };

        Ok(Self {
            file,
            mmap,
            meta,
            stats,
        })
    }

    /// Calculate the size of header based on given capacity for [Bucket]
    ///
    /// ### Size Calculation
    ///
    /// `sizeof(Meta) + (sizeof(Sign) * CAP) + (sizeof(PairBytes) * CAP)`
    #[inline(always)]
    const fn calc_header_size(capacity: usize) -> usize {
        Meta::size_of() + (size_of::<Sign>() * capacity) + (size_of::<PairBytes>() * capacity)
    }

    /// Calculate threshold w/ given capacity for [Bucket]
    ///
    /// NOTE: It's 80% of given capacity
    #[inline(always)]
    const fn calc_threshold(cap: usize) -> usize {
        cap.saturating_mul(4) / 5
    }

    #[inline(always)]
    pub fn get_pair_bytes(&self, idx: usize) -> PairBytes {
        // sanity check
        debug_assert!(
            idx < self.stats.capacity,
            "Index must not be bigger then the capacity"
        );

        unsafe {
            let ptr = (self.mmap.as_ptr().add(self.stats.pair_offset) as *const PairBytes).add(idx);
            std::ptr::read(ptr)
        }
    }

    #[inline(always)]
    pub fn set_pair_bytes(&mut self, idx: usize, bytes: PairBytes) {
        // sanity check
        debug_assert!(
            idx < self.stats.capacity,
            "Index must not be bigger then the capacity"
        );

        unsafe {
            let ptr =
                (self.mmap.as_mut_ptr().add(self.stats.pair_offset) as *mut PairBytes).add(idx);
            std::ptr::write(ptr, bytes);
        }
    }

    #[inline(always)]
    pub fn get_sign_slice(&self, slice_idx: usize) -> [Sign; ROW_SIZE] {
        // sanity check
        debug_assert!(
            slice_idx < self.stats.sign_rows,
            "Slice Index must not be bigger then total slices available"
        );

        unsafe {
            let base = self.mmap.as_ptr().add(self.stats.sign_offset);
            let ptr = base.add(slice_idx * ROW_SIZE * std::mem::size_of::<Sign>());

            *(ptr as *const [Sign; ROW_SIZE])
        }
    }

    #[inline(always)]
    pub fn set_sign(&mut self, idx: usize, sign: Sign) {
        // sanity check
        debug_assert!(
            idx < self.stats.capacity,
            "Index must not be bigger then the capacity"
        );

        unsafe {
            let ptr = (self.mmap.as_mut_ptr().add(self.stats.sign_offset) as *mut Sign).add(idx);
            std::ptr::write(ptr, sign);
        }
    }

    pub fn read_pair_key(&self, bytes: PairBytes) -> InternalResult<Key> {
        let pair = Pair::from_raw(bytes)?;

        let mut buf = vec![0u8; pair.klen as usize];
        read_exact_at(
            &self.file,
            &mut buf,
            self.stats.header_size as u64 + pair.offset,
        )?;

        Ok(buf)
    }

    pub fn read_pair_key_value(&self, bytes: PairBytes) -> InternalResult<KeyValue> {
        let pair = Pair::from_raw(bytes)?;

        let klen = pair.klen as usize;
        let vlen = pair.vlen as usize;

        let mut buf = vec![0u8; klen + vlen];
        read_exact_at(
            &self.file,
            &mut buf,
            self.stats.header_size as u64 + pair.offset,
        )?;

        let vbuf = buf[klen..(klen + vlen)].to_owned();
        buf.truncate(klen);

        Ok((buf, vbuf))
    }

    pub fn write_pair_key_value(
        &mut self,
        ns: Namespace,
        kv: KeyValue,
    ) -> InternalResult<PairBytes> {
        let klen = kv.0.len();
        let vlen = kv.1.len();
        let blen = klen + vlen;

        let mut buf = vec![0u8; blen];
        buf[..klen].copy_from_slice(&kv.0);
        buf[klen..].copy_from_slice(&kv.1);

        // this gets us write pointer before updating w/ current buffer length
        let offset = self.meta.update_write_offset(blen as u64);

        write_all_at(&self.file, &buf, self.stats.header_size as u64 + offset)?;

        let pair = Pair {
            offset,
            ns,
            klen: klen as u16,
            vlen: vlen as u16,
        };

        Ok(pair.to_raw()?)
    }

    pub fn is_full(&self) -> InternalResult<bool> {
        if self.meta.get_insert_count() >= self.stats.threshold {
            return Ok(true);
        }

        Ok(false)
    }

    pub fn upsert_kv(&mut self, sign: Sign, kv: KeyValue) -> InternalResult<()> {
        let start_idx = self.get_sign_hash(sign);
        let (idx, is_new) = self.lookup_upsert_slot(start_idx, sign, &kv.0)?;

        let pair_bytes = self.write_pair_key_value(Namespace::Base, kv)?;
        self.set_pair_bytes(idx, pair_bytes);

        if is_new {
            // sanity check (only for new insertions)
            debug_assert!(
                self.meta.get_insert_count() <= self.stats.threshold,
                "Insertions are not allowed beyound threshold limit"
            );

            self.meta.incr_insert_count();
            self.set_sign(idx, sign);
        }

        Ok(())
    }

    pub fn fetch_value(&mut self, sign: Sign, kv: KeyValue) -> InternalResult<Option<Value>> {
        let start_idx = self.get_sign_hash(sign);

        if let Some((_, vbuf)) = self.lookup_existing_pair(start_idx, sign, &kv.0)? {
            return Ok(Some(vbuf));
        }

        Ok(None)
    }

    pub fn yank_key(&mut self, sign: Sign, kv: KeyValue) -> InternalResult<Option<Value>> {
        let start_idx = self.get_sign_hash(sign);

        if let Some((idx, vbuf)) = self.lookup_existing_pair(start_idx, sign, &kv.0)? {
            self.meta.decr_insert_count();
            self.set_pair_bytes(idx, EMPTY_PAIR_BYTES);
            self.set_sign(idx, TOMBSTONE_SIGN);

            return Ok(Some(vbuf));
        }

        Ok(None)
    }

    fn lookup_existing_pair(
        &self,
        start_idx: usize,
        sign: Sign,
        key: &Key,
    ) -> InternalResult<Option<(usize, Vec<u8>)>> {
        let mut idx = start_idx;

        for _ in 0..self.stats.sign_rows {
            let sign_row = self.get_sign_slice(idx);

            for (i, ss) in sign_row.iter().enumerate() {
                let item_idx = (idx * ROW_SIZE + i) % self.stats.capacity;

                match *ss {
                    // empty slot
                    EMPTY_SIGN => return Ok(None),

                    // taken slot (check for update)
                    s if s == sign => {
                        let pair_bytes = self.get_pair_bytes(item_idx);
                        let (kbuf, vbuf) = self.read_pair_key_value(pair_bytes)?;

                        if key == &kbuf {
                            return Ok(Some((item_idx, vbuf)));
                        }
                    }

                    _ => continue,
                }
            }

            idx = (idx + 1) % self.stats.sign_rows;
        }

        Ok(None)
    }

    fn lookup_upsert_slot(
        &self,
        start_idx: usize,
        sign: Sign,
        key: &Key,
    ) -> InternalResult<(usize, bool)> {
        let mut idx = start_idx;

        for _ in 0..self.stats.sign_rows {
            let sign_row = self.get_sign_slice(idx);

            for (i, ss) in sign_row.iter().enumerate() {
                let item_idx = (idx * ROW_SIZE + i) % self.stats.capacity;

                match *ss {
                    // empty or deleted slot
                    EMPTY_SIGN | TOMBSTONE_SIGN => return Ok((item_idx, true)),

                    // taken slot (check for update)
                    s if s == sign => {
                        let pair_bytes = self.get_pair_bytes(item_idx);
                        let kbuf = self.read_pair_key(pair_bytes)?;

                        if key == &kbuf {
                            return Ok((item_idx, false));
                        }
                    }

                    _ => continue,
                }
            }

            idx = (idx + 1) % self.stats.sign_rows;
        }

        Err(InternalError::BucketFull)
    }

    pub fn get_sign_hash(&self, sign: Sign) -> usize {
        // sanity check
        debug_assert!(self.stats.sign_rows != 0, "No of rows must not be 0");

        sign as usize % self.stats.sign_rows
    }
}

/// ----------------------------------------
/// File I/O (pread syscall)
/// ----------------------------------------

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

#[cfg(test)]
mod patra_tests {
    use super::*;

    mod file_io_pread_syscall {
        use super::*;
        use std::fs::{File, OpenOptions};
        use std::io::{Read, Seek, SeekFrom, Write};
        use tempfile::tempfile;

        #[test]
        fn test_write_and_read_at_basic() {
            let mut f = tempfile().expect("tmpfile");

            write_all_at(&f, b"hello", 0).expect("write at 0");
            write_all_at(&f, b"world", 10).expect("write at 10");

            let mut buf = vec![0u8; 5];
            read_exact_at(&f, &mut buf, 0).expect("read hello");

            assert_eq!(&buf, b"hello");

            let mut buf2 = vec![0u8; 5];
            read_exact_at(&f, &mut buf2, 10).expect("read world");

            assert_eq!(&buf2, b"world");
        }

        #[test]
        fn test_read_at_non_written_area_returns_eof() {
            let mut f = tempfile().expect("tmpfile");

            write_all_at(&f, b"abc", 0).expect("write abc");
            let mut buf = vec![0u8; 5];
            let res = read_exact_at(&f, &mut buf, 0);

            assert!(res.is_err(), "expected UnexpectedEof");
        }

        #[test]
        fn test_overwrite_data_at_offset() {
            let mut f = tempfile().expect("tmpfile");

            write_all_at(&f, b"abcdef", 0).expect("write abcdef");
            write_all_at(&f, b"xyz", 2).expect("overwrite xyz");

            let mut buf = vec![0u8; 6];
            read_exact_at(&f, &mut buf, 0).expect("read whole file");

            assert_eq!(&buf, b"abxyzf");
        }

        #[test]
        fn test_write_and_read_large_offset() {
            let mut f = tempfile().expect("tmpfile");

            // simulate sparse file with large gap
            let offset = 4096;
            let data = b"offset-data";

            write_all_at(&f, data, offset).expect("write large offset");

            let mut buf = vec![0u8; data.len()];
            read_exact_at(&f, &mut buf, offset).expect("read back large offset");
            let end = f.seek(SeekFrom::End(0)).unwrap();

            assert_eq!(&buf, data);
            assert!(end >= offset as u64 + data.len() as u64);
        }

        #[test]
        fn test_zero_len_read_and_write_works() {
            let mut f = tempfile().expect("tmpfile");

            write_all_at(&f, b"", 0).expect("write empty");
            let mut buf = vec![];
            read_exact_at(&f, &mut buf, 0).expect("read empty");

            assert!(buf.is_empty());
        }
    }

    mod patra {
        use super::*;
        use crate::hasher::Hasher;
        use std::sync::atomic::Ordering;
        use tempfile::TempDir;

        const TEST_CAP: usize = ROW_SIZE * 2;

        fn open_patra() -> Patra {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("patra_test");

            Patra::new(path, TEST_CAP).expect("create patra")
        }

        fn open_patra_with_cap(cap: usize) -> Patra {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("patra_test");

            Patra::new(path, cap).expect("create patra")
        }

        #[test]
        fn test_new_init_file_meta_and_correct_defaults() {
            let patra = open_patra();

            // meta init
            assert!(patra.meta.is_current_version());
            assert_eq!(patra.meta.get_insert_count(), 0);
            assert_eq!(patra.meta.get_write_pointer(), 0);

            // test zeroed space for signatures
            for row in 0..patra.stats.sign_rows {
                let slice = patra.get_sign_slice(row);
                assert!(slice.iter().all(|&s| s == EMPTY_SIGN));
            }

            // test zeroed space for pair bytes
            for i in 0..patra.stats.capacity {
                let raw = patra.get_pair_bytes(i);
                let pair = Pair::from_raw(raw).unwrap();

                assert_eq!(pair.klen, 0);
                assert_eq!(pair.vlen, 0);
                assert_eq!(pair.offset, 0);
                assert_eq!(pair.ns, Namespace::Base);
            }
        }

        #[test]
        fn test_reopen_after_new_init() {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("patra_reopen");

            let _ = Patra::new(&path, TEST_CAP).unwrap();
            let reopened = Patra::open(&path, TEST_CAP).unwrap();

            assert!(reopened.meta.is_current_version());
            assert_eq!(reopened.meta.get_insert_count(), 0usize);
            assert_eq!(reopened.meta.get_write_pointer(), 0u64);
        }

        #[test]
        fn test_capacity_mismatch_failure() {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("patra_badcap");

            {
                let _ = Patra::new(&path, TEST_CAP).unwrap();
            }

            assert!(Patra::open(&path, TEST_CAP * 2).is_err());
        }

        #[test]
        fn test_space_integrity_for_signature_space_in_mmap() {
            let mut patra = open_patra_with_cap(128);

            for row in 0..patra.stats.sign_rows {
                let slice = patra.get_sign_slice(row);
                assert!(slice.iter().all(|&s| s == EMPTY_SIGN));
            }

            for i in 0..patra.stats.capacity {
                patra.set_sign(i, 1234u32);

                let row = i / ROW_SIZE;
                let slice = patra.get_sign_slice(row);

                assert!(slice.contains(&1234u32));
            }
        }

        #[test]
        fn test_upsert_kv_new_and_update() {
            let mut patra = open_patra();

            let val = b"world".to_vec();
            let val2 = b"rustacean".to_vec();

            let key = b"hello".to_vec();
            let sign1 = Hasher::new(&key);
            let start_idx1 = patra.get_sign_hash(sign1);

            patra.upsert_kv(sign1, (key.clone(), val.clone())).unwrap();

            // verify write
            let slot = patra.lookup_upsert_slot(start_idx1, sign1, &key).unwrap().0;
            let pb = patra.get_pair_bytes(slot);
            let (k, v) = patra.read_pair_key_value(pb).unwrap();

            assert_eq!(k, key);
            assert_eq!(v, val);

            // update
            patra.upsert_kv(sign1, (key.clone(), val2.clone())).unwrap();

            // verify write
            let slot = patra.lookup_upsert_slot(start_idx1, sign1, &key).unwrap().0;
            let pb = patra.get_pair_bytes(slot);
            let (k, v) = patra.read_pair_key_value(pb).unwrap();

            assert_eq!(k, key);
            assert_eq!(v, val2);

            assert_eq!(
                patra.meta.get_insert_count(),
                1,
                "Insert count should not incr for upsert for existing key"
            );
        }

        #[test]
        fn test_wraparound_for_row_when_inserting_with_custom_collisions() {
            let mut patra = open_patra();

            // same exact sign for every key (custom collision)
            let sign = Hasher::new("dummy".as_bytes());
            let row_idx = patra.get_sign_hash(sign);

            // dummy keys (all insert into same row)
            for i in 0..ROW_SIZE {
                let k = format!("key{i}").as_bytes().to_vec();
                patra.upsert_kv(sign, (k.clone(), b"x".to_vec())).unwrap();
            }

            // new key (collision case, as no space left in the row)
            let n_k = b"special".to_vec();
            let n_v = b"y".to_vec();

            patra.upsert_kv(sign, (n_k.clone(), n_v.clone())).unwrap();

            // verify write
            let slot = patra.lookup_upsert_slot(row_idx, sign, &n_k).unwrap().0;
            let pb = patra.get_pair_bytes(slot);
            let (k, v) = patra.read_pair_key_value(pb).unwrap();

            assert_eq!(k, n_k);
            assert_eq!(v, n_v);
        }

        #[test]
        fn test_set_and_get_pair_bytes() {
            let mut patra = open_patra();

            let pair = Pair {
                offset: 42,
                ns: Namespace::Base,
                klen: 3,
                vlen: 5,
            };

            let raw = pair.to_raw().unwrap();
            patra.set_pair_bytes(7, raw);

            let back = patra.get_pair_bytes(7);
            let parsed = Pair::from_raw(back).unwrap();

            assert_eq!(parsed.offset, 42);
            assert_eq!(parsed.klen, 3);
            assert_eq!(parsed.vlen, 5);
        }

        #[test]
        fn test_set_and_get_sign() {
            let mut patra = open_patra();

            patra.set_sign(5, 0xBEEF);

            let row = 5 / ROW_SIZE;
            let slice = patra.get_sign_slice(row);
            assert_eq!(slice[5 % ROW_SIZE], 0xBEEF);
        }

        #[test]
        fn test_write_and_read_key_value() {
            let mut patra = open_patra();

            let kv = (b"abc".to_vec(), b"def".to_vec());
            let raw = patra
                .write_pair_key_value(Namespace::Base, kv.clone())
                .unwrap();

            patra.set_pair_bytes(0, raw);
            patra.meta.incr_insert_count();

            let got_key = patra.read_pair_key(raw).unwrap();
            assert_eq!(got_key, kv.0);

            let got_kv = patra.read_pair_key_value(raw).unwrap();
            assert_eq!(got_kv, kv);
        }

        #[test]
        fn test_threshold_match_behavior_of_is_full_func() {
            let mut patra = open_patra();
            assert_eq!(patra.is_full().unwrap(), false);

            for _ in 0..patra.stats.threshold {
                patra.meta.incr_insert_count();
            }

            assert_eq!(patra.is_full().unwrap(), true);
        }

        #[test]
        fn test_rejection_on_courrepted_file() {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("patra_invalid");

            // invalid file
            let f = std::fs::File::create(&path).unwrap();
            f.set_len(8).unwrap();

            assert!(matches!(
                Patra::open(&path, TEST_CAP),
                Err(InternalError::InvalidFile)
            ));
        }

        #[test]
        fn test_insert_does_not_increase_count_on_update() {
            let mut patra = open_patra();

            let key = b"abc".to_vec();
            let val1 = b"v1".to_vec();
            let val2 = b"v2".to_vec();
            let sign = Hasher::new(&key);

            patra.upsert_kv(sign, (key.clone(), val1.clone())).unwrap();
            assert_eq!(patra.meta.get_insert_count(), 1);

            patra.upsert_kv(sign, (key.clone(), val2.clone())).unwrap();
            assert_eq!(patra.meta.get_insert_count(), 1); // must stay same
        }

        #[test]
        fn test_upsert_kv_func_with_inserts_and_updates() {
            let mut patra = open_patra();

            //
            // KV1
            //
            let key = b"hello".to_vec();
            let val1 = b"world1".to_vec();
            let val2 = b"world2".to_vec();
            let sign1 = Hasher::new(&key);
            let start_idx1 = patra.get_sign_hash(sign1);

            patra.upsert_kv(sign1, (key.clone(), val1.clone())).unwrap();
            assert_eq!(patra.meta.get_insert_count(), 1);

            // verify write
            let slot = patra.lookup_upsert_slot(start_idx1, sign1, &key).unwrap().0;
            let pb = patra.get_pair_bytes(slot);
            let (k, v) = patra.read_pair_key_value(pb).unwrap();

            assert_eq!(k, key);
            assert_eq!(v, val1);

            patra.upsert_kv(sign1, (key.clone(), val2.clone())).unwrap();
            assert_eq!(patra.meta.get_insert_count(), 1);

            // verify update
            let slot = patra.lookup_upsert_slot(start_idx1, sign1, &key).unwrap().0;
            let pb = patra.get_pair_bytes(slot);
            let (k, v) = patra.read_pair_key_value(pb).unwrap();

            assert_eq!(k, key);
            assert_eq!(v, val2);

            //
            // KV2
            //
            let other_key = b"foo".to_vec();
            let other_val = b"bar".to_vec();
            let sign2 = Hasher::new(&other_key);
            let start_idx2 = patra.get_sign_hash(sign2);

            patra
                .upsert_kv(sign2, (other_key.clone(), other_val.clone()))
                .unwrap();

            // verify update
            let slot = patra
                .lookup_upsert_slot(start_idx2, sign2, &other_key)
                .unwrap()
                .0;
            let pb = patra.get_pair_bytes(slot);
            let (k, v) = patra.read_pair_key_value(pb).unwrap();

            assert_eq!(k, other_key);
            assert_eq!(v, other_val);

            assert_eq!(patra.meta.get_insert_count(), 2);
        }

        #[test]
        fn test_tombstone_slot_reuse_for_insert() {
            let td = TempDir::new().unwrap();
            let path = td.path().join("patra_tombstone");
            let mut patra = Patra::new(path.clone(), 16).unwrap();

            //
            // KV1
            //
            let key1 = b"k1".to_vec();
            let val1 = b"v1".to_vec();
            let sign1 = Hasher::new(&key1);
            let start_idx1 = patra.get_sign_hash(sign1);

            patra.upsert_kv(sign1, (key1.clone(), val1)).unwrap();
            assert_eq!(patra.meta.get_insert_count(), 1);

            let slot = patra
                .lookup_upsert_slot(start_idx1, sign1, &key1)
                .unwrap()
                .0;
            patra.set_sign(slot, TOMBSTONE_SIGN);

            //
            // KV2
            //
            let key2 = b"k2".to_vec();
            let val2 = b"v2".to_vec();
            let sign2 = Hasher::new(&key2);
            let start_idx2 = patra.get_sign_hash(sign2);

            patra
                .upsert_kv(sign2, (key2.clone(), val2.clone()))
                .unwrap();

            assert_eq!(patra.meta.get_insert_count(), 2);

            let slot2 = patra
                .lookup_upsert_slot(start_idx2, sign2, &key2)
                .unwrap()
                .0;

            let pb2 = patra.get_pair_bytes(slot2);
            let (k, v) = patra.read_pair_key_value(pb2).unwrap();

            assert_eq!(k, key2);
            assert_eq!(v, val2);
        }

        #[test]
        fn test_new_inserts_correctly_updates_file_write_pointer() {
            let mut patra = open_patra();

            let key1 = b"k1".to_vec();
            let val1 = vec![1u8; 10];
            let sign1 = crate::hasher::Hasher::new(&key1);

            patra
                .upsert_kv(sign1, (key1.clone(), val1.clone()))
                .unwrap();
            let off1 = patra.meta.get_write_pointer();

            let key2 = b"k2".to_vec();
            let val2 = vec![2u8; 20];
            let sign2 = crate::hasher::Hasher::new(&key2);

            patra
                .upsert_kv(sign2, (key2.clone(), val2.clone()))
                .unwrap();
            let off2 = patra.meta.get_write_pointer();

            let key3 = b"k3".to_vec();
            let val3 = vec![3u8; 30];
            let sign3 = crate::hasher::Hasher::new(&key3);

            patra
                .upsert_kv(sign3, (key3.clone(), val3.clone()))
                .unwrap();
            let off3 = patra.meta.get_write_pointer();

            // offset must increase
            assert!(off1 < off2);
            assert!(off2 < off3);
        }

        #[test]
        fn test_upsert_on_collision_stores_sign_with_probing() {
            let mut patra = Patra::new(
                tempfile::tempdir().unwrap().path().join("patra_collision"),
                ROW_SIZE * 2,
            )
            .unwrap();

            // Craft two keys with same row hash
            let key1 = b"a_key".to_vec();
            let key2 = b"b_key".to_vec();

            let mut sign1 = crate::hasher::Hasher::new(&key1);
            let mut sign2 = crate::hasher::Hasher::new(&key1);

            let start_idx1 = patra.get_sign_hash(sign1);
            assert_eq!(start_idx1, patra.get_sign_hash(sign2));

            // insert k1
            patra
                .upsert_kv(sign1, (key1.clone(), b"v1".to_vec()))
                .unwrap();

            // insert k2 w/ collision
            patra
                .upsert_kv(sign2, (key2.clone(), b"v2".to_vec()))
                .unwrap();

            let slot1 = patra
                .lookup_upsert_slot(start_idx1, sign1, &key1)
                .unwrap()
                .0;

            let pb1 = patra.get_pair_bytes(slot1);
            let (rk1, rv1) = patra.read_pair_key_value(pb1).unwrap();

            assert_eq!(rk1, key1);
            assert_eq!(rv1, b"v1");

            let slot2 = patra
                .lookup_upsert_slot(start_idx1, sign2, &key2)
                .unwrap()
                .0;

            let pb2 = patra.get_pair_bytes(slot2);
            let (rk2, rv2) = patra.read_pair_key_value(pb2).unwrap();

            assert_eq!(rk2, key2);
            assert_eq!(rv2, b"v2");
        }

        #[test]
        fn test_patra_reopen_after_creation_correctly_preserves_state() {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("patra_reopen_state");

            // creation
            {
                let mut patra = Patra::new(&path, TEST_CAP).unwrap();

                let key = b"persist".to_vec();
                let val = b"data".to_vec();
                let sign = crate::hasher::Hasher::new(&key);

                patra.upsert_kv(sign, (key.clone(), val.clone())).unwrap();
                assert_eq!(patra.meta.get_insert_count(), 1);
            }

            // re-open
            {
                let mut reopened = Patra::open(&path, TEST_CAP).unwrap();
                assert_eq!(reopened.meta.get_insert_count(), 1);

                let key = b"persist".to_vec();
                let sign = crate::hasher::Hasher::new(&key);
                let start_idx = reopened.get_sign_hash(sign);
                let slot = reopened
                    .lookup_upsert_slot(start_idx, sign, &key)
                    .unwrap()
                    .0;

                let pb = reopened.get_pair_bytes(slot);
                let (rk, rv) = reopened.read_pair_key_value(pb).unwrap();

                assert_eq!(rk, b"persist");
                assert_eq!(rv, b"data");
            }
        }

        #[test]
        #[cfg(not(debug_assertions))]
        fn test_upsert_kv_till_full_capacity() {
            let mut patra = open_patra_with_cap(32);
            let mut rng = sphur::Sphur::new_seeded(0x10203040);

            loop {
                let i = rng.gen_u32();
                let k = format!("key{i}").into_bytes();
                let sign = Hasher::new(&k);

                if patra.upsert_kv(sign, (k.clone(), b"x".to_vec())).is_err() {
                    break;
                }
            }

            assert!(patra.is_full().unwrap());
        }

        #[test]
        #[cfg(not(debug_assertions))]
        fn test_full_capacity() {
            let mut patra = open_patra_with_cap(32);

            for i in 0..patra.stats.capacity {
                let k = format!("key{i}").into_bytes();
                let sign = Hasher::new(&k);

                patra.upsert_kv(sign, (k.clone(), b"x".to_vec())).unwrap();
            }

            assert!(patra.is_full().unwrap());

            let k = b"extra".to_vec();
            let v = b"boom".to_vec();
            let sign = Hasher::new(&k);

            assert!(
                patra.upsert_kv(sign, (k, v)).is_err(),
                "upserting new entry must fail when cap is full"
            );
        }
    }
}
