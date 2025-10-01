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
    kosh::meta::{Meta, Namespace, Pair, PairBytes},
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    usize,
};

pub(crate) type Sign = u32;
pub(crate) type Key = Vec<u8>;
pub(crate) type Value = Vec<u8>;
pub(crate) type KeyValue = (Key, Value);

pub(crate) const ROW_SIZE: usize = 16;

#[derive(Debug)]
pub(crate) struct Patra {
    meta: Meta,
    stats: Stats,
    mmap: MmapMut,
    file: File,
}

#[derive(Debug)]
struct Stats {
    header_size: usize,
    capacity: usize,
    sign_rows: usize,
    sign_offset: usize,
    pair_offset: usize,
    threshold: usize,
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

        let sign_offset = size_of::<Meta>();
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

        let sign_offset = size_of::<Meta>();
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
            let ptr = base.add(slice_idx * ROW_SIZE);

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

    pub fn read_pair_key(&mut self, bytes: PairBytes) -> InternalResult<Key> {
        let pair = Pair::from_raw(bytes)?;

        let mut buf = vec![0u8; pair.klen as usize];
        read_exact_at(
            &self.file,
            &mut buf,
            self.stats.header_size as u64 + pair.offset,
        )?;

        Ok(buf)
    }

    pub fn read_pair_key_value(&mut self, bytes: PairBytes) -> InternalResult<KeyValue> {
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

        if is_new {
            self.meta.incr_insert_count();
            self.set_sign(idx, sign);
        }

        self.set_pair_bytes(idx, pair_bytes);

        Ok(())
    }

    fn lookup_upsert_slot(
        &mut self,
        start_idx: usize,
        sign: Sign,
        key: &Key,
    ) -> InternalResult<(usize, bool)> {
        let mut idx = start_idx;

        for _ in 0..self.stats.sign_rows {
            let sign_row = self.get_sign_slice(idx);

            for (i, ss) in sign_row.iter().enumerate() {
                let item_idx = (start_idx * ROW_SIZE) + i;

                match *ss {
                    // empty slot
                    EMPTY_SIGN | TOMBSTONE_SIGN => return Ok((item_idx, true)),

                    // taken slot (check for update)
                    s if s == sign => {
                        let pair_bytes = self.get_pair_bytes(item_idx);
                        let kbuf = self.read_pair_key(pair_bytes)?;

                        if key == &kbuf {
                            return Ok((idx, false));
                        }
                    }

                    // invalid entry for sign,
                    // NOTE: May occur if mamory is currupt or underlying file
                    // is tampered w/
                    _ => {
                        return Err(InternalError::InvalidEntry(format!(
                            "Invalid sign bytes in sign row at {idx}"
                        )))
                    }
                }
            }

            idx = (idx + 1) % self.stats.sign_rows;
        }

        Ok((0, true))
    }

    pub fn get_sign_hash(&self, sign: Sign) -> usize {
        // sanity check
        debug_assert!(self.stats.sign_rows != 0, "No of rows must not be 0");

        sign as usize % self.stats.sign_rows
    }
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

#[cfg(test)]
mod patra_tests {
    use crate::kosh::patra;

    use super::*;
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;

    const TEST_CAP: usize = ROW_SIZE * 2;

    fn open_patra() -> Patra {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("patra_test");

        Patra::new(path, TEST_CAP).expect("create patra")
    }

    #[test]
    fn test_new_file_meta_and_layout() {
        let patra = open_patra();

        assert!(patra.meta.is_current_version());
        assert_eq!(patra.meta.get_insert_count(), 0);

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
    fn test_reopen_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("patra_reopen");

        let _ = Patra::new(&path, TEST_CAP).unwrap();
        let reopened = Patra::open(&path, TEST_CAP).unwrap();

        assert!(reopened.meta.is_current_version());
        assert_eq!(reopened.meta.get_insert_count(), 0usize);
        assert_eq!(reopened.meta.get_write_pointer(), 0u64);
    }

    #[test]
    fn test_capacity_mismatch_should_fail() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("patra_badcap");

        {
            let _ = Patra::new(&path, TEST_CAP).unwrap();
        }

        assert!(Patra::open(&path, TEST_CAP * 2).is_err());
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
    fn test_is_full_behavior() {
        let mut patra = open_patra();
        assert_eq!(patra.is_full().unwrap(), false);

        for _ in 0..patra.stats.threshold {
            patra.meta.incr_insert_count();
        }

        assert_eq!(patra.is_full().unwrap(), true);
    }

    #[test]
    fn test_invalid_file_rejects() {
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
}
