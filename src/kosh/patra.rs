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
    kosh::{
        meta::{Meta, Namespace, Pair, PairBytes},
        simd::ISA,
    },
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    usize,
};

type Sign = u32;

pub(crate) type Key = Vec<u8>;
pub(crate) type Value = Vec<u8>;
pub(crate) type KeyValue = (Key, Value);

pub(crate) const ROW_SIZE: usize = 16;

#[derive(Debug)]
pub(crate) struct Patra {
    meta: Meta,
    mmap: MmapMut,
    file: File,
    isa: ISA,
    stats: Stats,
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

        let isa = ISA::detect_isa();
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
            isa,
            stats,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        // sanity check
        debug_assert!(capacity % 16 == 0, "Capacity must be multiple of 16");

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
        let meta = Meta::new(&mut mmap);

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

        let isa = ISA::detect_isa();
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
            isa,
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
        size_of::<Meta>() + (size_of::<Sign>() * capacity) + (size_of::<PairBytes>() * capacity)
    }

    /// Calculate threshold w/ given capacity for [Bucket]
    ///
    /// NOTE: It's 80% of given capacity
    #[inline(always)]
    const fn calc_threshold(cap: usize) -> usize {
        cap.saturating_mul(4) / 5
    }

    #[inline(always)]
    fn get_pair_bytes(&self, idx: usize) -> PairBytes {
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
    fn get_sign_slice(&self, slice_idx: usize) -> [Sign; ROW_SIZE] {
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
            let ptr = (self.mmap.as_mut_ptr().add(self.stats.pair_offset) as *mut Sign).add(idx);
            std::ptr::write(ptr, sign);
        }
    }

    fn read_pair_key(&mut self, bytes: PairBytes) -> InternalResult<Key> {
        let pair = Pair::from_raw(bytes)?;

        let mut buf = vec![0u8; pair.klen as usize];
        read_exact_at(
            &self.file,
            &mut buf,
            self.stats.header_size as u64 + pair.offset,
        )?;

        Ok(buf)
    }

    fn read_pair_key_value(&mut self, bytes: PairBytes) -> InternalResult<KeyValue> {
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

    /// find a slot to insert a key
    ///
    /// ## Returns
    ///
    /// - (idx, true) -> Insert a new pair at `idx`
    /// - (idx, false) -> item w/ same key already exists, don't insert, update!
    ///
    pub fn lookup_insert_slot(
        &mut self,
        start_idx: usize,
        sign: Sign,
        key: &Key,
    ) -> InternalResult<(usize, bool)> {
        let mut idx = start_idx;

        for _ in 0..self.stats.sign_rows {
            let sign_row = self.get_sign_slice(idx);

            for (i, ss) in sign_row.iter().enumerate() {
                match *ss {
                    // empty slot
                    EMPTY_SIGN | TOMBSTONE_SIGN => return Ok((idx, true)),

                    // taken slot (check for update)
                    s if s == sign => {
                        let item_idx = (idx * ROW_SIZE) + i;

                        let pair_bytes = self.get_pair_bytes(item_idx);
                        let kbuf = self.read_pair_key(pair_bytes)?;

                        if key == &kbuf {
                            return Ok((idx, false));
                        }
                    }

                    // invalid entry for sign,
                    //
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

    /// fetch a value by key
    ///
    /// ## Returns
    ///
    /// - Value
    /// - index of pairs sign in signature space
    ///
    pub fn fetch_value_by_key(
        &mut self,
        start_idx: usize,
        sign: Sign,
        key: &Key,
    ) -> InternalResult<Option<(Value, usize)>> {
        let mut idx = start_idx;

        for _ in 0..self.stats.sign_rows {
            let sign_row = self.get_sign_slice(idx);

            for (i, ss) in sign_row.iter().enumerate() {
                match *ss {
                    EMPTY_SIGN => return Ok(None),
                    TOMBSTONE_SIGN => continue,

                    // taken slot (check for update)
                    s if s == sign => {
                        let item_idx = (idx * ROW_SIZE) + i;

                        let pair_bytes = self.get_pair_bytes(item_idx);
                        let (kbuf, vbuf) = self.read_pair_key_value(pair_bytes)?;

                        if key == &kbuf {
                            return Ok(Some((vbuf, item_idx)));
                        }
                    }

                    // invalid entry for sign,
                    //
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

        Ok(None)
    }

    pub fn is_full(&self) -> InternalResult<bool> {
        if self.meta.get_insert_count() >= self.stats.threshold {
            return Ok(true);
        }

        Ok(false)
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
    use super::*;
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;

    const TEST_CAP: usize = ROW_SIZE;

    fn open_patra() -> Patra {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("patra_test");

        Patra::new(path, TEST_CAP).unwrap()
    }

    #[test]
    fn test_new_and_meta_defaults() {
        let patra = open_patra();

        assert!(patra.meta.is_current_version());
        assert_eq!(patra.meta.get_insert_count(), 0);

        // all signatures should be empty
        for row in 0..patra.stats.sign_rows {
            let signs = patra.get_sign_slice(row);
            assert!(signs.iter().all(|&s| s == EMPTY_SIGN));
        }
    }

    #[test]
    fn test_open_and_capacity_mismatch() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("patra_open");

        {
            let _ = Patra::new(&path, TEST_CAP).unwrap();
        }

        // correct capacity works
        let _ = Patra::open(&path, TEST_CAP).unwrap();

        // wrong capacity should fail
        assert!(Patra::open(&path, TEST_CAP * 2).is_err());
    }

    #[test]
    fn test_write_and_read_pair_key_value() {
        let mut patra = open_patra();
        let kv = (b"foo".to_vec(), b"bar".to_vec());

        let raw = patra
            .write_pair_key_value(Namespace::Base, kv.clone())
            .unwrap();
        let got = patra.read_pair_key_value(raw).unwrap();

        assert_eq!(got, kv);
    }

    #[test]
    fn test_lookup_insert_and_fetch_value() {
        let mut patra = open_patra();

        let kv = (b"hello".to_vec(), b"world".to_vec());
        let sign = 0xBEEF;

        // write pair first
        let raw = patra
            .write_pair_key_value(Namespace::Base, kv.clone())
            .unwrap();
        let (slot, is_new) = patra.lookup_insert_slot(0, sign, &kv.0).unwrap();
        assert!(is_new);

        patra.set_pair_bytes(slot, raw);
        patra.set_sign(slot, sign);
        patra.meta.incr_insert_count();

        let res = patra.fetch_value_by_key(0, sign, &kv.0).unwrap();
        assert!(res.is_some());

        let (val, idx) = res.unwrap();
        assert_eq!(val, kv.1);
        assert_eq!(idx, slot);
    }

    #[test]
    fn test_tombstone_reuse() {
        let mut patra = open_patra();
        let sign = 0xCAFE;

        // First insert
        let (idx1, is_new1) = patra
            .lookup_insert_slot(0, sign, &"k".as_bytes().to_vec())
            .unwrap();
        assert!(is_new1);

        // Mark tombstone
        patra.set_sign(idx1 * ROW_SIZE, TOMBSTONE_SIGN);

        // Should reuse same slot
        let (idx2, is_new2) = patra
            .lookup_insert_slot(0, sign, &"k2".as_bytes().to_vec())
            .unwrap();
        assert!(is_new2);
        assert_eq!(idx1, idx2);
    }

    #[test]
    fn test_is_full_behavior() {
        let mut patra = open_patra();
        assert!(!patra.is_full().unwrap());

        for _ in 0..patra.stats.threshold {
            patra.meta.incr_insert_count();
        }

        assert!(patra.is_full().unwrap());
    }

    #[test]
    fn test_fetch_value_miss_and_invalid_sign() {
        let mut patra = open_patra();
        let sign = 0x1234;

        // Miss: nothing inserted
        let res = patra
            .fetch_value_by_key(0, sign, &"missing".as_bytes().to_vec())
            .unwrap();
        assert!(res.is_none());

        // Corruption: force invalid sign
        patra.set_sign(0, 0xFFFF_FFFF);
        assert!(patra
            .fetch_value_by_key(0, sign, &"bad".as_bytes().to_vec())
            .is_err());
    }
}
