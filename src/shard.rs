//! Shard is the fundamental unit of the storage,
//! the index is mmaped and values are appended to the end.
//!
//! ### Memory Overhead
//!
//! Memory usage of the mmapped shard header (`HEADER_SIZE`):
//!
//! ```md
//! ShardMeta   =   16    B  (8 B magic + 8 B version)
//! ShardStats  =    8    B  (4 B n_occupied + 4 B write_offset)
//! Padding     = 4072    B  (4096 B alignment − (16 B + 8 B) % 4096)
//! Index       = 327_680 B  (640 B × 512 rows where 640 B = 16
//!                          B×32 keys + 4 B×32 offsets)
//! ────────────────────────────────────────────────
//! HEADER_SIZE = 331_776 B  (~324 KiB)
//! ```  
//!
//! ### OnDisk Layout
//!
//! The shard file has the following structure,
//!
//! ```text
//! +--------------------------------------------------+  Offset 0
//! | ShardMeta                                        |
//! |  • magic: [u8; 8]        (8 bytes)               |
//! |  • version: u64          (8 bytes)               |
//! +--------------------------------------------------+
//! | ShardStats                                       |
//! |  • n_occupied: AtomicU32 (4 bytes)               |
//! |  • write_offset: AtomicU32 (4 bytes)             |
//! +--------------------------------------------------+
//! | PageAligned<[ShardSlot; ROWS_NUM]>               |
//! |  • each ShardSlot:                               |
//! |      – keys:    [SlotKey; ROWS_WIDTH]            |
//! |      – offsets: [SlotOffset; ROWS_WIDTH]         |
//! |    (4096‑byte alignment)                         |
//! +--------------------------------------------------+  Offset = HEADER_SIZE
//! | Value entries                                    |
//! |  • appended sequentially at                      |
//! |    (HEADER_SIZE + write_offset)                  |
//! +--------------------------------------------------+  EOF
//! ```
//!

use crate::{
    core::{TError, TResult, MAGIC, MAX_KEY_SIZE, ROWS_NUM, ROWS_WIDTH, VERSION},
    hasher::TurboHasher,
};
use memmap::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    mem::size_of,
    ops::Range,
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};

/// The size of the shard header in bytes.
pub(crate) const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;

/// Aligns the data to 4096-byte boundry, to improve perf of mmapped I/O
#[derive(Clone, Copy)]
#[repr(C, align(4096))]
struct PageAligned<T>(T);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct SlotKey([u8; MAX_KEY_SIZE]);

impl Default for SlotKey {
    fn default() -> Self {
        Self([0u8; MAX_KEY_SIZE])
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct SlotOffset(u32);

impl Default for SlotOffset {
    fn default() -> Self {
        Self(0)
    }
}

impl SlotOffset {
    /// Unpacks a packed u32 into (offset, vlen).
    fn from_self(packed: SlotOffset) -> (u32, u16) {
        const OFFSET_MASK: u32 = (1 << 20) - 1;

        let offset = packed.0 & OFFSET_MASK;
        let vlen = (packed.0 >> 20) as u16;

        (offset, vlen)
    }

    /// Packs a 12‑bit vlen and a 20‑bit offset into a single u32.
    fn to_self(vlen: u16, offset: u32) -> u32 {
        assert!(
            (vlen as u32) < (1 << 12),
            "vlen must be < 2^12, got {}",
            vlen
        );
        assert!(offset < (1 << 20), "offset must be < 2^20, got {}", offset);

        ((vlen as u32) << 20) | offset
    }
}

#[cfg(test)]
mod shard_slot_offset_tests {
    use super::SlotOffset;

    #[test]
    fn round_trip() {
        let cases = &[(0_u16, 0_u32), (1, 1), (0xABC, 0xFFFFF), (0x7FF, 0x12345)];

        for &(vlen, offset) in cases {
            let packed = SlotOffset::to_self(vlen, offset);
            let (off2, vlen2) = SlotOffset::from_self(SlotOffset(packed));

            assert_eq!((off2, vlen2), (offset, vlen));
        }
    }

    #[test]
    #[should_panic]
    fn vlen_too_large() {
        let _ = SlotOffset::to_self(0x1000, 0);
    }

    #[test]
    #[should_panic]
    fn offset_too_large() {
        let _ = SlotOffset::to_self(0, 1 << 20);
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct ShardSlot {
    keys: [SlotKey; ROWS_WIDTH],
    offsets: [SlotOffset; ROWS_WIDTH],
}

impl Default for ShardSlot {
    fn default() -> Self {
        Self {
            keys: [SlotKey::default(); ROWS_WIDTH],
            offsets: [SlotOffset::default(); ROWS_WIDTH],
        }
    }
}

impl ShardSlot {
    // lookup the index of the candidate in the slot, if not found
    // the index of the empty slot is returned
    fn lookup_candidate_or_empty(&self, candidate: SlotKey) -> (Option<usize>, Option<usize>) {
        let mut empty_idx = None;

        for (idx, &slot_k) in self.keys.iter().enumerate() {
            if slot_k == candidate {
                return (Some(idx), None);
            }

            if slot_k == SlotKey::default() {
                empty_idx = Some(idx);
            }
        }

        (None, empty_idx)
    }

    // lookup the index of the candidate in the slot
    fn lookup_candidate(&self, candidate: SlotKey) -> Option<usize> {
        for (idx, &slot_k) in self.keys.iter().enumerate() {
            if slot_k == candidate && slot_k != SlotKey::default() {
                return Some(idx);
            }
        }

        None
    }
}

#[repr(C)]
struct ShardMeta {
    magic: [u8; 8],
    version: u64,
}

impl Default for ShardMeta {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
        }
    }
}

#[repr(C)]
struct ShardStats {
    // Number of KV pairs curretnly inserted in the shard
    //
    // Note: U32 is for better alignment, otherwise maximum inserted number would
    // be less then `u16::Max`
    n_occupied: AtomicU32,

    // current write offset in the file
    write_offset: AtomicU32,
}

impl Default for ShardStats {
    fn default() -> Self {
        Self {
            n_occupied: AtomicU32::new(0),
            write_offset: AtomicU32::new(0),
        }
    }
}

/// The header of a shard file, containing metadata, stats, and the index.
#[repr(C)]
struct ShardHeader {
    meta: ShardMeta,
    stats: ShardStats,
    index: PageAligned<[ShardSlot; ROWS_NUM]>,
}

impl ShardHeader {
    #[inline(always)]
    const fn get_init_offset() -> u64 {
        0u64
    }

    fn get_default_buf() -> Vec<u8> {
        let header = ShardHeader {
            meta: ShardMeta::default(),
            stats: ShardStats::default(),
            index: PageAligned([ShardSlot::default(); ROWS_NUM]),
        };

        let size = size_of::<ShardHeader>();
        let mut buf = vec![0u8; size];

        unsafe {
            std::ptr::copy_nonoverlapping(
                &header as *const ShardHeader as *const u8,
                buf.as_mut_ptr(),
                size,
            );
        }

        buf
    }
}

struct ShardFile {
    file: File,
    mmap: MmapMut,
}

impl ShardFile {
    fn open(path: &PathBuf, is_new: bool) -> TResult<Self> {
        let file = {
            if is_new {
                Self::new(path)?
            } else {
                Self::file(path, is_new)?
            }
        };

        let mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;

        Ok(Self { file, mmap })
    }

    fn new(path: &PathBuf) -> TResult<File> {
        let file = Self::file(path, true)?;
        file.set_len(HEADER_SIZE)?;

        Self::init_header(&file)?;

        Ok(file)
    }

    fn init_header(file: &File) -> TResult<()> {
        let buf = ShardHeader::get_default_buf();
        let offset = ShardHeader::get_init_offset();

        Self::write_all_at(file, &buf, offset)?;

        Ok(())
    }

    fn file(path: &PathBuf, truncate: bool) -> TResult<File> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(truncate)
            .open(path)?;

        Ok(file)
    }

    /// Returns an immutable reference to the shard header
    #[inline(always)]
    fn header(&self) -> &ShardHeader {
        unsafe { &*(self.mmap.as_ptr() as *const ShardHeader) }
    }

    /// Returns a mutable reference to the shard header
    #[inline(always)]
    fn header_mut(&self) -> &mut ShardHeader {
        unsafe { &mut *(self.mmap.as_ptr() as *mut ShardHeader) }
    }

    /// Returns an immutable reference to a specific row in the index
    #[inline(always)]
    fn row(&self, idx: usize) -> &ShardSlot {
        &self.header().index.0[idx]
    }

    /// Returns a mutable reference to a specific row in the index
    #[inline(always)]
    fn row_mut(&self, idx: usize) -> &mut ShardSlot {
        &mut self.header_mut().index.0[idx]
    }

    fn write_slot(&self, vbuf: &[u8]) -> TResult<SlotOffset> {
        let vlen = vbuf.len();

        let write_offset: u32 = self
            .header()
            .stats
            .write_offset
            .fetch_add(vlen as u32, Ordering::SeqCst) as u32;

        Self::write_all_at(&self.file, &vbuf, write_offset as u64 + HEADER_SIZE)?;

        let offset = SlotOffset::to_self(vlen as u16, write_offset);

        Ok(SlotOffset(offset))
    }

    fn read_slot(&self, slot: SlotOffset) -> TResult<Vec<u8>> {
        let (offset, vlen) = SlotOffset::from_self(slot);
        let mut buf = vec![0u8; vlen as usize];

        Self::read_exact_at(&self.file, &mut buf, offset as u64 + HEADER_SIZE)?;

        Ok(buf)
    }

    /// Reads the exact number of bytes required to fill `buf` from a given offset.
    #[cfg(unix)]
    fn read_exact_at(f: &File, buf: &mut [u8], offset: u64) -> TResult<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)?;

        Ok(())
    }

    /// Writes a buffer to a file at a given offset.
    #[cfg(unix)]
    fn write_all_at(f: &File, buf: &[u8], offset: u64) -> TResult<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)?;

        Ok(())
    }

    /// Reads the exact number of bytes required to fill `buf` from a given offset.
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

    /// Writes a buffer to a file at a given offset.
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

/// A `Shard` represents a partition of the database, responsible for a specific
/// range of shard selectors.
pub(crate) struct Shard {
    /// The range of shard selectors this shard is responsible for.
    pub(crate) span: Range<u32>,

    /// The memory-mapped file that backs this shard.
    file: ShardFile,
}

impl Shard {
    /// Opens a shard, creating it if it doesn't exist or truncating it if requested.
    ///
    /// NOTE: The shard's filename is derived from its `span`.
    pub fn open(dirpath: &PathBuf, span: Range<u32>, truncate: bool) -> TResult<Self> {
        let filepath = dirpath.join(format!("shard_{:04x}-{:04x}", span.start, span.end));

        let file = ShardFile::open(&filepath, truncate)?;

        Ok(Self { span, file })
    }

    /// Sets a key-value pair in the shard.
    pub fn set(&self, kbuf: &[u8; MAX_KEY_SIZE], vbuf: &[u8], hash: TurboHasher) -> TResult<()> {
        let candidate = SlotKey(*kbuf);
        let row_idx = hash.row_selector() as usize;
        let row = self.file.row_mut(row_idx);

        let (cur_idx, new_idx) = ShardSlot::lookup_candidate_or_empty(row, candidate);

        // check if item already exists
        if let Some(idx) = cur_idx {
            let new_slot = self.file.write_slot(vbuf)?;
            row.offsets[idx] = new_slot;

            return Ok(());
        }

        // insert at a new slot
        if let Some(idx) = new_idx {
            let new_slot = self.file.write_slot(vbuf)?;

            row.keys[idx] = candidate;
            row.offsets[idx] = new_slot;

            self.file
                .header_mut()
                .stats
                .n_occupied
                .fetch_add(1, Ordering::SeqCst);

            return Ok(());
        }

        // if we ran out of room in this row
        Err(TError::RowFull(row_idx))
    }

    /// Retrieves a value by its key from the shard.
    ///
    /// Returns `Ok(Some(value))` if the key is found, `Ok(None)` if not, and
    /// an `Err` if an I/O error occurs.
    pub fn get(&self, kbuf: &[u8; MAX_KEY_SIZE], hash: TurboHasher) -> TResult<Option<Vec<u8>>> {
        let candidate = SlotKey(*kbuf);
        let row_idx = hash.row_selector() as usize;
        let row = self.file.row(row_idx);

        if let Some(idx) = ShardSlot::lookup_candidate(row, candidate) {
            let offset = row.offsets[idx];
            let vbuf = self.file.read_slot(offset)?;

            return Ok(Some(vbuf));
        }

        Ok(None)
    }

    /// Removes a key-value pair from the shard.
    ///
    /// Returns `Ok(true)` if the key was found and removed, `Ok(false)` if not.
    pub fn remove(&self, kbuf: &[u8; MAX_KEY_SIZE], hash: TurboHasher) -> TResult<Option<Vec<u8>>> {
        let candidate = SlotKey(*kbuf);
        let row_idx = hash.row_selector() as usize;
        let row = self.file.row_mut(row_idx);

        if let Some(idx) = ShardSlot::lookup_candidate(row, candidate) {
            let offset = row.offsets[idx];
            let vbuf = self.file.read_slot(offset)?;

            row.keys[idx] = SlotKey::default();
            row.offsets[idx] = SlotOffset::default();

            self.file
                .header_mut()
                .stats
                .n_occupied
                .fetch_sub(1, Ordering::SeqCst);

            return Ok(Some(vbuf));
        }

        Ok(None)
    }

    pub fn split(
        &self,
        dirpath: &PathBuf,
    ) -> TResult<(Shard, Shard, Vec<([u8; MAX_KEY_SIZE], Vec<u8>)>)> {
        let mut remaining_kvs: Vec<([u8; MAX_KEY_SIZE], Vec<u8>)> = Vec::new();

        let top = self.span.start;
        let bottom = self.span.end;
        let mid = (top + bottom) / 2;

        let top_filename = dirpath.join(format!("top_{:04x}-{:04x}", top, mid));
        let bottom_filename = dirpath.join(format!("bottom_{:04x}-{:04x}", mid, bottom));

        let top_file = ShardFile::open(&top_filename, true)?;
        let bottom_file = ShardFile::open(&bottom_filename, true)?;

        for (r_idx, &row) in self.file.header().index.0.iter().enumerate() {
            for (col, &key) in row.keys.iter().enumerate() {
                if key == SlotKey::default() {
                    continue;
                }

                let kbuf = key.0;
                let slot = row.offsets[col];
                let vbuf = self.file.read_slot(slot)?;
                let hash = TurboHasher::new(&kbuf);

                // Validate the row selector matches (sanity check)
                if hash.row_selector() as usize != r_idx {
                    // This could happen due to hash collisions or data corruption
                    continue;
                }

                // Determine which shard this entry belongs to
                let target_file = if hash.shard_selector() < mid {
                    &top_file
                } else {
                    &bottom_file
                };

                // Find a free slot in the target row
                let target_row = target_file.row_mut(hash.row_selector() as usize);
                let mut inserted = false;

                for target_col in 0..ROWS_WIDTH {
                    if target_row.keys[target_col] == SlotKey::default() {
                        let new_slot = target_file.write_slot(&vbuf)?;

                        target_row.offsets[target_col] = new_slot;
                        target_row.keys[target_col] = SlotKey(kbuf);
                        target_file
                            .header()
                            .stats
                            .n_occupied
                            .fetch_add(1, Ordering::SeqCst);

                        inserted = true;
                        break;
                    }
                }

                if !inserted {
                    remaining_kvs.push((kbuf, vbuf));
                }
            }
        }

        // Ensure all data is written to disk before renaming
        top_file.file.sync_all()?;
        bottom_file.file.sync_all()?;

        let new_top_filename = dirpath.join(format!("shard_{:04x}-{:04x}", top, mid));
        let new_bottom_filename = dirpath.join(format!("shard_{:04x}-{:04x}", mid, bottom));

        std::fs::rename(&bottom_filename, &new_bottom_filename)?;
        std::fs::rename(&top_filename, &new_top_filename)?;

        // Remove the original shard file
        let original_filename = dirpath.join(format!("shard_{:04x}-{:04x}", top, bottom));

        if original_filename.exists() {
            std::fs::remove_file(&original_filename)?;
        }

        let new_top_file = ShardFile::open(&new_top_filename, false)?;
        let new_bottom_file = ShardFile::open(&new_bottom_filename, false)?;

        let top_shard = Shard {
            span: top..mid,
            file: new_top_file,
        };

        let bottom_shard = Shard {
            span: mid..bottom,
            file: new_bottom_file,
        };

        Ok((top_shard, bottom_shard, remaining_kvs))
    }
}

#[cfg(test)]
mod shard_tests {
    use super::*;
    use tempfile::TempDir;

    fn new_shard(span: Range<u32>) -> TResult<(Shard, TempDir)> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;
        let s = Shard::open(&dir, span, true)?;

        Ok((s, tmp))
    }

    #[test]
    fn set_get_remove_roundtrip() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;
        let mut kbuf = [0u8; MAX_KEY_SIZE];
        let val: Vec<u8> = b"world".to_vec();

        let key_bytes = b"hello";
        kbuf[..key_bytes.len()].copy_from_slice(key_bytes);
        let h = TurboHasher::new(&kbuf);

        // initially not present
        assert_eq!(shard.get(&kbuf, h)?, None);

        // set then get
        shard.set(&kbuf, &val, h)?;

        assert_eq!(shard.get(&kbuf, h)?, Some(val));

        // remove => true, then gone
        assert!(shard.remove(&kbuf, h)? != None);
        assert_eq!(shard.get(&kbuf, h)?, None);

        // removing again returns false
        assert!(shard.remove(&kbuf, h)? == None);

        Ok(())
    }

    #[test]
    fn overwrite_existing_value() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;
        let key = b"dup";

        let mut kbuf = [0u8; MAX_KEY_SIZE];
        kbuf[..key.len()].copy_from_slice(key);

        let v1 = b"first".to_vec();
        let v2 = b"second".to_vec();
        let h = TurboHasher::new(&kbuf);

        shard.set(&kbuf, &v1, h)?;
        assert_eq!(shard.get(&kbuf, h)?, Some(v1.clone()));

        // overwrite
        shard.set(&kbuf, &v2, h)?;
        assert_eq!(shard.get(&kbuf, h)?, Some(v2.clone()));

        Ok(())
    }

    #[test]
    fn stats_n_occupied_and_n_deleted() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;
        let header = shard.file.header();
        let load = |f: &AtomicU32| f.load(Ordering::SeqCst);

        assert_eq!(load(&header.stats.n_occupied), 0);

        // insert two distinct keys
        let k1 = b"k1";
        let k2 = b"k2";

        let mut buf1 = [0u8; MAX_KEY_SIZE];
        let mut buf2 = [0u8; MAX_KEY_SIZE];

        buf1[..k1.len()].copy_from_slice(k1);
        buf2[..k2.len()].copy_from_slice(k2);

        let h1 = TurboHasher::new(&buf1);
        let h2 = TurboHasher::new(&buf2);

        shard.set(&buf1, b"v1", h1)?;
        shard.set(&buf2, b"v2", h2)?;

        assert_eq!(load(&header.stats.n_occupied), 2);

        // remove one
        assert!(shard.remove(&buf1, h1)?.is_some());
        assert_eq!(load(&header.stats.n_occupied), 1);

        // remove non‐existent does nothing
        let emt_buf = [0u8; MAX_KEY_SIZE];
        let emt_h = TurboHasher::new(&emt_buf);

        assert!(shard.remove(&emt_buf, emt_h)?.is_none());
        assert_eq!(load(&header.stats.n_occupied), 1);

        Ok(())
    }

    #[test]
    fn set_returns_row_full_error() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;

        // Attempt to insert into the full row
        let key = b"another_key";
        let val = b"another_value";

        let mut kbuf = [0u8; MAX_KEY_SIZE];
        kbuf[..key.len()].copy_from_slice(key);

        let hash = TurboHasher::new(&kbuf);

        // Simulate a row being full
        let row_idx = hash.row_selector();
        let row = shard.file.row_mut(row_idx);

        // fill in the selected row w/ dummy values
        for i in 0..ROWS_WIDTH {
            row.keys[i] = SlotKey([1u8; MAX_KEY_SIZE]);
        }

        let result = shard.set(&kbuf, val, hash);

        assert!(matches!(result, Err(TError::RowFull(idx)) if idx == row_idx));

        Ok(())
    }

    #[test]
    fn split_removes_old_shard_file() -> TResult<()> {
        let span = 0..0x1000;
        let (shard, tmp) = new_shard(span.clone())?;
        let old_path = tmp
            .path()
            .join(format!("shard_{:04x}-{:04x}", span.start, span.end));

        assert!(old_path.exists());

        let (_top, _bottom, _) = shard.split(&tmp.path().to_path_buf())?;

        assert!(!old_path.exists(), "Old shard file was not deleted");

        Ok(())
    }

    #[test]
    fn test_new_shard_initializes_header() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;
        let header = shard.file.header();

        // Verify ShardMeta
        assert_eq!(header.meta.magic, MAGIC);
        assert_eq!(header.meta.version, VERSION);

        // Verify ShardStats
        assert_eq!(header.stats.n_occupied.load(Ordering::SeqCst), 0);
        assert_eq!(header.stats.write_offset.load(Ordering::SeqCst), 0);

        Ok(())
    }
}

#[cfg(test)]
mod split_tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn new_shard(span: Range<u32>) -> TResult<(Shard, TempDir)> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;
        let s = Shard::open(&dir, span, true)?;

        Ok((s, tmp))
    }

    #[test]
    fn split_empty_shard() -> TResult<()> {
        let (shard, tmp) = new_shard(0..0x1000)?;
        let dir = tmp.path().to_path_buf();

        let (top, bottom, _) = shard.split(&dir)?;

        assert_eq!(top.span, 0..0x800);
        assert_eq!(bottom.span, 0x800..0x1000);

        // Both shards should be empty
        let top_header = top.file.header();
        let bottom_header = bottom.file.header();

        assert_eq!(top_header.stats.n_occupied.load(Ordering::SeqCst), 0);
        assert_eq!(bottom_header.stats.n_occupied.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[test]
    fn split_single_entry() -> TResult<()> {
        let (shard, tmp) = new_shard(0..0x1000)?;
        let dir = tmp.path().to_path_buf();

        let key = b"test_key";
        let val = b"test_value";

        let mut kbuf = [0u8; MAX_KEY_SIZE];
        kbuf[..key.len()].copy_from_slice(key);

        let hash = TurboHasher::new(&kbuf);

        shard.set(&kbuf, val, hash)?;

        let (top, bottom, _) = shard.split(&dir)?;
        let selector = hash.shard_selector();
        let expected = if selector < 0x800 { &top } else { &bottom };
        let other = if selector < 0x800 { &bottom } else { &top };

        assert_eq!(expected.get(&kbuf, hash)?, Some(val.to_vec()));
        assert_eq!(other.get(&kbuf, hash)?, None);

        assert_eq!(
            expected
                .file
                .header()
                .stats
                .n_occupied
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            other.file.header().stats.n_occupied.load(Ordering::SeqCst),
            0
        );

        Ok(())
    }

    #[test]
    fn split_multiple_entries_distributed() -> TResult<()> {
        let (shard, tmp) = new_shard(0..0x1000)?;
        let dir = tmp.path().to_path_buf();

        let mut test_data = HashMap::new();
        let mut top_count = 0;
        let mut bottom_count = 0;

        // Insert multiple entries that will be distributed across both shards
        for i in 0..50 {
            let key = format!("key_{i}");
            let val = format!("value_{i}");

            let mut kbuf = [0u8; MAX_KEY_SIZE];
            kbuf[..key.len()].copy_from_slice(&key.clone().into_bytes());

            let hash = TurboHasher::new(&kbuf);

            shard.set(&kbuf, &val.clone().into_bytes(), hash)?;
            test_data.insert(kbuf.clone(), (val, hash));

            if hash.shard_selector() < 0x800 {
                top_count += 1;
            } else {
                bottom_count += 1;
            }
        }

        // Ensure we have data for both shards
        assert!(top_count > 0, "No entries for top shard");
        assert!(bottom_count > 0, "No entries for bottom shard");

        let (top, bottom, _) = shard.split(&dir)?;

        // Verify all entries are in the correct shards
        for (key, (val, hash)) in test_data {
            let shard_selector = hash.shard_selector();

            let expected_shard = if shard_selector < 0x800 {
                &top
            } else {
                &bottom
            };

            let other_shard = if shard_selector < 0x800 {
                &bottom
            } else {
                &top
            };

            assert_eq!(
                expected_shard.get(&key.clone(), hash)?,
                Some(val.into_bytes())
            );
            assert_eq!(other_shard.get(&key, hash)?, None);
        }

        // Verify stats
        let top_header = top.file.header();
        let bottom_header = bottom.file.header();

        assert_eq!(
            top_header.stats.n_occupied.load(Ordering::SeqCst),
            top_count
        );
        assert_eq!(
            bottom_header.stats.n_occupied.load(Ordering::SeqCst),
            bottom_count
        );

        Ok(())
    }

    #[test]
    fn split_removes_original_file() -> TResult<()> {
        let span = 0..0x1000;
        let (shard, tmp) = new_shard(span.clone())?;
        let dir = tmp.path().to_path_buf();
        let original_path = dir.join(format!("shard_{:04x}-{:04x}", span.start, span.end));

        // Ensure the file exists before split
        assert!(original_path.exists());

        // Perform the split
        let (_top, _bottom, _) = shard.split(&dir)?;

        // Original file should be removed
        assert!(
            !original_path.exists(),
            "Original shard file was not deleted"
        );

        Ok(())
    }

    #[test]
    fn split_creates_correct_filenames() -> TResult<()> {
        let span = 0..0x1000;
        let (shard, tmp) = new_shard(span.clone())?;
        let dir = tmp.path().to_path_buf();

        let (top, bottom, _) = shard.split(&dir)?;

        let top_path = dir.join(format!("shard_{:04x}-{:04x}", 0, 0x800));
        let bottom_path = dir.join(format!("shard_{:04x}-{:04x}", 0x800, 0x1000));

        assert!(top_path.exists(), "Top shard file was not created");
        assert!(bottom_path.exists(), "Bottom shard file was not created");

        // Verify the spans are correct
        assert_eq!(top.span, 0..0x800);
        assert_eq!(bottom.span, 0x800..0x1000);

        Ok(())
    }

    #[test]
    fn split_preserves_all_data() -> TResult<()> {
        let (shard, tmp) = new_shard(0..0x1000)?;
        let dir = tmp.path().to_path_buf();

        let mut original_data = HashMap::new();

        // Insert data with various key-value sizes
        for i in 0..30 {
            let key = format!("key_{:03}", i);
            let val = format!("value_{:03}_{}", i, "x".repeat(i % 10));

            let mut kbuf = [0u8; MAX_KEY_SIZE];
            kbuf[..key.len()].copy_from_slice(&key.into_bytes());

            let hash = TurboHasher::new(&kbuf);

            shard.set(&kbuf, val.as_bytes(), hash)?;
            original_data.insert(kbuf, (val, hash));
        }

        let original_count = original_data.len();

        let (top, bottom, _) = shard.split(&dir)?;

        // Verify all original data is preserved
        let mut found_count = 0;

        for (key, (val, hash)) in original_data {
            let shard_selector = hash.shard_selector();

            let target_shard = if shard_selector < 0x800 {
                &top
            } else {
                &bottom
            };

            let retrieved = target_shard.get(&key, hash)?;

            assert_eq!(
                retrieved,
                Some(val.as_bytes().to_vec()),
                "Data not preserved for key: {:?}",
                key
            );

            found_count += 1;
        }

        assert_eq!(found_count, original_count, "Not all data was preserved");

        // Verify total count matches
        let top_count = top.file.header().stats.n_occupied.load(Ordering::SeqCst);
        let bottom_count = bottom.file.header().stats.n_occupied.load(Ordering::SeqCst);

        assert_eq!((top_count + bottom_count) as usize, original_count);

        Ok(())
    }

    #[test]
    fn split_handles_edge_case_spans() -> TResult<()> {
        // Test with span that has only 2 elements
        let (shard, tmp) = new_shard(0..2)?;
        let dir = tmp.path().to_path_buf();

        let (top, bottom, _) = shard.split(&dir)?;

        assert_eq!(top.span, 0..1);
        assert_eq!(bottom.span, 1..2);

        Ok(())
    }

    #[test]
    fn split_with_deleted_entries() -> TResult<()> {
        let (shard, tmp) = new_shard(0..0x1000)?;
        let dir = tmp.path().to_path_buf();

        // key1
        let k1 = b"key1";
        let val1 = b"value1";

        let mut key1 = [0u8; MAX_KEY_SIZE];
        key1[..k1.len()].copy_from_slice(k1);

        let hash1 = TurboHasher::new(&key1);

        // key2
        let k2 = b"key2";
        let val2 = b"value2";

        let mut key2 = [0u8; MAX_KEY_SIZE];
        key2[..k2.len()].copy_from_slice(k2);

        let hash2 = TurboHasher::new(&key2);

        // Insert two entries
        shard.set(&key1, val1, hash1)?;
        shard.set(&key2, val2, hash2)?;

        // Delete one entry
        shard.remove(&key1, hash1)?;

        let (top, bottom, _) = shard.split(&dir)?;
        // Only the non-deleted entry should be in the split shards
        let shard_selector = hash2.shard_selector();

        let expected_shard = if shard_selector < 0x800 {
            &top
        } else {
            &bottom
        };

        let other_shard = if shard_selector < 0x800 {
            &bottom
        } else {
            &top
        };

        assert_eq!(expected_shard.get(&key2, hash2)?, Some(val2.to_vec()));
        assert_eq!(other_shard.get(&key2, hash2)?, None);

        // Deleted entry should not be in either shard
        assert_eq!(top.get(&key1, hash1)?, None);
        assert_eq!(bottom.get(&key1, hash1)?, None);

        // Verify stats - only one occupied entry total
        let top_count = top.file.header().stats.n_occupied.load(Ordering::SeqCst);
        let bottom_count = bottom.file.header().stats.n_occupied.load(Ordering::SeqCst);

        assert_eq!(top_count + bottom_count, 1);

        Ok(())
    }
}
