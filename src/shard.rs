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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
        const OFFSET_MASK: u32 = (1 << 22) - 1;

        let offset = packed.0 & OFFSET_MASK;
        let vlen = (packed.0 >> 22) as u16;

        (offset, vlen)
    }

    /// Packs a 10‑bit vlen and a 22‑bit offset into a single u32.
    fn to_self(vlen: u16, offset: u32) -> TResult<u32> {
        if !((vlen as u32) < (1 << 10)) {
            return Err(TError::ValTooLarge(vlen as usize));
        }

        if !(offset < (1 << 22)) {
            return Err(TError::OffsetOob(offset as usize));
        }

        Ok(((vlen as u32) << 22) | offset)
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

            if slot_k == SlotKey::default() && empty_idx.is_none() {
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

#[cfg(test)]
mod shard_header_slot_tests {
    use super::{ShardSlot, SlotKey, SlotOffset, MAX_KEY_SIZE, ROWS_WIDTH};

    #[test]
    fn slot_key_default_is_zeroed() {
        let default_key = SlotKey::default();

        assert!(
            default_key.0.iter().all(|&b| b == 0),
            "Default SlotKey should be all zeros"
        );
    }

    #[test]
    fn slot_key_equality() {
        let key1 = SlotKey([1; MAX_KEY_SIZE]);
        let key2 = SlotKey([1; MAX_KEY_SIZE]);
        let key3 = SlotKey([2; MAX_KEY_SIZE]);
        let default_key = SlotKey::default();

        assert_eq!(key1, key2, "Keys with same content should be equal");
        assert_eq!(
            SlotKey::default(),
            default_key,
            "Two default keys should be equal"
        );

        assert_ne!(
            key1, key3,
            "Keys with different content should not be equal"
        );
        assert_ne!(
            key1, default_key,
            "A non-default key should not be equal to a default key"
        );
    }

    #[test]
    fn slot_offset_roundtrip() {
        // slice of candidates (vlen, offset)
        let cases = &[
            // normal cases
            (0_u16, 0_u32),
            (1, 1),
            (0x3FF, 0xFFFFF),
            (0x3FF, 0x12345),
            // some edge cases
            (0, (1 << 22) - 1),             // max offset, zero vlen
            ((1 << 10) - 1, 0),             // max vlen, zero offset
            ((1 << 10) - 1, (1 << 22) - 1), // max vlen, max offset
        ];

        for &(vlen, offset) in cases {
            let packed = SlotOffset::to_self(vlen, offset).unwrap();
            let (unpacked_offset, unpacked_vlen) = SlotOffset::from_self(SlotOffset(packed));

            assert_eq!(
                unpacked_offset, offset,
                "Offset did not match after roundtrip. Original: {}, Got: {}",
                offset, unpacked_offset
            );
            assert_eq!(
                unpacked_vlen, vlen,
                "vlen did not match after roundtrip. Original: {}, Got: {}",
                vlen, unpacked_vlen
            );
        }
    }

    #[test]
    #[should_panic(expected = "ValTooLarge(4096)")]
    fn slot_offset_invalid_vlen() {
        // 2^12 is invalid, cause vlen is 10 bits!
        // So max value is `2^10 - 1`.
        let _ = SlotOffset::to_self(1 << 12, 0).unwrap();
    }

    #[test]
    #[should_panic(expected = "OffsetOob(4194304)")]
    fn slot_offset_invalid_offset() {
        // 2^22 is invalid, cause offset is 22 bits!
        // So max value is `2^22 - 1`.
        let _ = SlotOffset::to_self(0, 1 << 22).unwrap();
    }

    #[test]
    fn shard_slot_lookup_candidate() {
        let mut slot = ShardSlot::default();

        let key1 = SlotKey([1; MAX_KEY_SIZE]);
        let key2 = SlotKey([2; MAX_KEY_SIZE]);
        let non_existent_key = SlotKey([40; MAX_KEY_SIZE]);

        // ▶ Empty slot
        assert_eq!(
            slot.lookup_candidate(key1),
            None,
            "Should not find key in an empty slot"
        );

        // ▶ Slot with just one key
        slot.keys[5] = key1;

        assert_eq!(
            slot.lookup_candidate(key1),
            Some(5),
            "Should find the existing key at index 5"
        );
        assert_eq!(
            slot.lookup_candidate(key2),
            None,
            "Should not find a non-existent key"
        );

        // ▶ Slot with multiple keys
        slot.keys[0] = key2;

        assert_eq!(
            slot.lookup_candidate(key1),
            Some(5),
            "Should find key1 even with other keys present"
        );
        assert_eq!(
            slot.lookup_candidate(key2),
            Some(0),
            "Should find key2 at index 0"
        );

        // ▶ Full slot
        for i in 0..ROWS_WIDTH {
            slot.keys[i] = SlotKey([(i + 1) as u8; MAX_KEY_SIZE]);
        }

        let last_key = SlotKey([ROWS_WIDTH as u8; MAX_KEY_SIZE]);

        assert_eq!(
            slot.lookup_candidate(last_key),
            Some(ROWS_WIDTH - 1),
            "Should find key in a full slot"
        );
        assert_eq!(
            slot.lookup_candidate(non_existent_key),
            None,
            "Should not find key in a full slot if it's not there"
        );

        // ▶ Should not find a default/empty key,
        //
        // Reason: cause default/empty is not a valid candidate
        // as its the default state
        slot.keys[10] = SlotKey::default();

        assert_eq!(
            slot.lookup_candidate(SlotKey::default()),
            None,
            "Should not find the default (empty) key"
        );
    }

    #[test]
    fn shard_slot_lookup_candidate_or_empty() {
        let mut slot = ShardSlot::default();
        let key1 = SlotKey([1; MAX_KEY_SIZE]);
        let key2 = SlotKey([2; MAX_KEY_SIZE]);

        // ▶ Completely empty slot, should return no candidate,
        // but the first empty slot index!
        let (found, empty) = slot.lookup_candidate_or_empty(key1);

        assert_eq!(
            found, None,
            "Should not find a candidate in a completely empty slots row"
        );
        assert_eq!(
            empty,
            Some(0),
            "Should return the first index as empty for a new slot"
        );

        // ▶ Slots row with one key, searching for that key
        slot.keys[3] = key1;
        let (found, empty) = slot.lookup_candidate_or_empty(key1);

        assert_eq!(found, Some(3), "Should find the candidate at index 3");
        assert_eq!(
            empty, None,
            "Should not return an empty slot when candidate is found"
        );

        // ▶ Slots row with one key, searching for another key
        let (found, empty) = slot.lookup_candidate_or_empty(key2);

        assert_eq!(
            found, None,
            "Should not find a candidate for a key that is not present",
        );
        assert_eq!(
            empty,
            Some(0),
            "Should return the first available empty slot (index 0)"
        );

        // ▶ Slots row with some keys, searching for a new key

        // key2 is now at index 0
        // empty slots are 1, 2, 4, 5...
        slot.keys[0] = key2;
        let (found, empty) = slot.lookup_candidate_or_empty(SlotKey([99; MAX_KEY_SIZE]));

        assert_eq!(found, None, "Should not find a non-existent key");
        assert_eq!(
            empty,
            Some(1),
            "Should return the next empty slot (index 1)"
        );

        // ▶ Full slots, searching for an existing key
        for i in 0..ROWS_WIDTH {
            slot.keys[i] = SlotKey([(i + 1) as u8; MAX_KEY_SIZE]);
        }

        let existing_key_in_full_slot = SlotKey([5; MAX_KEY_SIZE]);
        let (found, empty) = slot.lookup_candidate_or_empty(existing_key_in_full_slot);

        assert_eq!(
            found,
            Some(4),
            "Should find the existing key in a full slot"
        );
        assert_eq!(
            empty, None,
            "Should not return an empty slot when the slot is full and candidate is found"
        );

        // ▶ Full slots, searching for a new key, should get None!
        let non_existent_key_in_full_slot = SlotKey([100; MAX_KEY_SIZE]);
        let (found, empty) = slot.lookup_candidate_or_empty(non_existent_key_in_full_slot);

        assert_eq!(
            found, None,
            "Should not find a non-existent key in a full slot"
        );
        assert_eq!(
            empty, None,
            "Should not find an empty slot when the slot is full"
        );
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

#[cfg(test)]
mod shard_header_tests {
    use super::*;
    use std::{
        mem::{align_of, size_of},
        sync::atomic::Ordering,
    };

    #[test]
    fn shard_meta_default_values() {
        let meta = ShardMeta::default();

        assert_eq!(
            meta.magic, MAGIC,
            "Default magic value should match the constant"
        );
        assert_eq!(
            meta.version, VERSION,
            "Default version should match the constant"
        );
    }

    #[test]
    fn shard_meta_size_and_alignment() {
        assert_eq!(size_of::<ShardMeta>(), 16, "ShardMeta should be 16 bytes");

        assert_eq!(
            align_of::<ShardMeta>(),
            8,
            "ShardMeta should have 8-byte alignment"
        );
    }

    #[test]
    fn shard_stats_default_values() {
        let stats = ShardStats::default();

        assert_eq!(
            stats.n_occupied.load(Ordering::SeqCst),
            0,
            "Default `n_occupied` should be 0"
        );
        assert_eq!(
            stats.write_offset.load(Ordering::SeqCst),
            0,
            "Default `write_offset` should be 0"
        );
    }

    #[test]
    fn shard_stats_size_and_alignment() {
        assert_eq!(size_of::<ShardStats>(), 8, "ShardStats should be 8 bytes");

        assert_eq!(
            align_of::<ShardStats>(),
            4,
            "ShardStats should have 4-byte alignment"
        );
    }

    #[test]
    fn shard_header_size_and_alignment() {
        assert_eq!(
            size_of::<ShardHeader>(),
            HEADER_SIZE as usize,
            "ShardHeader size should match the HEADER_SIZE constant"
        );

        assert_eq!(
            align_of::<ShardHeader>(),
            4096,
            "ShardHeader should have 4096-byte alignment due to PageAligned index"
        );
    }

    #[test]
    fn shard_header_initial_offset() {
        assert_eq!(
            ShardHeader::get_init_offset(),
            0,
            "Initial offset for header should always be 0"
        );
    }

    #[test]
    fn page_aligned_struct_alignment() {
        assert_eq!(
            align_of::<PageAligned<[ShardSlot; ROWS_NUM]>>(),
            4096,
            "PageAligned index should enforce 4096-byte alignment"
        );
    }
}

#[derive(Debug)]
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

        // Validations for existing files,
        //
        // ▶ validate their size before memory mapping
        if !is_new {
            let metadata = file.metadata()?;

            if metadata.len() < HEADER_SIZE {
                return Err(TError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "shard file is smaller than header",
                )));
            }
        }

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

        let offset = SlotOffset::to_self(vlen as u16, write_offset)?;

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

#[cfg(test)]
mod shard_file_tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    // create a temp file path
    fn temp_path(dir: &tempfile::TempDir, name: &str) -> PathBuf {
        dir.path().join(name)
    }

    #[test]
    fn open_new_file_with_correct_initializes() -> TResult<()> {
        let dir = tempdir()?;
        let path = temp_path(&dir, "new_shard.db");

        // Open a new shard file
        let shard_file = ShardFile::open(&path, true)?;

        // ▶ Check file existence and size
        assert!(path.exists(), "Shard file should be created");

        let metadata = fs::metadata(&path)?;

        assert_eq!(
            metadata.len(),
            HEADER_SIZE,
            "File size should be exactly HEADER_SIZE"
        );

        // ▶ Check header content against default
        let header = shard_file.header();

        assert_eq!(
            header.meta.magic, MAGIC,
            "Magic number should match default"
        );
        assert_eq!(header.meta.version, VERSION, "Version should match default");
        assert_eq!(
            header.stats.n_occupied.load(Ordering::SeqCst),
            0,
            "`n_occupied` should be 0"
        );
        assert_eq!(
            header.stats.write_offset.load(Ordering::SeqCst),
            0,
            "`write_offset` should be 0"
        );

        // ▶ Check that the index is zeroed out
        let default_slot = ShardSlot::default();

        for i in 0..ROWS_NUM {
            let slot = &header.index.0[i];

            for j in 0..ROWS_WIDTH {
                assert_eq!(
                    slot.keys[j], default_slot.keys[j],
                    "Key at index [{}][{}] should be default",
                    i, j
                );
                assert_eq!(
                    slot.offsets[j].0, default_slot.offsets[j].0,
                    "Offset at index [{}][{}] should be default",
                    i, j
                );
            }
        }

        // ▶ Verify raw file content matches default buffer
        let file_content = fs::read(&path)?;
        let default_buf = ShardHeader::get_default_buf();

        assert_eq!(
            file_content, default_buf,
            "The entire file content should match the default header buffer"
        );

        Ok(())
    }

    #[test]
    fn header_init_correctness() -> TResult<()> {
        let dir = tempdir()?;
        let path = temp_path(&dir, "header_init_test.db");

        // ▶ Open a new shard file, which should initialize the header
        let shard_file = ShardFile::open(&path, true)?;

        let header = shard_file.header();

        // ▶ Verify ShardMeta fields
        assert_eq!(
            header.meta.magic, MAGIC,
            "Magic should be initialized correctly"
        );
        assert_eq!(
            header.meta.version, VERSION,
            "Version should be initialized correctly"
        );

        // ▶ Verify ShardStats fields
        assert_eq!(
            header.stats.n_occupied.load(Ordering::SeqCst),
            0,
            "n_occupied should be 0"
        );
        assert_eq!(
            header.stats.write_offset.load(Ordering::SeqCst),
            0,
            "write_offset should be 0"
        );

        // ▶ Verify index (ShardSlot array) is zeroed out
        let default_slot = ShardSlot::default();

        for i in 0..ROWS_NUM {
            let slot = &header.index.0[i];

            for j in 0..ROWS_WIDTH {
                assert_eq!(
                    slot.keys[j], default_slot.keys[j],
                    "Index key at [{}][{}] should be default",
                    i, j
                );
                assert_eq!(
                    slot.offsets[j].0, default_slot.offsets[j].0,
                    "Index offset at [{}][{}] should be default",
                    i, j
                );
            }
        }

        Ok(())
    }

    #[test]
    fn open_existing_file_loads_data() -> TResult<()> {
        let dir = tempdir()?;
        let path = temp_path(&dir, "existing_shard.db");

        // ▶ Create and modify a shard file manually
        {
            let shard_file = ShardFile::open(&path, true)?;
            let header = shard_file.header_mut();

            header.stats.n_occupied.store(123, Ordering::SeqCst);
            header.stats.write_offset.store(456, Ordering::SeqCst);

            // ▶ Modify a specific slot in the index
            let row_idx = 10;
            let col_idx = 5;
            let mut key = SlotKey::default();

            key.0[0] = 0xAB;
            header.index.0[row_idx].keys[col_idx] = key;
            header.index.0[row_idx].offsets[col_idx] = SlotOffset(0xCDEF);

            // ▶ Ensure changes are flushed to disk
            shard_file.mmap.flush()?;
        }

        // ▶ Re-open the existing file, and check that the
        // loaded data is correct
        let shard_file = ShardFile::open(&path, false)?;
        let header = shard_file.header();

        assert_eq!(
            header.stats.n_occupied.load(Ordering::SeqCst),
            123,
            "`n_occupied` should be loaded from existing file"
        );
        assert_eq!(
            header.stats.write_offset.load(Ordering::SeqCst),
            456,
            "`write_offset` should be loaded from existing file"
        );

        let row_idx = 10;
        let col_idx = 5;

        let mut expected_key = SlotKey::default();
        expected_key.0[0] = 0xAB;

        assert_eq!(
            header.index.0[row_idx].keys[col_idx], expected_key,
            "Index key data should be loaded correctly"
        );
        assert_eq!(
            header.index.0[row_idx].offsets[col_idx].0, 0xCDEF,
            "Index offset data should be loaded correctly"
        );

        Ok(())
    }

    #[test]
    fn write_read_slot_roundtrip() -> TResult<()> {
        let dir = tempdir()?;
        let path = temp_path(&dir, "slot_test.db");
        let shard_file = ShardFile::open(&path, true)?;

        let v1 = b"hello".to_vec();
        let v2 = b"world".to_vec();

        // A larger value
        let v3 = vec![0u8; 1023];

        // Empty value
        let v4 = b"".to_vec();

        // ▶ Write first value
        let slot1 = shard_file.write_slot(&v1)?;

        assert_eq!(
            shard_file
                .header()
                .stats
                .write_offset
                .load(Ordering::SeqCst),
            v1.len() as u32,
            "Write offset should be updated after first write"
        );

        // ▶ Write second value
        let slot2 = shard_file.write_slot(&v2)?;

        assert_eq!(
            shard_file
                .header()
                .stats
                .write_offset
                .load(Ordering::SeqCst),
            (v1.len() + v2.len()) as u32,
            "Write offset should be updated after second write"
        );

        // Write third and fourth values
        let slot3 = shard_file.write_slot(&v3)?;
        let slot4 = shard_file.write_slot(&v4)?;

        // ▶ Read back and verify

        assert_eq!(
            shard_file.read_slot(slot1)?,
            v1,
            "First value did not match after read"
        );
        assert_eq!(
            shard_file.read_slot(slot2)?,
            v2,
            "Second value did not match after read"
        );
        assert_eq!(
            shard_file.read_slot(slot3)?,
            v3,
            "Third value did not match after read"
        );
        assert_eq!(
            shard_file.read_slot(slot4)?,
            v4,
            "Fourth (empty) value did not match after read"
        );

        // ▶ Verify the packed offsets and lengths
        let (offset1, vlen1) = SlotOffset::from_self(slot1);
        let (offset2, vlen2) = SlotOffset::from_self(slot2);

        assert_eq!(offset1, 0, "Offset of first slot should be 0");
        assert_eq!(vlen1, v1.len() as u16, "Vlen of first slot is incorrect");
        assert_eq!(vlen2, v2.len() as u16, "Vlen of second slot is incorrect");
        assert_eq!(
            offset2,
            v1.len() as u32,
            "Offset of second slot is incorrect"
        );

        Ok(())
    }

    #[test]
    fn header_and_row_mut_access() -> TResult<()> {
        let dir = tempdir()?;
        let path = temp_path(&dir, "mut_access.db");
        let shard_file = ShardFile::open(&path, true)?;

        // ▶ Mutate stats via header_mut
        shard_file
            .header_mut()
            .stats
            .n_occupied
            .store(99, Ordering::SeqCst);

        assert_eq!(
            shard_file.header().stats.n_occupied.load(Ordering::SeqCst),
            99,
            "Change via header_mut should be reflected immediately"
        );

        // ▶ Mutate index via row_mut
        let row_idx = 42;
        let col_idx = 7;

        let mut key = SlotKey::default();
        key.0[0] = 0xFF;

        let offset = SlotOffset(12345);

        let row = shard_file.row_mut(row_idx);

        row.keys[col_idx] = key;
        row.offsets[col_idx] = offset;

        // ▶ Verify with immutable access
        let same_row = shard_file.row(row_idx);

        assert_eq!(
            same_row.keys[col_idx], key,
            "Key change via row_mut should be reflected"
        );
        assert_eq!(
            same_row.offsets[col_idx].0, offset.0,
            "Offset change via row_mut should be reflected"
        );

        Ok(())
    }

    #[test]
    #[should_panic]
    fn row_access_out_of_bounds() {
        let dir = tempdir().unwrap();
        let path = temp_path(&dir, "bounds_test.db");
        let shard_file = ShardFile::open(&path, true).unwrap();

        // This should panic
        let _ = shard_file.row(ROWS_NUM);
    }

    #[test]
    #[should_panic]
    fn row_mut_access_out_of_bounds() {
        let dir = tempdir().unwrap();
        let path = temp_path(&dir, "bounds_test_mut.db");
        let shard_file = ShardFile::open(&path, true).unwrap();

        // This should panic
        let _ = shard_file.row_mut(ROWS_NUM);
    }

    #[test]
    fn read_from_invalid_offset_fails() -> TResult<()> {
        let dir = tempdir()?;
        let path = temp_path(&dir, "invalid_read.db");
        let shard_file = ShardFile::open(&path, true)?;

        // ▶ Create a slot that points beyond the current end of the file!
        //
        // File size is HEADER_SIZE, write_offset is 0.
        // A read from offset HEADER_SIZE should fail.

        // 10 bytes from offset 0 in value area
        let invalid_slot = SlotOffset::to_self(10, 0).unwrap();
        let result = shard_file.read_slot(SlotOffset(invalid_slot));

        // On unix, this will be an `UnexpectedEof` from `read_exact_at`. The error type might
        // differ across platforms! So just checking for `is_err` is robust ;)
        assert!(
            result.is_err(),
            "Reading from an offset beyond EOF should fail"
        );

        Ok(())
    }

    #[test]
    fn open_truncated_file_fails_mapping() -> TResult<()> {
        let dir = tempdir()?;
        let path = temp_path(&dir, "truncated.db");

        // ▶ Create a file smaller than the header
        let file = File::create(&path)?;

        file.set_len(HEADER_SIZE - 1)?;
        drop(file);

        // ▶ Attempting to open it should fail at the mmap stage
        let result = ShardFile::open(&path, false);

        assert!(
            result.is_err(),
            "Opening a file smaller than HEADER_SIZE should fail"
        );

        // ▶ The error should be an I/O error!
        assert!(
            matches!(result, Err(TError::Io(_))),
            "Error should be an I/O error, but got {:?}",
            result
        );

        Ok(())
    }

    #[test]
    fn meta_and_stats_are_written_correctly() -> TResult<()> {
        let dir = tempdir()?;
        let path = temp_path(&dir, "meta_stats_test.db");

        // ▶ Read the raw header from the file
        let shard_file = ShardFile::open(&path, true)?;
        let header_from_disk = shard_file.header();

        // ▶ Verify the raw bytes
        assert_eq!(
            header_from_disk.meta.magic, MAGIC,
            "Magic bytes should be written correctly"
        );
        assert_eq!(
            header_from_disk.meta.version, VERSION,
            "Version should be written correctly"
        );
        assert_eq!(
            header_from_disk.stats.n_occupied.load(Ordering::SeqCst),
            0,
            "`n_occupied` should be written correctly"
        );
        assert_eq!(
            header_from_disk.stats.write_offset.load(Ordering::SeqCst),
            0,
            "`write_offset` should be written correctly"
        );

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

        if candidate == SlotKey::default() {
            return Err(TError::KeyTooSmall);
        }

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
    fn open_fails_for_invalid_dir() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("non_existent_dir");
        let span = 0..1;

        let result = Shard::open(&dir, span, true);

        assert!(
            result.is_err(),
            "Shard::open should fail if the parent directory does not exist"
        );
    }

    #[test]
    fn open_creates_correct_filename() -> TResult<()> {
        let span = 0x123..0x456;
        let (shard, tmp) = new_shard(span.clone())?;
        let expected_filename = format!("shard_{:04x}-{:04x}", span.start, span.end);
        let expected_path = tmp.path().join(expected_filename);

        assert!(
            expected_path.exists(),
            "Shard file was not created at the expected path: {:?}",
            expected_path
        );
        assert_eq!(
            shard.span, span,
            "Shard span should be initialized correctly"
        );

        Ok(())
    }

    #[test]
    fn set_with_empty_key_fails() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..1)?;
        let kbuf = [0u8; MAX_KEY_SIZE]; // This is SlotKey::default()
        let val = b"some value";
        let h = TurboHasher::new(&kbuf);

        let result = shard.set(&kbuf, val, h);

        assert!(
            matches!(result, Err(TError::KeyTooSmall)),
            "Setting a default/empty key should return KeyTooSmall error"
        );

        Ok(())
    }

    #[test]
    fn set_get_with_empty_and_large_values() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..1)?;
        let mut kbuf = [0u8; MAX_KEY_SIZE];
        kbuf[0] = 1;
        let h = TurboHasher::new(&kbuf);

        // Test with empty value
        let empty_val = b"";
        shard.set(&kbuf, empty_val, h)?;
        let retrieved = shard.get(&kbuf, h)?;
        assert_eq!(
            retrieved,
            Some(vec![]),
            "Should correctly get an empty value"
        );

        // Test with max-size value
        const MAX_VLEN: usize = (1 << 10) - 1;
        let large_val = vec![1u8; MAX_VLEN];
        shard.set(&kbuf, &large_val, h)?;
        let retrieved_large = shard.get(&kbuf, h)?;
        assert_eq!(
            retrieved_large,
            Some(large_val),
            "Should correctly get a max-size value"
        );

        Ok(())
    }

    #[test]
    #[should_panic(expected = "ValTooLarge(1024)")]
    fn set_with_value_too_large_panics() {
        let (shard, _tmp) = new_shard(0..1).unwrap();
        let mut kbuf = [0u8; MAX_KEY_SIZE];
        kbuf[0] = 1;
        let h = TurboHasher::new(&kbuf);

        const INVALID_VLEN: usize = 1 << 10;
        let too_large_val = vec![0u8; INVALID_VLEN];

        // This should panic inside SlotOffset::to_self
        let _ = shard.set(&kbuf, &too_large_val, h).unwrap();
    }

    #[test]
    fn get_non_existent_key() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..1)?;
        let mut k_exists = [0u8; MAX_KEY_SIZE];
        k_exists[0] = 1;
        let h_exists = TurboHasher::new(&k_exists);
        shard.set(&k_exists, b"value", h_exists)?;

        let mut k_missing = [0u8; MAX_KEY_SIZE];
        k_missing[0] = 2;
        let h_missing = TurboHasher::new(&k_missing);

        let result = shard.get(&k_missing, h_missing)?;
        assert_eq!(
            result, None,
            "Getting a non-existent key should return None"
        );

        Ok(())
    }

    #[test]
    fn remove_returns_correct_value() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..1)?;
        let mut kbuf = [0u8; MAX_KEY_SIZE];
        kbuf[0] = 1;
        let val = b"value to be returned".to_vec();
        let h = TurboHasher::new(&kbuf);

        shard.set(&kbuf, &val, h)?;
        let removed_val = shard.remove(&kbuf, h)?;

        assert_eq!(
            removed_val,
            Some(val),
            "Remove should return the value of the deleted key"
        );

        let removed_again = shard.remove(&kbuf, h)?;
        assert_eq!(
            removed_again, None,
            "Removing a key again should return None"
        );

        Ok(())
    }

    #[test]
    fn n_occupied_stat_on_overwrite() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..1)?;
        let mut kbuf = [0u8; MAX_KEY_SIZE];
        kbuf[0] = 1;
        let h = TurboHasher::new(&kbuf);

        shard.set(&kbuf, b"v1", h)?;
        let count1 = shard.file.header().stats.n_occupied.load(Ordering::SeqCst);
        assert_eq!(count1, 1, "n_occupied should be 1 after first insert");

        // Overwrite the key
        shard.set(&kbuf, b"v2", h)?;
        let count2 = shard.file.header().stats.n_occupied.load(Ordering::SeqCst);
        assert_eq!(
            count2, 1,
            "n_occupied should not change when a key is overwritten"
        );

        Ok(())
    }

    #[test]
    fn set_get_with_full_key() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..1)?;
        let kbuf = [1u8; MAX_KEY_SIZE];
        let val = b"value for full key".to_vec();
        let h = TurboHasher::new(&kbuf);

        shard.set(&kbuf, &val, h)?;
        let retrieved = shard.get(&kbuf, h)?;
        assert_eq!(
            retrieved,
            Some(val),
            "Should set and get a key of MAX_KEY_SIZE"
        );

        Ok(())
    }

    #[test]
    fn multiple_keys_in_same_row() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..1)?;
        let mut k1 = [0u8; MAX_KEY_SIZE];
        let mut k2 = [0u8; MAX_KEY_SIZE];
        let v1 = b"value1".to_vec();
        let v2 = b"value2".to_vec();

        let mut h1;
        let mut h2;

        // Find two keys that map to the same row
        let mut i: u32 = 0;
        loop {
            // Use a pseudo-random sequence to generate more varied keys
            k1[0] = i.wrapping_mul(17) as u8;
            k2[0] = i.wrapping_mul(31) as u8;
            h1 = TurboHasher::new(&k1);
            h2 = TurboHasher::new(&k2);

            if h1.row_selector() == h2.row_selector() && k1 != k2 {
                break;
            }
            i += 1;
            assert!(i < 10000, "Could not find two keys for the same row");
        }

        // Set both keys
        shard.set(&k1, &v1, h1)?;
        shard.set(&k2, &v2, h2)?;

        assert_eq!(
            shard.file.header().stats.n_occupied.load(Ordering::SeqCst),
            2,
            "Should have 2 occupied slots"
        );

        // Get both keys
        assert_eq!(
            shard.get(&k1, h1)?,
            Some(v1.clone()),
            "Should retrieve k1 correctly"
        );
        assert_eq!(
            shard.get(&k2, h2)?,
            Some(v2.clone()),
            "Should retrieve k2 correctly"
        );

        // Remove one key and check
        shard.remove(&k1, h1)?;
        assert_eq!(shard.get(&k1, h1)?, None, "k1 should be gone after removal");
        assert_eq!(
            shard.get(&k2, h2)?,
            Some(v2.clone()),
            "k2 should still exist after k1 is removed"
        );
        assert_eq!(
            shard.file.header().stats.n_occupied.load(Ordering::SeqCst),
            1,
            "Should have 1 occupied slot after removal"
        );

        Ok(())
    }
}

#[cfg(test)]
mod shard_split_tests {
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

        // ▶ Both shards should be empty
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

        // ▶ Inserting multiple entries which will be distributed
        // across both shards
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

        // ▶ Ensure we have data for both shards
        assert!(top_count > 0, "No entries for top shard");
        assert!(bottom_count > 0, "No entries for bottom shard");

        let (top, bottom, _) = shard.split(&dir)?;

        // ▶ Verify all entries are in the correct shards
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

            assert_eq!(other_shard.get(&key, hash)?, None);
            assert_eq!(
                expected_shard.get(&key.clone(), hash)?,
                Some(val.into_bytes())
            );
        }

        // ▶ Verify stats
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

        // ▶ Ensure the file exists before split
        assert!(original_path.exists());

        // ▶ Perform the split
        let (_top, _bottom, _) = shard.split(&dir)?;

        // ▶ Original file should be removed
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

        let mid = (span.start + span.end) / 2;
        let top_path = dir.join(format!("shard_{:04x}-{:04x}", span.start, mid));
        let bottom_path = dir.join(format!("shard_{:04x}-{:04x}", mid, span.end));

        assert!(
            top_path.exists(),
            "Top shard file {:?} was not created",
            top_path
        );
        assert!(
            bottom_path.exists(),
            "Bottom shard file {:?} was not created",
            bottom_path
        );
        assert_eq!(top.span, span.start..mid, "Top shard has incorrect span");
        assert_eq!(
            bottom.span,
            mid..span.end,
            "Bottom shard has incorrect span"
        );

        Ok(())
    }
}

#[cfg(test)]
mod shard_split_large_tests {
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
    #[ignore]
    fn test_split_with_large_number_of_entries() -> TResult<()> {
        let (shard, tmp) = new_shard(0..0x1000)?;
        let dir = tmp.path().to_path_buf();

        let num_entries = 10_000;

        let mut test_data = HashMap::new();
        let mut uninserted_kvs = Vec::new();

        let mut expected_top_count = 0;
        let mut expected_bottom_count = 0;

        // ▶ Insert a large number of entries
        for i in 0..num_entries {
            let key = format!("key_{:05}", i);
            let val = format!(
                "value_for_key_{:05}_with_some_extra_data_to_make_it_larger",
                i
            );

            let mut kbuf = [0u8; MAX_KEY_SIZE];
            kbuf[..key.len()].copy_from_slice(&key.clone().into_bytes());

            let hash = TurboHasher::new(&kbuf);

            match shard.set(&kbuf, &val.clone().into_bytes(), hash) {
                Ok(_) => {
                    test_data.insert(kbuf.clone(), (val, hash));

                    if hash.shard_selector() < 0x800 {
                        expected_top_count += 1;
                    } else {
                        expected_bottom_count += 1;
                    }
                }
                Err(TError::RowFull(_)) => {
                    uninserted_kvs.push((kbuf, val.into_bytes()));
                }
                Err(e) => panic!("Unexpected error during initial shard population: {:?}", e),
            }
        }

        // ▶ Ensure all intended entries are accounted for
        assert_eq!(
            test_data.len() + uninserted_kvs.len(),
            num_entries,
            "Total entries (inserted + uninserted) should match original count"
        );

        // ▶ Perform the split
        let (top, bottom, remaining_kvs) = shard.split(&dir)?;

        // ▶ Verify that the total count of items (migrated + remaining) matches the number of successfully inserted items
        let total_migrated_count = top.file.header().stats.n_occupied.load(Ordering::SeqCst)
            + bottom.file.header().stats.n_occupied.load(Ordering::SeqCst);

        assert_eq!(
            total_migrated_count + remaining_kvs.len() as u32,
            test_data.len() as u32,
            "Total entries after split (migrated + remaining) should match successfully inserted count"
        );

        // ▶ Verify all entries are in the correct shards
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
                Some(val.into_bytes()),
                "Key {:?} not found or value mismatch in expected shard",
                key
            );
            assert_eq!(
                other_shard.get(&key, hash)?,
                None,
                "Key {:?} found in unexpected shard",
                key
            );
        }

        // ▶ Verify stats
        let top_header = top.file.header();
        let bottom_header = bottom.file.header();

        assert_eq!(
            top_header.stats.n_occupied.load(Ordering::SeqCst),
            expected_top_count,
            "Top shard occupied count mismatch"
        );
        assert_eq!(
            bottom_header.stats.n_occupied.load(Ordering::SeqCst),
            expected_bottom_count,
            "Bottom shard occupied count mismatch"
        );

        Ok(())
    }

    #[test]
    #[ignore]
    fn split_with_all_keys_to_one_shard() -> TResult<()> {
        let span = 0..0x1000;
        let mid = (span.start + span.end) / 2;
        let (shard, tmp) = new_shard(span.clone())?;
        let dir = tmp.path().to_path_buf();

        let mut keys_in_top = 0;
        let mut test_data = HashMap::new();

        // ▶ Insert entries that will all fall into the top shard
        for i in 0..30 {
            let mut kbuf = [0u8; MAX_KEY_SIZE];
            let mut hash;
            let mut counter = 0;

            // ▶ Find a key that belongs to the top shard
            loop {
                let key = format!("key_{}_{}", i, counter);

                kbuf[..key.len()].copy_from_slice(key.as_bytes());
                hash = TurboHasher::new(&kbuf);

                if hash.shard_selector() < mid {
                    break;
                }

                counter += 1;

                assert!(counter < 10000, "Failed to find a key for the top shard");
            }

            let val = format!("value_{}", i);

            shard.set(&kbuf, val.as_bytes(), hash)?;
            test_data.insert(kbuf, val);

            keys_in_top += 1;
        }

        assert_eq!(
            shard.file.header().stats.n_occupied.load(Ordering::SeqCst),
            keys_in_top,
            "All keys should be in the original shard"
        );

        let (top, bottom, remaining) = shard.split(&dir)?;

        assert!(
            remaining.is_empty(),
            "There should be no remaining KVs when all keys go to one shard"
        );
        assert_eq!(
            top.file.header().stats.n_occupied.load(Ordering::SeqCst),
            keys_in_top,
            "All keys should have moved to the top shard"
        );
        assert_eq!(
            bottom.file.header().stats.n_occupied.load(Ordering::SeqCst),
            0,
            "Bottom shard should be empty"
        );

        // ▶ Verify all data is in the top shard
        for (kbuf, val) in test_data {
            let hash = TurboHasher::new(&kbuf);

            assert_eq!(
                top.get(&kbuf, hash)?,
                Some(val.into_bytes()),
                "Key should be in the top shard after split"
            );
            assert_eq!(
                bottom.get(&kbuf, hash)?,
                None,
                "Key should not be in the bottom shard after split"
            );
        }

        Ok(())
    }

    #[test]
    #[ignore]
    fn split_with_empty_and_large_values() -> TResult<()> {
        let (shard, tmp) = new_shard(0..0x1000)?;
        let dir = tmp.path().to_path_buf();

        // ▶ K1: empty value
        let mut k1 = [0u8; MAX_KEY_SIZE];
        k1[0] = 1;

        let h1 = TurboHasher::new(&k1);
        let v1 = b"";

        shard.set(&k1, v1, h1)?;

        // ▶ K2: large value
        let mut k2 = [0u8; MAX_KEY_SIZE];
        k2[0] = 2;

        let h2 = TurboHasher::new(&k2);
        const MAX_VLEN: usize = (1 << 10) - 1;
        let v2 = vec![1u8; MAX_VLEN];

        shard.set(&k2, &v2, h2)?;

        let (top, bottom, remaining) = shard.split(&dir)?;

        assert!(
            remaining.is_empty(),
            "Should be no remaining KVs for this test case"
        );

        // ▶ Check k1
        let shard1 = if h1.shard_selector() < 0x800 {
            &top
        } else {
            &bottom
        };

        let other1 = if h1.shard_selector() < 0x800 {
            &bottom
        } else {
            &top
        };

        assert_eq!(
            shard1.get(&k1, h1)?,
            Some(v1.to_vec()),
            "Empty value not retrieved correctly after split"
        );
        assert_eq!(
            other1.get(&k1, h1)?,
            None,
            "Key with empty value found in wrong shard"
        );

        // ▶ Check k2
        let shard2 = if h2.shard_selector() < 0x800 {
            &top
        } else {
            &bottom
        };

        let other2 = if h2.shard_selector() < 0x800 {
            &bottom
        } else {
            &top
        };

        assert_eq!(
            shard2.get(&k2, h2)?,
            Some(v2),
            "Large value not retrieved correctly after split"
        );
        assert_eq!(
            other2.get(&k2, h2)?,
            None,
            "Key with large value found in wrong shard"
        );

        Ok(())
    }

    #[test]
    #[ignore]
    fn split_with_row_collision() -> TResult<()> {
        let span = 0..0x1000;
        let mid = (span.start + span.end) / 2;
        let (shard, tmp) = new_shard(span.clone())?;
        let dir = tmp.path().to_path_buf();

        let mut k1 = [0u8; MAX_KEY_SIZE];
        let mut h1;

        let mut k2 = [0u8; MAX_KEY_SIZE];
        let mut h2;

        let mut counter: u64 = 0;

        // ▶ Find two keys that map to the same row in the original shard,
        // but to different new shards.
        loop {
            let key1_str = format!("key_{}", counter);
            k1[..key1_str.len()].copy_from_slice(key1_str.as_bytes());
            h1 = TurboHasher::new(&k1);

            let key2_str = format!("key_{}", counter + 1);
            k2[..key2_str.len()].copy_from_slice(key2_str.as_bytes());
            h2 = TurboHasher::new(&k2);

            if h1.row_selector() == h2.row_selector()
                && h1.shard_selector() < mid
                && h2.shard_selector() >= mid
            {
                break;
            }

            counter += 2;

            assert!(
                counter < 100000,
                "Could not find suitable colliding keys for test"
            );
        }

        let v1 = b"value1".to_vec();
        let v2 = b"value2".to_vec();

        shard.set(&k1, &v1, h1)?;
        shard.set(&k2, &v2, h2)?;

        assert_eq!(
            shard.file.header().stats.n_occupied.load(Ordering::SeqCst),
            2,
            "Should have two items before split"
        );

        let (top, bottom, remaining) = shard.split(&dir)?;

        assert!(remaining.is_empty(), "There should be no remaining KVs");
        assert_eq!(
            top.file.header().stats.n_occupied.load(Ordering::SeqCst),
            1,
            "Top shard should have one item"
        );
        assert_eq!(
            bottom.file.header().stats.n_occupied.load(Ordering::SeqCst),
            1,
            "Bottom shard should have one item"
        );
        assert_eq!(top.get(&k1, h1)?, Some(v1), "k1 should be in the top shard");
        assert_eq!(top.get(&k2, h2)?, None, "k2 should not be in the top shard");
        assert_eq!(
            bottom.get(&k1, h1)?,
            None,
            "k1 should not be in the bottom shard"
        );
        assert_eq!(
            bottom.get(&k2, h2)?,
            Some(v2),
            "k2 should be in the bottom shard"
        );

        Ok(())
    }

    #[test]
    #[ignore]
    fn split_handles_edge_case_spans() -> TResult<()> {
        // ▶ Test with span that has only 2 elements
        let (shard, tmp) = new_shard(0..2)?;
        let dir = tmp.path().to_path_buf();
        let (top, bottom, _) = shard.split(&dir)?;

        assert_eq!(top.span, 0..1);
        assert_eq!(bottom.span, 1..2);

        Ok(())
    }

    #[test]
    #[ignore]
    fn split_with_deleted_entries() -> TResult<()> {
        let (shard, tmp) = new_shard(0..0x1000)?;
        let dir = tmp.path().to_path_buf();

        // ▶ K1
        let k1 = b"key1";
        let val1 = b"value1";

        let mut key1 = [0u8; MAX_KEY_SIZE];
        key1[..k1.len()].copy_from_slice(k1);

        let hash1 = TurboHasher::new(&key1);

        // ▶ K2
        let k2 = b"key2";
        let val2 = b"value2";

        let mut key2 = [0u8; MAX_KEY_SIZE];
        key2[..k2.len()].copy_from_slice(k2);

        let hash2 = TurboHasher::new(&key2);

        // ▶ Insert two entries
        shard.set(&key1, val1, hash1)?;
        shard.set(&key2, val2, hash2)?;

        // ▶ Delete one entry
        shard.remove(&key1, hash1)?;

        let (top, bottom, _) = shard.split(&dir)?;

        // ▶ Only the non-deleted entry should be in the split shards
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

        // ▶ Deleted entry should not be in either shard
        assert_eq!(top.get(&key1, hash1)?, None);
        assert_eq!(bottom.get(&key1, hash1)?, None);

        // ▶ Verify stats - only one occupied entry total
        let top_count = top.file.header().stats.n_occupied.load(Ordering::SeqCst);
        let bottom_count = bottom.file.header().stats.n_occupied.load(Ordering::SeqCst);

        assert_eq!(top_count + bottom_count, 1);

        Ok(())
    }
}

#[cfg(test)]
mod shard_simulations {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::TempDir;

    fn new_shard(span: Range<u32>) -> TResult<(Shard, TempDir)> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;
        let s = Shard::open(&dir, span, true)?;

        Ok((s, tmp))
    }

    #[test]
    #[ignore]
    fn simulate() {
        let mut capacities = Vec::new();
        let mut rng = StdRng::seed_from_u64(07);

        for i in 0..50 {
            let (shard, _tmp) = new_shard(0..(u16::MAX as u32 + 1)).unwrap();

            println!(
                "\n--- Running Shard Insertion Simulation (Run {}) ---",
                i + 1
            );

            loop {
                let vlen = rng.random_range(8..=512);
                let val: Vec<u8> = (0..vlen).map(|_| rng.random()).collect();

                let mut kbuf = [0u8; MAX_KEY_SIZE];
                let key_bytes: Vec<u8> = (0..MAX_KEY_SIZE).map(|_| rng.random()).collect();
                kbuf[..key_bytes.len()].copy_from_slice(&key_bytes);

                let h = TurboHasher::new(&kbuf);

                if let Err(TError::RowFull(row_idx)) = shard.set(&kbuf, &val, h) {
                    println!("Shard simulation stopped. Row {} is full.", row_idx);
                    break;
                }
            }

            let n_occupied = shard.file.header().stats.n_occupied.load(Ordering::SeqCst);
            capacities.push(n_occupied);

            println!(
                "Run {} finished. Total keys inserted: {}",
                i + 1,
                n_occupied
            );
        }

        let min_capacity = capacities.iter().min().unwrap();
        let max_capacity = capacities.iter().max().unwrap();
        let avg_capacity: u32 = capacities.iter().sum::<u32>() / capacities.len() as u32;

        let total_possible_slots = (ROWS_NUM * ROWS_WIDTH) as f64;

        let avg_occupancy_percentage = (avg_capacity as f64 / total_possible_slots) * 100.0;
        let min_occupancy_percentage = (*min_capacity as f64 / total_possible_slots) * 100.0;
        let max_occupancy_percentage = (*max_capacity as f64 / total_possible_slots) * 100.0;

        println!("\n--- Simulation Results (Capacity) ---");

        println!(
            "Average capacity: {} ({:.2}%)",
            avg_capacity, avg_occupancy_percentage
        );
        println!(
            "Minimum capacity: {} ({:.2}%)",
            min_capacity, min_occupancy_percentage
        );
        println!(
            "Maximum capacity: {} ({:.2}%)",
            max_capacity, max_occupancy_percentage
        );
    }
}

#[cfg(test)]
mod shard_benchmarks {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn new_shard(span: Range<u32>) -> TResult<(Shard, TempDir)> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;
        let s = Shard::open(&dir, span, true)?;

        Ok((s, tmp))
    }

    #[test]
    #[ignore]
    fn bench() {
        let mut rng = StdRng::seed_from_u64(123);
        let num_iterations = 5000;
        let mut set_times = Vec::new();
        let mut get_times = Vec::new();
        let mut remove_times = Vec::new();

        let (shard, _tmp) = new_shard(0..(u16::MAX as u32 + 1)).unwrap();
        let mut inserted_keys: HashMap<[u8; MAX_KEY_SIZE], Vec<u8>> = HashMap::new();

        println!(
            "\n--- Running Shard Operations Benchmark ({} iterations) ---",
            num_iterations
        );

        for i in 0..num_iterations {
            let operation_type = rng.random_range(0..100); // 0-99

            let mut kbuf = [0u8; MAX_KEY_SIZE];
            let key_bytes: Vec<u8> = (0..MAX_KEY_SIZE).map(|_| rng.random()).collect();
            kbuf[..key_bytes.len()].copy_from_slice(&key_bytes);
            let h = TurboHasher::new(&kbuf);

            let vlen = rng.random_range(8..=512);
            let val: Vec<u8> = (0..vlen).map(|_| rng.random()).collect();

            if operation_type < 50 {
                // 50% chance for set
                let start = Instant::now();
                let _ = shard.set(&kbuf, &val, h);
                set_times.push(start.elapsed());
                inserted_keys.insert(kbuf, val);
            } else if operation_type < 90 {
                // 40% chance for get
                let start = Instant::now();
                let _ = shard.get(&kbuf, h);
                get_times.push(start.elapsed());
            } else {
                // 10% chance for remove
                let start = Instant::now();
                let _ = shard.remove(&kbuf, h);
                remove_times.push(start.elapsed());
                inserted_keys.remove(&kbuf);
            }

            if (i + 1) % 10 == 0 {
                println!("  Completed {} operations...", i + 1);
            }
        }

        let avg_duration = |times: Vec<Duration>| -> Duration {
            if times.is_empty() {
                return Duration::new(0, 0);
            }
            times.iter().sum::<Duration>() / times.len() as u32
        };

        println!("\n--- Benchmark Results ---");
        println!("Average Set Time: {:?}", avg_duration(set_times));
        println!("Average Get Time: {:?}", avg_duration(get_times));
        println!("Average Remove Time: {:?}", avg_duration(remove_times));
    }
}
