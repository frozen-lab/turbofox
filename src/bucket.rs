//! A `Bucket` is an on-disk, immutable, append-only HashTable to store the
//! Key-Value pairs. It uses a fix sized, memory-mapped Header.

use crate::error::{InternalError, InternalResult};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::atomic::{AtomicU32, AtomicU64},
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

#[repr(C)]
struct Meta {
    magic: [u8; 4],
    version: u32,
    inserts: AtomicU32,
    iter_idx: AtomicU32,
    write_pointer: AtomicU64,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            inserts: AtomicU32::new(0),
            iter_idx: AtomicU32::new(0),
            write_pointer: AtomicU64::new(0),
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

        // TODO: Validate that `meta.write_pointer < file_len`

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
}
