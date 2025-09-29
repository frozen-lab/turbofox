use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::MmapMut;

use crate::error::{InternalError, InternalResult};

/// ----------------------------------------
/// Constants and Types
/// ----------------------------------------

pub(crate) const VERSION: u32 = 3;
pub(crate) const MAGIC: [u8; 4] = *b"TCv3";

pub(crate) type Sign = u32;
pub(crate) type KeyValue = (Vec<u8>, Vec<u8>);
pub(crate) type Key = Vec<u8>;

/// ----------------------------------------
/// Namespaces
/// ----------------------------------------

/// This acts as an id for type of pair stored in [Kosh]
///
/// NOTE: The *index* must start from `0` cause the 0th item,
/// [Base] in this case, acts as an default in the [Patra].
/// Reason, [Patra] has zeroed space at init.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Namespace {
    Base = 0,
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
            err_id => Err(InternalError::InvalidEntry(format!(
                "Invalid namespace: {err_id}"
            ))),
        }
    }
}

/// ----------------------------------------
/// Pair
/// ----------------------------------------

#[repr(align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pair {
    ns: Namespace,
    klen: u16,
    vlen: u16,
    offset: u64,
}

pub(crate) type PairBytes = [u8; 10];

impl Pair {
    fn to_raw(&self) -> InternalResult<PairBytes> {
        //
        // Overflow check for [self.offset]
        //
        // NOTE: [self.offset] can not grow beyound (2^40 - 1)
        //
        if (self.offset & !((1u64 << 40) - 1)) != 0 {
            return Err(InternalError::BucketOverflow);
        };

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

        Ok(out)
    }

    fn from_raw(slice: PairBytes) -> InternalResult<Pair> {
        let ns = Namespace::try_from(slice[0])?;

        let klen = u16::from_le_bytes([slice[1], slice[2]]);
        let vlen = u16::from_le_bytes([slice[3], slice[4]]);

        let offset =
            u64::from_le_bytes([slice[5], slice[6], slice[7], slice[8], slice[9], 0, 0, 0]);

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
    use super::{Namespace, Pair};

    #[test]
    fn test_basic_round_trip() {
        let p = Pair {
            ns: Namespace::Base,
            offset: 123456789,
            klen: 100,
            vlen: 200,
        };

        let encoded = p.to_raw().expect("Encode raw pair");
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

        let encoded = p.to_raw().expect("Encode raw pair");
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

        let encoded = p2.to_raw().expect("Encode raw pair");
        let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

        assert_eq!(p2, decoded);
    }

    #[test]
    fn test_randomized_values() {
        for i in 0..100 {
            let offset = (i * 1234567) as u64 & ((1u64 << 40) - 1);
            let klen = (i * 37 % (1 << 16)) as u16;
            let vlen = (i * 91 % (1 << 16)) as u16;

            let r = 0u8;
            let ns = Namespace::try_from(r).expect("Expected valid [Namespace]");

            let p = Pair {
                ns: ns,
                offset,
                klen,
                vlen,
            };

            let encoded = p.to_raw().expect("Encode raw pair");
            let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

            assert_eq!(p, decoded, "Failed at iteration {i}");
        }
    }

    #[test]
    fn test_offset_guard() {
        let valid = Pair {
            ns: Namespace::Base,
            offset: (1u64 << 40) - 1,
            klen: 10,
            vlen: 10,
        };
        assert!(valid.to_raw().is_ok());

        let invalid = Pair {
            ns: Namespace::Base,
            offset: 1u64 << 40,
            klen: 10,
            vlen: 10,
        };
        assert!(invalid.to_raw().is_err());
    }
}

/// ----------------------------------------
/// Meta
/// ----------------------------------------

#[repr(C)]
struct MetaView {
    magic: [u8; 4],
    version: u32,
    inserts: AtomicU64,
    write_pointer: AtomicU64,
}

impl Default for MetaView {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            inserts: AtomicU64::new(0),
            write_pointer: AtomicU64::new(0),
        }
    }
}

pub(crate) struct Meta {
    ptr: *mut MetaView,
}

impl Meta {
    #[inline(always)]
    pub fn new(mmap: &mut MmapMut) -> Self {
        let ptr = mmap.as_mut_ptr() as *mut MetaView;
        Self { ptr }
    }

    #[inline(always)]
    fn meta(&self) -> &MetaView {
        unsafe { &*self.ptr }
    }

    #[inline(always)]
    pub fn get_insert_count(&self) -> usize {
        self.meta().inserts.load(Ordering::Acquire) as usize
    }

    #[inline(always)]
    pub fn incr_insert_count(&self) {
        self.meta().inserts.fetch_add(1, Ordering::Release);
    }

    #[inline(always)]
    pub fn decr_insert_count(&self) {
        self.meta().inserts.fetch_sub(1, Ordering::Release);
    }

    #[inline(always)]
    pub fn update_write_offset(&self, n: u64) -> u64 {
        self.meta().write_pointer.fetch_add(n, Ordering::Release)
    }

    #[inline(always)]
    pub fn is_current_version(&self) -> bool {
        let meta = self.meta();
        meta.magic == MAGIC && meta.version == VERSION
    }
}
