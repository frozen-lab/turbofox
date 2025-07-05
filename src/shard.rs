#![allow(dead_code)]

use crate::hasher::INVALID_SIGN;

/// Number of rows for signs and offsets in shard header
pub(crate) const NUM_ROWS: usize = 8 * 8;

/// Number of items per row in shard header
pub(crate) const ROW_WIDTH: usize = 8 * 64;

/// Maximum allowed size of key in KV pair
pub(crate) const MAX_KEY: u8 = u8::MAX;

/// Maximum allowed size of value in KV pair
///
/// NOTE: Even though the type is u32, the max value is
/// capped at u24 w/ zeroed upper 8 bits to merge both
/// key and value len toghether and save up on memory
pub(crate) const MAX_VAL: u32 = 0xFF_FFFF;

/// Current version of TurboCache shards
pub(crate) const VERSION: u8 = 0;

/// Versioned MAGIC value to help identify shards specific to TurboCache
const MAGIC: [u8; 8] = *b"TURBOv0\0";

/// NOTE: This test is to ensure constant values satisfy initial requirements
/// in case they are updated in the future ;)
#[cfg(test)]
mod const_values_tests {
    use super::*;

    #[test]
    fn test_constant_values() {
        assert_eq!(NUM_ROWS % 64, 0, "no. of rows must be multiple of 64");
        assert_eq!(
            ROW_WIDTH % 64,
            0,
            "no. of items in a row must be multiple of 64",
        );

        assert!(MAX_KEY <= u8::MAX, "maximum key len should be <= 255");
        assert!(
            MAX_VAL <= 0xFF_FFFF,
            "maximum val len should be <= 16_777_215"
        );

        assert_eq!(
            std::mem::size_of_val(&VERSION),
            1,
            "sizeof version should be in multiple of 8",
        );
        assert_eq!(
            std::mem::size_of_val(&MAGIC),
            8,
            "sizeof magic should be in multiple of 8",
        );
    }
}

/// Memory mapped slice w/ hash signs
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct ShardSigns([u32; ROW_WIDTH]);

impl Default for ShardSigns {
    fn default() -> Self {
        Self([INVALID_SIGN; ROW_WIDTH])
    }
}

impl ShardSigns {
    /// lookup a given sign from the given index
    fn lookup(&self, sign: u32, start_idx: &mut usize) -> Option<usize> {
        if let Some(rel_idx) = self.0[*start_idx..].iter().position(|x| *x == sign) {
            let abs_idx = *start_idx + rel_idx;
            *start_idx = abs_idx + 1;

            return Some(abs_idx);
        }

        None
    }
}

#[cfg(test)]
mod shard_signs_tests {
    use super::*;

    #[test]
    fn default_initializations() {
        let signs = ShardSigns::default();

        for &slot in signs.0.iter() {
            assert_eq!(slot, INVALID_SIGN);
        }
    }

    #[test]
    fn lookup_returns_none_if_not_found() {
        let signs = ShardSigns::default();

        let missing = INVALID_SIGN.wrapping_add(1);
        let mut start = 0;

        assert!(signs.lookup(missing, &mut start).is_none());
        assert_eq!(start, 0);
    }

    #[test]
    fn lookup_finds_first_occurrence_and_updates_start_idx() {
        let test_value = 0xDEAD_BEEF;
        let mut data = [INVALID_SIGN; ROW_WIDTH];

        data[3] = test_value;
        data[7] = test_value;

        let signs = ShardSigns(data);

        let mut start = 0;
        let first = signs.lookup(test_value, &mut start);

        assert_eq!(first, Some(3));
        assert_eq!(start, 4);

        let second = signs.lookup(test_value, &mut start);

        assert_eq!(second, Some(7));
        assert_eq!(start, 8);

        let third = signs.lookup(test_value, &mut start);

        assert!(third.is_none());
        assert_eq!(start, 8);
    }

    #[test]
    fn lookup_respects_start_idx_offset() {
        let mut data = [INVALID_SIGN; ROW_WIDTH];
        data[0] = 1;
        data[1] = 2;
        data[2] = 1;

        let signs = ShardSigns(data);

        let mut start = 1;

        assert_eq!(signs.lookup(1, &mut start), Some(2));
        assert_eq!(start, 3);
    }
}

/// KV offsets w/ following structure
/// key len (u8) + val len (u24) + file offset (u32)
#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct ShardOffsets([u64; ROW_WIDTH]);

impl Default for ShardOffsets {
    fn default() -> Self {
        Self([0u64; ROW_WIDTH])
    }
}

impl ShardOffsets {
    fn set(&mut self, idx: usize, klen: u8, vlen: u32, offset: u32) {
        let packed = Self::pack(klen, vlen, offset);

        self.0[idx] = packed;
    }

    fn get(&self, idx: usize) -> Option<(u8, u32, u32)> {
        if idx >= ROW_WIDTH {
            return None;
        }

        let packed = self.0[idx];

        if packed == 0 {
            return None;
        }

        Some(Self::unpack(packed))
    }

    fn pack(klen: u8, vlen: u32, offset: u32) -> u64 {
        assert!(vlen <= 0xFF_FFFF, "val_len exceeds 24-bit max");

        let k = (klen as u64) << 56;
        let v = (vlen as u64 & 0x00FF_FFFF) << 32;
        let o = offset as u64;

        k | v | o
    }

    fn unpack(packed: u64) -> (u8, u32, u32) {
        let klen = (packed >> 56) as u8;
        let vlen = ((packed >> 32) & 0x00FF_FFFF) as u32;
        let offset = (packed & 0xFFFF_FFFF) as u32;

        (klen, vlen, offset)
    }
}

#[cfg(test)]
mod shard_offset_tests {
    use super::*;

    #[test]
    fn default_initializes_with_zeros() {
        let offsets = ShardOffsets::default();

        for &slot in offsets.0.iter() {
            assert_eq!(slot, 0);
        }
    }

    #[test]
    fn get_returns_none_for_out_of_bounds() {
        let offsets = ShardOffsets::default();

        assert!(offsets.get(ROW_WIDTH).is_none());
        assert!(offsets.get(ROW_WIDTH + 10).is_none());
    }

    #[test]
    fn get_returns_none_for_zero_slot() {
        let offsets = ShardOffsets::default();

        assert!(offsets.get(0).is_none());
    }

    #[test]
    fn set_and_get_round_trip() {
        let mut offsets = ShardOffsets::default();

        let idx = 5;
        let klen = 42;
        let vlen = 0x00ABCD; // <= 24-bit
        let offset = 0xDEADBEEF;

        offsets.set(idx, klen, vlen, offset);
        let retrieved = offsets.get(idx);

        assert_eq!(retrieved, Some((klen, vlen, offset)));
    }

    #[test]
    #[should_panic(expected = "val_len exceeds 24-bit max")]
    fn pack_panics_on_vlen_overflow() {
        let _ = ShardOffsets::pack(1, 0x01_000000, 123); // 0x1000000 = 24-bit overflow
    }

    #[test]
    fn pack_and_unpack_manual_check() {
        let klen = 0x12;
        let vlen = 0x00FF_FFEE;
        let offset = 0xCAFEBABE;

        let packed = ShardOffsets::pack(klen, vlen, offset);
        let (k2, v2, o2) = ShardOffsets::unpack(packed);

        assert_eq!(k2, klen);
        assert_eq!(v2, vlen);
        assert_eq!(o2, offset);
    }

    #[test]
    fn multiple_set_and_get() {
        let mut offsets = ShardOffsets::default();

        let entries = [
            (0, 1, 0x100, 0xAAAA),
            (1, 2, 0x200, 0xBBBB),
            (2, 3, 0x300, 0xCCCC),
        ];

        for &(idx, k, v, o) in &entries {
            offsets.set(idx, k, v, o);
        }

        for &(idx, k, v, o) in &entries {
            let result = offsets.get(idx);

            assert_eq!(result, Some((k, v, o)));
        }
    }
}

#[repr(C)]
struct ShardMeta {
    version: u8,
    magic: [u8; 8],
}

impl Default for ShardMeta {
    fn default() -> Self {
        Self {
            version: VERSION,
            magic: MAGIC,
        }
    }
}

#[repr(C)]
struct ShardStats {
    slots_used: u32,
    slots_yanked: u32,
}

impl Default for ShardStats {
    fn default() -> Self {
        Self {
            slots_used: 0,
            slots_yanked: 0,
        }
    }
}

/// Memory mapped Header of the shard w/ approx ~38 KiB (393224 bytes) in memory
#[repr(C)]
struct ShardHeader {
    stats: ShardStats,                 // u64
    signs: [ShardSigns; NUM_ROWS],     // u32 * 512 * 64
    offsets: [ShardOffsets; NUM_ROWS], // u64 * 512 * 64
}

impl Default for ShardHeader {
    fn default() -> Self {
        Self {
            stats: ShardStats::default(),
            signs: [ShardSigns::default(); NUM_ROWS],
            offsets: [ShardOffsets::default(); NUM_ROWS],
        }
    }
}

pub struct Shard {
    header: ShardHeader,
}
