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
            std::mem::size_of_val(&VERSION) % 8,
            0,
            "sizeof version should be in multiple of 8",
        );
        assert_eq!(
            std::mem::size_of_val(&MAGIC) % 8,
            0,
            "sizeof magic should be in multiple of 8",
        );
    }
}

/// Memory mapped slice w/ hash signs
#[repr(C)]
struct ShardSigns([u32; ROW_WIDTH]);

impl Default for ShardSigns {
    fn default() -> Self {
        Self([INVALID_SIGN; ROW_WIDTH])
    }
}

impl ShardSigns {
    ///
    fn lookup(&self, sign: u32, start_idx: &mut usize) -> Option<usize> {
        if let Some(rel_idx) = self.0[*start_idx..].iter().position(|x| *x == sign) {
            let abs_idx = *start_idx + rel_idx;
            *start_idx = abs_idx + 1;

            return Some(abs_idx);
        }

        None
    }
}

/// KV offsets w/ following structure
/// key len (u8) + val len (u24) + file offset (u32)
#[repr(C)]
struct ShardOffsets([u64; ROW_WIDTH]);

#[repr(C)]
struct ShardMeta {
    version: u8,
    magic: [u8; 8],
}

#[repr(C)]
struct ShardStats {
    slots_used: u32,
    slots_yanked: u32,
}

/// Memory mapped Header of the shard w/ approx ~38 KiB (393224 bytes) in memory
#[repr(C)]
struct ShardHeader {
    meta: ShardMeta,                   // u8 + u64 (not mem mapped)
    stats: ShardStats,                 // u64
    signs: [ShardSigns; NUM_ROWS],     // u32 * 512 * 64
    offsets: [ShardOffsets; NUM_ROWS], // u64 * 512 * 64
}

pub struct Shard {
    header: ShardHeader,
}
