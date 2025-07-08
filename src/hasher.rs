#![allow(dead_code)]

use crate::shard::ROWS_NUM;
use xxhash::XxHash64;

const SEED: u64 = 0;
pub(crate) const INVALID_FP: u32 = 0;

pub(crate) struct TurboHasher(u64);

impl TurboHasher {
    pub fn new(buf: &[u8]) -> Self {
        Self::from_hash(XxHash64::oneshot(SEED, buf))
    }

    #[inline]
    pub fn fingerprint(&self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub fn shard_selector(&self) -> u32 {
        ((self.0 >> 48) & 0xffff) as u32
    }

    #[inline]
    pub fn row_selector(&self) -> usize {
        ((self.0 >> 32) as u16) as usize % ROWS_NUM
    }

    fn from_hash(hash: u64) -> Self {
        let mut sign = hash as u32;

        if sign == INVALID_FP {
            sign = (hash >> 32) as u32;

            if sign == INVALID_FP {
                sign = 0x1234_5678;
            }
        }

        let shard = hash & 0xffff_0000_0000_0000;
        let row = hash & 0x0000_ffff_0000_0000;
        let value = shard | row | (sign as u64);

        Self(value)
    }
}
