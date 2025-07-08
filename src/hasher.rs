#![allow(dead_code)]

use xxhash::XxHash64;

const SEED: u64 = 0;
pub(crate) const INVALID_SIGN: u32 = 0;

pub(crate) struct Hasher(u64);

impl Hasher {
    pub fn new(buf: &[u8]) -> Self {
        Self::from_hash(XxHash64::oneshot(SEED, buf))
    }

    #[inline]
    pub fn fingerprint(&self) -> u16 {
        self.0 as u16
    }

    fn from_hash(hash: u64) -> Self {
        let mut sign = hash as u32;

        if sign == INVALID_SIGN {
            sign = (hash >> 32) as u32;

            if sign == INVALID_SIGN {
                sign = 0x1234_5678;
            }
        }

        let shard = hash & 0xffff_0000_0000_0000;
        let row = hash & 0x0000_ffff_0000_0000;
        let value = shard | row | (sign as u64);

        Self(value)
    }
}
