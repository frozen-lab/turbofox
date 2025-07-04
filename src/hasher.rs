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
    pub fn sign(&self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn is_valid_sign(sign: u32) -> bool {
        sign == INVALID_SIGN
    }

    #[inline]
    pub fn shard(&self) -> u32 {
        ((self.0 >> 48) & 0xffff) as u32
    }

    #[inline]
    pub fn row(&self) -> u16 {
        (self.0 >> 32) as u16
    }

    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_deterministic_behaviour() {
        let buf = b"test_key";

        let h1 = Hasher::new(buf);
        let h2 = Hasher::new(buf);

        assert_eq!(
            h1.0, h2.0,
            "Equal hash should be generated for the same key every time a hash is created",
        );
        assert_eq!(
            h1.sign(),
            h2.sign(),
            "Equal sign should be generated for the same key each time",
        );
        assert_eq!(
            h1.shard(),
            h2.shard(),
            "Equal shard should be fetched for the same key each time"
        );
        assert_eq!(
            h1.row(),
            h2.row(),
            "Equal row should be fetched for the same key each time"
        );
    }

    #[test]
    fn fallback_to_high_bits_when_low_bits_zero() {
        // hash with low 32 bits zero, high 32 bits non-zero
        let high: u64 = 0xDEADBEEF;
        let hash = (high << 32) | 0;
        let h = Hasher::from_hash(hash);

        assert_eq!(
            h.sign(),
            high as u32,
            "Sign should fallback to high bits when low bits are zero"
        );
    }

    #[test]
    fn fallback_to_default_when_both_bits_zero() {
        let hash = 0;
        let h = Hasher::from_hash(hash);

        assert_eq!(
            h.sign(),
            0x1234_5678,
            "Sign should default when both low and high bits are zero"
        );
    }

    #[test]
    fn shard_and_row_extraction() {
        let shard: u64 = 0xABCD;
        let row: u16 = 0x1234;
        let sign: u32 = 0x87654321;

        let hash = (shard << 48) | ((row as u64) << 32) | (sign as u64);
        let h = Hasher::from_hash(hash);

        assert_eq!(h.shard(), shard as u32, "Shard should match bits 48-63");
        assert_eq!(h.row(), row, "Row should match bits 32-47 mod NUM_ROWS");
        assert_eq!(h.sign(), sign, "Sign should match low 32 bits");
    }

    #[test]
    fn is_valid_always_true_after_from_hash() {
        let hash = 0;

        assert!(
            Hasher::is_valid_sign(hash),
            "Hasher created via from_hash should always be valid"
        );
    }

    #[test]
    fn sign_never_zero_for_various_hashes() {
        let inputs = [0u64, (1u64 << 32), u64::MAX];

        for &hash in &inputs {
            let h = Hasher::from_hash(hash);

            assert_ne!(
                h.sign(),
                INVALID_SIGN,
                "Sign should never be zero for any input hash"
            );
        }
    }
}
