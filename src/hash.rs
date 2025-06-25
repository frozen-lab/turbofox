use siphasher::sip::SipHasher24;

use crate::NUM_ROWS;

pub(crate) const INVALID_HASH: u32 = 0;

pub(crate) struct TurboHash(u64);

#[allow(dead_code)]
impl TurboHash {
    pub fn new(buf: &[u8]) -> Self {
        Self::from_hash(SipHasher24::new().hash(&buf))
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.sign() != INVALID_HASH
    }

    #[inline]
    pub fn sign(&self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub fn row(&self) -> usize {
        ((self.0 >> 32) as u16) as usize % NUM_ROWS
    }

    #[inline]
    pub fn shard(&self) -> u32 {
        ((self.0 >> 48) & 0xffff) as u32
    }

    fn from_hash(hash: u64) -> Self {
        let mut sign = hash as u32;

        if sign == INVALID_HASH {
            sign = (hash >> 32) as u32;

            if sign == INVALID_HASH {
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

        let h1 = TurboHash::new(buf);
        let h2 = TurboHash::new(buf);

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
}
