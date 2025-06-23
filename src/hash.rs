use siphasher::sip::SipHasher24;

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct SimHash(u64);

impl SimHash {
    pub const INVALID_SIGN: u32 = 0u32;

    pub fn new(buf: &[u8]) -> Self {
        Self(SipHasher24::new().hash(buf))
    }

    pub fn sign(&self) -> u32 {
        if self.0 as u32 == Self::INVALID_SIGN {
            0x1234_5678
        } else {
            self.0 as u32
        }
    }

    pub fn row(&self, row_size: usize) -> usize {
        (self.0 as usize >> 32) % row_size
    }

    pub fn shard(&self) -> u32 {
        (self.0 >> 48) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::default::Default;

    #[test]
    fn test_default_sign() {
        let h = SimHash::default();
        assert_eq!(h.0, 0);
        assert_eq!(
            h.sign(),
            0x1234_5678,
            "If low half hash is zero, sign should be sentinel",
        );
    }

    #[test]
    fn test_sign_nonzero() {
        // Manually constructed hash w/ non-zero low 32 bits
        let value: u64 = 0x0000_0001_DEAD_BEEFu64;
        let h = SimHash(value);
        assert_eq!(
            h.sign(),
            0xDEAD_BEEF,
            "Sign should correctly return lower 32-bits of the hash",
        );
    }

    #[test]
    fn test_row_calculations() {
        let upper: u64 = 0xABCD_EF01;
        let lower: u64 = 0x2233_4455;
        let combined = (upper << 32) | lower;

        let h = SimHash(combined);
        let expected = (upper as usize) % 1000;

        assert_eq!(
            h.row(1000),
            expected,
            "`row()` should correctly return mod w/ upper 32 bits of hash",
        );
    }

    #[test]
    fn test_shard_calculations() {
        let top16: u16 = 0xFACE;
        let rest: u64 = 0x1234_5678_9ABC;

        let combined = ((top16 as u64) << 48) | rest;
        let h = SimHash(combined);

        assert_eq!(
            h.shard(),
            top16 as u32,
            "`shard()` should correctly return upper 16 bits of hash",
        );
    }

    #[test]
    fn test_new_consistency() {
        let buf = b"hello world";

        let h1 = SimHash::new(buf);
        let h2 = SimHash::new(buf);

        assert_eq!(h1, h2, "Multiple hashes for same buffer should be equal");
    }

    #[test]
    fn test_new_differs_for_different_input() {
        let h1 = SimHash::new(b"foo");
        let h2 = SimHash::new(b"bar");

        assert_ne!(h1, h2, "Different inputs should produce different hashes");
    }
}
