use siphasher::sip::SipHasher24;

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct SimHash(u64);

#[allow(dead_code)]
impl SimHash {
    const INVALID_SIGN: u32 = 0u32;

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
