use twox_hash::XxHash32;

/// Used as a tombstone state for a signature for deleated entries in [Bucket]
pub(crate) const TOMBSTONE_SIGN: u32 = 1u32;

/// Used as a default state for a signature for entry in [Bucket]
pub(crate) const EMPTY_SIGN: u32 = 0u32;

/// Default seed for [XxHash32]
const DEFAULT_SEED: u32 = 0;

/// Magic constant to substitute for reserved signatures
const REPLACEMENT: u32 = 0x6052_c9b7;

pub(crate) struct TurboHasher(pub u32);

impl TurboHasher {
    pub fn new(buf: &[u8]) -> Self {
        let mut h = XxHash32::oneshot(DEFAULT_SEED, buf);
        let fixed = Self::from_hash(&mut h);

        TurboHasher(fixed)
    }

    /// Replaces any reserved signature values with a magic constant [REPLACEMENT]
    #[inline(always)]
    fn from_hash(hash: &mut u32) -> u32 {
        let is_tomb = (*hash == TOMBSTONE_SIGN) as u32;
        let is_empty = (*hash == EMPTY_SIGN) as u32;

        // mask is all 1's (0xFFFF_FFFF) if its any of reserved signatures
        let mask = !(is_tomb | is_empty).wrapping_sub(1);

        // blend's hash w/ magic constant if `mask == 0xFFFF_FFFF`
        *hash = (*hash & !mask) | (REPLACEMENT & mask);

        *hash
    }
}
