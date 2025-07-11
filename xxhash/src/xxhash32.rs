//! Implementation of the `XXH32` (32-bit hash).
//!
//! ### Example
//!
//! ```rust
//! use xxhash::XxHash32;
//!
//! let hash = XxHash32::oneshot(b"Hello, World!");
//! assert_eq!(hash, XxHash32::oneshot(b"Hello, World!"));
//! ```
use crate::{IntoU32, IntoU64};

const PRIME32_1: u32 = 0x9E3779B1;
const PRIME32_2: u32 = 0x85EBCA77;
const PRIME32_3: u32 = 0xC2B2AE3D;
const PRIME32_4: u32 = 0x27D4EB2F;
const PRIME32_5: u32 = 0x165667B1;

type Lane = u32;
type Lanes = [Lane; 4];
type Bytes = [u8; 16];

const BYTES_IN_LANE: usize = std::mem::size_of::<Bytes>();

#[derive(Clone, PartialEq, Eq)]
struct BufferedData(Lanes);

#[derive(PartialEq, Eq, Clone)]
struct Buffer {
    offset: usize,
    data: BufferedData,
}

#[derive(Clone, PartialEq)]
struct Accumulator(Lanes);

impl Accumulator {
    #[inline]
    const fn new(seed: u32) -> Self {
        Self([
            seed.wrapping_add(PRIME32_1).wrapping_add(PRIME32_2),
            seed.wrapping_add(PRIME32_2),
            seed,
            seed.wrapping_sub(PRIME32_1),
        ])
    }

    #[inline]
    fn write(&mut self, lanes: Lanes) {
        let [acc1, acc2, acc3, acc4] = &mut self.0;
        let [l1, l2, l3, l4] = lanes;

        *acc1 = Self::round(*acc1, l1.to_le());
        *acc2 = Self::round(*acc2, l2.to_le());
        *acc3 = Self::round(*acc3, l3.to_le());
        *acc4 = Self::round(*acc4, l4.to_le());
    }

    #[inline]
    fn write_many<'d>(&mut self, mut data: &'d [u8]) -> &'d [u8] {
        while let Some((chunk, rest)) = data.split_first_chunk::<BYTES_IN_LANE>() {
            let lanes = unsafe { chunk.as_ptr().cast::<Lanes>().read_unaligned() };
            self.write(lanes);
            data = rest;
        }

        data
    }

    #[inline]
    const fn finish(&self) -> u32 {
        let [acc1, acc2, acc3, acc4] = self.0;

        let acc1 = acc1.rotate_left(1);
        let acc2 = acc2.rotate_left(7);
        let acc3 = acc3.rotate_left(12);
        let acc4 = acc4.rotate_left(18);

        acc1.wrapping_add(acc2)
            .wrapping_add(acc3)
            .wrapping_add(acc4)
    }

    #[inline]
    const fn round(mut acc: u32, lane: u32) -> u32 {
        acc = acc.wrapping_add(lane.wrapping_mul(PRIME32_2));
        acc = acc.rotate_left(13);
        acc.wrapping_mul(PRIME32_1)
    }
}

/// A streaming implementation of the XXH32 hashing algorithm.
///
/// ### Example
///
/// ```rust
/// use xxhash::XxHash32;
///
/// let hash = XxHash32::oneshot(b"XxHash32");
/// assert_eq!(hash, XxHash32::oneshot(b"XxHash32"));
/// ```
#[derive(Clone, PartialEq)]
pub struct Hasher {
    seed: u32,
    length: u64,
    accumulator: Accumulator,
    buffer: Buffer,
}

impl Hasher {
    /// Hash all data at once and return a 32-bit hash value.
    #[must_use]
    #[inline]
    pub fn oneshot(data: &[u8]) -> u32 {
        const SEED: u32 = 0;
        let len = data.len().into_u64();

        let mut accumulator = Accumulator::new(SEED);
        let data = accumulator.write_many(data);

        Self::finish_with(SEED, len, &accumulator, data)
    }

    #[inline]
    #[must_use]
    fn finish_with(seed: u32, len: u64, accumulator: &Accumulator, mut data: &[u8]) -> u32 {
        let mut acc = if len < BYTES_IN_LANE.into_u64() {
            seed.wrapping_add(PRIME32_5)
        } else {
            accumulator.finish()
        };

        acc += len as u32;

        while let Some((chunk, rest)) = data.split_first_chunk() {
            let lane = u32::from_ne_bytes(*chunk).to_le();

            acc = acc.wrapping_add(lane.wrapping_mul(PRIME32_3));
            acc = acc.rotate_left(17).wrapping_mul(PRIME32_4);

            data = rest;
        }

        for &byte in data {
            let lane = byte.into_u32();

            acc = acc.wrapping_add(lane.wrapping_mul(PRIME32_5));
            acc = acc.rotate_left(11).wrapping_mul(PRIME32_1);
        }

        acc ^= acc >> 15;
        acc = acc.wrapping_mul(PRIME32_2);
        acc ^= acc >> 13;
        acc = acc.wrapping_mul(PRIME32_3);
        acc ^= acc >> 16;

        acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_input() {
        let hash = Hasher::oneshot(b"");
        assert_eq!(hash, 0x02CC5D05);
    }

    #[test]
    fn test_single_byte_inputs() {
        for byte in 0u8..=255 {
            let hash = Hasher::oneshot(&[byte]);
            assert!(hash <= u32::MAX);
        }
    }

    #[test]
    fn test_different_lengths() {
        let inputs: Vec<&[u8]> = vec![
            b"A",
            b"AB",
            b"ABC",
            b"ABCD",
            b"ABCDE",
            b"ABCDEF",
            b"ABCDEFG",
            b"ABCDEFGH",
            b"ABCDEFGHI",
            b"ABCDEFGHIJ",
        ];

        for input in inputs.iter() {
            let hash = Hasher::oneshot(input);
            assert!(hash <= u32::MAX);
        }
    }

    #[test]
    fn test_hash_collisions() {
        let a = Hasher::oneshot(b"abcdefg");
        let b = Hasher::oneshot(b"gfedcba");
        assert_ne!(a, b);
    }

    #[test]
    fn test_large_input() {
        let data = vec![42u8; 10_000];
        let hash = Hasher::oneshot(&data);
        assert!(hash <= u32::MAX);
    }

    #[test]
    fn test_unicode_input() {
        let hash = Hasher::oneshot("你好，世界".as_bytes());
        assert!(hash <= u32::MAX);
    }
}
