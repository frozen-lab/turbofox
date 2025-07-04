use crate::IntoU64;
use std::{fmt, mem};

const PRIME64_1: u64 = 0x9E3779B185EBCA87;
const PRIME64_2: u64 = 0xC2B2AE3D27D4EB4F;
const PRIME64_3: u64 = 0x165667B19E3779F9;
const PRIME64_4: u64 = 0x85EBCA77C2B2AE63;
const PRIME64_5: u64 = 0x27D4EB2F165667C5;

type Lane = u64;
type Lanes = [Lane; 4];
type Bytes = [u8; 32];

// compile time assertion to verify alignment
const _: () = assert!(std::mem::size_of::<u8>() <= std::mem::size_of::<u32>());

const BYTES_IN_LANE: usize = mem::size_of::<Bytes>();

#[derive(Clone, PartialEq)]
struct BufferedData(Lanes);

impl BufferedData {
    const fn new() -> Self {
        Self([0; 4])
    }
}

impl fmt::Debug for BufferedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.iter()).finish()
    }
}

#[cfg(test)]
mod buffer_data_tests {
    use super::*;
    use std::mem;

    #[test]
    fn test_buffered_data_size_and_alignment() {
        assert_eq!(mem::size_of::<BufferedData>(), mem::size_of::<Lanes>());
        assert_eq!(mem::size_of::<Bytes>(), 32);
        assert!(mem::align_of::<u8>() <= mem::align_of::<u32>());
    }

    #[test]
    fn test_debug_format() {
        let mut buf = BufferedData::new();
        buf.0 = [1, 2, 3, 4];
        let debug_str = format!("{:?}", buf);

        assert_eq!(debug_str, "[1, 2, 3, 4]");
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Buffer {
    offset: usize,
    data: BufferedData,
}

impl Buffer {
    const fn new() -> Self {
        Self {
            offset: 0,
            data: BufferedData::new(),
        }
    }
}

#[derive(Clone, PartialEq)]
struct Accumulators(Lanes);

impl Accumulators {
    const fn new(seed: u64) -> Self {
        Self([
            seed.wrapping_add(PRIME64_1).wrapping_add(PRIME64_2),
            seed.wrapping_add(PRIME64_2),
            seed,
            seed.wrapping_sub(PRIME64_1),
        ])
    }

    #[inline]
    fn write(&mut self, lanes: Lanes) {
        let [acc1, acc2, acc3, acc4] = &mut self.0;
        let [lane1, lane2, lane3, lane4] = lanes;

        *acc1 = Self::round(*acc1, lane1.to_le());
        *acc2 = Self::round(*acc2, lane2.to_le());
        *acc3 = Self::round(*acc3, lane3.to_le());
        *acc4 = Self::round(*acc4, lane4.to_le());
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
    const fn finish(&self) -> u64 {
        let [acc1, acc2, acc3, acc4] = self.0;

        let mut acc = {
            let acc1 = acc1.rotate_left(1);
            let acc2 = acc2.rotate_left(7);
            let acc3 = acc3.rotate_left(12);
            let acc4 = acc4.rotate_left(18);

            acc1.wrapping_add(acc2)
                .wrapping_add(acc3)
                .wrapping_add(acc4)
        };

        acc = Self::merge_accumulator(acc, acc1);
        acc = Self::merge_accumulator(acc, acc2);
        acc = Self::merge_accumulator(acc, acc3);
        acc = Self::merge_accumulator(acc, acc4);

        acc
    }

    #[inline]
    const fn merge_accumulator(mut acc: u64, acc_n: u64) -> u64 {
        acc ^= Self::round(0, acc_n);
        acc = acc.wrapping_mul(PRIME64_1);
        acc.wrapping_add(PRIME64_4)
    }

    #[inline]
    const fn round(mut acc: u64, lane: u64) -> u64 {
        acc = acc.wrapping_add(lane.wrapping_mul(PRIME64_2));
        acc = acc.rotate_left(31);
        acc.wrapping_mul(PRIME64_1)
    }
}

impl fmt::Debug for Accumulators {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [acc1, acc2, acc3, acc4] = self.0;
        f.debug_struct("Accumulators")
            .field("acc1", &acc1)
            .field("acc2", &acc2)
            .field("acc3", &acc3)
            .field("acc4", &acc4)
            .finish()
    }
}

#[cfg(test)]
mod accumulator_tests {
    use super::*;

    #[test]
    fn test_accumulator_new() {
        let seed = 42;
        let acc = Accumulators::new(seed);

        assert_eq!(
            acc.0[0],
            seed.wrapping_add(PRIME64_1).wrapping_add(PRIME64_2)
        );
        assert_eq!(acc.0[1], seed.wrapping_add(PRIME64_2));
        assert_eq!(acc.0[2], seed);
        assert_eq!(acc.0[3], seed.wrapping_sub(PRIME64_1));
    }

    #[test]
    fn test_round_consistency() {
        let acc = Accumulators::round(1, 2);
        let mut exp = 1u64.wrapping_add(2u64.wrapping_mul(PRIME64_2));
        exp = exp.rotate_left(31).wrapping_mul(PRIME64_1);

        assert_eq!(acc, exp);
    }

    #[test]
    fn test_write_and_finish() {
        let mut acc = Accumulators::new(0);
        acc.write([1, 2, 3, 4]);
        let hash = acc.finish();

        assert!(hash <= u64::MAX);
    }

    #[test]
    fn test_write_many_exact_chunks() {
        let mut acc = Accumulators::new(0);
        let mut data = vec![];

        for i in 0..32u8 {
            data.push(i);
        }

        let rest = acc.write_many(&data);

        assert!(rest.is_empty());
    }

    #[test]
    fn test_write_many_with_remainder() {
        let mut acc = Accumulators::new(0);
        let mut data = vec![];

        for i in 0..(BYTES_IN_LANE as u8 + 3) {
            data.push(i);
        }

        let rest = acc.write_many(&data);

        assert_eq!(rest.len(), 3);
    }
}

/// Calculates the 64-bit hash.
#[derive(Debug, Clone, PartialEq)]
pub struct Hasher {
    seed: u64,
    accumulators: Accumulators,
    buffer: Buffer,
    length: u64,
}

impl Default for Hasher {
    fn default() -> Self {
        Self::with_seed(0)
    }
}

impl Hasher {
    #[must_use]
    #[inline]
    pub fn oneshot(seed: u64, data: &[u8]) -> u64 {
        let len = data.len();

        let mut accumulators = Accumulators::new(seed);

        let data = accumulators.write_many(data);

        Self::finish_with(seed, len.into_u64(), &accumulators, data)
    }

    /// Constructs the hasher with an initial seed.
    #[must_use]
    pub const fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            accumulators: Accumulators::new(seed),
            buffer: Buffer::new(),
            length: 0,
        }
    }

    /// The seed this hasher was created with.
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// The total number of bytes hashed.
    pub const fn total_len(&self) -> u64 {
        self.length
    }

    #[must_use]
    #[inline]
    fn finish_with(seed: u64, len: u64, accumulators: &Accumulators, mut remaining: &[u8]) -> u64 {
        let mut acc = if len < BYTES_IN_LANE.into_u64() {
            seed.wrapping_add(PRIME64_5)
        } else {
            accumulators.finish()
        };

        acc += len;

        while let Some((chunk, rest)) = remaining.split_first_chunk() {
            let lane = u64::from_ne_bytes(*chunk).to_le();

            acc ^= Accumulators::round(0, lane);
            acc = acc.rotate_left(27).wrapping_mul(PRIME64_1);
            acc = acc.wrapping_add(PRIME64_4);
            remaining = rest;
        }

        while let Some((chunk, rest)) = remaining.split_first_chunk() {
            let lane = u32::from_ne_bytes(*chunk).to_le() as u64;

            acc ^= lane.wrapping_mul(PRIME64_1);
            acc = acc.rotate_left(23).wrapping_mul(PRIME64_2);
            acc = acc.wrapping_add(PRIME64_3);

            remaining = rest;
        }

        for &byte in remaining {
            let lane = byte.into_u64();

            acc ^= lane.wrapping_mul(PRIME64_5);
            acc = acc.rotate_left(11).wrapping_mul(PRIME64_1);
        }

        acc ^= acc >> 33;
        acc = acc.wrapping_mul(PRIME64_2);
        acc ^= acc >> 29;
        acc = acc.wrapping_mul(PRIME64_3);
        acc ^= acc >> 32;

        acc
    }
}
