//! Implementation of the `XXH64` (64-bit hash).
//!
//! ### Example
//!
//! ```rust
//! use std::hash::Hasher;
//! use xxhash::XxHash64;
//!
//! let mut hasher = XxHash64::with_seed(0);
//! hasher.write(b"hello world");
//! let hash = hasher.finish();
//!
//! assert_eq!(hash, 0x45AB6734B21E6968);
//! ```
use crate::IntoU64;
use std::{fmt, hash::BuildHasher, mem};

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

    const fn bytes(&self) -> &Bytes {
        const _: () = assert!(mem::align_of::<u8>() <= mem::align_of::<Lane>());
        unsafe { &*self.0.as_ptr().cast() }
    }

    fn bytes_mut(&mut self) -> &mut Bytes {
        unsafe { &mut *self.0.as_mut_ptr().cast() }
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

    #[test]
    fn new_is_zeroed() {
        let buf = BufferedData::new();
        assert_eq!(buf.0, [0; 4]);
        assert_eq!(buf.bytes(), &[0; 32]);
    }

    #[test]
    fn bytes_mut_allows_modification() {
        let mut buf = BufferedData::new();
        buf.bytes_mut()[0] = 1;
        buf.bytes_mut()[31] = 2;
        assert_eq!(buf.bytes()[0], 1);
        assert_eq!(buf.bytes()[31], 2);
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

    #[inline]
    fn extend<'d>(&mut self, data: &'d [u8]) -> (Option<&Lanes>, &'d [u8]) {
        if self.offset == 0 {
            return (None, data);
        };

        let bytes = self.data.bytes_mut();
        debug_assert!(self.offset <= bytes.len());

        let empty = &mut bytes[self.offset..];
        let n_to_copy = usize::min(empty.len(), data.len());

        let dst = &mut empty[..n_to_copy];

        let (src, rest) = data.split_at(n_to_copy);

        dst.copy_from_slice(src);
        self.offset += n_to_copy;

        debug_assert!(self.offset <= bytes.len());

        if self.offset == bytes.len() {
            self.offset = 0;
            (Some(&self.data.0), rest)
        } else {
            (None, rest)
        }
    }

    #[inline]
    fn set(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        debug_assert_eq!(self.offset, 0);

        let n_to_copy = data.len();

        let bytes = self.data.bytes_mut();
        debug_assert!(n_to_copy < bytes.len());

        bytes[..n_to_copy].copy_from_slice(data);
        self.offset = data.len();
    }

    #[inline]
    fn remaining(&self) -> &[u8] {
        &self.data.bytes()[..self.offset]
    }
}

#[cfg(test)]
mod buffer_tests {
    use super::*;

    #[test]
    fn new_buffer_is_empty() {
        let buffer = Buffer::new();
        assert_eq!(buffer.offset, 0);
        assert!(buffer.remaining().is_empty());
    }

    #[test]
    fn set_stores_data_and_updates_offset() {
        let mut buffer = Buffer::new();
        let data = &[1, 2, 3];
        buffer.set(data);

        assert_eq!(buffer.offset, 3);
        assert_eq!(buffer.remaining(), &[1, 2, 3]);
    }

    #[test]
    fn set_with_empty_data_does_nothing() {
        let mut buffer = Buffer::new();
        buffer.set(&[]);

        assert_eq!(buffer.offset, 0);
        assert!(buffer.remaining().is_empty());
    }

    #[test]
    fn extend_with_empty_initial_buffer() {
        let mut buffer = Buffer::new();
        let data = &[1, 2, 3];
        let (lanes, rest) = buffer.extend(data);

        assert!(lanes.is_none());
        assert_eq!(rest, data);
        assert_eq!(buffer.offset, 0); // extend shouldn't change buffer if it starts empty
    }

    #[test]
    fn extend_partially_fills_buffer() {
        let mut buffer = Buffer::new();
        buffer.set(&[1, 2, 3]); // pre-fill

        let data_to_extend = &[4, 5];
        let (lanes, rest) = buffer.extend(data_to_extend);

        assert!(lanes.is_none());
        assert!(rest.is_empty());
        assert_eq!(buffer.offset, 5);
        assert_eq!(buffer.remaining(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn extend_exactly_fills_buffer() {
        let mut buffer = Buffer::new();
        let initial_data = &vec![1u8; 30];
        buffer.set(initial_data);

        let data_to_extend = &[2u8, 2u8];
        let (lanes, rest) = buffer.extend(data_to_extend);

        assert!(lanes.is_some());
        assert!(rest.is_empty());

        let mut expected_bytes = vec![1u8; 30];
        expected_bytes.extend_from_slice(&[2u8, 2u8]);
        let expected_lanes = unsafe { &*(expected_bytes.as_ptr() as *const Lanes) };
        assert_eq!(lanes.unwrap(), expected_lanes);
    }

    #[test]
    fn extend_over_fills_buffer() {
        let mut buffer = Buffer::new();
        let initial_data = &vec![1u8; 30];
        buffer.set(initial_data);

        let data_to_extend = &[2u8, 2u8, 3u8, 3u8, 3u8];
        let (lanes, rest) = buffer.extend(data_to_extend);

        assert!(lanes.is_some());
        assert_eq!(rest, &[3u8, 3u8, 3u8]);

        let mut expected_bytes = vec![1u8; 30];
        expected_bytes.extend_from_slice(&[2u8, 2u8]);
        let expected_lanes = unsafe { &*(expected_bytes.as_ptr() as *const Lanes) };
        assert_eq!(lanes.unwrap(), expected_lanes);
    }

    #[test]
    fn extend_with_empty_data_does_nothing() {
        let mut buffer = Buffer::new();
        buffer.set(&[1, 2, 3]);

        let (lanes, rest) = buffer.extend(&[]);
        assert!(lanes.is_none());
        assert!(rest.is_empty());
        assert_eq!(buffer.offset, 3);
        assert_eq!(buffer.remaining(), &[1, 2, 3]);
    }

    #[test]
    fn remaining_shows_correct_slice() {
        let mut buffer = Buffer::new();
        buffer.set(&[1, 2, 3, 4, 5]);
        assert_eq!(buffer.remaining(), &[1, 2, 3, 4, 5]);
        buffer.data.bytes_mut()[5] = 99; // should not be in remaining
        assert_eq!(buffer.remaining(), &[1, 2, 3, 4, 5]);
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
    /// Creates a new hasher with a seed of 0.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use xxhash::XxHash64;
    ///
    /// let hasher = XxHash64::default();
    /// assert_eq!(hasher.seed(), 0);
    /// ```
    fn default() -> Self {
        Self::with_seed(0)
    }
}

impl Hasher {
    /// Hashes a byte slice into 64-byte in a single shot.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use xxhash::XxHash64;
    ///
    /// let hash = XxHash64::oneshot(0, b"hello world");
    /// assert_eq!(hash, 0x45AB6734B21E6968);
    /// ```
    #[must_use]
    #[inline]
    pub fn oneshot(seed: u64, data: &[u8]) -> u64 {
        let len = data.len();

        let mut accumulators = Accumulators::new(seed);

        let data = accumulators.write_many(data);

        Self::finish_with(seed, len.into_u64(), &accumulators, data)
    }

    /// Constructs the hasher with an initial seed.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use xxhash::XxHash64;
    ///
    /// let hasher = XxHash64::with_seed(123);
    /// assert_eq!(hasher.seed(), 123);
    /// ```
    #[must_use]
    pub const fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            accumulators: Accumulators::new(seed),
            buffer: Buffer::new(),
            length: 0,
        }
    }

    /// Get the seed the hasher was created with.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use xxhash::XxHash64;
    ///
    /// let hasher = XxHash64::with_seed(123);
    /// assert_eq!(hasher.seed(), 123);
    /// ```
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// The total number of bytes hashed.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use std::hash::Hasher;
    /// use xxhash::XxHash64;
    ///
    /// let mut hasher = XxHash64::with_seed(0);
    ///
    /// hasher.write(b"hello");
    /// assert_eq!(hasher.total_len(), 5);
    /// hasher.write(b" world");
    ///
    /// assert_eq!(hasher.total_len(), 11);
    /// ```
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

impl std::hash::Hasher for Hasher {
    #[inline]
    fn write(&mut self, data: &[u8]) {
        let len = data.len();

        let (buffered_lanes, data) = self.buffer.extend(data);

        if let Some(&lanes) = buffered_lanes {
            self.accumulators.write(lanes);
        }

        let data = self.accumulators.write_many(data);

        self.buffer.set(data);

        self.length += len.into_u64();
    }

    #[inline]
    fn finish(&self) -> u64 {
        Self::finish_with(
            self.seed,
            self.length,
            &self.accumulators,
            self.buffer.remaining(),
        )
    }
}

/// A `BuildHasher` for creating `xxhash64::XxHash64` instances.
///
/// ### Example
///
/// ```rust
/// use std::hash::BuildHasher;
/// use xxhash::xxhash64::State;
///
/// let state = State::with_seed(0);
/// let hasher = state.build_hasher();
/// ```
#[derive(Clone)]
pub struct State(u64);

impl State {
    /// Creates a new `State` with the provided seed.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use xxhash::xxhash64::State;
    ///
    /// let state = State::with_seed(123);
    /// ```
    pub fn with_seed(seed: u64) -> Self {
        Self(seed)
    }
}

impl BuildHasher for State {
    type Hasher = Hasher;

    /// Creates a new `xxhash64::XxHash64` with the seed from this `State`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::hash::BuildHasher;
    /// use xxhash::xxhash64::State;
    ///
    /// let state = State::with_seed(0);
    /// let hasher = state.build_hasher();
    ///
    /// assert_eq!(hasher.seed(), 0);
    /// ```
    fn build_hasher(&self) -> Self::Hasher {
        Hasher::with_seed(self.0)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use core::{array, hash::Hasher as _};

    const _TRAITS: () = {
        const fn is_clone<T: Clone>() {}
        is_clone::<Hasher>();
        is_clone::<State>();
    };

    const EMPTY_BYTES: [u8; 0] = [];

    #[test]
    fn ingesting_byte_by_byte_is_equivalent_to_large_chunks() {
        let bytes = [0x9c; 32];

        let mut byte_by_byte = Hasher::with_seed(0);
        for byte in bytes.chunks(1) {
            byte_by_byte.write(byte);
        }
        let byte_by_byte = byte_by_byte.finish();

        let mut one_chunk = Hasher::with_seed(0);
        one_chunk.write(&bytes);
        let one_chunk = one_chunk.finish();

        assert_eq!(byte_by_byte, one_chunk);
    }

    #[test]
    fn hash_of_nothing_matches_c_implementation() {
        let mut hasher = Hasher::with_seed(0);
        hasher.write(&EMPTY_BYTES);
        assert_eq!(hasher.finish(), 0xef46_db37_51d8_e999);
    }

    #[test]
    fn hash_of_single_byte_matches_c_implementation() {
        let mut hasher = Hasher::with_seed(0);
        hasher.write(&[42]);
        assert_eq!(hasher.finish(), 0x0a9e_dece_beb0_3ae4);
    }

    #[test]
    fn hash_of_multiple_bytes_matches_c_implementation() {
        let mut hasher = Hasher::with_seed(0);
        hasher.write(b"Hello, world! ");
        assert_eq!(hasher.finish(), 0x7b06_c531_ea43_e89f);
    }

    #[test]
    fn hash_of_multiple_chunks_matches_c_implementation() {
        let bytes: [u8; 100] = array::from_fn(|i| i as u8);
        let mut hasher = Hasher::with_seed(0);
        hasher.write(&bytes);
        assert_eq!(hasher.finish(), 0x6ac1_e580_3216_6597);
    }

    #[test]
    fn hash_with_different_seed_matches_c_implementation() {
        let mut hasher = Hasher::with_seed(0xae05_4331_1b70_2d91);
        hasher.write(&EMPTY_BYTES);
        assert_eq!(hasher.finish(), 0x4b6a_04fc_df7a_4672);
    }

    #[test]
    fn hash_with_different_seed_and_multiple_chunks_matches_c_implementation() {
        let bytes: [u8; 100] = array::from_fn(|i| i as u8);
        let mut hasher = Hasher::with_seed(0xae05_4331_1b70_2d91);
        hasher.write(&bytes);
        assert_eq!(hasher.finish(), 0x567e_355e_0682_e1f1);
    }

    #[test]
    fn hashes_with_different_offsets_are_the_same() {
        let bytes = [0x7c; 4096];
        let expected = Hasher::oneshot(0, &[0x7c; 64]);

        let the_same = bytes
            .windows(64)
            .map(|w| {
                let mut hasher = Hasher::with_seed(0);
                hasher.write(w);
                hasher.finish()
            })
            .all(|h| h == expected);
        assert!(the_same);
    }
}
