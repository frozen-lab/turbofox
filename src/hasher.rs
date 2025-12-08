/// Used as a tombstone state for a signature for deleated entries in [Mark]
pub(crate) const TOMBSTONE_SIGN: u32 = 1u32;

/// Used as a default state for a signature for entry in [Mark]
pub(crate) const EMPTY_SIGN: u32 = 0u32;

/// Represents hashed value
pub(crate) type Sign = u32;
pub(crate) const SIGN_SIZE: usize = std::mem::size_of::<Sign>();

// WARN: [Mark] is zeroed on init, i.e. the sign space contains `0u32` values
// by default, which is treated as empty slot! So the value of [EMPTY_SIGN] must
// always be `0u32`, otherwise the lookup process will fail!
const _: () = assert!(EMPTY_SIGN == 0u32);

/// Default seed for [XxHash32]
const DEFAULT_SEED: u32 = 0;

/// Magic constant to substitute for reserved signatures
const REPLACEMENT: u32 = 0x6052_c9b7;

pub(crate) struct TurboHash;

impl TurboHash {
    pub fn new(buf: &[u8]) -> Sign {
        let mut h = XxHash32::oneshot(buf);
        Self::from_hash(&mut h)
    }

    /// Replaces any reserved signature values with a magic constant [REPLACEMENT]
    #[inline(always)]
    fn from_hash(hash: &mut Sign) -> Sign {
        let is_tomb = (*hash == TOMBSTONE_SIGN) as u32;
        let is_empty = (*hash == EMPTY_SIGN) as u32;

        // mask is all 1's (0xFFFF_FFFF) if its any of reserved signatures
        let mask = !(is_tomb | is_empty).wrapping_sub(1);

        // blend's hash w/ magic constant if `mask == 0xFFFF_FFFF`
        *hash = (*hash & !mask) | (REPLACEMENT & mask);

        *hash
    }
}

const PRIME32_1: u32 = 0x9E3779B1;
const PRIME32_2: u32 = 0x85EBCA77;
const PRIME32_3: u32 = 0xC2B2AE3D;
const PRIME32_4: u32 = 0x27D4EB2F;
const PRIME32_5: u32 = 0x165667B1;

type Lane = u32;
type Lanes = [Lane; 4];
type Bytes = [u8; 16];

const BYTES_IN_LANE: usize = std::mem::size_of::<Bytes>();

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

    const fn write(&mut self, lanes: Lanes) {
        let [acc1, acc2, acc3, acc4] = &mut self.0;
        let [l1, l2, l3, l4] = lanes;

        *acc1 = Self::round(*acc1, l1.to_le());
        *acc2 = Self::round(*acc2, l2.to_le());
        *acc3 = Self::round(*acc3, l3.to_le());
        *acc4 = Self::round(*acc4, l4.to_le());
    }

    const fn write_many<'d>(&mut self, mut data: &'d [u8]) -> &'d [u8] {
        while let Some((chunk, rest)) = data.split_first_chunk::<BYTES_IN_LANE>() {
            let lanes = unsafe { chunk.as_ptr().cast::<Lanes>().read_unaligned() };
            self.write(lanes);
            data = rest;
        }

        data
    }

    const fn finish(&self) -> u32 {
        let [acc1, acc2, acc3, acc4] = self.0;

        let acc1 = acc1.rotate_left(1);
        let acc2 = acc2.rotate_left(7);
        let acc3 = acc3.rotate_left(12);
        let acc4 = acc4.rotate_left(18);

        acc1.wrapping_add(acc2).wrapping_add(acc3).wrapping_add(acc4)
    }

    const fn round(mut acc: u32, lane: u32) -> u32 {
        acc = acc.wrapping_add(lane.wrapping_mul(PRIME32_2));
        acc = acc.rotate_left(13);

        acc.wrapping_mul(PRIME32_1)
    }
}

#[derive(Clone, PartialEq)]
struct XxHash32 {
    seed: u32,
    length: u64,
    accumulator: Accumulator,
}

impl XxHash32 {
    /// Hash all data at once and get a 32-bit hash value
    #[must_use]
    #[inline]
    pub fn oneshot(data: &[u8]) -> u32 {
        let seed = DEFAULT_SEED;
        let len = data.len().into_u64();

        let mut accumulator = Accumulator::new(seed);
        let data = accumulator.write_many(data);

        Self::finish_with(seed, len, &accumulator, data)
    }

    #[inline]
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

trait IntoU32 {
    fn into_u32(self) -> u32;
}

impl IntoU32 for u8 {
    #[inline(always)]
    fn into_u32(self) -> u32 {
        self.into()
    }
}

impl IntoU32 for usize {
    #[inline(always)]
    fn into_u32(self) -> u32 {
        self as u32
    }
}

trait IntoU64 {
    fn into_u64(self) -> u64;
}

impl IntoU64 for u8 {
    #[inline(always)]
    fn into_u64(self) -> u64 {
        self.into()
    }
}

impl IntoU64 for usize {
    #[inline(always)]
    fn into_u64(self) -> u64 {
        self as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod accumulator {
        use super::*;

        #[test]
        fn test_verify_alignment() {
            assert!(std::mem::size_of::<u8>() <= std::mem::size_of::<u32>());
        }

        #[test]
        fn test_accumulator_new() {
            let seed = 42;
            let acc = Accumulator::new(seed);

            assert_eq!(acc.0[0], seed.wrapping_add(PRIME32_1).wrapping_add(PRIME32_2));
            assert_eq!(acc.0[1], seed.wrapping_add(PRIME32_2));
            assert_eq!(acc.0[2], seed);
            assert_eq!(acc.0[3], seed.wrapping_sub(PRIME32_1));
        }

        #[test]
        fn test_round_consistency() {
            let acc = Accumulator::round(1, 2);
            let mut exp = 1u32.wrapping_add(2u32.wrapping_mul(PRIME32_2));
            exp = exp.rotate_left(13).wrapping_mul(PRIME32_1);

            assert_eq!(acc, exp);
        }

        #[test]
        fn test_write_and_finish() {
            let mut acc = Accumulator::new(0);
            acc.write([1, 2, 3, 4]);
            let hash = acc.finish();

            assert!(hash <= u32::MAX);
        }

        #[test]
        fn test_write_many_exact_chunks() {
            let mut acc = Accumulator::new(0);
            let mut data = vec![];

            for i in 0..32u8 {
                data.push(i);
            }

            let rest = acc.write_many(&data);

            assert!(rest.is_empty());
        }

        #[test]
        fn test_write_many_with_remainder() {
            let mut acc = Accumulator::new(0);
            let mut data = vec![];

            for i in 0..(BYTES_IN_LANE as u8 + 3) {
                data.push(i);
            }

            let rest = acc.write_many(&data);

            assert_eq!(rest.len(), 3);
        }
    }

    mod xx_hash_32 {
        use super::*;

        #[test]
        fn hash_of_nothing_matches_c_implementation() {
            let empty_bytes: [u8; 0] = [];
            let hash = XxHash32::oneshot(&empty_bytes);

            assert_eq!(hash, 0x02cc_5d05);
        }

        #[test]
        fn hash_of_single_byte_matches_c_implementation() {
            let hash = XxHash32::oneshot(&[42]);

            assert_eq!(hash, 0xe0fe_705f);
        }

        #[test]
        fn hash_of_multiple_bytes_matches_c_implementation() {
            let hash = XxHash32::oneshot(b"Hello, world!\0");

            assert_eq!(hash, 0x9e5e_7e93);
        }

        #[test]
        fn hash_of_multiple_chunks_matches_c_implementation() {
            let bytes: [u8; 100] = std::array::from_fn(|i| i as u8);
            let hash = XxHash32::oneshot(&bytes);

            assert_eq!(hash, 0x7f89_ba44);
        }

        #[test]
        fn hashes_with_different_offsets_are_the_same() {
            let bytes = [0x7c; 4096];
            let expected = XxHash32::oneshot(&[0x7c; 64]);

            let the_same = bytes.windows(64).map(|w| XxHash32::oneshot(w)).all(|h| h == expected);

            assert!(the_same);
        }
    }

    #[cfg(test)]
    mod turbo_hash {
        use super::*;
        use rand::{Rng, SeedableRng};

        #[test]
        fn sanity_check() {
            let buf = b"hello_world";

            let hash = TurboHash::new(buf);
            let raw = XxHash32::oneshot(buf);

            assert_eq!(hash, raw, "TurboHash output should match raw XxHash32");
        }

        #[test]
        fn test_hash_equals_tombstone_sign() {
            let mut hash = TOMBSTONE_SIGN;
            let result = TurboHash::from_hash(&mut hash);

            assert_eq!(
                result, REPLACEMENT,
                "TOMBSTONE_SIGN should be replaced with REPLACEMENT"
            );
            assert_eq!(hash, REPLACEMENT);
        }

        #[test]
        fn test_hash_equals_empty_sign() {
            let mut hash = EMPTY_SIGN;
            let result = TurboHash::from_hash(&mut hash);

            assert_eq!(result, REPLACEMENT, "EMPTY_SIGN should be replaced with REPLACEMENT");
            assert_eq!(hash, REPLACEMENT);
        }

        #[test]
        fn test_hash_non_reserved_value() {
            let mut hash = 0xDEADC0DE;
            let result = TurboHash::from_hash(&mut hash);

            assert_eq!(result, 0xDEADC0DE, "Non-reserved hash should remain unchanged");
            assert_eq!(hash, 0xDEADC0DE);
        }

        #[test]
        fn test_replacement_value_is_different() {
            assert_ne!(REPLACEMENT, TOMBSTONE_SIGN);
            assert_ne!(REPLACEMENT, EMPTY_SIGN);
        }

        #[test]
        #[ignore]
        fn bench_turbo_hash() {
            const COUNT: usize = 0x1000;

            let (cfg, _) = crate::cfg::TurboConfig::test_cfg("TurboHash");

            let mut candis: Vec<Vec<u8>> = Vec::with_capacity(COUNT);
            let mut timings: Vec<f64> = Vec::with_capacity(COUNT);
            let mut rng = rand::rngs::StdRng::seed_from_u64(0xDEADC0DE);

            // gen candis
            for _ in 0x00..COUNT {
                let len = rng.random_range(0x02..=0x40) as usize;

                // rnd candi buffer
                let mut buf: Vec<u8> = Vec::with_capacity(len);
                for _ in 0x00..len {
                    buf.push(rng.random());
                }

                candis.push(buf);
            }

            // warmup
            for i in 0x00..(COUNT / 0x02) {
                std::hint::black_box(TurboHash::new(&candis[i]));
            }

            // benching
            for buf in candis {
                let start = std::time::Instant::now();
                std::hint::black_box(TurboHash::new(&buf));
                let elapsed = start.elapsed();
                timings.push(elapsed.as_nanos() as f64);
            }

            let avg: f64 = timings.iter().sum::<f64>() / timings.len() as f64;
            cfg.logger.info(format!("Hash: {:.3} ns/op", avg));

            // validate
            #[cfg(not(debug_assertions))]
            {
                let threshold_ns = 0x30 as f64;
                assert!(
                    avg <= threshold_ns,
                    "lookup_one too slow: {:.2} ns/op (threshold: {} ns)",
                    avg,
                    threshold_ns
                );
            }
        }
    }
}
