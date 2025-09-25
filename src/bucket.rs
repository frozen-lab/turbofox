//! A `Bucket` is an on-disk, immutable, append-only HashTable to store the
//! Key-Value pairs. It uses a fix sized, memory-mapped Header.

use std::sync::atomic::{AtomicU32, AtomicU64};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Namespace {
    Base = 0,
    List = 1,
    ListItem = 2,
}

impl From<Namespace> for u8 {
    fn from(ns: Namespace) -> u8 {
        ns as u8
    }
}

impl TryFrom<u8> for Namespace {
    // TODO: Impl of internal error
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Namespace::Base),
            1 => Ok(Namespace::List),
            2 => Ok(Namespace::ListItem),
            // TODO: Handle error w/ grace
            _ => Err(()),
        }
    }
}

#[repr(C)]
struct Meta {
    magic: [u8; 4],
    version: u32,
    inserts: AtomicU32,
    iter_idx: AtomicU32,
    write_pointer: AtomicU64,
}

#[repr(align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pair {
    ns: Namespace,
    klen: u16,
    vlen: u16,
    offset: u64,
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PairRaw([u8; 10]);

impl PairRaw {
    fn to_raw(pair: Pair) -> Self {
        let mut out = [0u8; 10];

        // namespace
        out[0] = pair.ns as u8;

        // klen (LE)
        out[1..3].copy_from_slice(&pair.klen.to_le_bytes());

        // vlen (LE)
        out[3..5].copy_from_slice(&pair.vlen.to_le_bytes());

        // offset (only 5 bytes, LE)
        let offset_bytes = pair.offset.to_le_bytes();
        out[5..10].copy_from_slice(&offset_bytes[..5]);

        Self(out)
    }

    fn from_raw(&self) -> Pair {
        // TODO: Handle error w/ grace
        let slice = self.0;
        let ns = Namespace::try_from(slice[0]).unwrap();

        let klen = u16::from_le_bytes([slice[1], slice[2]]);
        let vlen = u16::from_le_bytes([slice[3], slice[4]]);

        let mut offset_bytes = [0u8; 8];
        offset_bytes[..5].copy_from_slice(&slice[5..10]);
        let offset = u64::from_le_bytes(offset_bytes);

        Pair {
            ns,
            klen,
            vlen,
            offset,
        }
    }
}

#[cfg(test)]
mod pair_tests {
    use super::{Namespace, Pair, PairRaw};

    #[test]
    fn test_basic_round_trip() {
        let p = Pair {
            ns: Namespace::Base,
            offset: 123456789,
            klen: 100,
            vlen: 200,
        };

        let encoded = PairRaw::to_raw(p);
        let decoded = encoded.from_raw();

        assert_eq!(p.ns, decoded.ns);
        assert_eq!(p.offset, decoded.offset);
        assert_eq!(p.klen, decoded.klen);
        assert_eq!(p.vlen, decoded.vlen);
    }

    #[test]
    fn test_boundaries() {
        let p = Pair {
            ns: Namespace::Base,
            offset: 0,
            klen: 0,
            vlen: 0,
        };

        let encoded = PairRaw::to_raw(p);
        let decoded = encoded.from_raw();

        assert_eq!(p, decoded);

        let max_off = (1u64 << 40) - 1;
        let max_klen = (1u16 << 12) - 1;
        let max_vlen = (1u16 << 12) - 1;

        let p2 = Pair {
            ns: Namespace::Base,
            offset: max_off,
            klen: max_klen,
            vlen: max_vlen,
        };

        let encoded = PairRaw::to_raw(p2);
        let decoded = encoded.from_raw();

        assert_eq!(p2, decoded);
    }

    #[test]
    fn test_randomized_values() {
        // TODO: Update this test for randomize Namespace tests
        for i in 0..100 {
            let offset = (i * 1234567) as u64 & ((1u64 << 40) - 1);
            let klen = (i * 37 % (1 << 12)) as u16;
            let vlen = (i * 91 % (1 << 12)) as u16;

            let p = Pair {
                ns: Namespace::Base,
                offset,
                klen,
                vlen,
            };

            let encoded = PairRaw::to_raw(p);
            let decoded = encoded.from_raw();

            assert_eq!(p, decoded, "Failed at iteration {i}");
        }
    }
}
