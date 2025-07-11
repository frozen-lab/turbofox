use xxhash::XxHash32;

const INVALID_HASH: u32 = 0;

/// Hasher struct to derive the shard and row from a 64-bit hash value.
///
/// The internal 32-bit value is structured as follows:
/// - The lower 16 bits are the row selector.
/// - The upper 16 bits are the shard selector.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TurboHasher {
    row: u16,
    shard: u16,
}

impl TurboHasher {
    /// Creates a new `TurboHasher` from a byte slice.
    ///
    /// The input is hashed using `XxHash64`, and the resulting 64-bit value is
    /// used to initialize the hasher.
    pub fn new(buf: &[u8]) -> Self {
        Self::from_hash(XxHash32::oneshot(buf))
    }

    /// Returns the 16-bit shard selector.
    #[inline]
    pub fn shard_selector(&self) -> u32 {
        self.shard as u32
    }

    /// Returns the row selector, which is an index into a shard's rows.
    #[inline]
    pub fn row_selector(&self, num_rows: usize) -> usize {
        self.row as usize % num_rows
    }

    fn from_hash(hash: u32) -> Self {
        let mut hash = hash;

        if hash == INVALID_HASH {
            hash = 0x1234_5678;
        }

        let row = ((hash >> 32) & 0xFFFF) as u16;
        let shard = ((hash >> 48) & 0xFFFF) as u16;

        Self { row, shard }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_fp_shard_row() {
        const NUM_ROWS: usize = 32;

        let data = b"some test data";
        let h1 = TurboHasher::new(data);
        let h2 = TurboHasher::new(data);

        assert_eq!(
            h1.shard_selector(),
            h2.shard_selector(),
            "fingerprints differ"
        );
        assert_eq!(
            h1.row_selector(NUM_ROWS),
            h2.row_selector(NUM_ROWS),
            "row_selector differs"
        );
        assert_eq!(
            h1.shard_selector(),
            h2.shard_selector(),
            "shard_selector differs"
        );
    }

    #[test]
    fn hash_is_never_invalid() {
        for i in 0..10 {
            let buf = format!("collision test {}", i).into_bytes();
            let hasher = TurboHasher::new(&buf);
            let hash = ((hasher.shard as u32) << 16) | (hasher.row as u32);

            assert_ne!(hash, INVALID_HASH, "got INVALID_FP for {:?}", buf);
        }
    }

    #[test]
    fn row_selector_in_bounds() {
        const ROWS_NUM: usize = 32;

        let data = b"row bounds";

        for _ in 0..100 {
            let h = TurboHasher::new(data);
            let row = h.row_selector(ROWS_NUM);

            assert!(row < ROWS_NUM, "row_selector {} out of bounds", row);
        }
    }

    #[test]
    fn different_inputs_produce_different() {
        let a = TurboHasher::new(b"a");
        let b = TurboHasher::new(b"b");

        assert_ne!(
            a.shard_selector(),
            b.shard_selector(),
            "shard collision a vs b"
        );
    }
}
