//! This module provides a specialized hashing utility, `TurboHasher`, designed for
//! mapping keys to specific shards, rows, and fingerprints within the database.
//!
//! The `TurboHasher` takes a byte slice and computes a 64-bit hash value, which is then
//! intelligently partitioned to derive three critical pieces of information:
//!
//! 1.  **Fingerprint**: A 32-bit value that uniquely identifies the key.
//! 2.  **Shard Selector**: A 16-bit value used to determine which shard the key belongs to.
//! 3.  **Row Selector**: A 16-bit value that maps the key to a specific row within a shard.
//!
//! This design allows for efficient and deterministic placement of data, ensuring that
//! a given key always maps to the same location.

use crate::shard::ROWS_NUM;
use xxhash::XxHash64;

const SEED: u64 = 0;
pub(crate) const INVALID_FP: u32 = 0;

/// A specialized hasher that derives a fingerprint, shard selector, and row selector
/// from a 64-bit hash value.
///
/// The internal 64-bit value is structured as follows:
/// - The lower 32 bits are the fingerprint.
/// - The next 16 bits are the row selector.
/// - The upper 16 bits are the shard selector.
#[derive(Clone, Copy)]
pub(crate) struct TurboHasher(u64);

impl TurboHasher {
    /// Creates a new `TurboHasher` from a byte slice.
    ///
    /// The input is hashed using `XxHash64`, and the resulting 64-bit value is
    /// used to initialize the hasher.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use turbocache::hasher::TurboHasher;
    ///
    /// let hasher = TurboHasher::new(b"hello world");
    /// ```
    pub fn new(buf: &[u8]) -> Self {
        Self::from_hash(XxHash64::oneshot(SEED, buf))
    }

    /// Returns the 32-bit fingerprint of the hash.
    ///
    /// The fingerprint is a unique identifier for the key and is derived from the
    /// lower 32 bits of the hash.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use turbocache::hasher::TurboHasher;
    ///
    /// let hasher = TurboHasher::new(b"hello world");
    /// let fingerprint = hasher.fingerprint();
    /// ```
    #[inline]
    pub fn fingerprint(&self) -> u32 {
        self.0 as u32
    }

    /// Returns the 16-bit shard selector.
    ///
    /// The shard selector is used to determine which shard the key belongs to and is
    /// derived from the upper 16 bits of the hash.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use turbocache::hasher::TurboHasher;
    ///
    /// let hasher = TurboHasher::new(b"hello world");
    /// let shard = hasher.shard_selector();
    /// ```
    #[inline]
    pub fn shard_selector(&self) -> u32 {
        ((self.0 >> 48) & 0xffff) as u32
    }

    /// Returns the row selector, which is an index into a shard's rows.
    ///
    /// The row selector maps the key to a specific row within a shard and is
    /// derived from the middle 16 bits of the hash.
    ///
    /// ### Example
    ///
    /// ```rust
    /// use turbocache::hasher::TurboHasher;
    ///
    /// let hasher = TurboHasher::new(b"hello world");
    /// let row = hasher.row_selector();
    /// ```
    #[inline]
    pub fn row_selector(&self) -> usize {
        ((self.0 >> 32) as u16) as usize % ROWS_NUM
    }

    /// Constructs a `TurboHasher` from a 64-bit hash, ensuring the fingerprint
    /// is never zero.
    ///
    /// If the lower 32 bits (the fingerprint) are zero, it attempts to use the
    /// upper 32 bits. If those are also zero, a fallback constant is used.
    fn from_hash(hash: u64) -> Self {
        let mut sign = hash as u32;

        if sign == INVALID_FP {
            sign = (hash >> 32) as u32;

            if sign == INVALID_FP {
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
    fn deterministic_fp_shard_row() {
        let data = b"some test data";
        let h1 = TurboHasher::new(data);
        let h2 = TurboHasher::new(data);

        assert_eq!(h1.fingerprint(), h2.fingerprint(), "fingerprints differ");
        assert_eq!(h1.row_selector(), h2.row_selector(), "row_selector differs");
        assert_eq!(
            h1.shard_selector(),
            h2.shard_selector(),
            "shard_selector differs"
        );
    }

    #[test]
    fn fingerprint_is_never_invalid_fp() {
        for i in 0..10 {
            let buf = format!("collision test {}", i).into_bytes();
            let h = TurboHasher::new(&buf);

            assert_ne!(h.fingerprint(), INVALID_FP, "got INVALID_FP for {:?}", buf);
        }
    }

    #[test]
    fn row_selector_in_bounds() {
        let data = b"row bounds";

        for _ in 0..100 {
            let h = TurboHasher::new(data);
            let row = h.row_selector();

            assert!(row < ROWS_NUM, "row_selector {} out of bounds", row);
        }
    }

    #[test]
    fn different_inputs_produce_different() {
        let a = TurboHasher::new(b"a");
        let b = TurboHasher::new(b"b");

        assert_ne!(
            a.fingerprint(),
            b.fingerprint(),
            "fingerprint collision a vs b"
        );
        assert_ne!(
            a.shard_selector(),
            b.shard_selector(),
            "shard collision a vs b"
        );
    }
}
