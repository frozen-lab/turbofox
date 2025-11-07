mod den;
mod trail;

pub(crate) const DEFAULT_CAP: usize = 1024;
pub(crate) const CAP_MULTIPLIER: usize = 2; // 2x increase
pub(crate) const PAGE_SIZE: usize = 4096;

// sanity checks
const _: () = assert!((PAGE_SIZE & (PAGE_SIZE - 1)) == 0, "Must be power of 2");
const _: () = assert!((DEFAULT_CAP & (DEFAULT_CAP - 1)) == 0, "Should be power of 2");
const _: () = assert!(CAP_MULTIPLIER > 1, "Must not be 0 or 1");
