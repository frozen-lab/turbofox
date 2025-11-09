mod den;
mod trail;

pub(crate) const DEFAULT_CAP: usize = 1024;
pub(crate) const DEFAULT_PAGE_SIZE: usize = 512; // 1/2 KiB
pub(crate) const CAP_MULTIPLIER: usize = 2; // 2x increase
pub(crate) const DEFAULT_NUM_PAGES: usize = 4096; // 16 KiB of disk space (each 0.5 KiB)

// sanity checks
const _: () = assert!(CAP_MULTIPLIER > 1, "Must not be 0 or 1");
const _: () = assert!((DEFAULT_CAP & (DEFAULT_CAP - 1)) == 0, "Should be power of 2");
const _: () = assert!((DEFAULT_PAGE_SIZE & (DEFAULT_PAGE_SIZE - 1)) == 0, "Must be power of 2");
