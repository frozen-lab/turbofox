mod den;
mod mark;
mod trail;

pub(crate) const DEFAULT_CAP: usize = 0x400; // 1024
pub(crate) const DEFAULT_PAGE_SIZE: usize = 0x200; // 0.5 KiB
pub(crate) const DEFAULT_NUM_PAGES: usize = 0x1000; // 16 KiB of disk space (each 0.5 KiB)

pub(crate) const CAP_MULTIPLIER: usize = 0x02; // 2x increase
pub(crate) const OS_PAGE_SIZE: usize = 0x1000;

// sanity checks
const _: () = assert!(CAP_MULTIPLIER > 0x01, "Must not be 0 or 1");
const _: () = assert!((DEFAULT_CAP & (DEFAULT_CAP - 0x01)) == 0x00, "Should be power of 2");
const _: () = assert!((OS_PAGE_SIZE & (OS_PAGE_SIZE - 0x01)) == 0x00, "Must be power of 2");
const _: () = assert!(
    (DEFAULT_PAGE_SIZE & (DEFAULT_PAGE_SIZE - 0x01)) == 0x00,
    "Must be power of 2"
);
const _: () = assert!(
    (DEFAULT_NUM_PAGES & (DEFAULT_NUM_PAGES - 0x01)) == 0x00,
    "Must be power of 2"
);
