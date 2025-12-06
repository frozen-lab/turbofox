mod den;
mod mark;
mod trail;

pub(crate) const DEFAULT_INIT_CAP: usize = 0x400; // 1024
pub(crate) const DEFAULT_PAGE_SIZE: usize = 0x80; // 128
pub(crate) const GROWTH_FACTOR: u64 = 0x02; // must preserve power of 2

// sanity checks
const _: () = assert!(
    (DEFAULT_INIT_CAP & (DEFAULT_INIT_CAP - 0x01)) == 0x00,
    "Default init capacity must be power of 2"
);
const _: () = assert!(
    (DEFAULT_PAGE_SIZE & (DEFAULT_PAGE_SIZE - 0x01)) == 0x00,
    "Default page size must be power of 2"
);
const _: () = assert!(
    (0x400 * GROWTH_FACTOR) & ((0x400 * GROWTH_FACTOR) - 0x01) == 0x00,
    "GROWTH_FACTOR must preserve power of 2 nature of values"
);
