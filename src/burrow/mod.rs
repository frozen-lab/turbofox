mod mark;
mod trail;

pub(crate) const DEFAULT_INIT_CAP: usize = 0x400; // 1024
pub(crate) const DEFAULT_KBUF_LEN: usize = 0x80; // 128

// sanity checks
const _: () = assert!(
    (DEFAULT_INIT_CAP & (DEFAULT_INIT_CAP - 0x01)) == 0x00,
    "Default init capacity must be power of 2"
);
