#![allow(dead_code)]

use twox_hash::XxHash3_64;

pub(crate) struct THasher(u64);

impl THasher {
    pub fn new(buf: &[u8]) -> Self {
        let h = XxHash3_64::oneshot(buf);

        Self(h)
    }
}
