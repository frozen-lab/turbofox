pub mod xxhash64;
pub use xxhash64::Hasher as XxHash64;

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
