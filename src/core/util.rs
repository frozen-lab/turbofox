/// Check if `x` is power of 2 or not
#[inline(always)]
pub(crate) const fn is_pow_of_2(x: usize) -> bool {
    (x & (x - 1)) == 0
}
