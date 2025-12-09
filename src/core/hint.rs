/// Hint for branch predictor that likely is mostly `true`
#[inline(always)]
pub(crate) fn likely(b: bool) -> bool {
    if !b {
        cold_fn();
    }
    b
}

/// Hint for branch predictor that unlikely is mostly `true`
#[inline(always)]
pub(crate) fn unlikely(b: bool) -> bool {
    if b {
        cold_fn();
    }
    b
}

/// empty function used as a placeholder to influence branch prediction,
/// by making in "unlikely" with use of #cold
#[inline(always)]
#[cold]
fn cold_fn() {}
