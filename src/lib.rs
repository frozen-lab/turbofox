mod bucket;
mod core;
mod hash;
mod router;

pub use core::TurboError;

pub struct TurboCache;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_check() {
        assert_eq!(std::mem::size_of_val(&TurboCache), 0);
    }
}
