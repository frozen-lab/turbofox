pub struct TurboCache;

#[cfg(test)]
mod turbo_tests {
    use super::*;

    #[test]
    fn sanity_check() {
        assert_eq!(std::mem::size_of_val(&TurboCache), 0);
    }
}
