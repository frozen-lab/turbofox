mod hasher;
mod shard;

pub struct TurboCache;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_check() {
        assert_eq!(std::mem::size_of_val(&TurboCache), 0);
    }
}
