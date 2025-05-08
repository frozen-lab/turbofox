const FNV1A_64_OFFSET_BASIS: usize = 14695981039346656037_usize;

pub trait Hashable {
    fn hash(&self) -> usize;
}

impl Hashable for &str {
    fn hash(&self) -> usize {
        let mut hash: usize = FNV1A_64_OFFSET_BASIS;

        for &b in self.as_bytes() {
            hash ^= b as usize;
            hash = hash.wrapping_mul(1099511628211);
        }

        hash
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn hash_empty_string() {
        assert_eq!("".hash(), FNV1A_64_OFFSET_BASIS);
    }

    #[test]
    fn deterministic_behaviour() {
        let words = vec!["Hello", "World", "Rust"];

        for word in words {
            assert_eq!(word.hash(), word.hash());
        }
    }

    #[test]
    fn unique_hashes_unique_strings() {
        assert_ne!("Hello".hash(), "World".hash());
    }
}
