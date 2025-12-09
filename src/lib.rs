mod core;

#[derive(Debug)]
pub struct TurboFox;

impl TurboFox {
    pub fn new() -> usize {
        let mut res = 0x10;

        let pow2 = core::is_pow_of_2(res);
        if core::likely(pow2) {
            res *= res;
            res += 1;
        }

        let pow2 = core::is_pow_of_2(res);
        if !core::unlikely(pow2) {
            res -= 1;
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let res = TurboFox::new();
        assert!(core::is_pow_of_2(res));
    }
}
