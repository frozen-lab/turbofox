#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub(crate) enum ISA {
    // This SIMD ISA is an upgrade over SSE2 if available at runtime
    AVX2,

    // This SIMD ISA is default on x64 (x86_64), as it's virtually
    // available on all x64 CPU's
    SSE2,

    // Neon is vurtually available on all aarch64 CPU's
    NEON,
}

impl ISA {
    #[cfg(target_arch = "x86_64")]
    fn detect_isa() -> ISA {
        if is_x86_feature_detected!("avx2") {
            return ISA::AVX2;
        }

        ISA::SSE2
    }

    #[cfg(target_arch = "aarch64")]
    fn detect_isa() -> ISA {
        ISA::NEON
    }
}

#[cfg(test)]
mod isa_tests {
    use super::ISA;

    #[test]
    fn test_detect_isa_is_correct() {
        let isa = ISA::detect_isa();

        #[cfg(target_arch = "x86_64")]
        match isa {
            ISA::AVX2 | ISA::SSE2 => {}
            _ => panic!("Unknown ISA detected for x86_64"),
        }

        #[cfg(target_arch = "aarch64")]
        match isa {
            ISA::NEON => {}
            _ => panic!("Unknown ISA detected for aarch64"),
        }
    }
}
