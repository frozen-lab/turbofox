/// ----------------------------------------
/// Constants and Types
/// ----------------------------------------

const GROWABLE: bool = true;
const DEFAULT_ROWS: usize = 64; // 1024 slots by default

pub(crate) const DEFAULT_BKT_NAME: &'static str = "default";

///
/// Configurations for the [TurboCache]
///
#[derive(Debug, Clone)]
pub struct TurboCfg {
    pub logging: bool,
    pub rows: usize,
    pub growable: bool,
}

impl Default for TurboCfg {
    #[inline(always)]
    fn default() -> Self {
        Self {
            logging: false,
            rows: DEFAULT_ROWS,
            growable: GROWABLE,
        }
    }
}

impl std::fmt::Display for TurboCfg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TurboCfg(logging={}, rows={}, growable={})",
            self.logging, self.rows, self.growable
        )
    }
}

impl TurboCfg {
    #[inline(always)]
    pub const fn logging(mut self, logging: bool) -> Self {
        self.logging = logging;
        self
    }

    #[inline(always)]
    pub const fn rows(mut self, rows: usize) -> Self {
        // sanity check
        assert!(rows > 0, "No of rows must not be zero!");

        self.rows = rows;
        self
    }

    #[inline(always)]
    pub const fn growable(mut self, grow: bool) -> Self {
        self.growable = grow;
        self
    }
}

///
/// Configurations for the [TurboBucket]
///
#[derive(Debug, Copy, Clone)]
pub struct BucketCfg {
    pub rows: usize,
    pub growable: bool,
}

impl std::fmt::Display for BucketCfg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BucketCfg(rows={}, growable={})",
            self.rows, self.growable
        )
    }
}

impl Default for BucketCfg {
    #[inline(always)]
    fn default() -> Self {
        Self {
            rows: DEFAULT_ROWS,
            growable: GROWABLE,
        }
    }
}

impl From<TurboCfg> for BucketCfg {
    fn from(value: TurboCfg) -> Self {
        Self {
            rows: value.rows,
            growable: value.growable,
        }
    }
}

impl BucketCfg {
    #[inline(always)]
    pub const fn rows(mut self, rows: usize) -> Self {
        // sanity check
        assert!(rows > 0, "No of rows must not be zero!");

        self.rows = rows;
        self
    }

    #[inline(always)]
    pub const fn growable(mut self, grow: bool) -> Self {
        self.growable = grow;
        self
    }
}

#[cfg(test)]
mod cfg_tests {
    use super::*;

    mod turbo_cfg {
        use super::*;

        #[test]
        fn test_default_values_are_correct() {
            let cfg = TurboCfg::default();
            assert!(!cfg.logging);
            assert_eq!(cfg.rows, DEFAULT_ROWS);
            assert_eq!(cfg.growable, GROWABLE);
        }

        #[test]
        fn test_display_format() {
            let cfg = TurboCfg {
                logging: true,
                rows: 128,
                growable: false,
            };
            let s = format!("{}", cfg);

            assert!(s.contains("TurboCfg(logging=true"));
            assert!(s.contains("rows=128"));
            assert!(s.contains("growable=false"));
        }

        #[test]
        fn test_builder_methods_update_fields_correctly() {
            let cfg = TurboCfg::default().logging(true).rows(256).growable(false);

            assert!(cfg.logging);
            assert_eq!(cfg.rows, 256);
            assert!(!cfg.growable);
        }

        #[test]
        #[should_panic(expected = "No of rows must not be zero!")]
        fn test_rows_zero_panics() {
            let _ = TurboCfg::default().rows(0);
        }

        #[test]
        fn test_immutability_preserved_on_chaining() {
            let cfg1 = TurboCfg::default();
            let cfg2 = cfg1.clone().rows(512);

            assert_eq!(cfg1.rows, DEFAULT_ROWS);
            assert_eq!(cfg2.rows, 512);
        }
    }

    mod bucket_cfg {
        use super::*;

        #[test]
        fn test_default_values_are_correct() {
            let cfg = BucketCfg::default();

            assert_eq!(cfg.rows, DEFAULT_ROWS);
            assert_eq!(cfg.growable, GROWABLE);
        }

        #[test]
        fn test_display_format() {
            let cfg = BucketCfg {
                rows: 256,
                growable: false,
            };
            let s = format!("{}", cfg);

            assert!(s.contains("BucketCfg(rows=256"));
            assert!(s.contains("growable=false"));
        }

        #[test]
        fn test_builder_methods_update_fields_correctly() {
            let cfg = BucketCfg::default().rows(128).growable(false);

            assert_eq!(cfg.rows, 128);
            assert!(!cfg.growable);
        }

        #[test]
        #[should_panic(expected = "No of rows must not be zero!")]
        fn test_rows_zero_panics() {
            let _ = BucketCfg::default().rows(0);
        }

        #[test]
        fn test_from_turbo_cfg_conversion() {
            let tcfg = TurboCfg::default().rows(512).growable(false);
            let bcfg: BucketCfg = tcfg.clone().into();

            assert_eq!(bcfg.rows, tcfg.rows);
            assert_eq!(bcfg.growable, tcfg.growable);
        }

        #[test]
        fn test_immutability_preserved_on_chaining() {
            let cfg1 = BucketCfg::default();
            let cfg2 = cfg1.rows(2048);

            assert_eq!(cfg1.rows, DEFAULT_ROWS);
            assert_eq!(cfg2.rows, 2048);
        }
    }
}
