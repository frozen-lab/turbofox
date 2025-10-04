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
