#![allow(unused)]

pub use crate::error::{TurboError, TurboResult};
use crate::{grantha::Grantha, logger::Logger};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

mod error;
mod grantha;
mod hasher;
mod kosh;
mod logger;

/// ----------------------------------------
/// Constants and Types
/// ----------------------------------------

const GROWABLE: bool = true;
const DEFAULT_ROWS: usize = 64; // 1024 slots by default
const DEFAULT_BKT_NAME: &'static str = "default";

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

impl TurboCfg {
    #[inline(always)]
    pub const fn logging(mut self, logging: bool) -> Self {
        self.logging = logging;
        self
    }

    #[inline(always)]
    pub const fn rows(mut self, cap: usize) -> Self {
        self.rows = cap;
        self
    }

    #[inline(always)]
    pub const fn growable(mut self, grow: bool) -> Self {
        self.growable = grow;
        self
    }
}

///
/// A persistent and efficient embedded KV database
///
pub struct TurboCache {
    logger: Logger,
    dirpath: PathBuf,
    config: TurboCfg,
    buckets: HashMap<&'static str, InternalBucket>,
}

impl TurboCache {
    pub fn new<P: AsRef<Path>>(dirpath: P, config: TurboCfg) -> TurboResult<Self> {
        // make sure the dir exists
        fs::create_dir_all(&dirpath)?;

        Ok(Self {
            config: config,
            buckets: HashMap::new(),
            dirpath: dirpath.as_ref().to_path_buf(),
            logger: Logger::new(false, "TurboCache"),
        })
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

impl Default for BucketCfg {
    #[inline(always)]
    fn default() -> Self {
        Self {
            rows: DEFAULT_ROWS,
            growable: GROWABLE,
        }
    }
}

impl BucketCfg {
    #[inline(always)]
    pub const fn rows(mut self, cap: usize) -> Self {
        self.rows = cap;
        self
    }

    #[inline(always)]
    pub const fn growable(mut self, grow: bool) -> Self {
        self.growable = grow;
        self
    }
}

///
/// Isolated containers w/ custom configs
///
pub struct TurboBucket<'a> {
    name: &'static str,
    cache: &'a mut TurboCache,
}

///
/// Internal Bucket
///
struct InternalBucket {
    config: BucketCfg,
    router: Option<Grantha>,
}
