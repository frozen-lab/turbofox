#![allow(unused)]

pub use crate::error::{TurboError, TurboResult};
use crate::{grantha::Grantha, kosh::ROW_SIZE, logger::Logger};
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
/// Internal Bucket
///
struct InternalBucket {
    cfg: BucketCfg,
    grantha: Option<Grantha>,
}

impl InternalBucket {
    #[inline(always)]
    fn new(cfg: BucketCfg) -> Self {
        Self { cfg, grantha: None }
    }
}

///
/// A persistent and efficient embedded KV database
///
pub struct TurboCache {
    logger: Logger,
    dirpath: PathBuf,
    cfg: TurboCfg,
    buckets: HashMap<&'static str, InternalBucket>,
}

impl TurboCache {
    pub fn new<P: AsRef<Path>>(dirpath: P, config: TurboCfg) -> TurboResult<Self> {
        // make sure the dir exists
        fs::create_dir_all(&dirpath)?;

        Ok(Self {
            cfg: config.clone(),
            buckets: HashMap::new(),
            dirpath: dirpath.as_ref().to_path_buf(),
            logger: Logger::new(false, "TurboCache"),
        })
    }

    pub fn bucket(&mut self, name: &'static str, config: Option<BucketCfg>) -> TurboBucket<'_> {
        if !self.buckets.contains_key(name) {
            let cfg = config.unwrap_or_else(|| self.cfg.clone().into());
            self.buckets.insert(name, InternalBucket::new(cfg));
        }

        TurboBucket { name, cache: self }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> TurboResult<()> {
        // sanity checks
        debug_assert!(key.len() < u16::MAX as usize, "Key is too large");
        debug_assert!(key.len() < u16::MAX as usize, "Value is too large");

        self.bucket(DEFAULT_BKT_NAME, None).set(key, value)
    }

    pub fn get(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        // sanity checks
        debug_assert!(key.len() < u16::MAX as usize, "Key is too large");

        self.bucket(DEFAULT_BKT_NAME, None).get(key)
    }

    pub fn del(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        // sanity checks
        debug_assert!(key.len() < u16::MAX as usize, "Key is too large");

        self.bucket(DEFAULT_BKT_NAME, None).del(key)
    }

    pub fn get_inserted_count(&mut self) -> TurboResult<usize> {
        self.bucket(DEFAULT_BKT_NAME, None).get_inserted_count()
    }

    fn get_or_init_grantha(&mut self, name: &'static str) -> TurboResult<&mut Grantha> {
        if let Some(entry) = self.buckets.get_mut(name) {
            if entry.grantha.is_none() {
                let grantha =
                    Grantha::open(&self.dirpath, name, Self::calc_new_cap(self.cfg.rows))?;
                entry.grantha = Some(grantha);
            }

            return Ok(entry.grantha.as_mut().unwrap());
        }

        // HACK: This should never occur!
        Err(TurboError::Unknown)
    }

    #[inline(always)]
    fn calc_new_cap(rows: usize) -> usize {
        debug_assert!(
            (rows * ROW_SIZE) % 16 == 0,
            "Capacity must be multiple of 16"
        );

        rows * ROW_SIZE
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

impl<'a> TurboBucket<'a> {
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> TurboResult<()> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;

        Ok(grantha.upsert((key.to_vec(), value.to_vec()))?)
    }

    pub fn get(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;

        Ok(grantha.fetch(key.to_vec())?)
    }

    pub fn del(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;

        Ok(grantha.yank(key.to_vec())?)
    }

    pub fn get_inserted_count(&mut self) -> TurboResult<usize> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;

        Ok(grantha.pair_count()?)
    }
}
