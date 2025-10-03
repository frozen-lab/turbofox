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

#[cfg(test)]
mod turbo_tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache(rows: usize) -> (TurboCache, TempDir) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path();
        let cache = TurboCache::new(path.to_path_buf(), TurboCfg::default().rows(rows)).unwrap();

        (cache, tmp)
    }

    #[test]
    fn insert_and_get() {
        let (mut cache, _tmp) = create_cache(10);
        let key = b"foo".to_vec();
        let value = b"bar".to_vec();

        cache.set(&key, &value).unwrap();
        assert_eq!(cache.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn overwrite_value() {
        let (mut cache, _tmp) = create_cache(10);
        let key = b"k1".to_vec();

        cache.set(&key, b"v1").unwrap();
        cache.set(&key, b"v2").unwrap();

        assert_eq!(cache.get(&key).unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_and_exists() {
        let (mut cache, _tmp) = create_cache(10);
        let key = b"hello".to_vec();

        cache.set(&key, b"world").unwrap();
        assert!(cache.get(&key).unwrap().is_some());

        cache.del(&key).unwrap();
        assert!(cache.get(&key).unwrap().is_none());
        assert_eq!(cache.get(&key).unwrap(), None);
    }

    #[test]
    fn total_count_reflects_inserts_and_deletes() {
        let (mut cache, _tmp) = create_cache(10);

        for i in 0..5 {
            cache.set(&[i], &[i]).unwrap();
        }

        assert_eq!(cache.get_inserted_count().unwrap(), 5);

        cache.del(&[0]).unwrap();
        cache.del(&[1]).unwrap();

        assert_eq!(cache.get_inserted_count().unwrap(), 3);
    }

    #[test]
    fn get_non_existent_key_returns_none() {
        let (mut cache, _tmp) = create_cache(10);
        assert_eq!(cache.get(b"nope").unwrap(), None);
    }

    #[test]
    fn default_bucket_works_out_of_box() {
        let (mut cache, _tmp) = create_cache(5);
        cache.set(b"default-key", b"default-val").unwrap();

        assert_eq!(
            cache.get(b"default-key").unwrap(),
            Some(b"default-val".to_vec())
        );
    }

    #[test]
    fn named_bucket_with_custom_config() {
        let (mut cache, _tmp) = create_cache(5);
        let mut users = cache.bucket("users", Some(BucketCfg::default().rows(50)));

        users.set(b"uid1", b"john").unwrap();
        users.set(b"uid2", b"jane").unwrap();

        assert_eq!(users.get(b"uid1").unwrap(), Some(b"john".to_vec()));
        assert_eq!(users.get(b"uid2").unwrap(), Some(b"jane".to_vec()));
    }

    #[test]
    fn bucket_inherits_global_config_if_not_provided() {
        let (mut cache, _tmp) = create_cache(123);

        cache.bucket("products", None);
        let entry = cache.buckets.get("products").unwrap();
        assert_eq!(entry.cfg.rows, 123);

        let mut products = cache.bucket("products", None);
        products.set(b"p1", b"item").unwrap();
        assert_eq!(products.get(b"p1").unwrap(), Some(b"item".to_vec()));
    }

    #[test]
    fn router_is_lazy_initialized() {
        let (mut cache, _tmp) = create_cache(5);

        assert!(cache.buckets.get("orders").is_none());

        {
            let mut orders = cache.bucket("orders", None);
            orders.set(b"id1", b"pending").unwrap();
        }

        assert!(cache.buckets.get("orders").unwrap().grantha.is_some());
    }

    #[test]
    fn error_on_nonexistent_bucket_router() {
        let (mut cache, _tmp) = create_cache(5);
        let result = cache.get_or_init_grantha("ghost");

        assert!(result.is_err());
    }
}
