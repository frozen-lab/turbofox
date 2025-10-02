#![allow(unused)]

use crate::router::Router;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

mod bucket;
mod error;
mod grantha;
mod hasher;
mod kosh;
mod logger;
mod router;

pub use crate::error::{TurboError, TurboResult};

pub struct TurboCache {
    dirpath: PathBuf,
    config: TurboConfig,
    buckets: HashMap<&'static str, InternalTurboBucket>,
}

impl TurboCache {
    const DEFAULT: &'static str = "default";

    pub fn new<P: AsRef<Path>>(dirpath: P, config: TurboConfig) -> TurboResult<Self> {
        // make sure the dir exists
        fs::create_dir_all(&dirpath)?;

        Ok(Self {
            dirpath: dirpath.as_ref().to_path_buf(),
            config: config,
            buckets: HashMap::new(),
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> TurboResult<()> {
        // sanity checks
        debug_assert!(key.len() < u16::MAX as usize, "Key is too large");
        debug_assert!(key.len() < u16::MAX as usize, "Value is too large");

        self.bucket(Self::DEFAULT, None).set(key, value)
    }

    pub fn get(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        // sanity checks
        debug_assert!(key.len() < u16::MAX as usize, "Key is too large");

        self.bucket(Self::DEFAULT, None).get(key)
    }

    pub fn del(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        // sanity checks
        debug_assert!(key.len() < u16::MAX as usize, "Key is too large");

        self.bucket(Self::DEFAULT, None).del(key)
    }

    pub fn get_inserted_count(&mut self) -> TurboResult<usize> {
        self.bucket(Self::DEFAULT, None).get_inserted_count()
    }

    pub fn bucket(&mut self, name: &'static str, config: Option<TurboConfig>) -> TurboBucket<'_> {
        if !self.buckets.contains_key(name) {
            let cfg = config.unwrap_or_else(|| self.config.clone());
            self.buckets.insert(
                name,
                InternalTurboBucket {
                    config: cfg,
                    router: None,
                },
            );
        }

        TurboBucket { name, cache: self }
    }

    fn get_or_init_router(&mut self, name: &'static str) -> TurboResult<&mut Router> {
        if let Some(entry) = self.buckets.get_mut(name) {
            if entry.router.is_none() {
                let router = Router::open(&self.dirpath, name, entry.config.capacity)?;
                entry.router = Some(router);
            }

            return Ok(entry.router.as_mut().unwrap());
        }

        // HACK: This should never occur!
        Err(TurboError::Unknown)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TurboConfig {
    pub capacity: usize,
    pub growable: bool,
}

impl Default for TurboConfig {
    #[inline(always)]
    fn default() -> Self {
        Self {
            capacity: 1024,
            growable: true,
        }
    }
}

impl TurboConfig {
    #[inline(always)]
    pub const fn capacity(mut self, cap: usize) -> Self {
        self.capacity = cap;
        self
    }

    #[inline(always)]
    pub const fn growable(mut self, grow: bool) -> Self {
        self.growable = grow;
        self
    }
}

struct InternalTurboBucket {
    config: TurboConfig,
    router: Option<Router>,
}

pub struct TurboBucket<'a> {
    name: &'static str,
    cache: &'a mut TurboCache,
}

impl<'a> TurboBucket<'a> {
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> TurboResult<()> {
        let router = self.cache.get_or_init_router(self.name)?;
        Ok(router.set((key.to_vec(), value.to_vec()))?)
    }

    pub fn get(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let router = self.cache.get_or_init_router(self.name)?;
        Ok(router.get(key.to_vec())?)
    }

    pub fn del(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let router = self.cache.get_or_init_router(self.name)?;
        Ok(router.del(key.to_vec())?)
    }

    pub fn get_inserted_count(&mut self) -> TurboResult<usize> {
        let router = self.cache.get_or_init_router(self.name)?;
        Ok(router.get_insert_count()?)
    }
}

#[cfg(test)]
mod turbo_tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache(capacity: usize) -> (TurboCache, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cache = TurboCache::new(
            tmp.path().to_path_buf(),
            TurboConfig::default().capacity(capacity),
        )
        .unwrap();

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
        let mut users = cache.bucket("users", Some(TurboConfig::default().capacity(50)));

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
        assert_eq!(entry.config.capacity, 123);

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

        assert!(cache.buckets.get("orders").unwrap().router.is_some());
    }

    #[test]
    fn error_on_nonexistent_bucket_router() {
        let (mut cache, _tmp) = create_cache(5);
        let result = cache.get_or_init_router("ghost");

        assert!(result.is_err());
    }
}
