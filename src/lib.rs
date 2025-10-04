mod cfg;
mod error;
mod grantha;
mod hasher;
mod kosh;
mod logger;

use crate::{cfg::DEFAULT_BKT_NAME, grantha::Grantha, logger::Logger};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

pub use crate::{
    cfg::{BucketCfg, TurboCfg},
    error::{TurboError, TurboResult},
};

///
/// Internal Bucket
///
#[derive(Debug)]
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
#[derive(Debug)]
pub struct TurboCache {
    logger: Logger,
    dirpath: PathBuf,
    cfg: TurboCfg,
    buckets: HashMap<&'static str, InternalBucket>,
}

impl TurboCache {
    pub fn new<P: AsRef<Path>>(dirpath: P, config: TurboCfg) -> TurboResult<Self> {
        let logger = Logger::new(config.logging, "TurboCache");

        // make sure the dir exists
        fs::create_dir_all(&dirpath).map_err(|e| {
            log_error!(logger, "Unable to create turbo dir: {e}");
            e
        })?;

        log_debug!(logger, "Initialized TurboCache w/ {config}");

        Ok(Self {
            logger,
            cfg: config.clone(),
            buckets: HashMap::new(),
            dirpath: dirpath.as_ref().to_path_buf(),
        })
    }

    pub fn bucket(&mut self, name: &'static str, config: Option<BucketCfg>) -> TurboBucket<'_> {
        if !self.buckets.contains_key(name) {
            let cfg = config.unwrap_or_else(|| self.cfg.clone().into());
            self.buckets.insert(name, InternalBucket::new(cfg));

            log_debug!(self.logger, "Created Bucket ({}) w/ {cfg}", name);
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

    #[inline(always)]
    pub fn pair_count(&mut self) -> TurboResult<usize> {
        self.bucket(DEFAULT_BKT_NAME, None).pair_count()
    }

    #[inline(always)]
    pub fn is_full(&mut self) -> TurboResult<bool> {
        self.bucket(DEFAULT_BKT_NAME, None).is_full()
    }

    fn get_or_init_grantha(&mut self, name: &'static str) -> TurboResult<&mut Grantha> {
        let entry = self.buckets.get_mut(name).ok_or(TurboError::Unknown)?;

        if entry.grantha.is_none() {
            entry.grantha = Some(Grantha::open(&self.dirpath, name, &entry.cfg)?);
        }

        entry.grantha.as_mut().ok_or(TurboError::Unknown)
    }
}

///
/// Isolated containers w/ custom configs
///
#[derive(Debug)]
pub struct TurboBucket<'a> {
    name: &'static str,
    cache: &'a mut TurboCache,
}

impl<'a> TurboBucket<'a> {
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> TurboResult<()> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;
        grantha.upsert((key.to_vec(), value.to_vec()))?;

        log_trace!(
            self.cache.logger,
            "Inserted pair w/ key ({key:?}) into Bucket ({})",
            self.name
        );

        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;
        let val = grantha.fetch(key.to_vec())?;

        log_trace!(
            self.cache.logger,
            "Fetched pair w/ key ({key:?}) into Bucket ({})",
            self.name
        );

        Ok(val)
    }

    pub fn del(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;
        let val = grantha.yank(key.to_vec())?;

        log_trace!(
            self.cache.logger,
            "Deleted pair w/ key ({key:?}) into Bucket ({})",
            self.name
        );

        Ok(val)
    }

    pub fn pair_count(&mut self) -> TurboResult<usize> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;
        let count = grantha.pair_count()?;

        log_trace!(
            self.cache.logger,
            "Fetched pairCount=({count}) for Bucket ({})",
            self.name
        );

        Ok(count)
    }

    pub fn is_full(&mut self) -> TurboResult<bool> {
        let grantha = self.cache.get_or_init_grantha(self.name)?;
        let res = grantha.is_full()?;

        log_trace!(
            self.cache.logger,
            "Fetched isFull=({res}) for Bucket ({})",
            self.name
        );

        Ok(res)
    }
}

#[cfg(test)]
mod turbo_tests {
    use super::*;
    use crate::logger::init_test_logger;
    use tempfile::TempDir;

    #[ctor::ctor]
    fn init() {
        init_test_logger();
    }

    fn create_cache(rows: usize) -> (TurboCache, TempDir) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path();
        let cache = TurboCache::new(
            path.to_path_buf(),
            TurboCfg::default().rows(rows).logging(true),
        )
        .unwrap();

        (cache, tmp)
    }

    mod turbo_init {
        use super::*;
        use tempfile::TempDir;

        #[test]
        fn test_turbo_init_creating_new_creates_directory() {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("subdir");
            assert!(!path.exists());

            let _cache = TurboCache::new(&path, TurboCfg::default()).unwrap();
            assert!(path.exists(), "Cache init should create dir");
        }

        #[test]
        fn test_turbo_init_uses_provided_config() {
            let tmp = TempDir::new().unwrap();
            let cfg = TurboCfg::default().rows(128).growable(false).logging(true);
            let cache = TurboCache::new(tmp.path(), cfg.clone()).unwrap();

            assert_eq!(cache.cfg.rows, 128);
            assert!(!cache.cfg.growable);
            assert!(cache.cfg.logging);
        }

        #[test]
        fn test_turbo_init_underlying_bucket_inherits_global_config_if_none_provided() {
            let tmp = TempDir::new().unwrap();
            let mut cache = TurboCache::new(tmp.path(), TurboCfg::default().rows(99)).unwrap();

            cache.bucket("products", None);
            let entry = cache.buckets.get("products").unwrap();

            assert_eq!(entry.cfg.rows, 99);
        }

        #[test]
        fn test_turbo_init_underlying_bucket_uses_custom_config_if_provided() {
            let tmp = TempDir::new().unwrap();
            let mut cache = TurboCache::new(tmp.path(), TurboCfg::default()).unwrap();

            cache.bucket("users", Some(BucketCfg::default().rows(50)));
            let entry = cache.buckets.get("users").unwrap();

            assert_eq!(entry.cfg.rows, 50);
        }

        #[test]
        fn test_turbo_init_underlying_grantha_has_lazy_init() {
            let tmp = TempDir::new().unwrap();
            let mut cache = TurboCache::new(tmp.path(), TurboCfg::default()).unwrap();

            assert!(cache.buckets.get("orders").is_none());

            {
                let mut orders = cache.bucket("orders", None);
                orders.set(b"id1", b"pending").unwrap();
            }

            assert!(cache.buckets.get("orders").unwrap().grantha.is_some());
        }

        #[test]
        fn test_turbo_init_error_on_nonexistent_bucket_router() {
            let tmp = TempDir::new().unwrap();

            let mut cache = TurboCache::new(tmp.path(), TurboCfg::default()).unwrap();
            let result = cache.get_or_init_grantha("ghost");

            assert!(result.is_err());
        }
    }

    mod turbo_default_bucket {
        use super::*;

        #[test]
        fn test_default_bkt_insert_and_get_roundtrip() {
            let (mut cache, _tmp) = create_cache(10);

            for i in 0..50 {
                let k = format!("key-{}", i).into_bytes();
                let v = format!("val-{}", i).into_bytes();

                cache.set(&k, &v).unwrap();
                assert_eq!(cache.get(&k).unwrap(), Some(v));
            }
        }

        #[test]
        fn test_default_bkt_upsert_works() {
            let (mut cache, _tmp) = create_cache(10);
            let k = b"user".to_vec();

            cache.set(&k, b"old").unwrap();
            cache.set(&k, b"new").unwrap();

            assert_eq!(cache.get(&k).unwrap(), Some(b"new".to_vec()));
        }

        #[test]
        fn test_default_bkt_delete_yanks_pair() {
            let (mut cache, _tmp) = create_cache(10);

            let k = b"gone".to_vec();
            cache.set(&k, b"bye").unwrap();

            assert_eq!(cache.del(&k).unwrap(), Some(b"bye".to_vec()));
            assert_eq!(cache.get(&k).unwrap(), None);
        }

        #[test]
        fn test_default_bkt_inserted_count_tracks_inserts_and_deletes() {
            let (mut cache, _tmp) = create_cache(10);

            for i in 0..5 {
                cache.set(&[i], &[i]).unwrap();
            }

            assert_eq!(cache.pair_count().unwrap(), 5);

            cache.del(&[0]).unwrap();
            cache.del(&[1]).unwrap();

            assert_eq!(cache.pair_count().unwrap(), 3);
        }

        #[test]
        fn test_default_bkt_handles_empty_key_and_empty_value() {
            let (mut cache, _tmp) = create_cache(10);

            let k = b"noval".to_vec();
            let empty_key: Vec<u8> = vec![];
            let empty_val: Vec<u8> = vec![];

            assert!(cache.set(&empty_key, b"empty-key").is_ok());
            assert!(cache.set(&k, &empty_val).is_ok());

            assert_eq!(cache.get(&empty_key).unwrap(), Some(b"empty-key".to_vec()));
            assert_eq!(cache.get(&k).unwrap(), Some(empty_val));
        }

        #[test]
        fn test_default_bkt_correctly_inserts_binary_garbage_values() {
            let (mut cache, _tmp) = create_cache(10);

            let k = b"bin".to_vec();
            let v = vec![0, 255, 128, 42, 0, 1];

            cache.set(&k, &v).unwrap();
            assert_eq!(cache.get(&k).unwrap(), Some(v));
        }

        #[test]
        fn test_default_bkt_supports_long_keys_and_values() {
            let (mut cache, _tmp) = create_cache(10);
            let long_key = vec![b'k'; 1024];
            let long_val = vec![b'v'; 8192];

            assert!(cache.set(&long_key, &long_val).is_ok());
            assert_eq!(cache.get(&long_key).unwrap(), Some(long_val));
        }

        #[test]
        fn test_default_bkt_non_existent_key_returns_none() {
            let (mut cache, _tmp) = create_cache(10);

            assert_eq!(cache.get(b"not-here").unwrap(), None);
            assert_eq!(cache.del(b"ghost").unwrap(), None);
        }

        #[test]
        fn test_default_bkt_stress_test_with_many_inserts_and_deletes() {
            let (mut cache, _tmp) = create_cache(50);
            let total = 500;

            for i in 0..total {
                let k = format!("k{}", i).into_bytes();
                let v = format!("v{}", i).into_bytes();

                assert!(cache.set(&k, &v).is_ok());
            }

            // delete half
            for i in 0..(total / 2) {
                let k = format!("k{}", i).into_bytes();
                assert!(cache.del(&k).is_ok());
            }

            for i in 0..total {
                let k = format!("k{}", i).into_bytes();
                let got = cache.get(&k).unwrap();

                if i < total / 2 {
                    assert!(got.is_none());
                } else {
                    assert_eq!(got, Some(format!("v{}", i).into_bytes()));
                }
            }
        }
    }

    mod turbo_custom_bucket {
        use super::*;

        #[test]
        fn test_creation_turbo_bucket_with_set_get_roundtrip() {
            let (mut cache, _tmp) = create_cache(10);
            let mut bucket = cache.bucket("custom", None);

            bucket.set(b"alpha", b"one").unwrap();
            assert_eq!(bucket.get(b"alpha").unwrap(), Some(b"one".to_vec()));
        }

        #[test]
        fn test_different_turbo_buckets_do_not_overlap() {
            let (mut cache, _tmp) = create_cache(10);

            {
                let mut a = cache.bucket("bucket-a", None);
                a.set(b"user", b"alice").unwrap();

                assert_eq!(a.get(b"user").unwrap(), Some(b"alice".to_vec()));
            }

            {
                let mut b = cache.bucket("bucket-b", None);
                b.set(b"user", b"bob").unwrap();

                assert_eq!(b.get(b"user").unwrap(), Some(b"bob".to_vec()));
            }
        }

        #[test]
        fn test_turbo_bucket_respects_custom_config() {
            let (mut cache, _tmp) = create_cache(10);

            let custom_cfg = BucketCfg::default().rows(128).growable(true);
            let mut bucket = cache.bucket("special", Some(custom_cfg));

            bucket.set(b"k", b"v").unwrap();
            assert_eq!(bucket.get(b"k").unwrap(), Some(b"v".to_vec()));

            let entry = cache.buckets.get("special").unwrap();
            assert_eq!(entry.cfg.rows, 128);
            assert_eq!(entry.cfg.growable, true);
        }

        #[test]
        fn test_roundtrip_in_turbo_bucket() {
            let (mut cache, _tmp) = create_cache(10);
            let mut bucket = cache.bucket("mybucket", None);

            let k = b"round".to_vec();
            let v = b"trip".to_vec();

            bucket.set(&k, &v).unwrap();
            assert_eq!(bucket.get(&k).unwrap(), Some(v.clone()));

            let deleted = bucket.del(&k).unwrap();
            assert_eq!(deleted, Some(v));
            assert_eq!(bucket.get(&k).unwrap(), None);
        }

        #[test]
        fn test_turbo_bucket_handles_binary_keys_and_values() {
            let (mut cache, _tmp) = create_cache(10);
            let mut bucket = cache.bucket("binbucket", None);

            let k = vec![0, 1, 2, 3, 255];
            let v = vec![42, 128, 0, 99];

            bucket.set(&k, &v).unwrap();
            assert_eq!(bucket.get(&k).unwrap(), Some(v));
        }

        #[test]
        fn test_multiple_turbo_buckets_work_together() {
            let (mut cache, _tmp) = create_cache(20);

            for i in 0..25 {
                let name = format!("bucket-{}", i % 5);
                let key = format!("k{}", i).into_bytes();
                let val = format!("v{}", i).into_bytes();

                let mut bucket = cache.bucket(Box::leak(name.into_boxed_str()), None);
                bucket.set(&key, &val).unwrap();
            }

            for i in 0..25 {
                let name = format!("bucket-{}", i % 5);
                let key = format!("k{}", i).into_bytes();
                let val = format!("v{}", i).into_bytes();

                let mut bucket = cache.bucket(Box::leak(name.into_boxed_str()), None);
                assert_eq!(bucket.get(&key).unwrap(), Some(val));
            }
        }

        #[test]
        fn test_inserted_count_is_tracked_per_bucket() {
            let (mut cache, _tmp) = create_cache(10);
            let mut users = cache.bucket("users", None);

            users.set(b"u1", b"a").unwrap();
            users.set(b"u2", b"b").unwrap();
            assert_eq!(users.pair_count().unwrap(), 2);

            users.del(b"u1").unwrap();
            assert_eq!(users.pair_count().unwrap(), 1);
        }
    }
}
