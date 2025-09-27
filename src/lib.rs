use crate::{error::TurboResult, router::Router};
use std::path::Path;

mod bucket;
mod error;
mod hasher;
mod logger;
mod router;

pub struct TurboCache {
    router: Router,
}

impl TurboCache {
    pub fn new<P: AsRef<Path>>(
        dirpath: P,
        name: &'static str,
        capacity: usize,
    ) -> TurboResult<Self> {
        // make sure the dir exists
        std::fs::create_dir_all(&dirpath)?;

        let router = Router::open(dirpath, name, capacity)?;
        Ok(Self { router })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> TurboResult<bool> {
        let pair = (key.to_vec(), value.to_vec());
        Ok(self.router.set(pair)?)
    }

    pub fn get(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        Ok(self.router.get(key.to_vec())?)
    }

    pub fn del(&mut self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        Ok(self.router.del(key.to_vec())?)
    }

    pub fn get_inserted_count(&self) -> TurboResult<usize> {
        Ok(self.router.get_insert_count()?)
    }
}

#[cfg(test)]
mod turbo_tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache(capacity: usize) -> TurboCache {
        let tmp = TempDir::new().unwrap();
        TurboCache::new(tmp.path().to_path_buf(), "test", capacity).unwrap()
    }

    #[test]
    fn insert_and_get() {
        let mut cache = create_cache(10);
        let key = b"foo".to_vec();
        let value = b"bar".to_vec();

        cache.set(&key, &value).unwrap();
        assert_eq!(cache.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn overwrite_value() {
        let mut cache = create_cache(10);
        let key = b"k1".to_vec();

        cache.set(&key, b"v1").unwrap();
        cache.set(&key, b"v2").unwrap();

        assert_eq!(cache.get(&key).unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_and_exists() {
        let mut cache = create_cache(10);
        let key = b"hello".to_vec();

        cache.set(&key, b"world").unwrap();
        assert!(cache.get(&key).unwrap().is_some());

        cache.del(&key).unwrap();
        assert!(cache.get(&key).unwrap().is_none());
        assert_eq!(cache.get(&key).unwrap(), None);
    }

    #[test]
    fn total_count_reflects_inserts_and_deletes() {
        let mut cache = create_cache(10);

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
        let mut cache = create_cache(10);
        assert_eq!(cache.get(b"nope").unwrap(), None);
    }
}
