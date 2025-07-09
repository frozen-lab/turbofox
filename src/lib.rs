mod hasher;
mod router;
mod shard;

use hasher::TurboHasher;
use router::Router;
pub use shard::{Error, TResult};
use std::path::PathBuf;

pub struct TurboCache {
    router: Router,
}

impl TurboCache {
    pub fn new(dirpath: PathBuf) -> TResult<Self> {
        Ok(Self {
            router: Router::open(&dirpath)?,
        })
    }

    pub fn set(&self, kbuf: &[u8], vbuf: &[u8]) -> TResult<()> {
        let hash = TurboHasher::new(kbuf);

        self.router.set((kbuf, vbuf), hash)
    }

    pub fn get(&self, kbuf: &[u8]) -> TResult<Option<Vec<u8>>> {
        let hash = TurboHasher::new(kbuf);

        self.router.get(kbuf, hash)
    }

    pub fn remove(&self, kbuf: &[u8]) -> TResult<bool> {
        let hash = TurboHasher::new(kbuf);

        self.router.remove(kbuf, hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache() -> (TurboCache, TempDir) {
        let tmp = TempDir::new().expect("create tempdir");
        let cache = TurboCache::new(tmp.path().to_path_buf()).expect("init cache");

        (cache, tmp)
    }

    #[test]
    fn set_and_get_returns_correct_value() -> TResult<()> {
        let (cache, _) = create_cache();

        let key = b"foo";
        let val = b"bar";

        cache.set(key, val)?;
        let fetched = cache.get(key)?;

        assert_eq!(fetched, Some(val.to_vec()));

        Ok(())
    }

    #[test]
    fn get_returns_none_for_missing_key() -> TResult<()> {
        let (cache, _) = create_cache();
        let key = b"no_such_key";
        let fetched = cache.get(key)?;

        assert_eq!(fetched, None);

        Ok(())
    }

    #[test]
    fn overwrite_value_for_key() -> TResult<()> {
        let (cache, _) = create_cache();

        let key = b"hello";
        let val1 = b"world";
        let val2 = b"world_2";

        cache.set(key, val1)?;
        assert_eq!(cache.get(key)?, Some(val1.to_vec()));

        cache.set(key, val2)?;
        assert_eq!(cache.get(key)?, Some(val2.to_vec()));

        Ok(())
    }

    #[test]
    fn remove_existing_key_returns_true() -> TResult<()> {
        let (cache, _) = create_cache();

        let key = b"remove-me";
        let val = b"soon";

        cache.set(key, val)?;
        assert_eq!(cache.get(key)?, Some(val.to_vec()));

        let removed = cache.remove(key)?;
        assert!(removed);

        let get_again = cache.get(key)?;
        assert_eq!(get_again, None);

        Ok(())
    }

    #[test]
    fn remove_nonexistent_key_returns_false() -> TResult<()> {
        let (cache, _) = create_cache();
        let removed = cache.remove(b"nope")?;

        assert!(!removed);
        Ok(())
    }

    #[test]
    fn data_persists_between_instances() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        {
            let cache = TurboCache::new(dir.clone())?;
            cache.set(b"persistent", b"data")?;
        }

        {
            let new_cache = TurboCache::new(dir.clone())?;
            let value = new_cache.get(b"persistent")?;

            assert_eq!(value, Some(b"data".to_vec()));
        }

        Ok(())
    }

    #[test]
    fn multiple_keys_can_be_stored() -> TResult<()> {
        let (cache, _) = create_cache();

        for i in 0..100 {
            let key = format!("key_{i}").into_bytes();
            let val = format!("val_{i}").into_bytes();

            cache.set(&key, &val)?;
        }

        for i in 0..100 {
            let key = format!("key_{i}").into_bytes();
            let expected = format!("val_{i}").into_bytes();
            let fetched = cache.get(&key)?;

            assert_eq!(fetched, Some(expected));
        }

        Ok(())
    }
}
