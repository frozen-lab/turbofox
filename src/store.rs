#![allow(dead_code)]

use crate::{
    hash::TurboHash,
    shard::{Buf, Result, Shard},
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub struct TurboCache {
    dir: PathBuf,
    shards: Arc<RwLock<Vec<Shard>>>,
}

impl TurboCache {
    pub fn open(dirpath: impl AsRef<Path>) -> Result<Self> {
        let dir = dirpath.as_ref().to_path_buf();

        if let Ok(metadata) = std::fs::metadata(&dir) {
            if !metadata.is_dir() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "Path exists but is not a directory",
                )
                .into());
            }
        } else {
            std::fs::create_dir_all(&dir)?;
        }

        let base_shard = Shard::open(dirpath, 0..(u16::MAX as u32 + 1))?;

        Ok(Self {
            dir,
            shards: Arc::new(RwLock::new(vec![base_shard])),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Buf>> {
        let th = TurboHash::new(key);
        let shards = self.shards.read().unwrap();

        for shard in shards.iter() {
            if th.shard() < shard.span.end {
                return shard.get(th, key);
            }
        }

        Ok(None)
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        let th = TurboHash::new(&key);

        loop {
            let mut shards = self.shards.write().unwrap();
            let mut split_index: usize = 0;

            for (i, shard) in shards.iter().enumerate() {
                if th.shard() < shard.span.end {
                    if shard.set(th, &key, &value)? {
                        return Ok(true);
                    }

                    split_index = i;
                    break;
                }
            }

            self.split_shard(&mut shards, split_index)?;
        }
    }

    pub fn remove(&self, key: &[u8]) -> Result<Option<Buf>> {
        let th = TurboHash::new(key);
        let mut shards = self.shards.write().unwrap();

        for shard in shards.iter_mut() {
            if th.shard() < shard.span.end {
                return shard.remove(th, key);
            }
        }

        Ok(None)
    }

    fn split_shard(&self, shards: &mut Vec<Shard>, idx: usize) -> Result<()> {
        let removed_shard = shards.remove(idx);

        let start = removed_shard.span.start;
        let end = removed_shard.span.end;
        let mid = end - start;

        let shard1 = Shard::open(&self.dir, start..mid)?;
        let shard2 = Shard::open(&self.dir, mid..end)?;

        for pair in removed_shard.iter() {
            let (key, value) = pair?;
            let th = TurboHash::new(&key);

            if th.shard() < mid {
                shard1.set(th, &key, &value)?;
            } else {
                shard2.set(th, &key, &value)?;
            }
        }

        // delete current shard
        std::fs::remove_file(self.dir.join(format!("{start}-{end}")))?;

        shards.push(shard1);
        shards.push(shard2);

        // sort shards for faster access
        shards.sort_by(|x, y| x.span.end.cmp(&y.span.end));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_open_and_empty_get() {
        let dir = tempdir().unwrap();
        let cache = TurboCache::open(dir.path()).unwrap();

        assert!(
            cache.get(b"missing").unwrap().is_none(),
            "`get()` should be `None` for newly created store",
        );
    }

    #[test]
    fn test_set_and_get() {
        let dir = tempdir().unwrap();
        let cache = TurboCache::open(dir.path()).unwrap();

        assert!(
            cache.set(b"foo", b"bar").unwrap(),
            "`set()` operation should work correctly for newly created store",
        );

        let v = cache.get(b"foo").unwrap().unwrap();
        assert_eq!(v, b"bar".to_vec(), "`get()` operation should fetch correct value");

        assert!(
            cache.set(b"foo", b"baz").unwrap(),
            "`set()` should be able to update value of pre-existing keys",
        );

        let v2 = cache.get(b"foo").unwrap().unwrap();
        assert_eq!(
            v2,
            b"baz".to_vec(),
            "`get()` should fetch correct value for update KV pair",
        );
    }

    #[test]
    fn test_remove() {
        let dir = tempdir().unwrap();
        let cache = TurboCache::open(dir.path()).unwrap();

        assert!(
            cache.set(b"key", b"value").unwrap(),
            "`set()` should work correctly for a new value",
        );
        assert_eq!(
            cache.get(b"key").unwrap(),
            Some(b"value".to_vec()),
            "`get()` should fetch correct value for existing key",
        );

        assert_eq!(
            cache.remove(b"key").unwrap(),
            Some(b"value".to_vec()),
            "`remove()` should correctly delete KV pair",
        );
        assert!(
            cache.get(b"key").unwrap().is_none(),
            "`get()` should return `None` for deleted KV pair",
        );
        assert!(
            cache.remove(b"key").unwrap().is_none(),
            "`remove()` should be `None` if referenced key is not found",
        );
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = tempdir().unwrap();

        // Open and set the value
        {
            let cache = TurboCache::open(dir.path()).unwrap();

            assert!(
                cache.set(b"persist", b"yes").unwrap(),
                "`set()` should work correctly for a newly created store",
            );
        }

        // Open and read the value
        {
            let cache2 = TurboCache::open(dir.path()).unwrap();
            let v = cache2.get(b"persist").unwrap();

            assert_eq!(
                v,
                Some(b"yes".to_vec()),
                "`get()` should fetch values correctly accross persisted sessions",
            );
        }
    }
}
