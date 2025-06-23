//! A persistent, high-performance, disk-backed Key-Value store w/ a novel sharding algorithm.
//!
//! Example
//! ```rs
//! use core::str;
//! use tempfile::tempdir;
//! use turbocache::TurboCache;
//!
//! fn main() -> std::io::Result<()> {
//!     let dir = tempdir().unwrap();
//!     let mut db = TurboCache::open(dir.path())?;
//!
//!     println!("{:?}", db.get(b"mykey")?); // None
//!
//!     db.set(b"mykey", b"myval")?;
//!     println!("{:?}", db.get(b"mykey")?); // Some([109, 121, 118, 97, 108])
//!
//!     println!("{:?}", db.remove(b"mykey")?); // Some([109, 121, 118, 97, 108])
//!     println!("{:?}", db.remove(b"mykey")?); // None
//!
//!     println!("{:?}", db.get(b"mykey")?); // None
//!
//!     for i in 0..10 {
//!         db.set(&format!("mykey{i}").into_bytes(), &format!("myval{i}").into_bytes())?;
//!     }
//!
//!     for res in db.iter() {
//!         let (k, v) = res?;
//!         println!("{} = {}", str::from_utf8(&k).unwrap(), str::from_utf8(&v).unwrap());
//!     }
//!
//!     Ok(())
//! }
//! ```

use std::path::{Path, PathBuf};

use hash::SimHash;
use shard::Shard;

mod hash;
mod shard;

pub(crate) const WIDTH: usize = 512;
pub(crate) const ROWS: usize = 64;

pub(crate) type Res<T> = std::io::Result<T>;
pub(crate) type Buf = Vec<u8>;
pub(crate) type KV = (Buf, Buf);

/// The TurboCache object to create an instance of the db
pub struct TurboCache {
    dirpath: PathBuf,
    shards: Vec<Shard>,
}

impl TurboCache {
    const MAX_SHARD: u32 = u16::MAX as u32 + 1;

    /// Opens or creates a new candystore
    pub fn open(dirpath: impl AsRef<Path>) -> Res<Self> {
        let dir = dirpath.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let first_shard = Shard::open(&dir, 0, Self::MAX_SHARD)?;

        Ok(TurboCache {
            dirpath: dir,
            shards: vec![first_shard],
        })
    }

    /// Gets the value of a key from the store.
    ///
    /// If the key does not exist, `None` will be return. The data is fully-owned,
    /// no references are returned.
    pub fn get(&self, key: &[u8]) -> Res<Option<Buf>> {
        let sh = SimHash::new(key);

        for shard in self.shards.iter() {
            if sh.shard() < shard.end {
                return shard.get(sh, key);
            }
        }

        unreachable!()
    }

    /// Inserts a key-value pair, creating it or replacing an existing pair.
    pub fn set(&mut self, key: &[u8], val: &[u8]) -> Res<bool> {
        let ph = SimHash::new(key);

        loop {
            let mut shard_to_split = None;

            for (i, shard) in self.shards.iter_mut().enumerate() {
                if ph.shard() < shard.end {
                    if shard.set(ph, key, val)? {
                        return Ok(true);
                    }
                    shard_to_split = Some(i);
                    break;
                }
            }

            self.split(shard_to_split.unwrap())?;
        }
    }

    /// Remove a key-value pair.
    pub fn remove(&mut self, key: &[u8]) -> Res<bool> {
        let sh = SimHash::new(key);

        for shard in self.shards.iter_mut() {
            if sh.shard() < shard.end {
                return shard.remove(sh, key);
            }
        }

        unreachable!()
    }

    /// Iterate over all key-value pairs
    pub fn iter<'a>(&'a self) -> impl Iterator<Item = Res<KV>> + 'a {
        self.shards.iter().flat_map(|shard| shard.iter())
    }

    fn split(&mut self, shard_idx: usize) -> Res<()> {
        let removed_shard = self.shards.remove(shard_idx);

        let start = removed_shard.start;
        let end = removed_shard.end;
        let mid = (start + end) / 2;

        let top = Shard::open(&self.dirpath, start, mid)?;
        let bottom = Shard::open(&self.dirpath, mid, end)?;

        for res in removed_shard.iter() {
            let (key, val) = res?;
            let ph = SimHash::new(&key);

            if ph.shard() < mid {
                bottom.set(ph, &key, &val)?;
            } else {
                top.set(ph, &key, &val)?;
            }
        }

        std::fs::remove_file(self.dirpath.join(format!("{start}-{end}")))?;

        self.shards.push(bottom);
        self.shards.push(top);
        self.shards.sort_by(|x, y| x.end.cmp(&y.end));

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
        let mut cache = TurboCache::open(dir.path()).unwrap();

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
        let mut cache = TurboCache::open(dir.path()).unwrap();

        assert!(
            cache.set(b"key", b"value").unwrap(),
            "`set()` should work correctly for a new value",
        );
        assert_eq!(
            cache.get(b"key").unwrap(),
            Some(b"value".to_vec()),
            "`get()` should fetch correct value for existing key",
        );

        assert!(
            cache.remove(b"key").unwrap(),
            "`remove()` should correctly delete KV pair",
        );
        assert!(
            cache.get(b"key").unwrap().is_none(),
            "`get()` should return `None` for deleted KV pair",
        );
        assert!(
            !cache.remove(b"key").unwrap(),
            "`remove()` should be `false` if referenced key is not found",
        );
    }

    #[test]
    fn test_iter_over_entries() {
        let dir = tempdir().unwrap();
        let mut cache = TurboCache::open(dir.path()).unwrap();

        let entries = vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        for (k, v) in &entries {
            assert!(cache.set(*k, *v).unwrap());
        }

        let mut all: Vec<_> = cache.iter().map(|r| r.unwrap()).collect();
        let mut expected: Vec<_> = entries.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();

        all.sort();
        expected.sort();

        assert_eq!(all, expected, "`iter()` should correctly return KV pairs");
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = tempdir().unwrap();

        // Open and set the value
        {
            let mut cache = TurboCache::open(dir.path()).unwrap();

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
