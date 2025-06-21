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

pub struct TurboCache {
    dirpath: PathBuf,
    shards: Vec<Shard>,
}

impl TurboCache {
    const MAX_SHARD: u32 = u16::MAX as u32 + 1;

    pub fn open(dirpath: impl AsRef<Path>) -> Res<Self> {
        let dir = dirpath.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let first_shard = Shard::open(&dir, 0, Self::MAX_SHARD)?;

        Ok(TurboCache {
            dirpath: dir,
            shards: vec![first_shard],
        })
    }

    pub fn get(&self, key: &[u8]) -> Res<Option<Buf>> {
        let sh = SimHash::new(key);

        for shard in self.shards.iter() {
            if sh.shard() < shard.end {
                return shard.get(sh, key);
            }
        }

        unreachable!()
    }

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

    pub fn remove(&mut self, key: &[u8]) -> Res<bool> {
        let sh = SimHash::new(key);

        for shard in self.shards.iter_mut() {
            if sh.shard() < shard.end {
                return shard.remove(sh, key);
            }
        }

        unreachable!()
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = Res<KV>> + 'a {
        self.shards.iter().flat_map(|shard| shard.iter())
    }

    fn split(&mut self, shard_idx: usize) -> Res<()> {
        let removed_shard = self.shards.remove(shard_idx);

        let start = removed_shard.start;
        let end = removed_shard.end;
        let mid = (start + end) / 2;
        println!("splitting [{start}, {end}) to [{start}, {mid}) and [{mid}, {end})");

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
}
