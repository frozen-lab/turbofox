//! `Router` is the central component for directing db operations to the correct shard.
use crate::{
    hasher::TurboHasher,
    shard::{Shard, TError, TResult},
};
use std::{path::PathBuf, sync::Mutex};

/// The `Router` manages a collection of shards and routes database operations
/// to the correct one based on the key's hash.
pub(crate) struct Router {
    dirpath: PathBuf,
    shards: Mutex<Vec<Shard>>,
}

impl Router {
    pub(crate) const END_OF_SHARDS: u32 = 1u32 << 16;

    /// Opens the database at the specified directory, loading existing shards or
    /// creating a new one if none are found.
    ///
    /// NOTE: This function will scan the directory for shard files, clean up any temporary
    /// or merge-related files, and initialize the `Router` with the loaded shards.
    pub fn open(dirpath: &PathBuf) -> TResult<Self> {
        let shards = Self::load(dirpath)?;

        if shards.len() == 0 {
            let shard = Shard::open(&dirpath, 0..(u16::MAX as u32 + 1), true)?;

            return Ok(Self {
                dirpath: dirpath.clone(),
                shards: Mutex::new(vec![shard]),
            });
        }

        Ok(Self {
            shards: Mutex::new(shards),
            dirpath: dirpath.clone(),
        })
    }

    /// Loads all valid shards from the specified directory.
    fn load(dirpath: &PathBuf) -> TResult<Vec<Shard>> {
        let mut found_shards: Vec<Shard> = vec![];

        for res in std::fs::read_dir(&dirpath)? {
            let entry = res?;
            let filename = entry.file_name();

            let Some(filename) = filename.to_str() else {
                continue;
            };

            let Ok(filetype) = entry.file_type() else {
                continue;
            };

            if !filetype.is_file() {
                continue;
            }

            if filename.starts_with("bottom_")
                || filename.starts_with("top_")
                || filename.starts_with("merge_")
            {
                std::fs::remove_file(entry.path())?;

                continue;
            } else if !filename.starts_with("shard_") {
                continue;
            }

            let Some((_, span)) = filename.split_once("_") else {
                continue;
            };

            let Some((start, end)) = span.split_once("-") else {
                continue;
            };

            let start = u32::from_str_radix(start, 16).expect(filename);
            let end = u32::from_str_radix(end, 16).expect(filename);
            let range = start..end;

            if start >= end || end > Self::END_OF_SHARDS {
                // NOTE: Invalid shard
                continue;
            }

            found_shards.push(Shard::open(&dirpath, range, false)?);
        }

        Ok(found_shards)
    }

    /// Sets a key-value pair in the appropriate shard.
    ///
    /// NOTE: The shard is determined by the `shard_selector` of the provided `hash`.
    pub fn set(&self, buf: (&[u8], &[u8]), hash: TurboHasher) -> TResult<()> {
        let s = hash.shard_selector();
        let mut shards = self.shards.lock().unwrap();

        // find the shard that contains this selector
        for i in 0..shards.len() {
            if shards[i].span.contains(&s) {
                // try inserting
                match shards[i].set(buf, hash) {
                    Ok(()) => return Ok(()),
                    Err(TError::RowFull(_)) => {
                        // take current shard by replacing it w/ a dummy one
                        let old = std::mem::replace(
                            &mut shards[i],
                            Shard::open(&self.dirpath, 0..0, true)?,
                        );
                        let (left, right) = old.split(&self.dirpath)?;

                        // remove the dummy placeholder
                        shards.remove(i);

                        // insert the two new shards in its place
                        shards.insert(i, right);
                        shards.insert(i, left);

                        return self.set(buf, hash);
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        // no shard covered this selector
        Err(TError::ShardOutOfRange(s))
    }

    /// Retrieves a value by its key from the appropriate shard.
    pub fn get(&self, buf: &[u8], hash: TurboHasher) -> TResult<Option<Vec<u8>>> {
        let s = hash.shard_selector();
        let shards = self.shards.lock().unwrap();

        for i in 0..shards.len() {
            if shards[i].span.contains(&s) {
                return shards[i].get(buf, hash);
            }
        }

        // if we ran out of room in this row
        Err(TError::ShardOutOfRange(s))
    }

    /// Removes a key-value pair from the appropriate shard.
    pub fn remove(&self, buf: &[u8], hash: TurboHasher) -> TResult<bool> {
        let s = hash.shard_selector();
        let shards = self.shards.lock().unwrap();

        for i in 0..shards.len() {
            if shards[i].span.contains(&s) {
                return shards[i].remove(buf, hash);
            }
        }

        // if we ran out of room in this row
        Err(TError::ShardOutOfRange(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn new_router() -> TResult<(Router, TempDir)> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;
        let router = Router::open(&dir)?;

        Ok((router, tmp))
    }

    #[test]
    fn set_get_remove_via_router() {
        let (router, _tmp) = new_router().unwrap();
        let key = b"key1";
        let val = b"val1".to_vec();
        let h = TurboHasher::new(key);

        // not present yet
        assert_eq!(router.get(key, h).unwrap(), None);

        // set and get
        router.set((key, &val), h).unwrap();

        assert_eq!(router.get(key, h).unwrap(), Some(val));

        // remove and gone
        assert!(router.remove(key, h).unwrap());
        assert_eq!(router.get(key, h).unwrap(), None);

        // removing again returns false
        assert!(!router.remove(key, h).unwrap());
    }

    #[test]
    fn persistence_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir).unwrap();

        {
            let router = Router::open(&dir).unwrap();
            let key = b"persist";
            let val = b"data".to_vec();
            let h = TurboHasher::new(key);

            router.set((key, &val), h).unwrap();
        }

        let router2 = Router::open(&dir).unwrap();
        let key = b"persist";
        let h2 = TurboHasher::new(key);

        assert_eq!(router2.get(key, h2).unwrap(), Some(b"data".to_vec()));
    }

    #[test]
    fn out_of_range_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&dir).unwrap();

        let s = Shard::open(&dir, 0..1, true).unwrap();
        let router = Router {
            shards: Mutex::new(vec![s]),
            dirpath: dir,
        };
        let key = b"x";
        let fake = TurboHasher::new(key);
        let res = router.get(key, fake);

        assert!(matches!(res, Err(TError::ShardOutOfRange(_))));
    }
}
