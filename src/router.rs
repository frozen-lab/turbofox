use std::path::Path;

use crate::{
    bucket::{Bucket, Key, KeyValue},
    error::InternalResult,
};

#[derive(Debug)]
pub(crate) struct Router {
    bucket: Bucket,
}

impl Router {
    pub fn open<P: AsRef<Path>>(
        dirpath: P,
        name: &'static str,
        capacity: usize,
    ) -> InternalResult<Self> {
        // open existing
        if let Some((path, cap)) = Self::fetch_bucket_path(&dirpath, name)? {
            return Ok(Self {
                bucket: Bucket::open(dirpath.as_ref().join(path), cap)?,
            });
        }

        let path = dirpath.as_ref().join(format!("{name}_{capacity}"));

        Ok(Self {
            bucket: Bucket::new(path, capacity)?,
        })
    }

    pub fn get_insert_count(&self) -> InternalResult<usize> {
        self.bucket.get_inserted_count()
    }

    pub fn set(&mut self, pair: KeyValue) -> InternalResult<()> {
        self.bucket.set(pair)
    }

    pub fn get(&mut self, key: Key) -> InternalResult<Option<Vec<u8>>> {
        self.bucket.get(key)
    }

    pub fn del(&mut self, key: Key) -> InternalResult<Option<Vec<u8>>> {
        self.bucket.del(key)
    }

    fn fetch_bucket_path<P: AsRef<Path>>(
        dirpath: P,
        name: &'static str,
    ) -> InternalResult<Option<(String, usize)>> {
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

            if filename.starts_with(name) {
                let Some(cap_str) = filename.split('_').last() else {
                    continue;
                };

                let Ok(cap) = cap_str.parse::<usize>() else {
                    continue;
                };

                return Ok(Some((filename.to_string(), cap)));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_open_new_bucket() {
        let dir = tempdir().unwrap();
        let name = "testbucket";
        let capacity = 128;

        let _router = Router::open(dir.path(), name, capacity).unwrap();

        // check file created with correct name
        let expected = dir.path().join(format!("{name}_{capacity}"));
        assert!(expected.exists(), "Bucket file should exist");
    }

    #[test]
    fn test_reopen_existing_bucket() {
        let dir = tempdir().unwrap();
        let name = "testbucket";
        let capacity = 256;

        // create once
        let router1 = Router::open(dir.path(), name, capacity).unwrap();
        drop(router1);

        // open again -> should reuse existing
        let router2 = Router::open(dir.path(), name, capacity).unwrap();
        drop(router2);

        let expected = dir.path().join(format!("{name}_{capacity}"));
        assert!(expected.exists(), "Existing bucket file should still exist");
    }

    #[test]
    fn test_fetch_bucket_path_none() {
        let dir = tempdir().unwrap();
        let res = Router::fetch_bucket_path(dir.path(), "nonexistent").unwrap();

        assert!(res.is_none());
    }

    #[test]
    fn test_fetch_bucket_path_valid() {
        let dir = tempdir().unwrap();
        let name = "bucketx";
        let cap = 64;
        let filename = format!("{name}_{cap}");
        let filepath = dir.path().join(&filename);

        fs::write(&filepath, b"dummy").unwrap();

        let res = Router::fetch_bucket_path(dir.path(), name).unwrap();
        assert!(res.is_some());

        let (found, found_cap) = res.unwrap();
        assert_eq!(found, filename);
        assert_eq!(found_cap, cap);
    }

    #[test]
    fn test_set_and_get() {
        let dir = tempdir().unwrap();
        let mut router = Router::open(dir.path(), "kvbucket", 64).unwrap();

        let key = Key::from("foo");
        let value = b"bar".to_vec();

        let inserted = router.set((key.clone(), value.clone()));
        assert!(
            inserted.is_ok(),
            "Set should not return `Err()` on first insert"
        );

        let retrieved = router.get(key.clone()).unwrap();
        assert_eq!(retrieved, Some(value));
    }

    #[test]
    fn test_del() {
        let dir = tempdir().unwrap();
        let mut router = Router::open(dir.path(), "delbucket", 64).unwrap();

        let key = Key::from("dead");
        let value = b"beef".to_vec();
        router.set((key.clone(), value.clone())).unwrap();

        let deleted = router.del(key.clone()).unwrap();
        assert_eq!(deleted, Some(value), "Del should return stored value");

        let check = router.get(key).unwrap();
        assert!(check.is_none(), "Key should be gone after delete");
    }

    #[test]
    fn test_get_insert_count() {
        let dir = tempdir().unwrap();
        let mut router = Router::open(dir.path(), "countbucket", 64).unwrap();

        assert_eq!(router.get_insert_count().unwrap(), 0);

        router.set((Key::from("a"), b"1".to_vec())).unwrap();
        router.set((Key::from("b"), b"2".to_vec())).unwrap();

        assert_eq!(router.get_insert_count().unwrap(), 2);
    }

    #[test]
    fn test_set_fails_when_bucket_full() {
        let dir = tempdir().unwrap();
        let mut router = Router::open(dir.path(), "fullbucket", 8).unwrap();

        let threshold = router.bucket.get_threshold().unwrap();

        // Fill until threshold
        for i in 0..threshold {
            let key = Key::from(format!("key{i}"));
            let value = format!("val{i}").into_bytes();

            let inserted = router.set((key, value));
            assert!(inserted.is_ok());
        }

        // Next insert should error
        let key = Key::from("overflow");
        let value = b"oops".to_vec();
        let res = router.set((key, value));

        assert!(res.is_err(), "Expected error when bucket is full");
    }
}
