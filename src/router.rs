use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::{bucket::Bucket, error::InternalResult};

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

        let router = Router::open(dir.path(), name, capacity).unwrap();

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
}
