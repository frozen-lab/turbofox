use crate::{
    error::InternalResult,
    kosh::{Key, KeyValue, Kosh, KoshConfig, Value},
};
use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub(crate) struct Grantha {
    kosh: Kosh,
}

impl Grantha {
    pub fn open(
        dirpath: impl AsRef<Path>,
        name: &'static str,
        new_cap: usize,
    ) -> InternalResult<Self> {
        let kosh = match Self::find_from_dir(dirpath.as_ref(), name)? {
            Some((path, cap)) => {
                let config = KoshConfig { path, name, cap };
                Kosh::open(config)?
            }

            None => {
                let path = Self::create_file_path(dirpath.as_ref(), name, new_cap);
                let config = KoshConfig {
                    path,
                    name,
                    cap: new_cap,
                };

                Kosh::new(config)?
            }
        };

        Ok(Self { kosh })
    }

    /// ## Errors
    ///
    /// - throws [InternalError::BucketFull] if slots are full (need to grow the bucket)
    /// - throws [InternalError::BucketOverflow] when bucket is full (can not be grown further)
    pub fn upsert(&mut self, kv: KeyValue) -> InternalResult<()> {
        self.kosh.upsert(kv)
    }

    pub fn fetch(&self, key: Key) -> InternalResult<Option<Value>> {
        self.kosh.fetch(key)
    }

    pub fn yank(&mut self, key: Key) -> InternalResult<Option<Value>> {
        self.kosh.yank(key)
    }

    #[inline(always)]
    pub fn pair_count(&self) -> InternalResult<usize> {
        Ok(self.kosh.pair_count()?)
    }

    #[inline(always)]
    pub fn is_full(&self) -> InternalResult<bool> {
        Ok(self.kosh.is_full()?)
    }

    fn find_from_dir(dirpath: &Path, name: &str) -> InternalResult<Option<(PathBuf, usize)>> {
        for entry in fs::read_dir(dirpath)? {
            let entry = entry?;
            let file_type = entry.file_type()?;

            if !file_type.is_file() {
                continue;
            }

            let filename = entry.file_name();

            let filename = match filename.to_str() {
                Some(f) => f,
                None => continue,
            };

            if let Some(cap) = Self::extract_capacity(filename, name) {
                return Ok(Some((entry.path(), cap)));
            }
        }

        Ok(None)
    }

    fn extract_capacity(filename: &str, name: &str) -> Option<usize> {
        if !filename.starts_with(name) {
            return None;
        }

        let (_, cap_str) = filename.rsplit_once('_')?;
        cap_str.parse::<usize>().ok()
    }

    fn create_file_path(dirpath: &Path, name: &str, cap: usize) -> PathBuf {
        dirpath.join(format!("{name}_{cap}"))
    }
}
