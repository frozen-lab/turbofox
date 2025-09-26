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
        if let Some((path, cap)) = Self::fetch_bucket_path(dirpath, name)? {
            return Ok(Self {
                bucket: Bucket::open(path, cap)?,
            });
        }

        let path = format!("{name}_{capacity}");

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
