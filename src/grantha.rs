use crate::{
    error::InternalResult,
    kosh::{Key, KeyValue, Kosh, KoshConfig, Value, ROW_SIZE},
    BucketCfg,
};
use std::{
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
        cfg: &BucketCfg,
    ) -> InternalResult<Self> {
        let kosh = match Self::find_from_dir(dirpath.as_ref(), name)? {
            Some((path, cap)) => {
                let config = KoshConfig::new(path, name, cap);
                let k = Kosh::open(config)?;

                k
            }
            None => {
                let new_cap = Self::calc_new_cap(cfg.rows);
                let path = Self::create_file_path(dirpath.as_ref(), name, new_cap);
                let config = KoshConfig::new(path, name, new_cap);

                let k = Kosh::new(config)?;

                k
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
        let mut best: Option<(PathBuf, usize)> = None;

        for entry in fs::read_dir(dirpath)? {
            let entry = entry?;
            let file_type = entry.file_type()?;

            if !file_type.is_file() {
                continue;
            }

            let tmp = entry.file_name();
            let filename = match tmp.to_str() {
                Some(f) => f,
                None => continue,
            };

            if let Some(cap) = Self::extract_capacity(filename, name) {
                match &best {
                    Some((_, best_cap)) if *best_cap >= cap => {}
                    _ => best = Some((entry.path(), cap)),
                }
            }
        }

        Ok(best)
    }

    fn extract_capacity(filename: &str, name: &str) -> Option<usize> {
        if !filename.starts_with(name) {
            return None;
        }

        let (_, cap_str) = filename.rsplit_once('_')?;
        cap_str.parse::<usize>().ok()
    }

    #[inline(always)]
    fn create_file_path(dirpath: &Path, name: &str, cap: usize) -> PathBuf {
        dirpath.join(format!("{name}_{cap}"))
    }

    #[inline(always)]
    const fn calc_new_cap(rows: usize) -> usize {
        debug_assert!(
            (rows * ROW_SIZE) % 16 == 0,
            "Capacity must be multiple of 16"
        );

        rows * ROW_SIZE
    }
}

#[cfg(test)]
mod grantha_tests {
    use super::*;
    use crate::logger::init_test_logger;
    use tempfile::TempDir;

    #[ctor::ctor]
    fn init() {
        init_test_logger();
    }

    const TEST_ROWS: usize = 2;

    fn open_grantha(temp: &TempDir, rows: usize) -> Grantha {
        let dir = temp.path();
        let cfg = BucketCfg::default().rows(rows);

        Grantha::open(dir, "grantha_test", &cfg).expect("grantha open")
    }

    #[test]
    fn test_create_new_grantha_creates_file() {
        let tmp = TempDir::new().unwrap();

        let g = open_grantha(&tmp, TEST_ROWS);
        let entries: Vec<_> = fs::read_dir(tmp.path()).unwrap().collect();
        let fname = entries[0]
            .as_ref()
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();

        assert!(g.pair_count().unwrap() == 0);
        assert!(fname.starts_with("grantha_test_"));
        assert_eq!(
            entries.len(),
            1,
            "grantha should create exactly one patra file"
        );
    }

    #[test]
    fn test_open_existing_grantha_reuses_file() {
        let tmp = TempDir::new().unwrap();

        // open w/ init
        let mut g1 = open_grantha(&tmp, TEST_ROWS);
        g1.upsert((b"k".to_vec(), b"v".to_vec())).unwrap();
        drop(g1);

        // re-open should not re-init
        let cfg = BucketCfg::default().rows(TEST_ROWS * 2);
        let g2 = Grantha::open(tmp.path(), "grantha_test", &cfg).unwrap();

        assert_eq!(
            g2.pair_count().unwrap(),
            1,
            "existing data should persist if valid"
        );
    }

    #[test]
    fn test_upsert_fetch_yank_cycle() {
        let tmp = TempDir::new().unwrap();
        let mut g = open_grantha(&tmp, TEST_ROWS);

        let k = b"hello".to_vec();
        let v = b"world".to_vec();
        g.upsert((k.clone(), v.clone())).unwrap();

        assert_eq!(g.fetch(k.clone()).unwrap(), Some(v.clone()));
        assert_eq!(g.pair_count().unwrap(), 1);

        let removed = g.yank(k.clone()).unwrap();
        assert_eq!(removed, Some(v));
        assert_eq!(g.fetch(k).unwrap(), None);
    }

    #[test]
    fn test_capacity_mismatch_reinits() {
        let tmp = TempDir::new().unwrap();

        let custom_cap = Grantha::calc_new_cap(TEST_ROWS);
        let path = Grantha::create_file_path(tmp.path(), "grantha_test", custom_cap);

        // manually corrupt file
        let f = fs::File::create(&path).unwrap();
        f.set_len(8).unwrap();

        // re-init
        let cfg = BucketCfg::default().rows(TEST_ROWS);
        let g = Grantha::open(tmp.path(), "grantha_test", &cfg).unwrap();

        assert_eq!(
            g.pair_count().unwrap(),
            0,
            "should reinit on mismatch/corrupt"
        );
    }

    #[test]
    fn test_grantha_skips_non_utf8_filename() {
        let tmp = TempDir::new().unwrap();
        let badfile = tmp.path().join("invalid_\u{FFFD}");

        fs::File::create(&badfile).unwrap();
        let g = open_grantha(&tmp, TEST_ROWS);

        assert!(
            g.pair_count().is_ok(),
            "should not fail on non-utf8 filename"
        );
    }

    #[test]
    fn test_multiple_files_picks_correct_one() {
        let tmp = TempDir::new().unwrap();

        let p16 = Grantha::create_file_path(tmp.path(), "grantha_test", 16);
        let p64 = Grantha::create_file_path(tmp.path(), "grantha_test", 64);

        let cfg16 = KoshConfig {
            path: p16.clone(),
            name: "grantha_test",
            cap: 16,
        };
        let cfg64 = KoshConfig {
            path: p64.clone(),
            name: "grantha_test",
            cap: 64,
        };

        {
            let mut k16 = Kosh::new(cfg16).unwrap();
            k16.upsert((b"from16".to_vec(), b"v16".to_vec())).unwrap();
        }

        {
            let mut k64 = Kosh::new(cfg64).unwrap();
            k64.upsert((b"from64".to_vec(), b"v64".to_vec())).unwrap();
        }

        let cfg = BucketCfg::default().rows(TEST_ROWS);
        let g = Grantha::open(tmp.path(), "grantha_test", &cfg).unwrap();

        assert_eq!(g.fetch(b"from64".to_vec()).unwrap(), Some(b"v64".to_vec()));
        assert_eq!(g.fetch(b"from16".to_vec()).unwrap(), None);
    }

    #[test]
    fn test_calc_new_cap_is_multiple_of_row_size() {
        let rows = 64;
        let cap = Grantha::calc_new_cap(rows);

        assert_eq!(cap, rows * ROW_SIZE);
        assert_eq!(cap % ROW_SIZE, 0);
    }
}
