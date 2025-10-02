use crate::{
    error::{InternalError, InternalResult},
    hasher::Hasher,
    kosh::patra::{Patra, Sign, ROW_SIZE},
};
use std::path::Path;

pub(crate) use crate::kosh::patra::{Key, KeyValue, Value};

mod meta;
mod patra;
mod simd;

#[derive(Debug)]
pub(crate) struct Kosh {
    patra: Patra,
}

impl Kosh {
    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let patra = match Patra::open(&path, capacity) {
            Ok(f) => f,

            Err(InternalError::InvalidFile) => {
                // returns IO error if something goes wrong
                std::fs::remove_file(&path)?;

                // now we create a new bucket file
                //
                // NOTE: if the same or any error occurs again,
                // we simply throw it out!
                Patra::new(&path, capacity)?
            }

            Err(e) => return Err(e),
        };

        Ok(Self { patra })
    }

    #[inline(always)]
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        // sanity check
        debug_assert!(capacity % ROW_SIZE == 0, "Capacity must be multiple of 16");

        Ok(Self {
            patra: Patra::new(path, capacity)?,
        })
    }

    /// ## Errors
    ///
    /// - throws [InternalError::BucketFull] if slots are full (need to grow the bucket)
    /// - throws [InternalError::BucketOverflow] when bucket is full (can not be grown further)
    pub fn upsert(&mut self, kv: KeyValue) -> InternalResult<()> {
        let sign = Hasher::new(&kv.0);

        // threshold has reached, so pair can't be inserted
        if self.patra.is_full() {
            return Err(InternalError::BucketFull);
        }

        self.patra.upsert_kv(sign, kv)
    }

    pub fn fetch(&self, key: Key) -> InternalResult<Option<Value>> {
        let sign = Hasher::new(&key);
        self.patra.fetch_value(sign, key)
    }

    pub fn yank(&mut self, key: Key) -> InternalResult<Option<Value>> {
        let sign = Hasher::new(&key);
        self.patra.yank_key(sign, key)
    }

    #[inline(always)]
    pub fn pair_count(&self) -> InternalResult<usize> {
        Ok(self.patra.pair_count())
    }

    #[inline(always)]
    pub fn is_full(&self) -> InternalResult<bool> {
        Ok(self.patra.is_full())
    }
}

#[cfg(test)]
mod kosh_tests {
    use super::*;
    use tempfile::TempDir;

    const TEST_CAP: usize = ROW_SIZE * 2;

    fn open_kosh() -> Kosh {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("kosh_test");

        Kosh::new(path, TEST_CAP).expect("create kosh")
    }

    fn open_kosh_with_cap(cap: usize) -> Kosh {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("kosh_test");

        Kosh::new(path, cap).expect("create kosh")
    }

    #[test]
    fn test_upsert_fetch_and_yank_full_cycle() {
        let mut k = open_kosh();

        let key = b"hello".to_vec();
        let val = b"world".to_vec();
        k.upsert((key.clone(), val.clone())).unwrap();

        assert_eq!(k.fetch(key.clone()).unwrap(), Some(val.clone()));
        assert_eq!(k.pair_count().unwrap(), 1);

        let removed = k.yank(key.clone()).unwrap();

        assert_eq!(removed, Some(val));
        assert_eq!(k.pair_count().unwrap(), 0);
        assert_eq!(k.fetch(key).unwrap(), None);
    }

    #[test]
    fn test_upsert_on_existing_key_does_not_increase_pair_count() {
        let mut k = open_kosh();

        let key = b"dup".to_vec();
        let v1 = b"v1".to_vec();
        let v2 = b"v2".to_vec();

        k.upsert((key.clone(), v1.clone())).unwrap();
        assert_eq!(k.pair_count().unwrap(), 1);

        k.upsert((key.clone(), v2.clone())).unwrap();
        assert_eq!(k.pair_count().unwrap(), 1);
        assert_eq!(k.fetch(key).unwrap(), Some(v2));
    }

    #[test]
    fn test_yank_on_non_existent_key_returns_none() {
        let mut k = open_kosh();
        let removed = k.yank(b"ghost".to_vec()).unwrap();

        assert!(removed.is_none());
    }

    #[test]
    fn test_reuse_slot_works_for_upsert_after_previous_yank() {
        let mut k = open_kosh_with_cap(16);
        let k1 = b"k1".to_vec();
        let v1 = b"v1".to_vec();

        k.upsert((k1.clone(), v1)).unwrap();
        assert_eq!(k.pair_count().unwrap(), 1);

        let _ = k.yank(k1.clone()).unwrap();
        assert_eq!(k.pair_count().unwrap(), 0);

        let k2 = b"k2".to_vec();
        let v2 = b"v2".to_vec();
        k.upsert((k2.clone(), v2.clone())).unwrap();

        assert_eq!(k.fetch(k2).unwrap(), Some(v2));
        assert_eq!(k.pair_count().unwrap(), 1);
    }

    #[test]
    fn test_kosh_open_on_corrupt_patra_creates_new_and_reinits() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("kosh_corrupt");

        // custom corrupted file
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(8).unwrap();

        let k = Kosh::open(&path, TEST_CAP).unwrap();
        assert_eq!(k.pair_count().unwrap(), 0);
        assert_eq!(k.is_full().unwrap(), false);
    }

    #[test]
    fn test_kosh_open_on_capacity_mismatch_reinits_without_failure() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("kosh_cap");
        let _ = Kosh::new(&path, TEST_CAP).unwrap();

        assert!(
            Kosh::open(&path, TEST_CAP * 2).is_ok(),
            "invalid or diff cap creates invalid file, so we must re-init it"
        );
    }

    #[test]
    fn test_upsert_until_full_returns_bucket_full() {
        let mut k = open_kosh_with_cap(32);
        let mut inserted = 0usize;

        loop {
            let key = format!("key{}", inserted).into_bytes();
            let res = k.upsert((key.clone(), b"x".to_vec()));

            match res {
                Ok(_) => {
                    inserted += 1;
                }

                Err(e) => {
                    assert!(matches!(e, InternalError::BucketFull));
                    break;
                }
            }
        }

        assert!(k.is_full().unwrap());
    }
}
