use crate::{
    error::{InternalError, InternalResult},
    hasher::Hasher,
    kosh::patra::{Key, KeyValue, Patra, Sign, Value},
};
use std::path::Path;

mod meta;
mod patra;
mod simd;

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
