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

    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let patra = Patra::new(path, capacity)?;

        Ok(Self { patra })
    }

    /// ## Errors
    ///
    /// - throws [InternalError::BucketFull] if slots are full (need to grow the bucket)
    /// - throws [InternalError::BucketOverflow] when bucket is full (can not be grown further)
    pub fn set(&mut self, kv: KeyValue) -> InternalResult<()> {
        let sign = Hasher::new(&kv.0);

        // threshold has reached, so pair can't be inserted
        if self.patra.is_full()? {
            return Err(InternalError::BucketFull);
        }

        self.patra.upsert_kv(sign, kv)
    }
}
