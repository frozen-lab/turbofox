#![allow(dead_code)]

use router::{Router, RouterIterator};
use std::{
    path::Path,
    sync::{Arc, RwLock},
};
use types::InternalConfig;

mod bucket;
mod common;
mod hasher;
mod index;
mod router;
mod types;

pub use crate::types::{TurboError, TurboResult};

pub struct TurboCache {
    router: Arc<RwLock<Router>>,
}

impl TurboCache {
    pub fn new<P: AsRef<Path>>(dirpath: P, initial_capacity: usize) -> TurboResult<Self> {
        let config = InternalConfig {
            initial_capacity,
            dirpath: dirpath.as_ref().to_path_buf(),
        };

        let router = Router::new(config)?;

        Ok(Self {
            router: Arc::new(RwLock::new(router)),
        })
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> TurboResult<()> {
        let mut write_lock = self.write_lock()?;

        let k = key.to_vec();
        let v = value.to_vec();

        write_lock.set((k, v))?;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let lock = self.read_lock()?;
        let res = lock.get(key.to_vec())?;

        Ok(res)
    }

    pub fn del(&self, key: &[u8]) -> TurboResult<Option<Vec<u8>>> {
        let mut write_lock = self.write_lock()?;
        let res = write_lock.del(key.to_vec())?;

        Ok(res)
    }

    /// Return an iterator over all KV pairs.
    /// Each `Item` is an `InternalResult<KVPair>`.
    /// The iterator keeps the router's read-lock until it is dropped.
    pub fn iter(&self) -> TurboResult<TurboCacheIter<'_>> {
        let router_guard = self.read_lock()?;
        let router_iter = router_guard.iter()?;

        Ok(TurboCacheIter {
            _guard: router_guard,
            iter: router_iter,
        })
    }

    pub fn total_count(&self) -> TurboResult<usize> {
        let lock = self.read_lock()?;
        let count = lock.get_insert_count()?;

        Ok(count)
    }

    // Acquire the read lock while mapping a lock poison error into [TurboError]
    fn read_lock(&self) -> Result<std::sync::RwLockReadGuard<'_, Router>, TurboError> {
        Ok(self.router.read()?)
    }

    // Acquire the write lock while mapping a lock poison error into [TurboError]
    fn write_lock(&self) -> Result<std::sync::RwLockWriteGuard<'_, Router>, TurboError> {
        Ok(self.router.write()?)
    }
}

pub struct TurboCacheIter<'a> {
    _guard: std::sync::RwLockReadGuard<'a, Router>,
    iter: RouterIterator,
}

impl<'a> Iterator for TurboCacheIter<'a> {
    type Item = TurboResult<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(pair)) => return Some(Ok(pair)),

            Some(Err(e)) => {
                let err = TurboError::from(e);

                return Some(Err(err));
            }

            None => None,
        }
    }
}
