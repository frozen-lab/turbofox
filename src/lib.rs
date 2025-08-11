#![allow(dead_code)]

use router::Router;
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

    // Acquire the read lock while mapping a lock poison error into [TurboError]
    fn read_lock(&self) -> Result<std::sync::RwLockReadGuard<'_, Router>, TurboError> {
        Ok(self.router.read()?)
    }

    // Acquire the write lock while mapping a lock poison error into [TurboError]
    fn write_lock(&self) -> Result<std::sync::RwLockWriteGuard<'_, Router>, TurboError> {
        Ok(self.router.write()?)
    }
}
