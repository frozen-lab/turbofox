mod bucket;
mod core;
mod hash;
mod router;

use core::TurboConfig;
use router::Router;
use std::path::Path;

pub use core::{TurboError, TurboResult};

pub struct TurboCache<P: AsRef<Path>> {
    router: Router<P>,
}

impl<P: AsRef<Path>> TurboCache<P> {
    pub fn new(dirpath: P, initial_capacity: usize) -> TurboResult<Self> {
        let config = TurboConfig {
            initial_capacity,
            dirpath,
        };

        let router = Router::new(config)?;

        Ok(Self { router })
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> TurboResult<()> {
        self.router.set((key, value))
    }

    pub fn get(&self, key: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        self.router.get(key)
    }

    pub fn del(&mut self, key: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        self.router.del(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = TurboResult<(Vec<u8>, Vec<u8>)>> + '_ {
        self.router.iter()
    }
}

#[cfg(test)]
mod tests {}
