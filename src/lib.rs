use hasher::TurboHasher;
use router::Router;
use std::path::PathBuf;

mod hasher;
mod router;
mod shard;

pub use shard::TResult;

pub struct TurboCache {
    router: Router,
}

impl TurboCache {
    pub fn new(dirpath: PathBuf) -> TResult<Self> {
        Ok(Self {
            router: Router::open(&dirpath)?,
        })
    }

    pub fn set(&self, kbuf: &[u8], vbuf: &[u8]) -> TResult<()> {
        let hash = TurboHasher::new(kbuf);

        self.router.set((kbuf, vbuf), hash)
    }

    pub fn get(&self, kbuf: &[u8]) -> TResult<Option<Vec<u8>>> {
        let hash = TurboHasher::new(kbuf);

        self.router.get(kbuf, hash)
    }

    pub fn remove(&self, kbuf: &[u8]) -> TResult<bool> {
        let hash = TurboHasher::new(kbuf);

        self.router.remove(kbuf, hash)
    }
}
