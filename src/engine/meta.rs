use crate::{
    core::{TurboFile, TurboMMap},
    error::InternalResult,
    logger::Logger,
    TurboConfig,
};
use std::{path::PathBuf, sync::Arc};

const PATH: &'static str = "metadata";
const META_SIZE: usize = std::mem::size_of::<InternalMeta>();

const VERSION: u32 = 0;
const MAGIC: [u8; 4] = *b"tbf0";

#[derive(Debug, Clone)]
#[repr(C)]
pub(in crate::engine) struct InternalMeta {
    version: u32,
    magic: [u8; 4],
    _padd: [u8; 0x18],
    pub(in crate::engine) num_bufs: u64,
    pub(in crate::engine) capacity: u64,
    pub(in crate::engine) buf_size: u64,
    pub(in crate::engine) max_klen: u64,
}

impl InternalMeta {
    const fn new(cfg: &TurboConfig) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            _padd: [0; 0x18],
            num_bufs: cfg.initial_capacity.to_u64() * cfg.growth_factor,
            capacity: cfg.initial_capacity.to_u64(),
            buf_size: cfg.buf_size.to_u64(),
            max_klen: cfg.max_key_len.to_u64(),
        }
    }
}

const _: () = assert!(META_SIZE == 0x40);

#[derive(Debug)]
pub(in crate::engine) struct Metadata {
    file: TurboFile,
    mmap: TurboMMap,
    logger: Arc<Logger>,
}

impl Metadata {
    #[inline]
    pub(in crate::engine) fn exists(dirpath: &PathBuf) -> bool {
        dirpath.join(PATH).exists()
    }

    #[inline]
    pub(in crate::engine) fn new(dirpath: &PathBuf, cfg: &TurboConfig, logger: Arc<Logger>) -> InternalResult<Self> {
        let path = dirpath.join(PATH);
        let meta = InternalMeta::new(cfg);

        let file = TurboFile::new(&path)?;
        file.zero_extend(META_SIZE)?;

        let mmap = TurboMMap::new(file.fd(), META_SIZE, 0)?;
        let slf = Self { file, mmap, logger };

        slf.with_mut(|m| *m = meta);
        Ok(slf)
    }

    #[inline]
    pub(in crate::engine) fn open(dirpath: &PathBuf, cfg: &TurboConfig, logger: Arc<Logger>) -> InternalResult<Self> {
        let path = dirpath.join(PATH);

        let file = TurboFile::open(&path)?;
        let mmap = TurboMMap::new(file.fd(), META_SIZE, 0)?;

        Ok(Self { file, mmap, logger })
    }

    #[inline]
    pub(in crate::engine) fn with<R>(&self, f: impl FnOnce(&InternalMeta) -> R) -> R {
        let view = self.mmap.read::<InternalMeta>(0);
        f(view.read())
    }

    #[inline]
    pub(in crate::engine) fn with_mut(&self, f: impl FnOnce(&mut InternalMeta)) {
        self.mmap.write::<InternalMeta>(0).write(f);
    }

    pub(in crate::engine) fn sync(&self) -> InternalResult<()> {
        self.mmap.sync()?;
        Ok(())
    }
}

impl Drop for Metadata {
    fn drop(&mut self) {
        let _ = self.mmap.unmap();
        let _ = self.file.close();
    }
}
