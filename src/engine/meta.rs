use crate::{
    core::{TurboFile, TurboMMap},
    error::InternalResult,
    logger::Logger,
    TurboConfig,
};
use std::path::Path;

const VERSION: u32 = 0;
const MAGIC: [u8; 4] = *b"tbf0";
const PATH: &'static str = "meta";
const META_SIZE: usize = std::mem::size_of::<Meta>();

#[derive(Debug, Clone)]
#[repr(C)]
pub(super) struct Meta {
    _padd: u64,
    version: u32,
    magic: [u8; 4],
    pub(super) num_bufs: u64,
    pub(super) capacity: u64,
    pub(super) buf_size: u64,
    pub(super) max_klen: u64,
    pub(super) growth_x: u64,
    pub(super) init_cap: u64,
}

impl Meta {
    const fn new(cfg: &TurboConfig) -> Self {
        Self {
            _padd: 0,
            magic: MAGIC,
            version: VERSION,
            num_bufs: cfg.initial_capacity.to_u64() * cfg.growth_factor,
            capacity: cfg.initial_capacity.to_u64(),
            buf_size: cfg.buf_size.to_u64(),
            max_klen: cfg.max_key_len.to_u64(),
            growth_x: cfg.growth_factor,
            init_cap: cfg.initial_capacity.to_u64(),
        }
    }
}

const _: () = assert!(META_SIZE == 0x40);

pub(super) struct MetaFile {
    file: TurboFile,
    mmap: TurboMMap,
}

impl MetaFile {
    #[inline]
    pub(super) fn new(dirpath: &Path, cfg: &TurboConfig) -> InternalResult<Self> {
        let path = dirpath.join(PATH);

        let file = TurboFile::new(&path)?;
        file.zero_extend(META_SIZE)?;

        let mmap = TurboMMap::new(file.fd(), META_SIZE, 0)?;

        let meta = Meta::new(cfg);
        mmap.write(&meta, 0);

        mmap.flush()?;

        Ok(Self { file, mmap })
    }

    #[inline]
    pub(super) fn open(dirpath: &Path, cfg: &TurboConfig) -> InternalResult<Self> {
        let path = dirpath.join(PATH);

        let file = TurboFile::open(&path)?;
        let mmap = TurboMMap::new(file.fd(), META_SIZE, 0)?;

        Ok(Self { file, mmap })
    }

    #[inline]
    pub(super) fn meta(&self) -> *mut Meta {
        self.mmap.read::<Meta>(0)
    }

    #[inline]
    pub(super) fn flush(&self) -> InternalResult<()> {
        self.mmap.flush()
    }
}
