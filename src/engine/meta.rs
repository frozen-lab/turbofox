use crate::{
    core::{TurboFile, TurboMMap},
    error::InternalResult,
    logger::Logger,
    TurboConfig,
};
use std::path::PathBuf;

const VERSION: u32 = 0;
const MAGIC: [u8; 4] = *b"tbf0";
const PATH: &'static str = "meta";
const META_SIZE: usize = std::mem::size_of::<Meta>();

#[derive(Debug, Clone)]
#[repr(C)]
pub(crate) struct Meta {
    version: u32,
    magic: [u8; 4],
    _padd: [u8; 0x18],
    pub(crate) num_bufs: u64,
    pub(crate) capacity: u64,
    pub(crate) buf_size: u64,
    pub(crate) max_klen: u64,
}

impl Meta {
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

pub(crate) struct MetaFile {
    file: TurboFile,
    mmap: TurboMMap,
}

impl MetaFile {
    #[inline]
    pub(crate) fn exists(dirpath: &PathBuf) -> bool {
        dirpath.join(PATH).exists()
    }

    #[inline]
    pub(crate) fn new(dirpath: &PathBuf, cfg: &TurboConfig) -> InternalResult<Self> {
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
    pub(crate) fn open(dirpath: &PathBuf, cfg: &TurboConfig) -> InternalResult<Self> {
        let path = dirpath.join(PATH);

        let file = TurboFile::open(&path)?;
        let mmap = TurboMMap::new(file.fd(), META_SIZE, 0)?;

        Ok(Self { file, mmap })
    }

    #[inline]
    pub(crate) fn meta(&self) -> *mut Meta {
        self.mmap.read::<Meta>(0)
    }

    #[inline]
    pub(crate) fn flush(&self) -> InternalResult<()> {
        self.mmap.flush()
    }
}
