use super::InternalConfig;
use crate::{core::TurboFile, error::InternalResult};
use std::{path::PathBuf, sync::Arc};

const PATH: &'static str = "cache";

#[derive(Debug)]
pub(super) struct Cache {
    cfg: Arc<InternalConfig>,
    file: TurboFile,
}

impl Cache {
    #[inline]
    pub(in crate::engine) fn exists(cfg: &InternalConfig) -> bool {
        cfg.dirpath.join(PATH).exists()
    }

    pub(in crate::engine) fn new(cfg: Arc<InternalConfig>) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);
        let init_len = cfg.meta.with(|meta| meta.buf_size * meta.num_bufs);

        let file = TurboFile::new(&path)?;
        file.zero_extend(init_len as usize)?;

        Ok(Self { cfg, file })
    }

    pub(in crate::engine) fn open(cfg: Arc<InternalConfig>) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);
        let file = TurboFile::new(&path)?;

        Ok(Self { cfg, file })
    }
}
