use super::InternalConfig;
use crate::{core::TurboFile, error::InternalResult, logger::Logger};
use std::{path::PathBuf, sync::Arc};

const PATH: &'static str = "cache";

#[derive(Debug)]
pub(super) struct Cache {
    cfg: Arc<InternalConfig>,
    logger: Arc<Logger>,
    file: TurboFile,
}

impl Cache {
    #[inline]
    pub(in crate::engine) fn exists(dirpath: &PathBuf) -> bool {
        dirpath.join(PATH).exists()
    }

    pub(in crate::engine) fn new(
        dirpath: &PathBuf,
        logger: Arc<Logger>,
        cfg: Arc<InternalConfig>,
    ) -> InternalResult<Self> {
        let path = dirpath.join(PATH);
        let init_len = cfg.meta.with(|meta| meta.buf_size * meta.num_bufs);

        let file = TurboFile::new(&path)?;
        file.zero_extend(init_len as usize)?;

        Ok(Self { cfg, file, logger })
    }

    pub(in crate::engine) fn open(
        dirpath: &PathBuf,
        logger: Arc<Logger>,
        cfg: Arc<InternalConfig>,
    ) -> InternalResult<Self> {
        let path = dirpath.join(PATH);
        let file = TurboFile::new(&path)?;
        let _len = file.len()?;

        Ok(Self { cfg, file, logger })
    }

    #[inline]
    pub(in crate::engine) fn flush(&self) -> InternalResult<()> {
        self.file.flush()
    }

    pub(in crate::engine) fn extend(&self, new_len: usize) -> InternalResult<()> {
        self.file.zero_extend(new_len)?;
        self.flush()?;

        Ok(())
    }

    pub(in crate::engine) fn write(&self, off: usize, buf: &[u8]) -> InternalResult<()> {
        self.file.write(off, buf)?;
        Ok(())
    }

    pub(in crate::engine) fn read(&self, off: usize, buf_size: usize) -> InternalResult<Vec<u8>> {
        let bytes = self.file.read(off, buf_size)?;
        Ok(bytes)
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        let _ = self.file.close();
    }
}
