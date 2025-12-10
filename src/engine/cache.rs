use crate::{
    cfg::{TurboConfig, PAGE_MULT_FACTOR},
    error::{InternalError, InternalResult},
    file::TurboFile,
    logger::{LogCtx, Logger},
};

pub(super) const PATH: &'static str = "cache";

#[derive(Debug)]
pub(super) struct Cache {
    cfg: TurboConfig,
    file: TurboFile,
    n_pages: usize,
}

impl Cache {
    pub(super) fn new(cfg: &TurboConfig) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);
        let n_pages = cfg.init_cap * PAGE_MULT_FACTOR;
        let file_len = n_pages * cfg.page_size;

        let file = TurboFile::new(&path)?;
        file.zero_extend(file_len)?;

        Ok(Self {
            file,
            n_pages,
            cfg: cfg.clone(),
        })
    }

    pub(super) fn open(cfg: &TurboConfig, page_size: usize) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);

        let file = TurboFile::open(&path)?;
        let file_len = file.len()?;

        // stored data must always be correctly paged to page_size
        if file_len % page_size != 0 {
            let err = InternalError::InvalidDbState("Invalid db state due to misaligned stored data".into());
            cfg.logger
                .error(LogCtx::InvDb, format!("Missing crucial data in TurboFox: {err}"));
            return Err(err);
        }

        let n_pages = file_len / page_size;

        Ok(Self {
            file,
            n_pages,
            cfg: cfg.clone(),
        })
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        let _ = self.file.flush();
        let _ = self.file.close();
    }
}
