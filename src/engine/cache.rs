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

        let mut file = TurboFile::new(&path).map_err(|e| {
            cfg.logger
                .error(LogCtx::Cache, format!("Failed to create file due to err: {e}"));
            e
        })?;
        file.zero_extend(file_len).map_err(|e| {
            cfg.logger
                .error(LogCtx::Cache, format!("Failed to zero extend due to err: {e}"));

            // NOTE: This error states we were unable to correctly init `[TurboFile]`, so we must
            // `CLOSE + DELETE` the created file, so upon retry, the current process would work
            // without any issues

            // HACK: We ignore errors from CLOSE and DELETE, as we are already in the errored state!
            // The zero-extend error is more important and direct to throw outside, so we just ignore
            // these two errors (if any).

            file.close_delete(&path).map_err(|e| {
                cfg.logger.warn(
                    LogCtx::Cache,
                    format!("Failed to clear file after init failure due to err: {e}"),
                );
            });

            e
        })?;

        cfg.logger.trace(
            LogCtx::Cache,
            format!("Created new file w/ len={file_len} & pages={n_pages}"),
        );

        Ok(Self {
            file,
            n_pages,
            cfg: cfg.clone(),
        })
    }

    pub(super) fn open(cfg: &TurboConfig, page_size: usize) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);

        let file = TurboFile::open(&path).map_err(|e| {
            cfg.logger
                .error(LogCtx::Cache, format!("Failed to open file due to err: {e}"));
            e
        })?;
        let file_len = file.len().map_err(|e| {
            cfg.logger
                .error(LogCtx::Cache, format!("Failed to read file len due to err: {e}"));
            e
        })?;

        // stored data must always be correctly paged to page_size
        if file_len % page_size != 0 {
            let err = InternalError::InvalidDbState("Invalid db state due to misaligned stored data".into());
            cfg.logger
                .error(LogCtx::InvDb, format!("Missing crucial data in TurboFox: {err}"));
            return Err(err);
        }

        let n_pages = file_len / page_size;

        cfg.logger.trace(
            LogCtx::Cache,
            format!("Opened existing file w/ len={file_len} & pages={n_pages}"),
        );

        Ok(Self {
            file,
            n_pages,
            cfg: cfg.clone(),
        })
    }

    pub(super) fn write(&self) -> InternalResult<()> {
        self.file.write()
    }

    pub(super) fn read(&self) -> InternalResult<()> {
        self.file.read()
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        let mut is_err = false;

        self.file.flush().map_err(|e| {
            self.cfg
                .logger
                .warn(LogCtx::Cache, format!("Faile to save file on drop due to err: {e}"));
            is_err = true;
        });
        self.file.close().map_err(|e| {
            self.cfg
                .logger
                .warn(LogCtx::Cache, format!("Faile to close file on drop due to err: {e}"));
            is_err = true;
        });

        if !is_err {
            self.cfg.logger.trace(LogCtx::Cache, "Dropped");
        }
    }
}
