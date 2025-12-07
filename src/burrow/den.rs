use crate::{
    burrow::DEFAULT_PAGE_SIZE,
    core::TurboFile,
    errors::{InternalError, InternalResult},
    TurboConfig,
};

const PATH: &'static str = "den";

#[derive(Debug)]
pub(super) struct Den {
    file: TurboFile,
    cfg: TurboConfig,
    len: usize,
}

impl Den {
    pub(super) fn new(cfg: &TurboConfig) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);
        let new_file_len = cfg.init_cap * DEFAULT_PAGE_SIZE;

        // new file
        let file = TurboFile::new(&cfg, PATH)?;
        file.zero_extend(new_file_len, true)?;

        cfg.logger.debug("(Den) [new] Created new Den");

        Ok(Self {
            file,
            cfg: cfg.clone(),
            len: new_file_len,
        })
    }

    pub(super) fn open(cfg: &TurboConfig) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);

        // file must exists
        if !path.exists() {
            let err = InternalError::InvalidFile("Path does not exists".into());
            cfg.logger.error(format!("(Den) [open] Invalid path: {err}"));
            return Err(err);
        }

        let file = TurboFile::open(&cfg, PATH)?;
        let file_len = file.len()?;

        // make sure file is page aligned
        if file_len % DEFAULT_PAGE_SIZE != 0x00 {
            let err = InternalError::InvalidFile("Den is not page aligned".into());
            cfg.logger.error(format!(
                "(Den) [open] TurboFile is not page aligned w/ len={file_len} and error: {err}"
            ));
            return Err(err);
        }

        cfg.logger.debug("(Den) [open] open is successful");

        Ok(Self {
            file,
            cfg: cfg.clone(),
            len: file_len,
        })
    }

    pub(super) fn write(&self, buf: &[u8], page_idx: usize) -> InternalResult<()> {
        let offset = DEFAULT_PAGE_SIZE * page_idx;

        // sanity checks
        #[cfg(debug_assertions)]
        {
            let num_pages = self.len / DEFAULT_PAGE_SIZE;
            let req_pages = buf.len() / DEFAULT_PAGE_SIZE;

            debug_assert!(offset < self.len, "Offset is out of bounds");
            debug_assert!(page_idx < num_pages, "page_idx is out of bounds");
            debug_assert!(page_idx + req_pages < num_pages, "buffer is out of bounds");
            debug_assert!(buf.len() % DEFAULT_PAGE_SIZE == 0x00, "Buffer must be page aligned");
        }

        self.file.async_write(buf, offset)
    }

    pub(super) fn read(&self, page_idx: usize, n_page: usize) -> InternalResult<Vec<u8>> {
        let offset = DEFAULT_PAGE_SIZE * page_idx;
        let mut buf = vec![0u8; n_page * DEFAULT_PAGE_SIZE];

        // sanity checks
        #[cfg(debug_assertions)]
        {
            let num_pages = self.len / DEFAULT_PAGE_SIZE;
            let req_pages = buf.len() / DEFAULT_PAGE_SIZE;

            debug_assert!(offset < self.len, "Offset is out of bounds");
            debug_assert!(page_idx < num_pages, "page_idx is out of bounds");
        }

        self.file.pread(&mut buf, offset)?;
        Ok(buf)
    }
}
