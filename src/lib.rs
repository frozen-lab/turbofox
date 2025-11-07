#![allow(unused)]

mod burrow;
mod errors;
mod linux;
mod logger;

pub struct TurboFox;

#[derive(Debug, Clone)]
pub(crate) struct InternalCfg {
    pub(crate) dirpath: std::path::PathBuf,
    pub(crate) logging_enabled: bool,
    pub(crate) init_cap: usize,
    pub(crate) page_size: usize,
}

impl InternalCfg {
    pub(crate) fn new(dirpath: std::path::PathBuf) -> Self {
        Self {
            dirpath: dirpath,
            logging_enabled: true,
            init_cap: crate::burrow::DEFAULT_CAP,
            page_size: crate::burrow::DEFAULT_PAGE_SIZE,
        }
    }
}
