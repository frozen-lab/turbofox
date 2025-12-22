use crate::{error::InternalResult, logger::Logger, TurboConfig};
use meta::{Meta, MetaFile};
use std::path::PathBuf;

mod meta;

#[derive(Debug)]
pub(crate) struct InternalConfig {
    pub(crate) init_cap: u64,
    pub(crate) growth_x: u64,
    pub(crate) logger: Logger,
    pub(crate) dirpath: PathBuf,
    pub(crate) meta: *mut Meta,
}

pub(crate) struct Engine {
    meta_file: MetaFile,
    cfg: InternalConfig,
}

impl Engine {
    pub(crate) fn new(dirpath: PathBuf, cfg: &TurboConfig, logger: Logger) -> InternalResult<Self> {
        let meta_exists = MetaFile::exists(&dirpath);
        let meta_file = if meta_exists {
            MetaFile::open(&dirpath, cfg)?
        } else {
            MetaFile::new(&dirpath, cfg)?
        };

        let cfg = InternalConfig {
            init_cap: cfg.initial_capacity.to_u64(),
            growth_x: cfg.growth_factor,
            logger,
            dirpath,
            meta: meta_file.meta(),
        };

        Ok(Self { meta_file, cfg })
    }
}
