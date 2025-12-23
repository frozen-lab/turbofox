use crate::{error::InternalResult, logger::Logger, TurboConfig};
use cache::Cache;
use meta::Metadata;
use std::{path::PathBuf, sync::Arc};

mod cache;
mod meta;

#[derive(Debug)]
pub(in crate::engine) struct InternalConfig {
    pub(in crate::engine) init_cap: u64,
    pub(in crate::engine) growth_x: u64,
    pub(in crate::engine) logger: Logger,
    pub(in crate::engine) dirpath: PathBuf,
    pub(in crate::engine) meta: Metadata,
}

pub(crate) struct Engine {
    cfg: Arc<InternalConfig>,
    cache: Cache,
}

impl Engine {
    pub(crate) fn new(dirpath: PathBuf, turbo_cfg: &TurboConfig, logger: Logger) -> InternalResult<Self> {
        let meta_exists = Metadata::exists(&dirpath);
        let meta_file = if meta_exists {
            Metadata::open(&dirpath, turbo_cfg)?
        } else {
            Metadata::new(&dirpath, turbo_cfg)?
        };

        let cfg = Arc::new(InternalConfig {
            logger,
            dirpath,
            meta: meta_file,
            growth_x: turbo_cfg.growth_factor,
            init_cap: turbo_cfg.initial_capacity.to_u64(),
        });

        let cache = if Cache::exists(&cfg) {
            Cache::open(cfg.clone())?
        } else {
            Cache::new(cfg.clone())?
        };

        Ok(Self { cfg, cache })
    }
}
