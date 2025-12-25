use crate::{error::InternalResult, logger::Logger, TurboConfig};
use cache::Cache;
use meta::Metadata;
use std::{path::PathBuf, sync::Arc};

mod cache;
mod meta;
mod trail;

#[derive(Debug)]
pub(in crate::engine) struct InternalConfig {
    pub(in crate::engine) init_cap: u64,
    pub(in crate::engine) growth_x: u64,
    pub(in crate::engine) meta: Metadata,
}

pub(crate) struct Engine {
    cfg: Arc<InternalConfig>,
    logger: Arc<Logger>,
    cache: Cache,
}

impl Engine {
    pub(crate) fn new(dirpath: PathBuf, turbo_cfg: &TurboConfig, logger: Logger) -> InternalResult<Self> {
        let logger = Arc::new(logger);

        let meta_exists = Metadata::exists(&dirpath);
        let meta_file = if meta_exists {
            Metadata::open(&dirpath, turbo_cfg, logger.clone())?
        } else {
            Metadata::new(&dirpath, turbo_cfg, logger.clone())?
        };

        let cfg = Arc::new(InternalConfig {
            meta: meta_file,
            growth_x: turbo_cfg.growth_factor,
            init_cap: turbo_cfg.initial_capacity.to_u64(),
        });

        let cache = if Cache::exists(&dirpath) {
            Cache::open(&dirpath, logger.clone(), cfg.clone())?
        } else {
            Cache::new(&dirpath, logger.clone(), cfg.clone())?
        };

        Ok(Self { cfg, cache, logger })
    }
}
