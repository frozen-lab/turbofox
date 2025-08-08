use crate::{
    bucket::Bucket,
    constants::{DEFAULT_BUCKET_NAME, INDEX_NAME, STAGING_BUCKET_NAME},
    index::Index,
    types::{InternalConfig, InternalResult},
};
use std::sync::{Arc, RwLock};

pub(crate) struct Router {
    config: InternalConfig,
    index: Arc<RwLock<Index>>,
    live_bucket: Arc<RwLock<Bucket>>,
    staging_bucket: Arc<RwLock<Option<Bucket>>>,
}

impl Router {
    pub fn new(config: InternalConfig) -> InternalResult<Self> {
        // make sure the dir exists
        std::fs::create_dir_all(&config.dirpath)?;

        let index_path = config.dirpath.join(INDEX_NAME);
        let index = Index::open(&index_path, config.initial_capacity)?;

        let bucket_path = config.dirpath.join(DEFAULT_BUCKET_NAME);
        let live_bucket = Bucket::new(&bucket_path, index.get_capacity())?;

        let num_entries = live_bucket.get_inserted_count()?;
        let threshold = live_bucket.get_threshold()?;

        let staging_bucket: Option<Bucket> = if num_entries >= threshold {
            let bucket_path = config.dirpath.join(STAGING_BUCKET_NAME);
            let bucket = Bucket::new(&bucket_path, index.get_staging_capacity())?;

            Some(bucket)
        } else {
            None
        };

        Ok(Self {
            config,
            index: Arc::new(RwLock::new(index)),
            live_bucket: Arc::new(RwLock::new(live_bucket)),
            staging_bucket: Arc::new(RwLock::new(staging_bucket)),
        })
    }
}
