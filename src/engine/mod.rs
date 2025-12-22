mod meta;

use crate::logger::Logger;
use std::path::PathBuf;

#[derive(Debug)]
pub(crate) struct TurboMeta<'a> {
    num_bufs: usize,
    capacity: usize,
    buf_size: usize,
    max_klen: usize,
    growth_x: usize,
    init_cap: usize,
    logger: &'a Logger,
    dirpath: &'a PathBuf,
}

pub(crate) struct Engine {}

impl Engine {}
