//! TurboFox is a persistent and efficient embedded KV database

#![deny(missing_docs)]
#![deny(unused_must_use)]
#![allow(unsafe_op_in_unsafe_fn)]

use kosa::{Kosa, KosaCfg};
use std::{path, time};

mod index;

pub use frozen_core::error::{FrozenError, FrozenResult};
pub use kosa::{AckTicket, BufferSize};

/// Module ID used in [`frozen_core::error::FrozenError`]
pub(crate) const MODULE_ID: u8 = 0x02;

/// All the available configurations for [`TurboFox`]
///
/// ## Example
///
/// ```
/// use turbofox::{TurboFoxCfg, BufferSize};
/// use std::time::Duration;
///
/// let dir = tempfile::tempdir().unwrap();
/// let cfg = TurboFoxCfg {
///     path: dir.path().to_path_buf(),
///     buffer_size: BufferSize::S64,
///     initial_available_buffers: 0x1000,
///     flush_duration: Duration::from_millis(2),
///     max_memory: 0x400 * 0x400 * 0x40, // 64 MB
/// };
///
/// assert!(cfg.max_memory > 0);
/// assert_eq!(cfg.buffer_size as usize, 0x40);
/// ```
#[derive(Debug)]
pub struct TurboFoxCfg {
    /// The root directory path where database files (`data` and `bmap`) will be stored
    pub path: path::PathBuf,

    /// Size (in bytes) of an individual page/buffer unit in the storage file
    pub buffer_size: BufferSize,

    /// Number of pre-allocated buffer slots in the internal bitmap tracker
    pub initial_available_buffers: usize,

    /// Time interval used by the background `WritePipe` to perform a hard sync to the OS
    pub flush_duration: time::Duration,

    /// Maximum allowed memory (in bytes) to be allocated simultaneously by the engine
    pub max_memory: usize,
}

/// TurboFox is a persistent and efficient embedded KV database
#[derive(Debug)]
pub struct TurboFox {
    kosa: Kosa,
    index: index::Index,
}

impl TurboFox {
    /// Creates or initializes a new [`TurboFox`] db instance
    pub fn new(cfg: TurboFoxCfg) -> FrozenResult<Self> {
        let kosa_cfg = KosaCfg {
            path: cfg.path.clone(),
            buffer_size: cfg.buffer_size,
            initial_available_buffers: cfg.initial_available_buffers,
            max_memory: cfg.max_memory,
            flush_duration: cfg.flush_duration,
        };
        let kosa = Kosa::new(kosa_cfg)?;

        let init_pages = if cfg.initial_available_buffers < index::ITEMS_PER_ROW {
            1
        } else {
            (cfg.initial_available_buffers + index::ITEMS_PER_ROW - 1) / index::ITEMS_PER_ROW
        };
        let index = index::Index::new(cfg.path.join("index"), init_pages, cfg.flush_duration)?;

        Ok(Self { kosa, index })
    }

    /// Writes a key-value pair into the database
    #[inline(always)]
    pub fn write(&self, key: &[u8], value: &[u8]) -> FrozenResult<AckTicket> {
        debug_assert!(key.len() <= 0x10, "key length must be <= 16");

        let mut index_key = [0u8; 0x10];
        index_key[..key.len()].copy_from_slice(key);

        let (ticket, storage_id, n_buffers) = self.kosa.write(value)?;
        self.index.write(index_key, storage_id, n_buffers)?;

        Ok(ticket)
    }
}
