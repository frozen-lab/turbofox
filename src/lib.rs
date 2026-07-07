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
#[derive(Debug, Clone)]
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

    /// Read the value assoicated w/ the key from the database
    #[inline(always)]
    pub fn read(&self, key: &[u8]) -> FrozenResult<Option<Vec<u8>>> {
        debug_assert!(key.len() <= 0x10, "key length must be <= 16");

        let mut index_key = [0u8; 0x10];
        index_key[..key.len()].copy_from_slice(key);

        if let Some((id, n_buffers)) = self.index.read(index_key)? {
            let value = self.kosa.read(id, n_buffers as usize)?;
            return Ok(value);
        }

        Ok(None)
    }

    /// Delete the key-value pair from the database
    #[inline(always)]
    pub fn delete(&self, key: &[u8]) -> FrozenResult<()> {
        debug_assert!(key.len() <= 0x10, "key length must be <= 16");

        let mut index_key = [0u8; 0x10];
        index_key[..key.len()].copy_from_slice(key);

        if let Some((id, n_bufs)) = self.index.delete(index_key)? {
            self.kosa.delete(id, n_bufs as usize)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const INIT_BUFFERS: usize = 0x1000;
    const MAX_MEMORY: usize = 64 * 1024 * 1024;

    fn init() -> (tempfile::TempDir, TurboFox) {
        let dir = tempfile::tempdir().expect("create tempdir");

        let db = TurboFox::new(TurboFoxCfg {
            path: dir.path().to_path_buf(),
            buffer_size: BufferSize::S64,
            initial_available_buffers: INIT_BUFFERS,
            flush_duration: Duration::from_millis(1),
            max_memory: MAX_MEMORY,
        })
        .expect("create db");

        (dir, db)
    }

    fn key(id: u8) -> Vec<u8> {
        vec![id]
    }

    #[test]
    fn ok_max_key_length() {
        let (_dir, db) = init();
        let key = [0xAA; 0x10];

        let ticket = db.write(&key, b"value").unwrap();
        ticket.wait().unwrap();

        assert_eq!(db.read(&key).unwrap(), Some(b"value".to_vec()));

        db.delete(&key).unwrap();
        assert_eq!(db.read(&key).unwrap(), None);
    }

    mod write_read {
        use super::*;

        #[test]
        fn ok_single() {
            let (_dir, db) = init();

            let ticket = db.write(&key(1), b"hello").unwrap();
            ticket.wait().unwrap();

            assert_eq!(db.read(&key(1)).unwrap(), Some(b"hello".to_vec()));
        }

        #[test]
        fn ok_multiple() {
            let (_dir, db) = init();
            let mut last = None;

            for i in 0..0x80u8 {
                last = Some(db.write(&key(i), &[i]).unwrap());
            }

            last.unwrap().wait().unwrap();
            for i in 0..0x80u8 {
                assert_eq!(db.read(&key(i)).unwrap(), Some(vec![i]));
            }
        }

        #[test]
        fn ok_missing() {
            let (_dir, db) = init();

            assert_eq!(db.read(b"missing").unwrap(), None);
        }

        #[test]
        fn ok_overwrite() {
            let (_dir, db) = init();

            db.write(b"abc", b"one").unwrap();
            db.write(b"abc", b"two").unwrap().wait().unwrap();

            assert_eq!(db.read(b"abc").unwrap(), Some(b"two".to_vec()));
        }

        #[test]
        fn ok_variable_sizes() {
            let (_dir, db) = init();

            for len in 1..=0x10 {
                let key = vec![0xAB; len];
                let value = vec![0xCD; len * 0x40];

                let ticket = db.write(&key, &value).unwrap();
                ticket.wait().unwrap();

                assert_eq!(db.read(&key).unwrap(), Some(value));
            }
        }
    }

    mod delete {
        use super::*;

        #[test]
        fn ok_existing() {
            let (_dir, db) = init();

            db.write(b"a", b"value").unwrap().wait().unwrap();
            db.delete(b"a").unwrap();

            assert_eq!(db.read(b"a").unwrap(), None);
        }

        #[test]
        fn ok_missing() {
            let (_dir, db) = init();

            db.delete(b"missing").unwrap();
            db.delete(b"missing").unwrap();

            assert_eq!(db.read(b"missing").unwrap(), None);
        }

        #[test]
        fn ok_preserve_other_keys() {
            let (_dir, db) = init();
            let mut last = None;

            for i in 0..0x40u8 {
                last = Some(db.write(&key(i), &[i]).unwrap());
            }

            last.unwrap().wait().unwrap();
            db.delete(&key(0x32)).unwrap();

            for i in 0..0x40u8 {
                if i == 0x32 {
                    assert_eq!(db.read(&key(i)).unwrap(), None);
                } else {
                    assert_eq!(db.read(&key(i)).unwrap(), Some(vec![i]));
                }
            }
        }
    }

    mod persistence {
        use super::*;

        #[test]
        fn ok_reopen() {
            let dir = tempfile::tempdir().expect("create tempdir");

            let cfg = TurboFoxCfg {
                path: dir.path().to_path_buf(),
                buffer_size: BufferSize::S64,
                initial_available_buffers: INIT_BUFFERS,
                flush_duration: Duration::from_millis(1),
                max_memory: MAX_MEMORY,
            };

            {
                let db = TurboFox::new(cfg.clone()).unwrap();

                db.write(b"a", b"one").unwrap();
                db.write(b"b", b"two").unwrap();
            }

            {
                let db = TurboFox::new(cfg).unwrap();

                assert_eq!(db.read(b"a").unwrap(), Some(b"one".to_vec()));
                assert_eq!(db.read(b"b").unwrap(), Some(b"two".to_vec()));
            }
        }
    }

    mod stress {
        use super::*;

        #[test]
        fn ok_large_values() {
            let (_dir, db) = init();

            for i in 0..0x20u8 {
                let value = vec![i; 0x40 * 0x0A];

                db.write(&key(i), &value).unwrap().wait().unwrap();
                assert_eq!(db.read(&key(i)).unwrap(), Some(value));
            }
        }
    }
}
