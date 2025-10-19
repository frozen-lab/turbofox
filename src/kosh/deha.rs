use crate::errors::InternalResult;
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

pub(super) const PAGE_SIZE: usize = 128;
const PATH: &'static str = "deha";

pub(super) struct Deha {
    file: File,
}

impl Deha {
    pub(super) fn open(dir: &PathBuf) -> InternalResult<Self> {
        let path = dir.join(PATH);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        // TODO: If file is not paged correctly, we should either
        // - delete and create a new one
        // - throw error to user stating `TurboCache` is corrupted

        Ok(Self { file })
    }

    pub(super) fn write(&mut self, buffer: &[u8]) -> InternalResult<()> {
        // sanity check
        debug_assert!(buffer.len() % PAGE_SIZE == 0, "Buffer must paged correctly");

        Ok(())
    }
}
