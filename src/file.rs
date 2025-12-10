use crate::{core::unlikely, error::InternalResult};
use std::path::Path;

#[derive(Debug)]
pub(crate) struct TurboFile {
    #[cfg(target_os = "linux")]
    file: crate::linux::File,
}

impl TurboFile {
    /// Creates a new `[TurboFile]` at given `Path`
    pub(crate) fn new(path: &Path) -> InternalResult<Self> {
        #[cfg(target_os = "linux")]
        unsafe {
            let file = crate::linux::File::new(path)?;
            return Ok(Self { file });
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    /// Open an existing `[TurboFile]` at given `Path`
    pub(crate) fn open(path: &Path) -> InternalResult<Self> {
        #[cfg(target_os = "linux")]
        unsafe {
            let file = crate::linux::File::open(path)?;
            return Ok(Self { file });
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    /// Fetches current length of `[TurboFile]`
    pub(crate) fn len(&self) -> InternalResult<usize> {
        #[cfg(target_os = "linux")]
        unsafe {
            return self.file.len();
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    /// Flushes dirty pages to disk for persistence of `[TurboFile]`
    pub(crate) fn flush(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe {
            return self.file.fsync();
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    /// Zero-extend's the `[TurboFile]` w/ zero-bytes (`0x00`)
    ///
    /// **WARN:** If `len` is smaller then the current length of the `[TurboFile]`,
    /// file length will be reduced!
    pub(crate) fn zero_extend(&self, len: usize) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe {
            return self.file.ftruncate(len);
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    /// Closes the `[TurboFile]`
    pub(crate) fn close(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe {
            return self.file.close();
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    /// Deletes the `[TurboFile]` from disk
    ///
    /// **WARN:** We must only use this for cleanup of incorrectly initialized `[TurboFile]`'s.
    /// As this will end up cleaning all the data from disk, which may result in data loss!
    #[allow(unused)]
    pub(crate) fn delete(&self, path: &Path) -> InternalResult<()> {
        // sanity check
        if unlikely(!path.exists()) {
            return Ok(());
        }

        std::fs::remove_file(path).map_err(|e| e.into())
    }
}
