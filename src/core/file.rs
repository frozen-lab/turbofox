use crate::{errors::InternalResult, logger::Logger, TurboConfig};
use std::path::PathBuf;

#[derive(Debug)]
pub(crate) struct TurboFile {
    cfg: TurboConfig,
    target: &'static str,

    #[cfg(target_os = "linux")]
    file: crate::linux::File,

    #[cfg(not(target_os = "linux"))]
    file: (),
}

impl TurboFile {
    pub(crate) fn new(cfg: &TurboConfig, target: &'static str) -> InternalResult<Self> {
        let path = cfg.dirpath.join(target);

        #[cfg(target_os = "linux")]
        let file = unsafe { Self::new_linux(cfg, &path, target) }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        cfg.logger
            .debug(format!("({target}) [new] Created new TurboFile at {:?}", path));

        Ok(Self {
            target,
            cfg: cfg.clone(),
            file,
        })
    }

    pub(crate) fn open(cfg: &TurboConfig, target: &'static str) -> InternalResult<Self> {
        let path = cfg.dirpath.join(target);

        #[cfg(target_os = "linux")]
        let file = unsafe { Self::open_linux(cfg, &path, target) }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        cfg.logger
            .debug(format!("({target}) [open] Opened TurboFile at {:?}", path));

        Ok(Self {
            target,
            cfg: cfg.clone(),
            file,
        })
    }

    pub(crate) fn zero_extend(&self, len: usize, clear_on_fail: bool) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe { self.zero_extend_linux(len, clear_on_fail) }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        self.cfg
            .logger
            .debug(format!("({}) [zero-extend] TurboFile now has len={len}", self.target));

        Ok(())
    }

    pub(crate) fn pread(&self, buf: &mut [u8], off: usize) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe { self.pread_linux(buf, off) }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        Ok(())
    }

    pub(crate) fn len(&self) -> InternalResult<usize> {
        #[cfg(target_os = "linux")]
        let len = unsafe { self.file_len_linux() }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        Ok(len)
    }

    pub(crate) fn sync(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe { self.sync_linux() }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        self.cfg
            .logger
            .debug(format!("({}) [sync] Sync on TurboFile", self.target));

        Ok(())
    }

    pub(crate) fn close(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe { self.close_linux() }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        self.cfg
            .logger
            .debug(format!("({}) [close] TurboFile closed", self.target));

        Ok(())
    }

    pub(crate) fn del(&self) -> InternalResult<()> {
        let path = self.cfg.dirpath.join(self.target);
        Self::_del(&path, &self.cfg, self.target)?;

        self.cfg
            .logger
            .debug(format!("({}) [del] Deleted TurboFile", self.target));

        Ok(())
    }

    #[inline]
    pub(crate) fn fd(&self) -> i32 {
        self.file.fd()
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn new_linux(cfg: &TurboConfig, path: &PathBuf, target: &'static str) -> InternalResult<crate::linux::File> {
        crate::linux::File::new(&path)
            .inspect(|_| {
                cfg.logger
                    .trace(format!("({target}) [new] TurboFile created at {:?}", path))
            })
            .map_err(|e| {
                cfg.logger
                    .error(format!("({target}) [new] Failed to create TurboFile: {e}"));

                // NOTE: we must delete file (only if created), so new init could work w/o any issues
                let _ = Self::_del(&path, &cfg, target);
                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn open_linux(
        cfg: &TurboConfig,
        path: &PathBuf,
        target: &'static str,
    ) -> InternalResult<crate::linux::File> {
        crate::linux::File::open(&path)
            .inspect(|_| {
                cfg.logger
                    .trace(format!("({target}) [open] TurboFile opened at {:?}", path))
            })
            .map_err(|e| {
                cfg.logger
                    .error(format!("({target}) [open] Failed to open TurboFile: {e}"));
                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn zero_extend_linux(&self, len: usize, clear_on_fail: bool) -> InternalResult<()> {
        self.file
            .zero_extend(len)
            .inspect(|_| {
                self.cfg.logger.trace(format!(
                    "({}) [zero-extend] Zero extended TurboFile w/ len={len}",
                    self.target
                ))
            })
            .map_err(|e| {
                self.cfg.logger.error(format!(
                    "({}) [zero-extend] Failed to zero extend TurboFile: {e}",
                    self.target
                ));

                // NOTE: In this error state we must CLOSE + DELETE the created file, so when called again,
                // our process could get a clean slate to work w/o having any issues

                // HACK: We ignore errors from CLOSE and DELETE, as we are already in the errored state!
                // The zero-extend error is more important and direct to throw outside, so we just ignore
                // these two errors (if any).

                // NOTE: We should only delete the file, if file handle (fd on linux) is released or closed!
                if self.close_linux().is_ok() && clear_on_fail {
                    let path = self.cfg.dirpath.join(self.target);
                    let _ = Self::_del(&path, &self.cfg, self.target);
                }

                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn pread_linux(&self, buf: &mut [u8], off: usize) -> InternalResult<()> {
        self.file
            .pread(buf, off as i64)
            .inspect(|_| self.cfg.logger.trace(format!("({}) [pread] PRead done", self.target)))
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("({}) [pread] Failed to pread from TurboFile: {e}", self.target));
                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn file_len_linux(&self) -> InternalResult<usize> {
        self.file
            .len()
            .inspect(|len| {
                let path = self.cfg.dirpath.join(self.target);
                self.cfg
                    .logger
                    .trace(format!("({}) [len] TurboFile has len={len}", self.target))
            })
            .map_err(|e| {
                self.cfg.logger.error(format!(
                    "({}) [len] Failed to get length for TurboFile: {e}",
                    self.target
                ));
                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn sync_linux(&self) -> InternalResult<()> {
        self.file
            .fsync()
            .inspect(|_| {
                self.cfg
                    .logger
                    .trace(format!("({}) [sync] Sync'ed TurboFile", self.target))
            })
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("({}) [sync] Sync failed for TurboFile: {e}", self.target));
                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn close_linux(&self) -> InternalResult<()> {
        self.file
            .close()
            .inspect(|_| {
                self.cfg
                    .logger
                    .trace(format!("({}) [close] Successfully closed TurboFile", self.target))
            })
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("({}) [close] Failed to close TurboFile: {e}", self.target));
                e
            })
    }

    /// Deletes the [File] from filesystem
    #[inline]
    fn _del(path: &PathBuf, cfg: &TurboConfig, target: &'static str) -> InternalResult<()> {
        // quick pass
        if !path.exists() {
            return Ok(());
        }

        std::fs::remove_file(path)
            .inspect(|_| {
                cfg.logger.trace(format!("({target}) [delete] Deleted the TurboFile"));
            })
            .map_err(|e| {
                cfg.logger
                    .error(format!("({target}) [delete] Failed to delete TurboFile: {e}"));

                e.into()
            })
    }
}

impl Drop for TurboFile {
    fn drop(&mut self) {
        unsafe {
            let mut is_err = false;

            // sync the file (save and exit)
            is_err = self.sync().is_err();

            // close the file
            is_err = self.close().is_err();

            if is_err {
                self.cfg
                    .logger
                    .warn(format!("{} [drop] Failed to drop TurboFile", self.target));
            } else {
                self.cfg
                    .logger
                    .trace(format!("{} [drop] Dropped TurboFile", self.target));
            }
        }
    }
}

// NOTE: We do not tests OS specific functions, e.g. `TurboFile::new_linux()`, as they are
// already tested in there impl, and we here only wrap them for error logging, no logical
// changes being made, so we are good!

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn test_new_works() {
        let (cfg, _dir) = TurboConfig::test_cfg("new_works");
        let file = TurboFile::new(&cfg, "TurboFile").expect("New turbofile");

        assert!(file.len().is_ok(), "New file must have valid permissions");
        assert!(
            Path::new(&_dir.path().join("TurboFile")).exists(),
            "New must create file on disk"
        );
    }

    #[test]
    fn test_new_has_zero_len() {
        let (cfg, _dir) = TurboConfig::test_cfg("new_has_zero_len");
        let file = TurboFile::new(&cfg, "TurboFile").expect("New turbofile");
        let len = file.len().expect("Read length");

        assert_eq!(len, 0x00, "New file must have 0 len");
    }

    #[test]
    fn test_open_works_after_new() {
        let (cfg, _dir) = TurboConfig::test_cfg("open_works");

        let file1 = TurboFile::new(&cfg, "TurboFile").expect("New turbofile");
        assert!(file1.len().is_ok(), "New file must have valid permissions");

        let file2 = TurboFile::open(&cfg, "TurboFile").expect("Open existing file");
        assert!(file2.len().is_ok(), "Existing file must have valid permissions");
    }

    #[test]
    fn test_zero_extend_works() {
        let (cfg, _dir) = TurboConfig::test_cfg("zero_extend_works");
        let file = TurboFile::new(&cfg, "TurboFile").expect("New turbofile");

        // extend
        assert!(file.zero_extend(0x80, true).is_ok(), "Zero-extend should work");

        // fetch new len
        let len = file.len().expect("Fetch file len");
        assert_eq!(len, 0x80, "Len should update after zero-extend");
    }

    #[test]
    fn test_zero_extend_clears_file_on_error() {
        let (cfg, _dir) = TurboConfig::test_cfg("zero_extend_clears_on_err");
        let path = _dir.path().join("TurboFile");
        let file = TurboFile::new(&cfg, "TurboFile").expect("New file");

        let bad_len = usize::MAX;
        let res = file.zero_extend(bad_len, true);

        assert!(res.is_err(), "zero_extend should fail on closed fd");
        assert!(
            !path.exists(),
            "zero_extend(clear_on_fail=true) must delete file on failure"
        );
    }

    #[test]
    fn test_len_works_across_new_and_reopen() {
        let (cfg, _dir) = TurboConfig::test_cfg("len_works_across_new_open");
        let file = TurboFile::new(&cfg, "TurboFile").expect("New turbofile");

        // extend
        assert!(file.zero_extend(0x80, true).is_ok(), "Zero-extend should work");

        // new len validation
        let len = file.len().expect("Fetch file len");
        assert_eq!(len, 0x80, "Len should update after zero-extend");
        drop(file);

        // open len validation
        let file2 = TurboFile::open(&cfg, "TurboFile").expect("Open existing file");
        let len2 = file2.len().expect("Fetch file len");

        assert_eq!(len, len2, "Len mismatch between new and open");
    }

    #[test]
    fn test_sync_works() {
        let (cfg, _dir) = TurboConfig::test_cfg("sync_works");
        let file = TurboFile::new(&cfg, "TurboFile").expect("New turbofile");
        let data: &'static str = "Dummy Data";
        let path = _dir.path().join("TurboFile").to_path_buf();

        std::fs::write(&path, data).expect("Write to file");
        assert!(file.sync().is_ok());

        let sd = std::fs::read(&path).expect("Read from file");
        assert_eq!(sd, data.as_bytes());
    }

    #[test]
    fn test_close_works() {
        let (cfg, _dir) = TurboConfig::test_cfg("close_works");
        let mut file = TurboFile::new(&cfg, "TurboFile").expect("New turbofile");
        assert!(file.close().is_ok());

        // NOTE: Expect drop to fail
    }

    #[test]
    fn test_close_after_close_fails() {
        let (cfg, _dir) = TurboConfig::test_cfg("close_fails");
        let file = TurboFile::new(&cfg, "TurboFile").expect("New turbofile");

        assert!(file.close().is_ok());
        assert!(file.close().is_err());

        // NOTE: Expect drop to fail
    }

    #[test]
    fn test_pread_full_read() {
        let (cfg, _dir) = TurboConfig::test_cfg("pread_full");
        let path = _dir.path().join("TurboFile");

        // write 16 bytes
        std::fs::write(&path, b"ABCDEFGHIJKLMNOP").expect("write to file");

        let file = TurboFile::open(&cfg, "TurboFile").expect("open works");
        let mut buf = [0u8; 0x10];
        file.pread(&mut buf, 0x00).expect("pread full");

        assert_eq!(&buf, b"ABCDEFGHIJKLMNOP");
    }

    #[test]
    fn test_pread_partial_read() {
        let (cfg, _dir) = TurboConfig::test_cfg("pread_partial");
        let path = _dir.path().join("TurboFile");

        // write 16 bytes
        std::fs::write(&path, b"ABCDEFGHIJKLMNOP").expect("write to file");

        let file = TurboFile::open(&cfg, "TurboFile").expect("open works");
        let mut buf = [0u8; 0x04];
        file.pread(&mut buf, 0x0A).expect("pread partial");

        assert_eq!(&buf, b"KLMN");
    }

    #[test]
    fn test_pread_fails_on_closed_file() {
        let (cfg, _dir) = TurboConfig::test_cfg("pread_fail_closed");
        let file = TurboFile::new(&cfg, "TurboFile").expect("new file");

        // close underlying fd
        assert!(file.close().is_ok());

        let mut buf = [0u8; 0x04];
        let res = file.pread(&mut buf, 0x00);

        assert!(res.is_err(), "pread must fail on closed file");
    }
}
