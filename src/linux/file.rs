use crate::errors::{InternalError, InternalResult};
use libc::{
    close, fstat, fsync, ftruncate, off_t, open, stat, sync_file_range, O_CLOEXEC, O_CREAT, O_NOATIME, O_RDWR, O_TRUNC,
    SYNC_FILE_RANGE_WAIT_AFTER, SYNC_FILE_RANGE_WAIT_BEFORE,
};
use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

#[derive(Debug, Clone, Copy)]
pub(crate) struct File(pub(crate) i32);

impl File {
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn new(path: &Path) -> InternalResult<Self> {
        let cpath = Self::get_ffi_valid_path(path)?;
        let fd = open(
            cpath.as_ptr(),
            Self::_get_flags(true),
            // file permission mode (used for O_CREATE)
            0o644,
        );

        if fd < 0 {
            eprintln!("ERROR FD: {fd}");
            return Err(Self::_last_os_error());
        }

        Ok(Self(fd))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn open(path: &Path) -> InternalResult<Self> {
        let cpath = Self::get_ffi_valid_path(path)?;
        let fd = open(cpath.as_ptr(), Self::_get_flags(false));

        if fd < 0 {
            return Err(Self::_last_os_error());
        }

        Ok(Self(fd))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn fstat(&self) -> InternalResult<stat> {
        let mut stat = std::mem::zeroed::<stat>();

        if fstat(self.0, &mut stat) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(stat)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn fsync(&self) -> InternalResult<()> {
        if fsync(self.0) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn zero_extend(&self, new_len: usize) -> InternalResult<()> {
        if ftruncate(self.0, new_len as off_t) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    /// Close the file descriptor (i.e. File Handle)
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn close(&self) -> InternalResult<()> {
        if close(self.0) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    /// Delete the file from file system
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline]
    pub(crate) unsafe fn del(path: &Path) -> InternalResult<()> {
        std::fs::remove_file(path).map_err(|e| e.into())
    }

    fn _get_flags(is_new: bool) -> i32 {
        // NOTE: We use the `O_NOATIME` flag to disable access time updates on the file. Normally every
        // I/O ops triggers an atime write to disk, which is counter productive and increases latency
        // for our ops.
        //
        // WARN: We can only do this if we own the file, which is true in our case.
        const BASE: i32 = O_RDWR | O_NOATIME | O_CLOEXEC;
        const NEW: i32 = O_CREAT | O_TRUNC;

        BASE | ((is_new as i32) * NEW)
    }

    #[inline]
    fn get_ffi_valid_path(path: &Path) -> InternalResult<CString> {
        CString::new(path.as_os_str().as_bytes()).map_err(|_| InternalError::Misc("Invalid file path".into()))
    }

    #[inline]
    fn _last_os_error() -> InternalError {
        std::io::Error::last_os_error().into()
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_file() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().expect("TempDir");
        let path = dir.path().join("file");

        (dir, path)
    }

    #[test]
    fn test_create_new_file() {
        let (_dir, path) = create_file();

        unsafe {
            let f = File::new(&path).expect("new() should succeed");
            assert_ne!(f.0, 0);

            let st = f.fstat().expect("Get File Stats");
            assert_eq!(st.st_size, 0);

            f.close().expect("Close fd");
        }
    }

    #[test]
    fn test_invalid_file_path_for_new() {
        let dir = TempDir::new().expect("TempDir");
        let bad = dir.path().join("a\0b");

        unsafe {
            assert!(File::new(&bad).is_err());
        }
    }

    #[test]
    fn test_open_works_after_new() {
        let (_dir, path) = create_file();

        unsafe {
            let f1 = File::new(&path).expect("Create new file");
            assert_ne!(f1.0, 0);
            f1.close().expect("Close the file");
        }

        unsafe {
            let f2 = File::open(&path).expect("open() should succeed");
            assert_ne!(f2.0, 0);
            f2.close().expect("Close the file");
        }
    }

    #[test]
    fn test_open_failes_without_new() {
        let (_dir, path) = create_file();

        unsafe {
            assert!(File::open(&path).is_err());
        }
    }

    #[test]
    fn test_close_works() {
        let (_dir, path) = create_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            assert!(file.close().is_ok());
        }
    }

    #[test]
    fn test_close_after_close_fails() {
        let (_dir, path) = create_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            assert!(file.close().is_ok(), "Close should work correctly");
            assert!(file.close().is_err(), "Close after close must fail");
        }
    }

    #[test]
    fn test_fstat_works() {
        let (_dir, path) = create_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            let st = file.fstat().expect("File Stats");

            assert_ne!(st.st_mode, 0);
            file.close().expect("Close the fd");
        }
    }

    #[test]
    fn test_fstat_fails_on_closed_file() {
        let (_dir, path) = create_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            file.close().expect("Close the fd");

            assert!(file.fstat().is_err());
        }
    }

    #[test]
    fn test_fsync_works() {
        let (_dir, path) = create_file();
        let data: &'static str = "Dummy Data";

        unsafe {
            let file = File::new(&path).expect("Create new file");
            std::fs::write(&path, data).expect("Write to file");

            assert!(file.fsync().is_ok());

            let sd = std::fs::read(&path).expect("Read from file");
            assert_eq!(sd, data.as_bytes());

            file.close().expect("Close the fd");
        }
    }

    #[test]
    fn test_fsync_fails_on_closed_file() {
        let (_dir, path) = create_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            file.close().expect("Close the fd");

            assert!(file.fsync().is_err());
        }
    }

    #[test]
    fn test_zero_extend_grows() {
        let (_dir, path) = create_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");

            // first grow (4 KiB)
            assert!(file.zero_extend(0x1000).is_ok());
            assert_eq!(file.fstat().expect("FStat").st_size, 0x1000);
            let data1 = std::fs::read(&path).expect("Read from file");
            assert_eq!(data1.len(), 0x1000);
            assert!(data1.iter().all(|&b| b == 0x00));

            // second grow (8 KiB)
            assert!(file.zero_extend(0x2000).is_ok());
            assert_eq!(file.fstat().expect("FStat").st_size, 0x2000);
            let data2 = std::fs::read(&path).expect("Read from file");
            assert_eq!(data2.len(), 0x2000);
            assert!(data2.iter().all(|&b| b == 0x00));

            file.close().expect("Close the fd");
        }
    }

    #[test]
    fn test_del_works() {
        let (_dir, path) = create_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            file.close().expect("Close the fd");

            assert!(File::del(&path).is_ok());
            assert!(!path.exists());
        }
    }

    #[test]
    fn test_del_fails_on_dne() {
        let (_dir, path) = create_file();

        unsafe {
            // NOTE: this is a guard to make sure file simply does not exists
            assert!(!path.exists());

            assert!(File::del(&path).is_err());
        }
    }
}
