use crate::errors::{InternalError, InternalResult};
use libc::{
    close, fstat, fsync, ftruncate, getegid, geteuid, off_t, open, stat, sync_file_range, O_CLOEXEC, O_CREAT,
    O_NOATIME, O_RDWR, O_TRUNC, SYNC_FILE_RANGE_WAIT_AFTER, SYNC_FILE_RANGE_WAIT_BEFORE, S_IRGRP, S_IROTH, S_IRUSR,
    S_IWGRP, S_IWOTH, S_IWUSR,
};
use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

#[derive(Debug)]
pub(crate) struct File(i32);

impl File {
    /// Create a new file at [Path]
    #[allow(unsafe_op_in_unsafe_fn)]
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

    /// Open an existing file at [Path]
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn open(path: &Path) -> InternalResult<Self> {
        let cpath = Self::get_ffi_valid_path(path)?;
        let fd = open(cpath.as_ptr(), Self::_get_flags(false), 0x00);

        if fd < 0 {
            return Err(Self::_last_os_error());
        }

        Ok(Self(fd))
    }

    /// Read the [File] fd
    #[inline]
    pub(crate) fn fd(&self) -> i32 {
        self.0
    }

    /// Validates [File] permissions, then fetches current length of the [File]
    ///
    /// **NOTE:** Use this wisely as it costs an entire `syscall` to function
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn len(&self) -> InternalResult<usize> {
        let st = Self::fetch_stats(self.0)?;

        if !Self::validate_permission(&st) {
            return Err(InternalError::IO("Permission denied for READ or WIRTE".into()));
        }

        Ok(st.st_size as usize)
    }

    /// Flushes dirty pages to Disk
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn fsync(&self) -> InternalResult<()> {
        if fsync(self.0) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    /// Zero extends an existing file to increase its length
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn zero_extend(&self, new_len: usize) -> InternalResult<()> {
        if ftruncate(self.0, new_len as off_t) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    /// Close the file descriptor (i.e. File Handle)
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn close(&self) -> InternalResult<()> {
        if close(self.0) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    /// Delete the file from file system
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline]
    #[deprecated]
    pub(crate) unsafe fn del(path: &Path) -> InternalResult<()> {
        // quick pass
        if !path.exists() {
            return Ok(());
        }

        std::fs::remove_file(path).map_err(|e| e.into())
    }

    /// Validates if we have both `READ` and `WRITE` permissions to the [File]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn validate_permission(st: &stat) -> bool {
        let uid = geteuid();
        let gid = getegid();
        let mode = st.st_mode;

        // Do we have permission to read?
        let readable = if uid == st.st_uid {
            (mode & S_IRUSR) != 0
        } else if gid == st.st_gid {
            (mode & S_IRGRP) != 0
        } else {
            (mode & S_IROTH) != 0
        };

        // Do we have permission to write?
        let writable = if uid == st.st_uid {
            (mode & S_IWUSR) != 0
        } else if gid == st.st_gid {
            (mode & S_IWGRP) != 0
        } else {
            (mode & S_IWOTH) != 0
        };

        // we must have both :)
        readable && writable
    }

    /// Fetch metadata for file
    ///
    /// *NOTE:* Mainly used to read current file length
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn fetch_stats(fd: i32) -> InternalResult<stat> {
        let mut stat = std::mem::zeroed::<stat>();

        if fstat(fd, &mut stat) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(stat)
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

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn tmp_file() -> (TempDir, PathBuf) {
        let dir = TempDir::new().expect("TempDir");
        let path = dir.path().join("linux_file");

        (dir, path)
    }

    #[test]
    fn test_create_new_file() {
        let (_dir, path) = tmp_file();

        unsafe {
            let f = File::new(&path).expect("new() should succeed");
            assert_ne!(f.0, 0);

            let st = File::fetch_stats(f.0).expect("Get File Stats");
            assert_eq!(st.st_size, 0);

            assert!(File::validate_permission(&st), "File must have read/write permissions");

            f.close().expect("Close fd");
        }
    }

    #[test]
    fn test_open_works_after_new() {
        let (_dir, path) = tmp_file();

        unsafe {
            let f1 = File::new(&path).expect("Create new file");
            assert_ne!(f1.0, 0);
            f1.close().expect("Close the file");
        }

        unsafe {
            let f2 = File::open(&path).expect("open() should succeed");
            assert_ne!(f2.0, 0);

            let st = File::fetch_stats(f2.0).expect("Get File Stats");
            assert_eq!(st.st_size, 0);

            assert!(File::validate_permission(&st), "File must have read/write permissions");

            f2.close().expect("Close the file");
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
    fn test_open_failes_without_new() {
        let (_dir, path) = tmp_file();

        unsafe {
            assert!(File::open(&path).is_err());
        }
    }

    #[test]
    fn test_close_works() {
        let (_dir, path) = tmp_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            assert!(file.close().is_ok());
        }
    }

    #[test]
    fn test_close_after_close_fails() {
        let (_dir, path) = tmp_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            assert!(file.close().is_ok(), "Close should work correctly");
            assert!(file.close().is_err(), "Close after close must fail");
        }
    }

    #[test]
    fn test_fstat_works() {
        let (_dir, path) = tmp_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            let st = File::fetch_stats(file.0).expect("Get File Stats");

            assert_ne!(st.st_mode, 0x00);
            file.close().expect("Close the fd");
        }
    }

    #[test]
    fn test_fstat_fails_on_closed_file() {
        let (_dir, path) = tmp_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            file.close().expect("Close the fd");

            assert!(File::fetch_stats(file.0).is_err());
        }
    }

    #[test]
    fn test_fsync_works() {
        let (_dir, path) = tmp_file();
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
    fn test_fsync_fails_after_close() {
        let (_dir, path) = tmp_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            file.close().expect("Close the fd");
            assert!(file.fsync().is_err());
        }
    }

    #[test]
    fn test_fsync_fails_on_closed_file() {
        let (_dir, path) = tmp_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");
            file.close().expect("Close the fd");

            assert!(file.fsync().is_err());
        }
    }

    #[test]
    fn test_zero_extend_grows() {
        let (_dir, path) = tmp_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");

            // first grow (4 KiB)
            assert!(file.zero_extend(0x1000).is_ok());
            assert_eq!(File::fetch_stats(file.0).expect("FStat").st_size, 0x1000);
            let data1 = std::fs::read(&path).expect("Read from file");
            assert_eq!(data1.len(), 0x1000);
            assert!(data1.iter().all(|&b| b == 0x00));

            // second grow (8 KiB)
            assert!(file.zero_extend(0x2000).is_ok());
            assert_eq!(File::fetch_stats(file.0).expect("FStat").st_size, 0x2000);
            let data2 = std::fs::read(&path).expect("Read from file");
            assert_eq!(data2.len(), 0x2000);
            assert!(data2.iter().all(|&b| b == 0x00));

            file.close().expect("Close the fd");
        }
    }

    #[test]
    fn test_len_works() {
        let (_dir, path) = tmp_file();

        unsafe {
            let file = File::new(&path).expect("Create new file");

            let l1 = file.len().expect("Read file length");
            assert_eq!(l1, 0x00, "New file must have 0 length");

            file.zero_extend(0x80).expect("Zero-extend the file");
            let l2 = file.len().expect("Read file length");
            assert_eq!(l2, 0x80, "Should read correct file len after zero-extend");
        }
    }

    #[test]
    fn test_file_permission_validation_works() {
        let dir: &'static str = "/tmp/turbofox/tests";
        let file: String = format!("{dir}/test_file_permission_validation");

        let dirpath = Path::new(dir).to_path_buf();
        let filepath = Path::new(&file).to_path_buf();

        // delete existing file
        if filepath.exists() {
            std::fs::remove_file(&filepath).expect("Delete existing file");
        }

        // create directory if missing
        if !dirpath.exists() {
            std::fs::create_dir_all(dirpath).expect("Create missing directory");
        }

        unsafe {
            let file = File::new(&filepath).expect("Create new file");
            let st = File::fetch_stats(file.0).expect("Read file stats");

            assert!(File::validate_permission(&st), "File must have read/write permission");
        }
    }
}
