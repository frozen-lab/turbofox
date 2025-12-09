use crate::{
    core::unlikely,
    error::{InternalError, InternalResult},
};
use libc::{
    close, fstat, fsync, ftruncate, getegid, geteuid, off_t, open, stat, O_CLOEXEC, O_CREAT, O_NOATIME, O_RDWR,
    O_TRUNC, S_IRGRP, S_IROTH, S_IRUSR, S_IWGRP, S_IWOTH, S_IWUSR,
};
use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

#[derive(Debug)]
pub(crate) struct File(i32);

impl File {
    /// Create a new `[file]` at given `path`
    pub(crate) unsafe fn new(path: &Path) -> InternalResult<Self> {
        let cpath = Self::path_to_cstring(path)?;

        let fd = open(cpath.as_ptr(), Self::prep_flags(true));
        if unlikely(fd < 0) {
            return Self::last_os_error();
        }

        Ok(Self(fd))
    }

    /// Open a new `[file]` at given `path`
    pub(crate) unsafe fn open(path: &Path) -> InternalResult<Self> {
        let cpath = Self::path_to_cstring(path)?;

        let fd = open(cpath.as_ptr(), Self::prep_flags(false));
        if unlikely(fd < 0) {
            return Self::last_os_error();
        }

        Ok(Self(fd))
    }

    /// Get file descriptor (fd) of `[file]`
    #[inline]
    pub(crate) fn fd(&self) -> i32 {
        self.0
    }

    /// Fetch current length for `[file]`
    pub(crate) unsafe fn len(&self) -> InternalResult<usize> {
        let st = self.fstat()?;
        Ok(st.st_size as usize)
    }

    /// Flushes dirty pages to disk for persistence of `[file]`
    pub(crate) unsafe fn fsync(&self) -> InternalResult<()> {
        let res = fsync(self.fd());
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// Zero extend length for `[file]`
    ///
    /// **WARN:** If `len` is smaller then the current length of the `[file]`,
    /// file length will be reduced!
    pub(crate) unsafe fn ftruncate(&self, len: usize) -> InternalResult<()> {
        let res = ftruncate(self.fd(), len as off_t);
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// Closes the opened `[file]` via fd
    pub(crate) unsafe fn close(&self) -> InternalResult<()> {
        let res = close(self.fd());
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// Syscall to fetch `[stat]` (i.e. Metadata) for `[File]`
    unsafe fn fstat(&self) -> InternalResult<stat> {
        let mut st = std::mem::zeroed::<stat>();
        let res = fstat(self.fd(), &mut st);
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(st)
    }

    #[allow(unused)]
    unsafe fn validate_io_permission(st: &stat) -> bool {
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

        readable && writable
    }

    /// Prepares kernel flags for syscall.
    ///
    /// ## Access Time Updates (O_NOATIME)
    ///
    /// We use the `O_NOATIME` flag to disable access time updates on the [File].
    /// Normally every I/O operation triggers an `atime` update/write to disk.
    ///
    /// This is counter productive and increases latency for I/O ops in our case!
    ///
    /// **WARN:** `O_NOATIME` flag only works when calling processes UID matches file owners UID. Also not all
    /// filesystems support this flag. In all such cases, this flag is silently ignored.
    #[inline]
    const fn prep_flags(is_new: bool) -> i32 {
        const BASE: i32 = O_RDWR | O_NOATIME | O_CLOEXEC;
        const NEW: i32 = O_CREAT | O_TRUNC;

        BASE | ((is_new as i32) * NEW)
    }

    #[inline]
    fn path_to_cstring(path: &Path) -> InternalResult<CString> {
        CString::new(path.as_os_str().as_bytes())
            .map_err(|e| InternalError::InvalidPath(format!("Error due to invalid Path: {e}")))
    }

    #[inline]
    fn last_os_error<T>() -> InternalResult<T> {
        Err(std::io::Error::last_os_error().into())
    }
}
