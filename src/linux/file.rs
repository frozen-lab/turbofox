use crate::{
    error::{InternalError, InternalResult},
    utils::unlikely,
};
use libc::{
    c_void, close, fstat, fsync, ftruncate, off_t, open, pread, pwrite, size_t, stat, O_CLOEXEC, O_CREAT, O_NOATIME,
    O_RDWR, O_TRUNC,
};
use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

#[derive(Debug)]
pub(crate) struct File(i32);

impl File {
    /// Creates a new [File] at given `Path`
    pub(crate) unsafe fn new(path: &Path) -> InternalResult<Self> {
        let cpath = Self::path_to_cstring(path)?;
        let fd = open(cpath.as_ptr(), Self::prep_flags(true));
        if unlikely(fd < 0) {
            return Self::last_os_error();
        }

        Ok(Self(fd))
    }

    /// Opens an existing [File] at given `Path`
    pub(crate) unsafe fn open(path: &Path) -> InternalResult<Self> {
        let cpath = Self::path_to_cstring(path)?;
        let fd = open(cpath.as_ptr(), Self::prep_flags(false));
        if unlikely(fd < 0) {
            return Self::last_os_error();
        }

        Ok(Self(fd))
    }

    /// Gets file descriptor (fd) of [File]
    #[inline]
    pub(crate) const fn fd(&self) -> i32 {
        self.0
    }

    /// Fetches current length for [File]
    pub(crate) unsafe fn len(&self) -> InternalResult<usize> {
        let st = self.stats()?;
        Ok(st.st_size as usize)
    }

    /// Flushes dirty pages to disk for persistence of [File]
    pub(crate) unsafe fn sync(&self) -> InternalResult<()> {
        let res = fsync(self.fd());
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// truncates or extends length for [File] w/ zero bytes
    ///
    /// **WARN:** If `len` is smaller then the current length of [File], the file length will be reduced
    pub(crate) unsafe fn ftruncate(&self, len: usize) -> InternalResult<()> {
        let res = ftruncate(self.fd(), len as off_t);
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// Closes the [File] via fd
    pub(crate) unsafe fn close(&self) -> InternalResult<()> {
        let res = close(self.fd());
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// Fetches [stat] (i.e. Metadata) via syscall for [File]
    #[inline]
    unsafe fn stats(&self) -> InternalResult<stat> {
        let mut st = std::mem::zeroed::<stat>();
        let res = fstat(self.fd(), &mut st);
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(st)
    }

    /// Performs positional read on [File]
    pub(crate) unsafe fn pread(&self, off: usize, buf_size: usize) -> InternalResult<Vec<u8>> {
        let mut buf = vec![0u8; buf_size];
        let mut done = 0usize;

        let ptr = buf.as_mut_ptr() as *mut c_void;

        while done < buf_size {
            let res = pread(
                self.fd(),
                ptr.add(done),
                (buf_size - done) as size_t,
                (off + done) as i64,
            );

            if res == 0 {
                return Err(InternalError::IO("unexpected EOF during pread".into()));
            }

            if res < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }

                return Err(err.into());
            }

            done += res as usize;
        }

        Ok(buf)
    }

    /// Performs positional write on [File]
    pub(crate) unsafe fn pwrite(&self, off: usize, buf: &[u8]) -> InternalResult<()> {
        let ptr = buf.as_ptr() as *mut c_void;
        let count = buf.len() as size_t;

        let mut done = 0usize;
        while done < count {
            let res = pwrite(
                self.fd(),
                ptr.add(done) as *const c_void,
                (count - done) as size_t,
                (off + done) as i64,
            );

            if res < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }

                return Err(err.into());
            }

            if res == 0 {
                return Err(InternalError::IO("pwrite returned 0".into()));
            }

            done += res as usize;
        }

        Ok(())
    }

    /// Prepares kernel flags for syscall
    ///
    /// ## Access Time Updates (O_NOATIME)
    ///
    /// We use the `O_NOATIME` flag to disable access time updates on the [File]
    /// Normally every I/O operation triggers an `atime` update/write to disk
    ///
    /// This is counter productive and increases latency for I/O ops in our case!
    ///
    /// *NOTE:* Not all filesystems support this flag. In all such cases, this flag is silently ignored
    /// *WARN:* This flag only works when UID's match for calling processe and file owner
    #[inline]
    const fn prep_flags(is_new: bool) -> i32 {
        const BASE: i32 = O_RDWR | O_NOATIME | O_CLOEXEC;
        const NEW: i32 = O_CREAT | O_TRUNC;
        BASE | ((is_new as i32) * NEW)
    }

    #[inline]
    fn path_to_cstring(path: &Path) -> InternalResult<CString> {
        CString::new(path.as_os_str().as_bytes())
            .map_err(|e| InternalError::IO(format!("Error due to invalid Path: {e}")))
    }

    #[inline]
    fn last_os_error<T>() -> InternalResult<T> {
        Err(std::io::Error::last_os_error().into())
    }
}
