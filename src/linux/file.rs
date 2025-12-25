use crate::error::{InternalError, InternalResult};
use libc::{
    c_int, c_void, close, fdatasync, fstat, ftruncate, off_t, open, pread, pwrite, size_t, stat, EPERM, O_CLOEXEC,
    O_CREAT, O_NOATIME, O_RDWR, O_TRUNC,
};
use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

#[derive(Debug)]
pub(crate) struct File(i32);

unsafe impl Send for File {}
unsafe impl Sync for File {}

impl File {
    /// Creates a new [File] at given `Path`
    pub(crate) unsafe fn new(path: &Path) -> InternalResult<Self> {
        let fd = Self::open_with_flags(path, Self::prep_flags(true))?;
        Ok(Self(fd))
    }

    /// Opens an existing [File] at given `Path`
    pub(crate) unsafe fn open(path: &Path) -> InternalResult<Self> {
        let fd = Self::open_with_flags(path, Self::prep_flags(false))?;
        Ok(Self(fd))
    }

    /// Gets file descriptor (fd) of [File]
    #[inline]
    pub(crate) const fn fd(&self) -> i32 {
        self.0
    }

    /// Fetches current length for [File]
    #[inline]
    pub(crate) unsafe fn len(&self) -> InternalResult<usize> {
        let st = self.stats()?;
        Ok(st.st_size as usize)
    }

    /// Flushes dirty pages to disk for persistence of [File]
    #[inline]
    pub(crate) unsafe fn sync(&self) -> InternalResult<()> {
        let res = fdatasync(self.fd() as c_int);
        if res != 0 {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// truncates or extends length for [File] w/ zero bytes
    ///
    /// **WARN:** If `len` is smaller then the current length of [File], the file length will be reduced
    #[inline]
    pub(crate) unsafe fn ftruncate(&self, len: usize) -> InternalResult<()> {
        let res = ftruncate(self.fd(), len as off_t);
        if res != 0 {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// Closes the [File] via fd
    #[inline]
    pub(crate) unsafe fn close(&self) -> InternalResult<()> {
        let res = close(self.fd());
        if res != 0 {
            return Self::last_os_error();
        }

        Ok(())
    }

    /// Performs positional read on [File]
    pub(crate) unsafe fn pread(&self, off: usize, buf_size: usize) -> InternalResult<Vec<u8>> {
        let mut buf = vec![0u8; buf_size];
        let mut done = 0usize;

        let ptr = buf.as_mut_ptr();

        while done < buf_size {
            let res = pread(
                self.fd(),
                ptr.add(done) as *mut c_void,
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
        let ptr = buf.as_ptr();
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

    /// Fetches [stat] (i.e. Metadata) via syscall for [File]
    #[inline]
    unsafe fn stats(&self) -> InternalResult<stat> {
        let mut st = std::mem::zeroed::<stat>();
        let res = fstat(self.fd(), &mut st);
        if res != 0 {
            return Self::last_os_error();
        }

        Ok(st)
    }

    /// Creates/opens a [File] w/ provided `flags`
    ///
    /// ## Limitations on Use of `O_NOATIME` (`EPERM` Error)
    ///
    /// `open()` with `O_NOATIME` may fail with `EPERM` instead of silently ignoring the flag
    ///
    /// `EPERM` indicates a kernel level permission violation, as the kernel rejects the
    /// request outright, even though the flag only affects metadata behavior
    ///
    /// To remain sane across ownership models, containers, and shared filesystems,
    /// we explicitly retry the `open()` w/o `O_NOATIME` when `EPERM` is encountered
    #[inline]
    unsafe fn open_with_flags(path: &Path, flags: i32) -> InternalResult<i32> {
        let cpath = File::path_to_cstring(path)?;

        let fd = open(cpath.as_ptr(), flags);
        if fd >= 0 {
            return Ok(fd);
        }

        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(EPERM) {
            #[cfg(test)]
            debug_assert!((flags & O_NOATIME) != 0, "O_NOATIME flag is not being used");

            let fd = open(cpath.as_ptr(), flags & !O_NOATIME);
            if fd >= 0 {
                return Ok(fd);
            }
        }

        Err(err.into())
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
    /// ## Limitations of `O_NOATIME`
    ///
    /// Not all filesystems support this flag, on many it is silently ignored, but some rejects
    /// it with `EPERM` error
    ///
    /// Also, this flag only works when UID's match for calling processe and file owner
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
