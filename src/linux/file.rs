use crate::errors::{InternalError, InternalResult};
use libc::{
    close, fstat, fsync, ftruncate, off_t, open, stat, sync_file_range, O_CLOEXEC, O_CREAT, O_NOATIME, O_RDWR, O_TRUNC,
    SYNC_FILE_RANGE_WAIT_AFTER, SYNC_FILE_RANGE_WAIT_BEFORE,
};
use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

pub(crate) struct File(pub(crate) i32);

impl File {
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn new(path: &Path) -> InternalResult<Self> {
        let fd = open(
            Self::get_ffi_valid_path(path)?,
            // read, write, create & truncate
            O_RDWR | O_CREAT | O_TRUNC | O_NOATIME | O_CLOEXEC,
            // file permission mode (used for O_CREATE)
            0o644,
        );

        if fd < 0 {
            return Err(Self::_last_os_error());
        }

        Ok(Self(fd))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn open(path: &Path) -> InternalResult<Self> {
        let fd = open(Self::get_ffi_valid_path(path)?, O_RDWR | O_NOATIME | O_CLOEXEC);

        if fd < 0 {
            return Err(Self::_last_os_error());
        }

        Ok(Self(fd))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn fstat(&self) -> InternalResult<stat> {
        let mut stat = std::mem::zeroed::<stat>();
        let res = fstat(self.0, &mut stat);

        if res != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(stat)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn fsync(&self) -> InternalResult<()> {
        let res = fsync(self.0);

        if res != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn zero_extend(&self, new_len: usize) -> InternalResult<()> {
        let res = ftruncate(self.0, new_len as off_t);

        if res != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn close(&self) -> InternalResult<()> {
        let res = close(self.0);

        if res != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[inline]
    fn get_ffi_valid_path(path: &Path) -> InternalResult<*const i8> {
        let path = CString::new(path.as_os_str().as_bytes());

        // sanity check
        debug_assert!(path.is_ok());

        // NOTE: This can only occur when the static path is invalid!
        // This error is also protected by the above sanity check
        if path.is_err() {
            return Err(InternalError::Misc("Invalid file path".into()));
        }

        Ok(path.unwrap().as_ptr())
    }

    #[inline]
    fn _last_os_error() -> InternalError {
        std::io::Error::last_os_error().into()
    }
}
