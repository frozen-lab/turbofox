use crate::{
    errors::{InternalError, InternalResult},
    InternalCfg,
};
use libc::{
    c_void, fstat, mmap, munmap, stat, MAP_FAILED, MAP_SHARED, PROT_READ, PROT_WRITE, SYNC_FILE_RANGE_WAIT_AFTER,
    SYNC_FILE_RANGE_WAIT_BEFORE,
};

pub(crate) struct MMap {
    ptr: *mut c_void,
    len: usize,
}

impl MMap {
    /// Create a new mmap (read & write) instance
    ///
    /// **NOTE**: w/ the use of `MAP_SHARED` flag, we offload the burden of sync/flush on the kernel,
    /// **WARN**: the sync/flush may not happen if the system crashed while the updates are in flight
    /// **IMP**: mmap must be preceded by fsync to sync any in flight updates
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn new(fd: i32, len: usize) -> InternalResult<Self> {
        let ptr = mmap(std::ptr::null_mut(), len, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0i64);

        if ptr == MAP_FAILED {
            return Err(Self::_last_os_error());
        }

        Ok(Self { ptr, len })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn sync_file(fd: i32) -> InternalResult<()> {
        // NOTE: Kernel treats `nbytes = 0` as "until EOF", which is what exactly we want!
        let res = libc::sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER);

        if res != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline]
    pub(crate) unsafe fn unmap(&self) -> InternalResult<()> {
        let res = munmap(self.ptr, self.len);

        if res != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline]
    pub(crate) unsafe fn fsync(&self, fd: i32) -> InternalResult<()> {
        let res = munmap(self.ptr, self.len);

        if res != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    fn _last_os_error() -> InternalError {
        let err = std::io::Error::last_os_error();
        err.into()
    }
}
