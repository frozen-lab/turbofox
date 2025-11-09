use crate::errors::{InternalError, InternalResult};
use libc::{c_void, mmap, msync, munmap, stat, MAP_FAILED, MAP_SHARED, MS_ASYNC, MS_SYNC, PROT_READ, PROT_WRITE};

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
    #[inline]
    pub(crate) unsafe fn unmap(&self) -> InternalResult<()> {
        if munmap(self.ptr, self.len) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline]
    pub(crate) unsafe fn ms_async(&self) -> InternalResult<()> {
        if msync(self.ptr, self.len, MS_ASYNC) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline]
    pub(crate) unsafe fn ms_sync(&self) -> InternalResult<()> {
        if msync(self.ptr, self.len, MS_SYNC) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[inline]
    fn _last_os_error() -> InternalError {
        std::io::Error::last_os_error().into()
    }
}
