use crate::errors::{InternalError, InternalResult};
use libc::{
    c_void, fstat, mmap, stat, MAP_FAILED, MAP_SHARED, PROT_READ, PROT_WRITE, SYNC_FILE_RANGE_WAIT_AFTER,
    SYNC_FILE_RANGE_WAIT_BEFORE,
};

pub(crate) struct MMap(*mut c_void);

impl MMap {
    /// Create a new mmap (read & write) instance
    ///
    /// **NOTE**: w/ the use of `MAP_SHARED` flag, we offload the burden of sync/flush on the kernel,
    /// **WARN**: the sync/flush may not happen if the system crashed while the updates are in flight
    /// **IMP**: mmap must be preceded by fsync to sync any in flight updates
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn new(fd: i32, len: usize) -> InternalResult<Self> {
        let res = mmap(std::ptr::null_mut(), len, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0i64);

        if res == MAP_FAILED {
            return Err(Self::_last_os_error());
        }

        Ok(Self(res))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn fstat_len(fd: i32) -> InternalResult<usize> {
        let mut stat = std::mem::zeroed::<stat>();
        let res = fstat(fd, &mut stat);

        if res != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(stat.st_size as usize)
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

    fn _last_os_error() -> InternalError {
        let err = std::io::Error::last_os_error();
        err.into()
    }
}

impl Drop for MMap {
    fn drop(&mut self) {
        
    }
}
