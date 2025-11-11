use std::collections::linked_list;

use crate::errors::{InternalError, InternalResult};
use libc::{c_void, mmap, msync, munmap, stat, MAP_FAILED, MAP_SHARED, MS_ASYNC, MS_SYNC, PROT_READ, PROT_WRITE};

#[derive(Debug, Clone, Copy)]
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

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn write<T: Copy>(&self, off: usize, val: &T) {
        // sanity check
        debug_assert!(
            off + std::mem::size_of::<T>() <= self.len,
            "Offset must not exceed mmap size"
        );

        let dst = (self.ptr as *mut u8).add(off) as *mut T;
        std::ptr::write(dst, *val);
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn read<T: Copy>(&self, off: usize) -> T {
        // sanity check
        debug_assert!(
            off + std::mem::size_of::<T>() <= self.len,
            "Offset must not exceed mmap size"
        );

        let src = (self.ptr as *const u8).add(off) as *const T;
        std::ptr::read(src)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn read_mut<T>(&self, off: usize) -> *mut T {
        // sanity check
        debug_assert!(
            off + std::mem::size_of::<T>() <= self.len,
            "Offset must not exceed mmap size"
        );

        (self.ptr as *mut u8).add(off) as *mut T
    }

    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub(crate) fn ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    #[inline(always)]
    pub(crate) fn ptr_mut(&self) -> *mut u8 {
        self.ptr as *mut u8
    }

    #[inline]
    fn _last_os_error() -> InternalError {
        std::io::Error::last_os_error().into()
    }
}
