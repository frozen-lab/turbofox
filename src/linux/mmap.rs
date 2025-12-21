use crate::error::InternalResult;
use libc::{c_void, mmap, msync, munmap, off_t, MAP_FAILED, MAP_SHARED, MS_ASYNC, MS_SYNC, PROT_READ, PROT_WRITE};

#[derive(Debug, Clone)]
pub(crate) struct MMap {
    ptr: *mut c_void,
    len: usize,
}

impl MMap {
    /// Creates a new [MMap] instance w/ read + write permissions
    ///
    /// w/ the use of `MAP_SHARED` flag, we offload the burden of sync/flush on the kernel
    pub(crate) unsafe fn map(fd: i32, len: usize, off: usize) -> InternalResult<Self> {
        let ptr = mmap(
            std::ptr::null_mut(),
            len,
            PROT_WRITE | PROT_READ,
            MAP_SHARED,
            fd,
            off as off_t,
        );

        if ptr == MAP_FAILED {
            return Self::last_os_error();
        }

        Ok(Self { ptr, len })
    }

    /// Unmap [MMap] region
    #[inline]
    pub(crate) unsafe fn unmap(&self) -> InternalResult<()> {
        if munmap(self.ptr, self.len) != 0 {
            return Self::last_os_error();
        }
        Ok(())
    }

    /// Asynchronous flush for [MMap]
    #[inline]
    pub(crate) unsafe fn masync(&self) -> InternalResult<()> {
        if msync(self.ptr, self.len, MS_ASYNC) != 0 {
            return Self::last_os_error();
        }
        Ok(())
    }

    /// Synchronous flush for [MMap]
    #[inline]
    pub(crate) unsafe fn msync(&self) -> InternalResult<()> {
        if msync(self.ptr, self.len, MS_SYNC) != 0 {
            return Self::last_os_error();
        }
        Ok(())
    }

    /// Write to [MMap]
    #[inline]
    pub(crate) unsafe fn write<T: Clone>(&self, off: usize, val: &T) {
        #[cfg(test)]
        {
            let size = std::mem::size_of::<T>();
            let align = std::mem::align_of::<T>();

            debug_assert!(off + size <= self.len, "Offset must not exceed mmap size");
            debug_assert!(off % align == 0, "Detected unaligned access for type");
        }

        let dst = self.ptr_mut().add(off) as *mut T;
        std::ptr::write(dst, val.clone());
    }

    /// Read from `[MMap]`
    #[inline]
    pub(crate) unsafe fn read<T>(&self, off: usize) -> *mut T {
        #[cfg(test)]
        {
            let size = std::mem::size_of::<T>();
            let align = std::mem::align_of::<T>();

            debug_assert!(off + size <= self.len, "Offset must not exceed mmap size");
            debug_assert!(off % align == 0, "Detected unaligned access for type");
        }

        self.ptr_mut().add(off) as *mut T
    }

    #[inline]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    #[inline]
    const fn ptr_mut(&self) -> *mut u8 {
        self.ptr as *mut u8
    }

    #[inline]
    fn last_os_error<T>() -> InternalResult<T> {
        Err(std::io::Error::last_os_error().into())
    }
}
