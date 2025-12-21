use crate::error::InternalResult;

#[derive(Debug)]
pub(crate) struct TurboMMap {
    #[cfg(target_os = "linux")]
    mmap: crate::linux::MMap,

    #[cfg(not(target_os = "linux"))]
    mmap: (),
}

impl TurboMMap {
    #[inline]
    pub(crate) fn new(fd: i32, len: usize, off: usize) -> InternalResult<Self> {
        #[cfg(target_os = "linux")]
        let mmap = unsafe { crate::linux::MMap::map(fd, len, off) }?;

        #[cfg(not(target_os = "linux"))]
        let mmap = ();

        Ok(Self { mmap })
    }

    #[inline]
    pub(crate) fn unmap(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe {
            self.mmap.unmap()
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    #[inline]
    pub(crate) fn flush(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe {
            self.mmap.masync()
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    #[inline]
    pub(crate) const fn len(&self) -> usize {
        #[cfg(target_os = "linux")]
        return self.mmap.len();

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    #[inline]
    pub(crate) fn write<T: Copy>(&self, val: &T, off: usize) {
        #[cfg(target_os = "linux")]
        unsafe {
            self.mmap.write(off, val);
        };

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    #[inline]
    fn read<T>(&self, off: usize) -> *mut T {
        #[cfg(target_os = "linux")]
        unsafe {
            self.mmap.read(off)
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }
}
