use crate::error::InternalResult;
use std::marker::PhantomData;

#[derive(Debug)]
pub(crate) struct TurboMMapView<T> {
    ptr: *mut T,
    _pd: PhantomData<T>,
}

impl<T> TurboMMapView<T> {
    #[inline]
    const fn new(ptr: *mut T) -> Self {
        Self { ptr, _pd: PhantomData }
    }

    #[inline]
    pub(crate) fn get(&self) -> &T {
        unsafe { &*self.ptr }
    }

    #[inline]
    pub(crate) fn update(&self, f: impl FnOnce(&mut T)) {
        unsafe { f(&mut *self.ptr) }
    }
}

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
    pub(crate) fn view<T>(&self, off: usize) -> TurboMMapView<T> {
        #[cfg(target_os = "linux")]
        unsafe {
            let ptr = self.mmap.read::<T>(off);
            TurboMMapView::new(ptr)
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }
}
