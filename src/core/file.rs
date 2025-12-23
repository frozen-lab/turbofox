use crate::error::InternalResult;

#[derive(Debug)]
pub(crate) struct TurboFile {
    #[cfg(target_os = "linux")]
    file: crate::linux::File,

    #[cfg(not(target_os = "linux"))]
    file: (),
}

impl TurboFile {
    #[inline]
    pub(crate) fn new(path: &std::path::Path) -> InternalResult<Self> {
        #[cfg(not(target_os = "linux"))]
        let file = ();

        #[cfg(target_os = "linux")]
        let file = unsafe { crate::linux::File::new(path) }?;

        Ok(Self { file })
    }

    #[inline]
    pub(crate) fn open(path: &std::path::Path) -> InternalResult<Self> {
        #[cfg(not(target_os = "linux"))]
        let file = ();

        #[cfg(target_os = "linux")]
        let file = unsafe { crate::linux::File::open(path) }?;

        Ok(Self { file })
    }

    #[inline]
    pub(crate) fn close(&self) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.close()
        }
    }

    #[inline]
    pub(crate) fn flush(&self) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.sync()
        }
    }

    #[inline]
    pub(crate) fn zero_extend(&self, new_len: usize) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.ftruncate(new_len)
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> InternalResult<usize> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.len()
        }
    }

    #[inline]
    pub(crate) fn read(&self, off: usize, buf_size: usize) -> InternalResult<Vec<u8>> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.pread(off, buf_size)
        }
    }

    #[inline]
    pub(crate) fn write(&self, off: usize, buf: &[u8]) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.pwrite(off, buf)
        }
    }

    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) const fn fd(&self) -> i32 {
        self.file.fd()
    }
}
