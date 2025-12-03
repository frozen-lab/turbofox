use crate::errors::{InternalError, InternalResult};
use libc::{c_void, mmap, msync, munmap, stat, MAP_FAILED, MAP_SHARED, MS_ASYNC, MS_SYNC, PROT_READ, PROT_WRITE};

#[derive(Debug, Clone)]
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
    pub(crate) unsafe fn new(fd: i32, len: usize) -> InternalResult<Self> {
        let ptr = mmap(std::ptr::null_mut(), len, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0i64);

        if ptr == MAP_FAILED {
            return Err(Self::_last_os_error());
        }

        Ok(Self { ptr, len })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn munmap(&self) -> InternalResult<()> {
        if munmap(self.ptr, self.len) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn masync(&self) -> InternalResult<()> {
        if msync(self.ptr, self.len, MS_ASYNC) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn msync(&self) -> InternalResult<()> {
        if msync(self.ptr, self.len, MS_SYNC) != 0 {
            return Err(Self::_last_os_error());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn write<T: Copy>(&self, off: usize, val: &T) {
        #[cfg(debug_assertions)]
        debug_assert_eq!(off % std::mem::align_of::<T>(), 0, "Detected unaligned access for type");

        let dst = (self.ptr as *mut u8).add(off) as *mut T;
        std::ptr::write(dst, *val);
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn read<T>(&self, off: usize) -> T {
        #[cfg(debug_assertions)]
        debug_assert_eq!(off % std::mem::align_of::<T>(), 0, "Detected unaligned access for type");

        let src = (self.ptr as *const u8).add(off) as *const T;
        std::ptr::read(src)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn read_mut<T>(&self, off: usize) -> *mut T {
        #[cfg(debug_assertions)]
        debug_assert_eq!(off % std::mem::align_of::<T>(), 0, "Detected unaligned access for type");

        (self.ptr as *mut u8).add(off) as *mut T
    }

    #[inline]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) const fn ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    #[inline]
    pub(crate) const fn ptr_mut(&self) -> *mut u8 {
        self.ptr as *mut u8
    }

    #[inline]
    fn _last_os_error() -> InternalError {
        std::io::Error::last_os_error().into()
    }
}

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::linux::file::File;
    use tempfile::TempDir;

    fn create_file(len: usize) -> (TempDir, std::path::PathBuf, File) {
        let dir = TempDir::new().expect("Tempdir");
        let path = dir.path().join("mmap_file");

        unsafe {
            let f = File::new(&path).expect("Create new file");
            f.zero_extend(len).expect("Zero extend new file");
            (dir, path, f)
        }
    }

    #[test]
    fn test_mmap_new_works() {
        let (_dir, _path, file) = create_file(0x1000);

        unsafe {
            let m = MMap::new(file.fd(), 0x1000).expect("Mmap");

            assert!(!m.ptr().is_null());
            assert_eq!(m.len(), 0x1000);
            m.munmap().expect("Munmap");
        }
    }

    #[test]
    fn test_unmap_works() {
        let (_dir, _path, file) = create_file(0x1000);

        unsafe {
            let m = MMap::new(file.fd(), 0x1000).expect("Mmap");
            assert!(m.munmap().is_ok());
        }
    }

    // HACK/TODO: This is an intresting feature, we'd not need to zero_extend and then re-map,
    // we could just re-map w/ larger area, intresting for future optimizations!
    #[test]
    fn test_mmap_new_works_for_len_larger_then_file_len() {
        let (_dir, _path, file) = create_file(0x1000);

        unsafe {
            let m = MMap::new(file.fd(), 0x2000).expect("Mmap should work even if len > filesize");

            assert!(!m.ptr().is_null());
            assert_eq!(m.len(), 0x2000);
            m.munmap().expect("Munmap");
        }
    }

    #[test]
    fn test_mmap_new_fails_on_zero_len() {
        let (_dir, _path, file) = create_file(0x1000);

        unsafe {
            assert!(MMap::new(file.fd(), 0).is_err(), "mmap(len=0) must fail");
        }
    }

    #[test]
    fn test_mmap_new_fails_on_invalid_fd() {
        let (_dir, _path, _file) = create_file(0x1000);

        unsafe {
            assert!(MMap::new(-1, 4096).is_err(), "mmap(invalid fd) must fail");
        }
    }

    #[test]
    fn test_mmap_new_fails_on_closed_fd() {
        let (_dir, _path, file) = create_file(0x1000);

        unsafe {
            file.close().expect("Close the file handle");
            assert!(MMap::new(file.fd(), 4096).is_err());
        }
    }

    #[test]
    fn test_mmap_write_read_cycle() {
        let (_dir, _path, file) = create_file(0x1000);
        let val: u64 = 0xDEADC0DEDEADC0DE;

        unsafe {
            let mmap = MMap::new(file.fd(), 0x1000).expect("Create new mmap");

            mmap.write(0, &val);
            assert_eq!(mmap.read::<u64>(0), val);

            mmap.munmap().expect("Munmap");
        }
    }

    #[test]
    fn test_mmap_read_works_after_update() {
        let (_dir, _path, file) = create_file(0x1000);
        let val1: u64 = 0xDEADC0DEDEADC0DE;
        let val2: u64 = 0xC0DEDEADC0DEDEAD;

        unsafe {
            let mmap = MMap::new(file.fd(), 0x1000).expect("Create new mmap");

            // write
            mmap.write(0, &val1);
            assert_eq!(mmap.read::<u64>(0), val1);

            // update
            mmap.write(0, &val2);
            assert_eq!(mmap.read::<u64>(0), val2);

            mmap.munmap().expect("Munmap");
        }
    }

    #[test]
    fn test_write_works_on_read_mut() {
        let (_dir, _path, file) = create_file(0x1000);
        let offset: usize = 64;
        let val: u64 = 0xDEADC0DEDEADC0DE;

        unsafe {
            let mmap = MMap::new(file.fd(), 0x1000).expect("Create new mmap");

            let ptr = mmap.read_mut::<u64>(offset);
            assert!(!ptr.is_null());
            *ptr = val;
            assert_eq!(mmap.read::<u64>(offset), val);

            mmap.munmap().expect("Munmap");
        }
    }

    #[test]
    fn test_ms_async_works() {
        let (_dir, _path, file) = create_file(0x1000);

        unsafe {
            let mmap = MMap::new(file.fd(), 0x1000).expect("Create new mmap");
            assert!(mmap.masync().is_ok());
            mmap.munmap().expect("Munmap");
        }
    }

    #[test]
    fn test_ms_sync_works() {
        let (_dir, path, file) = create_file(0x1000);
        let val: u64 = 0xFEEDFACEFEEDFACE;

        unsafe {
            let mmap = MMap::new(file.fd(), 0x1000).expect("Create new mmap");
            mmap.write(64, &val);
            mmap.msync().expect("ms_sync should succeed");
            mmap.munmap().expect("Munmap");
        }

        let data = std::fs::read(&path).expect("Read from file");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&data[64..72]);

        assert_eq!(u64::from_ne_bytes(buf), val);
    }

    #[test]
    fn test_persistence_after_close_and_reopen() {
        let (_dir, path, file) = create_file(0x1000);
        let val: u64 = 0xFEEDFACEFEEDFACE;

        unsafe {
            let mmap = MMap::new(file.fd(), 0x1000).expect("Create new mmap");
            mmap.write(64, &val);
            mmap.msync().expect("ms_sync should succeed");
            mmap.munmap().expect("Munmap");
            file.close().expect("Close file handle");
        }

        let data = std::fs::read(&path).expect("Read from file");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&data[64..72]);

        assert_eq!(u64::from_ne_bytes(buf), val);
    }

    #[test]
    fn test_ptr_ops_sanity() {
        let (_dir, _path, file) = create_file(0x1000);
        let val: u64 = 0xFEEDFACEFEEDFACE;

        unsafe {
            let mmap = MMap::new(file.fd(), 0x1000).expect("Create new mmap");
            let ptr = mmap.ptr();
            let ptr_mut = mmap.ptr_mut();

            assert!(!ptr.is_null());
            assert!(!ptr_mut.is_null());
            assert_eq!(ptr, ptr_mut as *const u8);

            let p = ptr_mut.add(128) as *mut u64;
            *p = val;

            assert_eq!(mmap.read::<u64>(128), val);
            mmap.munmap().expect("Munmap");
        }
    }

    #[test]
    fn test_instant_writeback_works_on_mmap() {
        let (_dir, _path, file) = create_file(0x1000);
        let val: u64 = 0xFEEDFACEFEEDFACE;

        unsafe {
            let mmap1 = MMap::new(file.fd(), 0x1000).expect("Create new mmap");
            let mmap2 = MMap::new(file.fd(), 0x1000).expect("Create new mmap");

            mmap1.write(0, &val);
            assert_eq!(mmap2.read::<u64>(0), val);

            mmap1.munmap().expect("Munmap");
            mmap2.munmap().expect("Munmap");
        }
    }
}
