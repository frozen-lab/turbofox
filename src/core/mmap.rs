use super::TurboFile;
use crate::{errors::InternalResult, TurboConfig};

#[derive(Debug)]
pub(crate) struct TurboMMap {
    len: usize,
    cfg: TurboConfig,
    target: &'static str,

    #[cfg(target_os = "linux")]
    mmap: crate::linux::MMap,

    #[cfg(not(target_os = "linux"))]
    mmap: (),
}

impl TurboMMap {
    pub(crate) fn new(cfg: &TurboConfig, target: &'static str, file: &TurboFile, len: usize) -> InternalResult<Self> {
        // sanity check
        debug_assert!(len > 0x00, "Length must not be zero");

        #[cfg(target_os = "linux")]
        let mmap = unsafe { Self::mmap_linux(file, len, cfg, target) }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        cfg.logger.debug(format!("({target}) [mmap] TurboMMap created"));

        Ok(Self {
            len,
            mmap,
            target,
            cfg: cfg.clone(),
        })
    }

    pub(crate) fn munmap(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe { self.munmap_linux() }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        self.cfg
            .logger
            .debug(format!("({}) [munmap] TurboMMap is unmapped", self.target));

        Ok(())
    }

    pub(crate) fn masync(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe { self.masync_linux() }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        self.cfg
            .logger
            .debug(format!("({}) [masync] masync on TurboMMap", self.target));

        Ok(())
    }

    pub(crate) fn msync(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe { self.msync_linux() }?;

        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        self.cfg
            .logger
            .debug(format!("({}) [msync] msync on TurboMMap", self.target));

        Ok(())
    }

    #[inline]
    pub(crate) const fn len(&self) -> usize {
        #[cfg(target_os = "linux")]
        return self.mmap.len();

        #[cfg(not(target_os = "linux"))]
        unimplemented!();
    }

    pub(crate) fn write<T: Copy>(&self, off: usize, val: &T) {
        #[cfg(target_os = "linux")]
        unsafe {
            self.mmap.write(off, val);
        };

        #[cfg(not(target_os = "linux"))]
        unimplemented!();
    }

    pub(crate) fn read<T>(&self, off: usize) -> T {
        #[cfg(target_os = "linux")]
        return unsafe { self.mmap.read(off) };

        #[cfg(not(target_os = "linux"))]
        unimplemented!();
    }

    pub(crate) fn read_mut<T>(&self, off: usize) -> *mut T {
        #[cfg(target_os = "linux")]
        return unsafe { self.mmap.read_mut(off) };

        #[cfg(not(target_os = "linux"))]
        unimplemented!();
    }

    #[inline]
    pub(crate) const fn ptr(&self) -> *const u8 {
        #[cfg(target_os = "linux")]
        return unsafe { self.mmap.ptr() };

        #[cfg(not(target_os = "linux"))]
        unimplemented!();
    }

    #[inline]
    pub(crate) const fn ptr_mut(&self) -> *mut u8 {
        #[cfg(target_os = "linux")]
        return unsafe { self.mmap.ptr_mut() };

        #[cfg(not(target_os = "linux"))]
        unimplemented!();
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_linux(
        file: &TurboFile,
        len: usize,
        cfg: &TurboConfig,
        target: &'static str,
    ) -> InternalResult<crate::linux::MMap> {
        crate::linux::MMap::new(file.fd(), len)
            .inspect(|m| {
                cfg.logger
                    .trace(format!("({target}) [mmap] TurboMMap created w/ len={}", m.len()))
            })
            .map_err(|e| {
                cfg.logger
                    .error(format!("({target}) [mmap] Failed to create TurboMMap: {e}"));
                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn munmap_linux(&self) -> InternalResult<()> {
        self.mmap
            .munmap()
            .inspect(|m| {
                self.cfg
                    .logger
                    .trace(format!("({}) [munmap] TurboMMap is unmapped", self.target))
            })
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("({}) [munmap] Failed to unmap TurboMMap: {e}", self.target));
                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn masync_linux(&self) -> InternalResult<()> {
        self.mmap
            .masync()
            .inspect(|m| {
                self.cfg
                    .logger
                    .trace(format!("({}) [masync] masync on TurboMMap", self.target))
            })
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("({}) [masync] masync failed on TurboMMap: {e}", self.target));
                e
            })
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn msync_linux(&self) -> InternalResult<()> {
        self.mmap
            .msync()
            .inspect(|m| {
                self.cfg
                    .logger
                    .trace(format!("({}) [msync] msync on TurboMMap", self.target))
            })
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("({}) [msync] msync failed on TurboMMap: {e}", self.target));
                e
            })
    }
}

impl Drop for TurboMMap {
    fn drop(&mut self) {
        // sanity check
        //
        // NOTE: This is required as we already unmap the mmap in some tests manually, so for this
        // tests, our `drop` ends up creating warning that the process failed!
        if !self.mmap.ptr().is_null() {
            return;
        }

        unsafe {
            let mut is_err = false;

            // sync the mmap (save and exit)
            is_err = self.msync().is_err();

            // unmap
            is_err = self.munmap().is_err();

            if is_err {
                self.cfg
                    .logger
                    .warn(format!("{} [drop] Failed to drop TurboMMap", self.target));
            } else {
                self.cfg
                    .logger
                    .trace(format!("{} [drop] Dropped TurboMMap", self.target));
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{linux::File as LFile, TurboConfig};
    use tempfile::TempDir;

    const PATH: &'static str = "TurboMMap";

    fn tmp_file(len: usize, target: &'static str) -> (TempDir, TurboConfig, TurboFile) {
        let (cfg, dir) = TurboConfig::test_cfg(target);
        let file = TurboFile::new(&cfg, PATH).expect("New turbo file");
        file.zero_extend(len, true).expect("Zero extend");
        (dir, cfg, file)
    }

    #[test]
    fn test_new_works() {
        let (_dir, cfg, file) = tmp_file(0x1000, "new_works");
        let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");

        assert!(!mmap.ptr().is_null());
        assert_eq!(mmap.len(), 0x1000);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn test_new_fails_on_zero_len() {
        let (_dir, cfg, file) = tmp_file(0x1000, "new_fails_w/_zero_len");
        let mmap = TurboMMap::new(&cfg, PATH, &file, 0x00);
        assert!(mmap.is_err());
    }

    #[test]
    fn test_new_fails_on_closed_fd() {
        unsafe {
            let (_dir, cfg, file) = tmp_file(0x1000, "new_fails_w/_close_fd");
            file.close().expect("Close file");
            let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000);
            assert!(mmap.is_err());
        }
    }

    #[test]
    fn test_munmap_works() {
        let (_dir, cfg, file) = tmp_file(0x1000, "munmap_works");
        let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");
        assert!(mmap.munmap().is_ok());
    }

    #[test]
    fn test_write_read_cycle() {
        let (_dir, cfg, file) = tmp_file(0x1000, "write_read_cycle");
        let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");
        let val: u64 = 0xDEADC0DEDEADC0DE;

        mmap.write(0, &val);
        assert_eq!(mmap.read::<u64>(0), val);
    }

    #[test]
    fn test_read_works_after_update() {
        let (_dir, cfg, file) = tmp_file(0x1000, "read_works");
        let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");

        let v1: u64 = 0xAAAA_BBBB_CCCC_DDDD;
        let v2: u64 = 0x1111_2222_3333_4444;

        mmap.write(64, &v1);
        assert_eq!(mmap.read::<u64>(64), v1);

        mmap.write(64, &v2);
        assert_eq!(mmap.read::<u64>(64), v2);
    }

    #[test]
    fn test_read_mut_ptr_write_back() {
        unsafe {
            let (_dir, cfg, file) = tmp_file(0x1000, "read_mut_ptr_write_back");
            let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");

            let val: u64 = 0xCAFEBABECAFEBABE;

            let p = mmap.read_mut::<u64>(128);
            *p = val;

            assert_eq!(mmap.read::<u64>(128), val);
        }
    }

    #[test]
    fn test_masync_works() {
        let (_dir, cfg, file) = tmp_file(0x1000, "msync_works");
        let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");
        assert!(mmap.masync().is_ok());
    }

    #[test]
    fn test_msync_writes_back_to_disk() {
        let (_dir, cfg, file) = tmp_file(0x1000, "msync_writes_back");
        let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");
        let val: u64 = 0xFFFF_EEEE_DDDD_CCCC;

        mmap.write(128, &val);
        mmap.msync().expect("MSync");
        mmap.munmap().expect("unmap");

        let data = std::fs::read(cfg.dirpath.join(PATH)).expect("Read from file");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&data[128..136]);

        assert_eq!(u64::from_ne_bytes(buf), val);
    }

    #[test]
    fn test_persistence_across_close_reopen() {
        unsafe {
            let (_dir, cfg, file) = tmp_file(0x1000, "save_w/_reopen");
            let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");
            let val: u64 = 0xABCD_EF01_ABCD_EF01;

            mmap.write(32, &val);
            mmap.msync().expect("MSync");
            mmap.munmap().expect("unmap");
            file.close().expect("Close file");

            let data = std::fs::read(cfg.dirpath.join(PATH)).expect("Read from file");
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&data[32..40]);

            assert_eq!(u64::from_ne_bytes(buf), val);
        }
    }

    #[test]
    fn test_ptr_ops_sanity() {
        unsafe {
            let (_dir, cfg, file) = tmp_file(0x1000, "ptr_ops");
            let mmap = TurboMMap::new(&cfg, PATH, &file, 0x1000).expect("Create new TurboMMap");

            let val: u64 = 0x1234_5678_ABCD_EF00;
            let p = mmap.ptr_mut();

            let q = p.add(256) as *mut u64;
            *q = val;

            assert_eq!(mmap.read::<u64>(256), val);
        }
    }

    #[test]
    fn test_instant_write_propagation_between_mmaps() {
        let (_dir, cfg, file) = tmp_file(0x1000, "instant_write");
        let v: u64 = 0xCAFED00DCAFED00D;

        let m1 = TurboMMap::new(&cfg, "mmap_share", &file, 0x1000).expect("Create new TurboMMap");
        let m2 = TurboMMap::new(&cfg, "mmap_share", &file, 0x1000).expect("Create new TurboMMap");

        m1.write(0, &v);
        assert_eq!(m2.read::<u64>(0), v);
    }
}
