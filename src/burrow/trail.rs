use crate::{
    errors::{InternalError, InternalResult},
    linux::{file::File, mmap::MMap},
    logger::Logger,
    InternalCfg,
};

const VERSION: u32 = 0x01;
const MAGIC: [u8; 0x04] = *b"trl1";
const PATH: &'static str = "trail";

// sanity check
const _: () = assert!(
    (std::mem::size_of_val(&VERSION) + std::mem::size_of_val(&MAGIC)) % 0x08 == 0x00,
    "Must be 8 bytes aligned"
);

//
// Meta
//

#[derive(Debug, Copy, Clone)]
#[repr(C, align(0x20))]
struct Meta {
    magic: [u8; 0x04],
    version: u32,
    free: u64,
    nwords: u64,
    _padd: [u8; 0x08], // 8 bytes padding to align struct to 32 bytes
}

const META_SIZE: usize = std::mem::size_of::<Meta>();

impl Meta {
    const fn new(words: u64) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            nwords: words,
            free: words * 0x40,
            _padd: [0u8; 0x08],
        }
    }
}

// sanity check
const _: () = assert!(META_SIZE == 0x20, "Must be 32 bytes aligned");
const _: () = assert!(Meta::new(0x02).free == (0x02 * 0x40), "Must be correctly initialised");

//
// BitMap
//

#[repr(C, align(0x08))]
#[derive(Debug)]
struct BitMapPtr(u64);

//
// Trail
//

#[derive(Debug)]
pub(super) struct Trail {
    file: File,
    mmap: MMap,
    cfg: InternalCfg,
    meta_ptr: *mut Meta,
    bmap_ptr: *mut BitMapPtr,
}

impl Trail {
    /// Creates a new [Trail] file
    ///
    /// *NOTE* Returns an [IO] error if something goes wrong
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn new(cfg: &InternalCfg) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);
        let nwords = cfg.init_cap >> 0x06;
        let init_len = META_SIZE + (nwords << 0x03);

        // sanity checks
        debug_assert!(nwords * 0x40 == cfg.init_cap, "Must be u64 aligned");

        // create new file
        let file = File::new(&path)
            .inspect(|_| cfg.logger.trace("(TRAIL) [new] New file created"))
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) [new] Failed to create new file at {:?}: {e}", path));

                // NOTE: we must delete file if created, so new init could work w/o any issues
                File::del(&path).map_err(|e| {
                    cfg.logger
                        .warn(format!("(TRAIL) [new] Failed to delete the newly created file: {e}"));
                });

                e
            })?;

        // zero init the file
        file.zero_extend(init_len)
            .inspect(|_| cfg.logger.trace("(TRAIL) [new] Zero-Extended the file"))
            .map_err(|e| {
                cfg.logger.error(format!(
                    "(TRAIL) [new] Failed to zero extend new file at ({:?}): {e}",
                    path
                ));

                // NOTE: Close + Delete the created file, so new init could work w/o any issues
                //
                // HACK: We ignore error from `close_and_del` as we are already in an errored
                // state, and primary error is more imp then this!
                Self::close_and_del_file(&cfg, &file);

                e
            })?;

        let mmap = MMap::new(file.0, init_len)
            .inspect(|_| {
                cfg.logger
                    .trace(format!("(TRAIL) [new] MMap successful w/ len={init_len}"))
            })
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) [new] MMap Failed: {e}"));

                // NOTE: Close + Delete the created file, so new init could work w/o any issues
                //
                // HACK: We ignore error from `close_and_del` as we are already in an errored
                // state, and primary error is more imp then this!
                Self::close_and_del_file(&cfg, &file);

                e
            })?;

        // sanity check
        debug_assert_eq!(mmap.len(), init_len, "MMap len must be same as file len");

        // NOTE: we use `ms_sync` here to make sure metadata is persisted before
        // any other updates are conducted on the mmap,
        //
        // NOTE: we can afford this syscall here, as init does not come under the fast
        // path. Also it's just one time thing!
        mmap.write(0, &Meta::new(nwords as u64));
        mmap.ms_sync()
            .inspect(|_| cfg.logger.trace("(TRAIL) [new] MsSync successful on Meta"))
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) [new] Failed to write Meta on MMap: {e}"));
                e
            })?;

        let meta_ptr = mmap.read_mut::<Meta>(0);
        let bmap_ptr = mmap.read_mut::<BitMapPtr>(META_SIZE);

        cfg.logger.debug("(TRAIL) [new] New successfully completed");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            bmap_ptr,
            cfg: cfg.clone(),
        })
    }

    /// Open an existing [Trail] file
    ///
    /// *NOTE*: Returns an [InvalidFile] error when the underlying file is corrupted,
    /// may happen when the file is invalid or tampered with
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn open(cfg: &InternalCfg) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);

        // file must exists
        if !path.exists() {
            let err = InternalError::InvalidFile("Path does not exists".into());
            cfg.logger.error(format!("(TRAIL) [open] Invalid path: {err}"));
            return Err(err);
        }

        // open existing file (file handle)
        let file = File::open(&path)
            .inspect(|_| cfg.logger.trace("(TRAIL) [open] File open successful"))
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) [open] Failed to open file: {e}"));
                e
            })?;

        // existing file len (for mmap)
        let file_len = file
            .fstat()
            .inspect(|s| {
                cfg.logger.trace(format!(
                    "(TRAIL) [open] FStat success! Existing file has len={}",
                    s.st_size
                ))
            })
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) [open] FStat failed: {e}"));
                e
            })?
            .st_size as usize;

        // NOTE: File must always be os page aligned
        //
        // WARN: As this is a fatel scenerio, we delete the existing file, hence clean up the whole db,
        // as we can not simply make any sense of the data!
        let bmap_len = file_len.wrapping_sub(META_SIZE);
        if bmap_len == 0x00 || bmap_len & 0x07 != 0 {
            let err = InternalError::InvalidFile("File is not page aligned".into());
            cfg.logger
                .error(format!("(TRAIL) [open] Existing file is invalid: {err}"));

            // NOTE: Close + Delete the created file, so new init could work w/o any issues
            //
            // HACK: We ignore error from `close_and_del` as we are already in an errored
            // state, and primary error is more imp then this!
            Self::close_and_del_file(&cfg, &file);

            return Err(err);
        }

        let mmap = MMap::new(file.0, file_len)
            .inspect(|_| cfg.logger.trace("(TRAIL) [open] MMap successful"))
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) [open] MMap failed: {e}"));
                e
            })?;

        // sanity check
        debug_assert_eq!(mmap.len(), file_len, "MMap len must be same as file len");

        let meta_ptr = mmap.read_mut::<Meta>(0);
        let bmap_ptr = mmap.read_mut::<BitMapPtr>(META_SIZE);

        // metadata validations
        //
        // NOTE/TODO: In future, we need to support the old file versions, if any!
        if (*meta_ptr).magic != MAGIC || (*meta_ptr).version != VERSION {
            cfg.logger.warn("(TRAIL) [open] File has invalid VERSION or MAGIC");
        }

        cfg.logger.debug("(TRAIL) [open] open is successful");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            bmap_ptr,
            cfg: cfg.clone(),
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn extend_remap(&mut self) -> InternalResult<()> {
        let curr_nwords = (*self.meta_ptr).nwords;
        let slots_to_add = self.cfg.init_cap as u64;
        let nwords_to_add = slots_to_add >> 0x06;
        let new_len = self.mmap.len() + (nwords_to_add << 0x03) as usize;
        let new_nwords = curr_nwords + nwords_to_add as u64;

        // sanity checks
        debug_assert!(nwords_to_add > 0, "Words to be added must not be 0");
        debug_assert!(new_len > self.mmap.len(), "New len must be larger then current len");

        // STEP 1: Unmap
        self.mmap
            .unmap()
            .inspect(|_| self.cfg.logger.trace("(TRAIL) [extend_remap] Munmap successful"))
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("(TRAIL) [extend_remap] Munmap failed: {e}"));
                e
            })?;

        // STEP 2: Zero extend the file
        self.file
            .zero_extend(new_len)
            .inspect(|_| self.cfg.logger.trace("(TRIAL) [extend_remap] Zero extend successful"))
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("(TRAIL) [extend_remap] Failed on zero extend: {e}"));
                e
            })?;

        // STEP 3: Re-MMap
        self.mmap = MMap::new(self.file.0, new_len)
            .inspect(|_| self.cfg.logger.trace("(TRIAL) [extend_remap] Mmap successful"))
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("(TRAIL) [extend_remap] MMap Failed: {e}"));
                e
            })?;
        self.meta_ptr = self.mmap.read_mut::<Meta>(0);
        self.bmap_ptr = self.mmap.read_mut::<BitMapPtr>(META_SIZE);

        // STEP 4: Update & Sync Meta
        (*self.meta_ptr).nwords = new_nwords;
        (*self.meta_ptr).free += slots_to_add;
        self.mmap
            .ms_sync()
            .inspect(|_| self.cfg.logger.trace("(TRIAL) [extend_remap] MsSync Successful"))
            .map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("(TRAIL) [extend_remap] Failed to write Metadata: {e}"));
                e
            })?;

        Ok(())
    }

    /// Close & Delete [Trail] file
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn close_and_del_file(cfg: &InternalCfg, file: &File) {
        let path = cfg.dirpath.join(PATH);

        // close the file handle (NOTE: always before the delete)
        let res = file.close().map_err(|e| {
            cfg.logger
                .warn(format!("(TRAIL) [close_and_del] Failed to close the file: {e}"));
            e
        });

        // NOTE: We can only delete the file, if file fd is released or closed, e.g. on windows
        if res.is_ok() {
            File::del(&path).map_err(|e| {
                cfg.logger
                    .warn(format!("(TRAIL) [close_and_del] Failed to delete the file: {e}"));
            });
        }
    }
}

impl Drop for Trail {
    fn drop(&mut self) {
        unsafe {
            let mut is_err = false;

            // flush dirty pages
            self.mmap
                .ms_sync()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) [drop] Fsync successful for mmap");
                })
                .map_err(|e| {
                    is_err = true;
                    self.cfg
                        .logger
                        .warn(format!("(TRAIL) [drop] Failed to fsync on mmap: {e}"));
                });

            // munmap the memory mappings
            self.mmap
                .unmap()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) [drop] Mummap successful for mmap");
                })
                .map_err(|e| {
                    is_err = true;
                    self.cfg.logger.warn(format!("(TRAIL) [drop] Failed to munmap: {e}"));
                });

            // close the file descriptor
            self.file
                .close()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) [drop] Closed the file fd");
                })
                .map_err(|e| {
                    is_err = true;
                    self.cfg
                        .logger
                        .warn(format!("(TRAIL) [drop] Failed to close the file fd: {e}"));
                });

            if !is_err {
                self.cfg.logger.debug("(TRAIL) [drop] Dropped Successfully!");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logger::init_test_logger;
    use tempfile::TempDir;

    fn temp_dir() -> TempDir {
        let _ = init_test_logger(None);
        TempDir::new().expect("temp dir")
    }

    mod trail {
        use super::*;

        #[test]
        fn test_new_is_valid() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");
            let nwords = cfg.init_cap >> 0x06;
            let init_len = META_SIZE + (nwords << 0x03);
            let t1 = unsafe { Trail::new(&cfg) }.expect("new trail");

            unsafe {
                let meta = (*t1.meta_ptr);

                assert!(t1.file.0 >= 0x00, "File fd must be valid");
                assert!(t1.mmap.len() > 0x00, "Mmap must be non zero");

                assert_eq!(meta.magic, MAGIC, "Correct file MAGIC");
                assert_eq!(meta.version, VERSION, "Correct file VERSION");
                assert_eq!(meta.nwords, nwords as u64);
                assert_eq!(meta.free, cfg.init_cap as u64);

                assert!(!t1.meta_ptr.is_null());
                assert!(!t1.bmap_ptr.is_null());
            }
        }

        #[test]
        fn test_open_is_valid() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let t0 = Trail::new(&cfg).expect("new trail");

                (*t0.meta_ptr).free = 0x01;
                (*t0.meta_ptr).nwords = 0x02;

                drop(t0);
            }

            let t1 = unsafe { Trail::open(&cfg) }.expect("open existing");

            unsafe {
                let meta = (*t1.meta_ptr);

                assert!(t1.file.0 >= 0x00, "File fd must be valid");
                assert!(t1.mmap.len() > 0x00, "Mmap must be non zero");

                assert_eq!(meta.magic, MAGIC, "Correct file MAGIC");
                assert_eq!(meta.version, VERSION, "Correct file VERSION");
                assert_eq!(meta.nwords, 0x02);
                assert_eq!(meta.free, 0x01);

                assert!(!t1.meta_ptr.is_null());
                assert!(!t1.bmap_ptr.is_null());
            }
        }

        #[test]
        fn test_open_panics_on_invalid_metadata_in_file() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let t0 = unsafe { Trail::new(&cfg) }.expect("new trail");
                t0.file.zero_extend(META_SIZE).expect("Update file len");
            }

            // should panic
            assert!(unsafe { Trail::open(&cfg) }.is_err());
        }

        #[test]
        fn test_extend_remap_grows_file_and_updates_meta() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");

                let og_nwords = (*trail.meta_ptr).nwords;
                let og_free = (*trail.meta_ptr).free;
                let og_len = trail.mmap.len();

                trail.extend_remap().expect("extend");

                let meta = *trail.meta_ptr;

                assert!(!trail.bmap_ptr.is_null());
                assert_eq!(meta.free, og_free + cfg.init_cap as u64);
                assert!(trail.mmap.len() > og_len, "mmap len must grow");
                assert_eq!(meta.nwords, og_nwords + (cfg.init_cap >> 6) as u64);
            }
        }

        #[test]
        fn test_extend_remap_twice_accumulates_meta_correctly() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");

                let nw0 = (*trail.meta_ptr).nwords;
                let fr0 = (*trail.meta_ptr).free;
                let inc = (cfg.init_cap >> 6) as u64;

                trail.extend_remap().expect("first extend works");
                let w1 = (*trail.meta_ptr).nwords;
                let f1 = (*trail.meta_ptr).free;

                assert_eq!(w1, nw0 + inc);
                assert_eq!(f1, fr0 + cfg.init_cap as u64);

                trail.extend_remap().expect("second extend works");
                let w2 = (*trail.meta_ptr).nwords;
                let f2 = (*trail.meta_ptr).free;

                assert_eq!(w2, nw0 + inc * 2);
                assert_eq!(f2, fr0 + cfg.init_cap as u64 * 2);
            }
        }
    }
}
