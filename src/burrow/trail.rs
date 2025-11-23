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
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn new(cfg: &InternalCfg) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);
        let nwords = cfg.init_cap >> 0x06;
        let init_len = META_SIZE + (nwords << 0x03);

        // sanity checks
        debug_assert!(nwords * 0x40 == cfg.init_cap, "Must be u64 aligned");

        // create new file
        let file = File::new(&path)
            .inspect(|_| cfg.logger.trace("(TRAIL) Created new file"))
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) Failed to open file({:?}): {e}", path));

                // NOTE: we must delete file if created, so new init could work w/o any issues
                File::del(&path).map_err(|e| {
                    cfg.logger
                        .warn(format!("(TRAIL) Failed to delete the newly created file: {e}"));
                });

                e
            })?;

        // zero init the file
        file.zero_extend(init_len)
            .inspect(|_| cfg.logger.trace("(TRAIL) Zero-Extended new file"))
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) Failed to zero extend new file({:?}): {e}", path));

                // NOTE: Close + Delete the created file, so new init could work w/o any issues
                //
                // HACK: We ignore error from `close_and_del` as we are already in an errored
                // state, and primary error is more imp then this!
                Self::close_and_del_file(&cfg, &file);

                e
            })?;

        let mmap = MMap::new(file.0, init_len)
            .inspect(|_| cfg.logger.trace(format!("(TRAIL) Mmaped new file w/ len={init_len}")))
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) Failed to mmap: {e}"));

                // NOTE: Close + Delete the created file, so new init could work w/o any issues
                //
                // HACK: We ignore error from `close_and_del` as we are already in an errored
                // state, and primary error is more imp then this!
                Self::close_and_del_file(&cfg, &file);

                e
            })?;
        mmap.write(0, &Meta::new(nwords as u64));

        // NOTE: we use `ms_sync` here to make sure metadata is persisted before
        // any other updates are conducted on the mmap,
        //
        // NOTE: we can afford this syscall here, as init does not come under the fast
        // path. Also it's just one time thing!
        mmap.ms_sync().map_err(|e| {
            cfg.logger
                .error(format!("(TRAIL) Failed to write Metadata to mmaped file: {e}"));
            e
        })?;

        let meta_ptr = mmap.read_mut::<Meta>(0);
        let bmap_ptr = mmap.read_mut::<BitMapPtr>(META_SIZE);

        cfg.logger.debug("(TRAIL) Created a new file");

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
            let err = InternalError::InvalidFile("File does not exists".into());
            cfg.logger.error(format!("(TRAIL) File does not exsits: {err}"));
            return Err(err);
        }

        // open existing file (file handle)
        let file = File::open(&path)
            .inspect(|_| cfg.logger.trace("(TRAIL) Opened existing file"))
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) Failed to open existing file: {e}"));
                e
            })?;

        // existing file len (for mmap)
        let file_len = file
            .fstat()
            .inspect(|s| cfg.logger.trace(format!("(TRAIL) Existing file has len={}", s.st_size)))
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) FStat failed for existing file: {e}"));
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
            cfg.logger.error(format!("(TRAIL) Existing file is invalid: {err}"));

            // NOTE: Close + Delete the created file, so new init could work w/o any issues
            //
            // HACK: We ignore error from `close_and_del` as we are already in an errored
            // state, and primary error is more imp then this!
            Self::close_and_del_file(&cfg, &file);

            return Err(err);
        }

        let mmap = MMap::new(file.0, file_len)
            .inspect(|_| cfg.logger.trace("(TRAIL) Created mmap for existing file"))
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) Failed to create mmap for existing file: {e}"));
                e
            })?;

        let meta_ptr = mmap.read_mut::<Meta>(0);
        let bmap_ptr = mmap.read_mut::<BitMapPtr>(META_SIZE);

        // metadata validations
        //
        // NOTE/TODO: In future, we need to support the old file versions, if any!
        if (*meta_ptr).magic != MAGIC || (*meta_ptr).version != VERSION {
            cfg.logger.warn("(TRAIL) Existing file has invalid VERSION or MAGIC");
        }

        cfg.logger.debug("(TRAIL) Opened an existing file");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            bmap_ptr,
            cfg: cfg.clone(),
        })
    }

    // #[allow(unsafe_op_in_unsafe_fn)]
    // #[inline(always)]
    // unsafe fn extend_and_remap(&mut self) -> InternalResult<()> {
    //     // STEP 1: Unmap the file
    //     self.mmap
    //         .unmap()
    //         .inspect(|_| self.cfg.logger.trace("(TRAIL) Successfully unmapped (extend & remap)"))
    //         .map_err(|e| {
    //             self.cfg
    //                 .logger
    //                 .error(format!("(TRAIL) Unable to unmap (extend & remap): {e}"));
    //             e
    //         })?;

    //     // STEP 2: Zero extend the file
    //     let new_nwords = (*self.meta_ptr).nwords as usize + self.cfg.init_cap >> 0x06;
    //     let new_len = META_SIZE + (new_nwords << 0x03);
    //     self.file
    //         .zero_extend(new_len)
    //         .inspect(|_| self.cfg.logger.trace("(TRIAL) Zero extend successful (extend & remap)"))
    //         .map_err(|e| {
    //             self.cfg
    //                 .logger
    //                 .error(format!("(TRAIL) Unable to unmap (extend & remap): {e}"));
    //             e
    //         })?;

    //     Ok(())
    // }

    /// Close & Delete [Trail] file
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn close_and_del_file(cfg: &InternalCfg, file: &File) {
        let path = cfg.dirpath.join(PATH);

        // close the file handle (NOTE: always before the delete)
        let res = file.close().map_err(|e| {
            cfg.logger
                .warn(format!("(TRAIL) Failed to close the newly created file: {e}"));
            e
        });

        // NOTE: If we are unable to close the file handle, we may not be able to
        // delete the file (is OS dependent, e.g. on Windows)
        if res.is_ok() {
            File::del(&path).map_err(|e| {
                cfg.logger
                    .warn(format!("(TRAIL) Failed to delete the newly created file: {e}"));
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
                    self.cfg.logger.trace("(TRAIL) Fsync successful for mmap");
                })
                .map_err(|e| {
                    is_err = true;
                    self.cfg.logger.warn(format!("(TRAIL) Failed to fsync on mmap: {e}"));
                });

            // munmap the memory mappings
            self.mmap
                .unmap()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) Mummap successful for mmap");
                })
                .map_err(|e| {
                    is_err = true;
                    self.cfg.logger.warn(format!("(TRAIL) Failed to munmap: {e}"));
                });

            // close the file descriptor
            self.file
                .close()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) Closed the file fd");
                })
                .map_err(|e| {
                    is_err = true;
                    self.cfg
                        .logger
                        .warn(format!("(TRAIL) Failed to close the file fd: {e}"));
                });

            if !is_err {
                self.cfg.logger.debug("(TRAIL) Dropped Successfully!");
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
    }
}
