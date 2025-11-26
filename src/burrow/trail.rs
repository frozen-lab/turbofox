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
    cw_idx: u64,
}

const META_SIZE: usize = std::mem::size_of::<Meta>();

impl Meta {
    const fn new(words: u64) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            cw_idx: 0x00,
            nwords: words,
            free: words * 0x40,
        }
    }
}

// sanity check
const _: () = assert!(META_SIZE == 0x20, "Must be 32 bytes aligned");
const _: () = assert!(Meta::new(0x02).free == (0x02 * 0x40), "Must be correctly initialised");

//
// BMap
//

#[repr(C, align(0x08))]
#[derive(Debug)]
struct BMapPtr(u64);

impl BMapPtr {
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn lookup_one(&mut self, wi: usize, meta: &mut Meta) -> Option<usize> {
        if self.0 == u64::MAX {
            return None;
        }

        let inv = !self.0;
        let off = core::arch::x86_64::_tzcnt_u64(inv) as usize;

        self.0 = self.0 | (1u64 << off);
        meta.free -= 0x01;
        meta.cw_idx = wi as u64;

        Some((wi << 0x06) + off)
    }
}

//
// Trail
//

#[derive(Debug)]
pub(super) struct Trail {
    file: File,
    mmap: MMap,
    cfg: InternalCfg,
    meta_ptr: *mut Meta,
    bmap_ptr: *mut BMapPtr,
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
        mmap.write(0x00, &Meta::new(nwords as u64));
        mmap.ms_sync()
            .inspect(|_| cfg.logger.trace("(TRAIL) [new] MsSync successful on Meta"))
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) [new] Failed to write Meta on MMap: {e}"));
                e
            })?;

        let meta_ptr = mmap.read_mut::<Meta>(0x00);
        let bmap_ptr = mmap.read_mut::<BMapPtr>(META_SIZE);

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
        if bmap_len == 0x00 || bmap_len & 0x07 != 0x00 {
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

        let meta_ptr = mmap.read_mut::<Meta>(0x00);
        let bmap_ptr = mmap.read_mut::<BMapPtr>(META_SIZE);

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
    /// Lookup one slot in the [BitMap]
    ///
    /// ## Perf
    ///  - On scalar about `2.7 ns/ops`
    ///
    /// ## TODO's
    ///  - Impl of SIMD
    unsafe fn lookup_one(&mut self) -> Option<usize> {
        let meta = &mut *self.meta_ptr;

        // no slots left
        if meta.free == 0x00 {
            return None;
        }

        let nwords = meta.nwords as usize;
        let mut remaining = nwords;

        // normalized `w_idx`
        let mut w_idx = meta.cw_idx as usize;
        if w_idx >= nwords {
            w_idx = 0x00;
        }

        while remaining >= 0x04 {
            // NOTE: We prefetch next batch to avoid cache miss
            #[cfg(target_arch = "x86_64")]
            {
                let pf = {
                    let p = w_idx + 0x04;
                    if p >= nwords {
                        0x00
                    } else {
                        p
                    }
                };
                let pf_ptr = self.bmap_ptr.add(pf) as *const i8;
                core::arch::x86_64::_mm_prefetch(pf_ptr, core::arch::x86_64::_MM_HINT_T0);
            }

            // compute indexes

            let i1 = {
                let x = w_idx + 0x01;
                if x >= nwords {
                    x - nwords
                } else {
                    x
                }
            };
            let i2 = {
                let x = w_idx + 0x02;
                if x >= nwords {
                    x - nwords
                } else {
                    x
                }
            };
            let i3 = {
                let x = w_idx + 0x03;
                if x >= nwords {
                    x - nwords
                } else {
                    x
                }
            };

            let w0 = &mut *self.bmap_ptr.add(w_idx);
            if let Some(idx) = w0.lookup_one(w_idx, meta) {
                return Some(idx);
            }

            let w1 = &mut *self.bmap_ptr.add(i1);
            if let Some(idx) = w1.lookup_one(i1, meta) {
                return Some(idx);
            }

            let w2 = &mut *self.bmap_ptr.add(i2);
            if let Some(idx) = w2.lookup_one(i2, meta) {
                return Some(idx);
            }

            let w3 = &mut *self.bmap_ptr.add(i3);
            if let Some(idx) = w3.lookup_one(i3, meta) {
                return Some(idx);
            }

            // next up

            remaining -= 0x04;
            w_idx += 0x04;
            if w_idx >= nwords {
                w_idx = 0x00;
            }
        }

        while remaining > 0x00 {
            let word = &mut *self.bmap_ptr.add(w_idx);
            if let Some(idx) = word.lookup_one(w_idx, meta) {
                return Some(idx);
            }

            w_idx += 0x01;
            if w_idx >= nwords {
                w_idx = 0x00;
            }
            remaining -= 0x01;
        }

        None
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    /// Lookup `N` slots in the [BitMap]
    ///
    /// ## Perf
    ///  - On scalar about `7 ns/ops`
    ///
    /// ## TODO's
    ///  - Impl of SIMD
    unsafe fn lookup_n(&mut self, n: usize) -> Option<usize> {
        // sanity checks
        debug_assert!(n > 0x00, "N must not be zero");

        let meta = &mut *self.meta_ptr;
        let nwords = meta.nwords as usize;
        let mut scanned: usize = 0x00;

        // not enough free slots
        if meta.free < n as u64 {
            return None;
        }

        // normalized `w_idx`
        let mut w_idx = meta.cw_idx as usize;
        if w_idx >= nwords {
            w_idx = 0x00;
        }

        // contineous free slots found w/ it's start idx
        let mut run_len: usize = 0x00;
        let mut run_start: usize = 0x00;

        while scanned < nwords {
            // NOTE: We prefetch next batch to avoid cache miss
            #[cfg(target_arch = "x86_64")]
            {
                let pf = {
                    let p = w_idx + 0x04;
                    if p >= nwords {
                        0x00
                    } else {
                        p
                    }
                };
                let pf_ptr = self.bmap_ptr.add(pf) as *const i8;
                core::arch::x86_64::_mm_prefetch(pf_ptr, core::arch::x86_64::_MM_HINT_T0);
            }

            let w_ptr = self.bmap_ptr.add(w_idx);
            let mut word = !(*w_ptr).0;

            // current word is full, reset and continue to next
            if word == 0x00 {
                run_len = 0x00;
                scanned += 0x01;

                w_idx += 0x01;
                w_idx -= (w_idx == nwords) as usize * nwords;

                continue;
            }

            // NOTE: If prev run existed, but this word does not have a free slot at 0th idx,
            // the current run can't continue!
            if run_len > 0x00 && (word & 0x01) == 0x00 {
                run_len = 0x00;
            }

            let base_bit = w_idx << 0x06;
            while word != 0x00 {
                let pos = core::arch::x86_64::_tzcnt_u64(word) as usize;
                let suffix = word >> pos;
                let chunk = suffix.trailing_ones() as usize;

                if run_len == 0x00 {
                    run_start = base_bit + pos;
                    run_len = chunk;
                } else {
                    let expected = run_start + run_len;
                    let this_start = base_bit + pos;

                    // is not contiguous!
                    if this_start != expected {
                        run_start = this_start;
                        run_len = chunk;
                    } else {
                        run_len += chunk;
                    }
                }

                if run_len >= n {
                    let mut remaining = n;
                    let mut bitpos = run_start;

                    let first_wi = bitpos >> 0x06;
                    let first_off = bitpos & 0x3F;
                    let end_bit = run_start + n;
                    let last_wi = (end_bit - 0x01) >> 0x06;
                    let last_off = (end_bit - 0x01) & 0x3F;

                    // entirely within one word
                    if first_wi == last_wi {
                        let take = n;
                        let mask = ((!0x00u64) >> (0x40 - take)) << first_off;
                        (*self.bmap_ptr.add(first_wi)).0 |= mask;
                    } else {
                        if first_off != 0x00 {
                            let head_mask = (!0x00) << first_off;
                            (*self.bmap_ptr.add(first_wi)).0 |= head_mask;
                        } else {
                            (*self.bmap_ptr.add(first_wi)).0 = !0x00;
                        }

                        if last_wi > first_wi + 0x01 {
                            let mut wi = first_wi + 0x01;
                            while wi < last_wi {
                                (*self.bmap_ptr.add(wi)).0 = !0x00;
                                wi += 0x01;
                            }
                        }

                        let tail_mask = (!0x00) >> (0x3F - (last_off as u64));
                        (*self.bmap_ptr.add(last_wi)).0 |= tail_mask;
                    }

                    meta.free -= n as u64;
                    meta.cw_idx = (run_start / 0x40) as u64;
                    return Some(run_start);
                }

                let shift = pos + chunk;
                if shift >= 0x40 {
                    break;
                }
                word >>= shift;
            }

            scanned += 0x01;
            w_idx += 0x01;
            w_idx -= (w_idx == nwords) as usize * nwords;
        }

        None
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
        self.bmap_ptr = self.mmap.read_mut::<BMapPtr>(META_SIZE);

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
        use std::os::unix::fs::PermissionsExt;

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
        fn test_new_fails_when_dir_is_not_writable() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir.clone()).log(true).log_target("Trail");

            // NOTE: w/ chmod 000 we simulate unwriteable directory
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).expect("Set permission");

            assert!(
                unsafe { Trail::new(&cfg) }.is_err(),
                "Trail::new should fail on unwritable directory"
            );

            // WARN: Must always restore back to avoid shutdown issues
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).expect("Re-Set Permission");
        }

        #[test]
        fn test_open_fails_when_dir_is_not_readable() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir.clone()).log(true).log_target("Trail");
            let path = dir.join("trail");

            std::fs::write(&path, &[0u8; 64]).expect("Write");
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).expect("Set Permission");

            let res = unsafe { Trail::open(&cfg) };
            assert!(res.is_err(), "Trail::open should fail when directory is unreadable");

            // WARN: Must always restore back to avoid shutdown issues
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).expect("Re-Set Permission");
        }
    }

    mod extend_remap {
        use super::*;

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

                trail.extend_remap().expect("extend and remap");

                let meta = *trail.meta_ptr;

                assert!(!trail.bmap_ptr.is_null());
                assert_eq!(meta.free, og_free + cfg.init_cap as u64);
                assert!(trail.mmap.len() > og_len, "mmap len must grow");
                assert_eq!(meta.nwords, og_nwords + (cfg.init_cap >> 0x06) as u64);
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
                let inc = (cfg.init_cap >> 0x06) as u64;

                trail.extend_remap().expect("first extend works");
                let w1 = (*trail.meta_ptr).nwords;
                let f1 = (*trail.meta_ptr).free;

                assert_eq!(w1, nw0 + inc);
                assert_eq!(f1, fr0 + cfg.init_cap as u64);

                trail.extend_remap().expect("second extend works");
                let w2 = (*trail.meta_ptr).nwords;
                let f2 = (*trail.meta_ptr).free;

                assert_eq!(w2, nw0 + inc * 0x02);
                assert_eq!(f2, fr0 + cfg.init_cap as u64 * 0x02);
            }
        }

        #[test]
        fn test_extend_remap_zero_inits_correctly() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let curr_words = (*trail.meta_ptr).nwords as usize;

                // Fill in current words
                for i in 0..curr_words {
                    (*trail.bmap_ptr.add(i)).0 = 0xFFFF_FFFF_FFFF_FFFF;
                }

                trail.extend_remap().expect("extend and remap");

                let meta = *trail.meta_ptr;
                let total_words = meta.nwords as usize;
                let new_words = total_words - curr_words;

                for i in curr_words..total_words {
                    assert_eq!(
                        (*trail.bmap_ptr.add(i)).0,
                        0x00,
                        "Newly allocated word {} must be zero",
                        i
                    );
                }

                assert!(new_words > 0x00, "Extend must add at least one word");
            }
        }

        #[test]
        fn test_extend_remap_refreshes_pointers() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");

                let og_meta = trail.meta_ptr;
                let og_bmap = trail.bmap_ptr;

                trail.extend_remap().expect("extend and remap");

                assert!(trail.mmap.len() > 0);
                assert!(!trail.meta_ptr.is_null());
                assert!(!trail.bmap_ptr.is_null());
            }
        }

        #[test]
        fn test_extend_remap_preserves_free_invariant() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");

                for _ in 0x00..0x04 {
                    trail.extend_remap().expect("extend and remap");
                    assert_eq!(
                        (*trail.meta_ptr).free,
                        (*trail.meta_ptr).nwords * 0x40,
                        "free must always equal nwords * 64 bits"
                    );
                }
            }
        }
    }

    mod bmap {
        use super::*;

        #[test]
        fn test_lookup_one_works() {
            unsafe {
                let mut meta = Meta::new(0x01);
                let mut w = BMapPtr(0x00);

                assert_eq!(w.lookup_one(0x00, &mut meta), Some(0x00));
                assert_eq!(w.0, 0b1);

                assert_eq!(w.lookup_one(0x00, &mut meta), Some(0x01));
                assert_eq!(w.0, 0b11);

                assert_eq!(meta.free, 0x3E);
            }
        }

        #[test]
        fn test_lookup_one_returns_none_on_full() {
            unsafe {
                let mut meta = Meta::new(0x01);
                let mut w = BMapPtr(u64::MAX);

                assert!(w.lookup_one(0x00, &mut meta).is_none());
                assert_eq!(meta.free, 0x40);
            }
        }
    }

    mod trail_lookup_one {
        use super::*;
        use std::hint::black_box;
        use std::time::Instant;

        #[test]
        fn test_trail_lookup_one_sequential_filling() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new trail");
                let total = (*trail.meta_ptr).nwords * 0x40;

                for i in 0x00..total {
                    let got = trail.lookup_one().expect("slot");
                    assert_eq!(got as u64, i);
                }

                assert!(trail.lookup_one().is_none());
                assert_eq!((*trail.meta_ptr).free, 0x00);
            }
        }

        #[test]
        fn test_trail_lookup_one_wraps_around_correctly() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new trail");
                let nwords = (*trail.meta_ptr).nwords as usize;

                // We fill in word(0) completely
                for i in 0x00..0x40 {
                    assert_eq!(trail.lookup_one(), Some(i));
                }

                assert_eq!((*trail.meta_ptr).cw_idx, 0x00);

                assert_eq!(trail.lookup_one(), Some(0x40));
                assert_eq!((*trail.meta_ptr).cw_idx, 0x01);

                // NOTE: We force alloc starting point by setting the near last bit
                (*trail.meta_ptr).cw_idx = (nwords - 0x01) as u64;

                // Nxt alloc should be first free bit in the last word
                let idx = trail.lookup_one().expect("Lookup");
                assert_eq!(idx, (nwords - 0x01) * 0x40);
            }
        }

        #[test]
        fn test_trail_lookup_one_preserves_meta_free_invariant() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new trail");
                let total = (*trail.meta_ptr).nwords * 0x40;

                for i in 0x00..total {
                    let before = (*trail.meta_ptr).free;

                    assert!(trail.lookup_one().is_some());
                    assert_eq!((*trail.meta_ptr).free, before - 0x01);
                }

                assert!(trail.lookup_one().is_none());
                assert_eq!((*trail.meta_ptr).free, 0x00);
            }
        }

        #[test]
        fn test_trail_lookup_one_bit_consistency() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new trail");
                let total = (*trail.meta_ptr).nwords as usize * 0x40;
                let mut seen = vec![false; total];

                for _ in 0x00..total {
                    let ix = trail.lookup_one().expect("Insert slot");
                    seen[ix] = true;
                }

                for s in seen {
                    assert!(s, "every index must be returned exactly once");
                }
            }
        }

        #[test]
        #[ignore]
        fn bench_lookup_one() {
            const INIT_CAP: usize = 0x7D000; // 512K

            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir)
                .init_cap(INIT_CAP)
                .log(true)
                .log_target("[BENCH] Trail::lookup_one");

            cfg.logger.info("----------Lookup(1)----------");

            unsafe {
                let rounds = 0x0A;
                let iters = INIT_CAP;
                cfg.logger.info(format!("Rounds={rounds}, Iters={iters}"));

                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let mut results: Vec<f64> = Vec::with_capacity(rounds);

                // NOTE: warmup to eliminate cold cache & cold cpu, and branch predictor effects
                let meta = &mut *trail.meta_ptr;
                let nwords = meta.nwords as usize;
                let total = nwords * 0x40;
                for _ in 0x00..total {
                    let _ = trail.lookup_one();
                }

                for r in 0x00..rounds {
                    // HACK: We reset bmap so lookup always does real work
                    let meta = &mut *trail.meta_ptr;
                    meta.free = meta.nwords * 0x40;
                    meta.cw_idx = 0x00;

                    // Clear up all bmap words
                    let nwords = meta.nwords as usize;
                    for i in 0x00..nwords {
                        (*trail.bmap_ptr.add(i)).0 = 0x00;
                    }

                    let start = Instant::now();
                    for _ in 0x00..iters {
                        assert!(trail.lookup_one().is_some());
                    }
                    let elapsed = start.elapsed();

                    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
                    cfg.logger
                        .info(format!("[Round {r}] Time={:?}  =>  {:.2} ns/op", elapsed, ns_per_op));

                    results.push(ns_per_op);
                }

                let avg: f64 = results.iter().sum::<f64>() / results.len() as f64;
                cfg.logger.info(format!("AVERAGE: {:.2} ns/op", avg));

                #[cfg(not(debug_assertions))]
                {
                    let threshold_ns = 0x05 as f64;
                    assert!(
                        avg <= threshold_ns,
                        "lookup_one too slow: {:.2} ns/op (threshold: {} ns)",
                        avg,
                        threshold_ns
                    );
                }
            }
        }
    }

    mod trail_lookup_n {
        use super::*;
        use std::hint::black_box;
        use std::time::Instant;

        #[test]
        fn test_lookup_n_correctly_works() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let total = (*trail.meta_ptr).nwords * 0x40;

                // allocate chunks of 2
                for i in (0x00..total).step_by(0x02) {
                    let ix = trail.lookup_n(0x02).expect("slot");
                    assert_eq!(ix as u64, i);
                }

                assert!(trail.lookup_n(0x02).is_none());
                assert_eq!((*trail.meta_ptr).free, 0x00);
            }
        }

        #[test]
        fn test_lookup_n_returns_contineous_blocks() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let total = (*trail.meta_ptr).nwords * 0x40;

                let want = 0x0B;
                let ix = trail.lookup_n(want).expect("slot");
                assert_eq!(ix, 0x00);

                for b in 0x00..want {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    assert!(((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01 == 0x01);
                }

                assert_eq!((*trail.meta_ptr).free, total - want as u64);
            }
        }

        #[test]
        fn test_lookup_n_wraps_correctly() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let nw = (*trail.meta_ptr).nwords as usize;

                // fill all of word(0)
                assert!(trail.lookup_n(0x40).is_some());
                assert_eq!((*trail.meta_ptr).cw_idx, 0x00);

                // next must start at word(1)
                let ix = trail.lookup_n(0x03).expect("slot");
                assert_eq!(ix, 0x40);
                assert_eq!((*trail.meta_ptr).cw_idx, 0x01);

                // force start near last word
                (*trail.meta_ptr).cw_idx = (nw - 0x01) as u64;
                let ix2 = trail.lookup_n(0x02).expect("slot");
                assert_eq!(ix2, (nw - 0x01) * 0x40);
            }
        }

        #[test]
        fn test_lookup_n_bit_consistency() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let total = (*trail.meta_ptr).nwords as usize * 0x40;

                // Snap of [BitMap]
                let mut bitmap = vec![0x00u64; (*trail.meta_ptr).nwords as usize];
                let mut taken = 0x00usize;

                for n in [0x01, 0x02, 0x03, 0x04, 0x05, 0x07, 0x0B, 0x10, 0x1F, 0x40].repeat(0x08) {
                    if taken + n > total {
                        break;
                    }

                    let start = trail.lookup_n(n).expect("slot");
                    assert_eq!(start, taken, "lookup_n must allocate sequentially");

                    for b in start..start + n {
                        let wi = b >> 0x06;
                        let off = b & 0x3F;
                        bitmap[wi] |= 0x01 << off;
                    }
                    taken += n;
                    for (i, word) in bitmap.iter().enumerate() {
                        assert_eq!((*trail.bmap_ptr.add(i)).0, *word, "bitmap mismatch at word {i}");
                    }
                }

                // all bits up to `taken` must be 1, all after must be 0
                for b in 0x00..total {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    let actual = ((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01;

                    if b < taken {
                        assert_eq!(actual, 0x01, "bit {b} must be allocated (1)");
                    } else {
                        assert_eq!(actual, 0x00, "bit {b} must remain free (0)");
                    }
                }
            }
        }

        #[test]
        #[ignore]
        fn bench_lookup_n() {
            const INIT_CAP: usize = 0x7D000; // 512K bits
            const ROUNDS: usize = 0x14;
            const CHUNKS: [usize; 0x0C] = [0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x14, 0x18, 0x20];

            let sum: usize = CHUNKS.into_iter().sum();
            let iters = INIT_CAP / sum;

            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir)
                .init_cap(INIT_CAP)
                .log(true)
                .log_target("[BENCH] Trail::lookup_n");

            cfg.logger.info("----------Lookup(N)----------");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("New Trail");

                // STEP 1: Warmup
                //
                // NOTE: warmup to eliminate cold cache & cold cpu, and branch predictor effects
                let meta = &mut *trail.meta_ptr;
                let nwords = meta.nwords as usize;
                let total = nwords * 0x40;
                for _ in 0x00..total {
                    let _ = trail.lookup_one();
                }

                // STEP 2: Benchmark
                let mut results = Vec::with_capacity(ROUNDS);
                for r in 0x00..ROUNDS {
                    // HACK: We reset bmap so lookup always does real work
                    let meta = &mut *trail.meta_ptr;
                    meta.free = meta.nwords * 0x40;
                    meta.cw_idx = 0x00;

                    // Clear up all bmap words
                    let nwords = meta.nwords as usize;
                    for i in 0x00..nwords {
                        (*trail.bmap_ptr.add(i)).0 = 0x00;
                    }

                    let start = Instant::now();
                    for _ in 0x00..iters {
                        for n in CHUNKS {
                            assert!(trail.lookup_n(n).is_some());
                        }
                    }
                    let elapsed = start.elapsed();

                    let ops = (iters * CHUNKS.len()) as f64;
                    let ns_op = elapsed.as_nanos() as f64 / ops;
                    cfg.logger.info(format!("[Round {r}] {ns_op:.2} ns/op"));
                    results.push(ns_op);
                }

                // STEP 3: Compute Results
                let avg: f64 = results.iter().sum::<f64>() / results.len() as f64;
                cfg.logger.info(format!("AVERAGE: {:.2} ns/op", avg));

                #[cfg(not(debug_assertions))]
                {
                    let threshold = 0x0A as f64;
                    assert!(avg <= threshold, "lookup_n too slow: {avg} ns/op");
                }
            }
        }
    }
}
