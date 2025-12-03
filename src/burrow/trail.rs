use crate::{
    core::{TurboFile, TurboMMap},
    errors::{InternalError, InternalResult},
    logger::Logger,
    TurboConfig,
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

#[repr(C, align(0x20))]
#[derive(Debug)]
struct BMapPtr(u64);

impl BMapPtr {
    #[inline(always)]
    fn lookup_one(&mut self, wi: usize, meta: &mut Meta) -> Option<usize> {
        if self.0 == u64::MAX {
            return None;
        }

        let inv = !self.0;
        let off = unsafe { core::arch::x86_64::_tzcnt_u64(inv) as usize };

        self.0 = self.0 | (1u64 << off);
        meta.free -= 0x01;
        meta.cw_idx = wi as u64;

        Some((wi << 0x06) + off)
    }

    #[inline(always)]
    fn free_one(&mut self, idx: usize, meta: &mut Meta) {
        // sanity check
        debug_assert!(self.0 & (0x01 << (idx & 0x3F)) != 0x00, "Double free detected");

        self.0 &= !(0x01 << (idx & 0x3F));
        meta.free += 0x01;
    }
}

//
// Trail
//

#[derive(Debug)]
pub(super) struct Trail {
    file: TurboFile,
    mmap: TurboMMap,
    cfg: TurboConfig,
    meta_ptr: *mut Meta,
    bmap_ptr: *mut BMapPtr,
}

impl Trail {
    /// Creates a new [Trail] file
    ///
    /// *NOTE* Returns an [IO] error if something goes wrong
    pub(super) fn new(cfg: &TurboConfig) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);
        let nwords = cfg.init_cap >> 0x06; // 1 byte = 8 slots
        let new_file_len = META_SIZE + (nwords << 0x03);

        // sanity checks
        debug_assert!(nwords * 0x40 == cfg.init_cap, "Must be u64 aligned");

        // new file
        let file = TurboFile::new(&cfg, PATH)?;
        file.zero_extend(new_file_len, true)?;

        let mmap = TurboMMap::new(&cfg, PATH, &file, new_file_len).map_err(|e| {
            // NOTE: Close + Delete the created file, so new init could work w/o any issues
            //
            // HACK: We ignore error from `close` and `del` as we are already in errored state, and primary
            // error is more imp then this!

            // NOTE: We can only delete the file, if file fd is released or closed, e.g. on windows
            if file.close().is_ok() {
                let _ = file.del();
            }

            e
        })?;

        // sanity check
        debug_assert_eq!(mmap.len(), new_file_len, "MMap len must be same as file len");

        let meta = Meta::new(nwords as u64);
        mmap.write(0x00, &meta);

        // NOTE: we use `ms_sync` here to make sure metadata is persisted before
        // any other updates are conducted on the mmap,
        //
        // HACK: we can afford this syscall here, as init does not come under the fast path
        mmap.msync()?;

        let meta_ptr = mmap.read_mut::<Meta>(0x00);
        let bmap_ptr = mmap.read_mut::<BMapPtr>(META_SIZE);

        cfg.logger.debug("(Trail) [new] Created new Trail");

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
    pub(super) fn open(cfg: &TurboConfig) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);

        // file must exists
        if !path.exists() {
            let err = InternalError::InvalidFile("Path does not exists".into());
            cfg.logger.error(format!("(Trail) [open] Invalid path: {err}"));
            return Err(err);
        }

        // open existing file (file handle)
        let file = TurboFile::open(&cfg, PATH)?;
        let file_len = file.len()?;

        // NOTE: File must always be BMap aligned
        let bmap_len = file_len.wrapping_sub(META_SIZE);
        if bmap_len == 0x00 || bmap_len & 0x07 != 0x00 {
            let err = InternalError::InvalidFile("Trail is not BitMap aligned".into());
            cfg.logger
                .error(format!("(Trail) [open] Existing file is invalid: {err}"));
            return Err(err);
        }

        let mmap = TurboMMap::new(&cfg, PATH, &file, file_len)?;
        let meta_ptr = mmap.read_mut::<Meta>(0x00);
        let bmap_ptr = mmap.read_mut::<BMapPtr>(META_SIZE);

        // sanity check
        debug_assert_eq!(mmap.len(), file_len, "MMap len must be same as file len");

        // metadata validations
        //
        // NOTE/TODO: In future, we need to support the old file versions, if any!
        unsafe {
            if (*meta_ptr).magic != MAGIC || (*meta_ptr).version != VERSION {
                cfg.logger.warn("(Trail) [open] File has invalid VERSION or MAGIC");
            }
        }

        cfg.logger.debug("(Trail) [open] open is successful");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            bmap_ptr,
            cfg: cfg.clone(),
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) fn extend_remap(&mut self) -> InternalResult<()> {
        let curr_nwords = unsafe { (*self.meta_ptr).nwords };
        let slots_to_add = self.cfg.init_cap as u64;
        let nwords_to_add = slots_to_add >> 0x06;
        let new_len = self.mmap.len() + (nwords_to_add << 0x03) as usize;
        let new_nwords = curr_nwords + nwords_to_add as u64;

        // sanity checks
        debug_assert!(nwords_to_add > 0, "Words to be added must not be 0");
        debug_assert!(new_len > self.mmap.len(), "New len must be larger then current len");

        // STEP 1: Zero extend the file
        self.file.zero_extend(new_len, false)?;

        // STEP 2: Re-MMap
        self.mmap = TurboMMap::new(&&self.cfg, PATH, &self.file, new_len)?;
        self.meta_ptr = self.mmap.read_mut::<Meta>(0);
        self.bmap_ptr = self.mmap.read_mut::<BMapPtr>(META_SIZE);

        // STEP 3: Update & Sync Meta
        unsafe {
            (*self.meta_ptr).nwords = new_nwords;
            (*self.meta_ptr).free += slots_to_add;
            (*self.meta_ptr).cw_idx = curr_nwords; // we start at the first new idx
        }
        self.mmap.msync()?;

        Ok(())
    }

    /// Lookup `N` slots in the [BitMap]
    ///
    /// ## Perf
    ///  - On scalar about `8 ns/ops` (`24 ns/ops` amortizied)
    ///
    /// ## TODO's
    ///  - Impl of SIMD
    #[inline(always)]
    pub(super) fn lookup(&mut self, n: usize) -> Option<usize> {
        let meta = unsafe { &mut *self.meta_ptr };

        // sanity checks
        debug_assert!(n != 0x00, "N must not be zero");

        // just one slot to get
        if n == 0x01 {
            // no slot left
            if meta.free == 0x00 {
                return None;
            }

            return self.lookup_one();
        }

        // not enough slots
        if meta.free < n as u64 {
            return None;
        }

        self.lookup_n(n)
    }

    /// Free `N` slots in the [BitMap]
    ///
    /// ## Perf
    ///  - On scalar about `~ 2 ns/op`
    ///
    /// ## TODO's
    ///  - Impl of SIMD
    #[inline(always)]
    pub(super) fn free(&mut self, idx: usize, n: usize) {
        let meta = unsafe { &mut *self.meta_ptr };

        // sanity checks
        debug_assert!(n != 0x00, "N must not be zero");
        debug_assert!(idx < (meta.nwords as usize) * 64, "Idx is out of bounds");

        // just one slot to get
        if n == 0x01 {
            let w_idx = idx >> 0x06;
            let word = unsafe { &mut *self.bmap_ptr.add(w_idx) };
            word.free_one(idx, meta);
            return;
        }

        self.free_n(idx, n);
    }

    /// Lookup one slot in the [BitMap]
    ///
    /// ## Perf
    ///  - On scalar about `2.7 ns/ops`
    ///
    /// ## TODO's
    ///  - Impl of SIMD
    #[inline(always)]
    fn lookup_one(&mut self) -> Option<usize> {
        let meta = unsafe { &mut *self.meta_ptr };

        // sanity checks
        debug_assert!(meta.free != 0x00, "No free slots found");

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
            unsafe {
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

            let w0 = unsafe { &mut *self.bmap_ptr.add(w_idx) };
            if let Some(idx) = w0.lookup_one(w_idx, meta) {
                return Some(idx);
            }

            let w1 = unsafe { &mut *self.bmap_ptr.add(i1) };
            if let Some(idx) = w1.lookup_one(i1, meta) {
                return Some(idx);
            }

            let w2 = unsafe { &mut *self.bmap_ptr.add(i2) };
            if let Some(idx) = w2.lookup_one(i2, meta) {
                return Some(idx);
            }

            let w3 = unsafe { &mut *self.bmap_ptr.add(i3) };
            if let Some(idx) = w3.lookup_one(i3, meta) {
                return Some(idx);
            }

            // next up

            remaining -= 0x04;
            w_idx += 0x04;
            if w_idx >= nwords {
                w_idx = 0x00;
            }

            while remaining > 0x00 {
                let word = unsafe { &mut *self.bmap_ptr.add(w_idx) };
                if let Some(idx) = word.lookup_one(w_idx, meta) {
                    return Some(idx);
                }

                w_idx += 0x01;
                if w_idx >= nwords {
                    w_idx = 0x00;
                }
                remaining -= 0x01;
            }
        }

        None
    }

    /// Lookup `N` slots in the [BitMap]
    ///
    /// ## Perf
    ///  - On scalar about `7 ns/ops`
    ///
    /// ## TODO's
    ///  - Impl of SIMD
    #[inline(always)]
    fn lookup_n(&mut self, n: usize) -> Option<usize> {
        let meta = unsafe { &mut *self.meta_ptr };

        // sanity checks
        debug_assert!(n > 0x00, "N must not be zero");
        debug_assert!(meta.free >= n as u64, "Must be enough slots to fill in");

        let nwords = meta.nwords as usize;
        let mut scanned: usize = 0x00;

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
            unsafe {
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

            unsafe {
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
        }

        None
    }

    /// Free up `N` slots in the [BitMap]
    ///
    /// ## Perf
    ///  - On scalar about `~ 2.5 ns/ops`
    ///
    /// ## TODO's
    ///  - Impl of SIMD
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    fn free_n(&mut self, idx: usize, n: usize) {
        let meta = unsafe { &mut *self.meta_ptr };
        let nwords = meta.nwords as usize;

        let first_wi = idx >> 0x06;
        let first_off = idx & 0x3F;
        let end_bit = idx + n;
        let last_wi = (end_bit - 0x01) >> 0x06;
        let last_off = (end_bit - 0x01) & 0x3F;

        // sanity checks
        debug_assert!(n > 0x00, "n must not be zero");
        debug_assert!(end_bit <= nwords * 0x40, "free_n range out of bounds");
        debug_assert!(
            meta.free + (n as u64) <= (meta.nwords * 0x40),
            "Meta::free is grown out of bounds"
        );
        debug_assert!(
            first_wi < nwords && last_wi < nwords,
            "Index calculation is out of range"
        );

        unsafe {
            // single word to operate on
            if first_wi == last_wi {
                let mask = ((!0x00) >> (0x40 - n)) << first_off;
                (*self.bmap_ptr.add(first_wi)).0 &= !mask;
                meta.free += n as u64;
                return;
            }

            // At Head
            if first_off != 0x00 {
                let head_mask = (!0x00) << first_off;
                (*self.bmap_ptr.add(first_wi)).0 &= !head_mask;
            } else {
                (*self.bmap_ptr.add(first_wi)).0 = 0x00;
            }

            // Middle words (if any)
            if last_wi > first_wi + 0x01 {
                let mut wi = first_wi + 0x01;
                while wi < last_wi {
                    (*self.bmap_ptr.add(wi)).0 = 0x00;
                    wi += 0x01;
                }
            }

            // At Tail
            let tail_mask = (!0x00) >> (0x3F - (last_off as u64));
            (*self.bmap_ptr.add(last_wi)).0 &= !tail_mask;
        }

        meta.free += n as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        #[test]
        fn test_free_one_works_correctly() {
            unsafe {
                let mut meta = Meta::new(0x01);
                let mut w = BMapPtr(0x00);

                // allocate two bits
                assert_eq!(w.lookup_one(0x00, &mut meta), Some(0x00));
                assert_eq!(w.lookup_one(0x00, &mut meta), Some(0x01));
                assert_eq!(w.0, 0b11);
                assert_eq!(meta.free, 0x3E);

                // free the second one
                w.free_one(0x01, &mut meta);
                assert_eq!(w.0, 0b01);
                assert_eq!(meta.free, 0x3F);

                // free the first one
                w.free_one(0x00, &mut meta);
                assert_eq!(w.0, 0b00);
                assert_eq!(meta.free, 0x40);
            }
        }

        #[test]
        #[cfg(debug_assertions)]
        #[should_panic]
        fn test_free_one_should_panic_on_double_free() {
            unsafe {
                let mut meta = Meta::new(0x01);
                let mut w = BMapPtr(0x00);

                // allocate one bit
                assert_eq!(w.lookup_one(0x00, &mut meta), Some(0x00));
                assert_eq!(w.0, 0b1);

                // free it
                w.free_one(0x00, &mut meta);
                assert_eq!(w.0, 0b0);

                // free again (must panic)
                w.free_one(0x00, &mut meta);
            }
        }

        #[test]
        fn test_free_one_does_not_affect_other_bits() {
            unsafe {
                let mut meta = Meta::new(0x01);
                let mut w = BMapPtr(0x00);

                // allocate some bits
                assert_eq!(w.lookup_one(0x00, &mut meta), Some(0x00));
                assert_eq!(w.lookup_one(0x00, &mut meta), Some(0x01));
                assert_eq!(w.lookup_one(0x00, &mut meta), Some(0x02));
                assert_eq!(w.0, 0b111);

                // free the bit
                w.free_one(0x01, &mut meta);

                // ensure only that bit is flipped
                assert_eq!(w.0, 0b101);
                assert_eq!(meta.free, 0x3E);
            }
        }
    }

    mod trail {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        #[test]
        fn test_new_works() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_new_works");

            let nwords = cfg.init_cap >> 0x06;
            let t1 = unsafe { Trail::new(&cfg) }.expect("new trail");

            unsafe {
                let meta = *t1.meta_ptr;

                assert!(t1.file.fd() >= 0x00, "File fd must be valid");
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
        fn test_open_works() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_open_works");

            unsafe {
                let t0 = Trail::new(&cfg).expect("new trail");

                (*t0.meta_ptr).free = 0x01;
                (*t0.meta_ptr).nwords = 0x02;

                drop(t0);
            }

            let t1 = unsafe { Trail::open(&cfg) }.expect("open existing");

            unsafe {
                let meta = (*t1.meta_ptr);

                assert!(t1.file.fd() >= 0x00, "File fd must be valid");
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
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_open_panics");

            unsafe {
                let t0 = unsafe { Trail::new(&cfg) }.expect("new trail");
                t0.file.zero_extend(META_SIZE, true).expect("Update file len");
            }

            // should panic
            assert!(unsafe { Trail::open(&cfg) }.is_err());
        }

        #[test]
        fn test_new_fails_when_dir_is_not_writable() {
            let (cfg, _tmp) = TurboConfig::test_cfg("traol_new_fails");
            let dir = _tmp.path().to_path_buf();

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
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_open_fails");
            let dir = _tmp.path().to_path_buf();
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
            let (cfg, _tmp) = TurboConfig::test_cfg("ext_rmp_grows");

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
            let (cfg, _tmp) = TurboConfig::test_cfg("ext_rmp_twice_works");

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
            let (cfg, _tmp) = TurboConfig::test_cfg("ext_rmp_zero_init");

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
            let (cfg, _tmp) = TurboConfig::test_cfg("ext_rmp_ptr_refresh");

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
            let (cfg, _tmp) = TurboConfig::test_cfg("ext_rmp_preserves");

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

    mod lookup {
        use super::*;
        use std::time::Instant;

        #[test]
        fn test_lookup_maps_correctly_to_lookup_one() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_maps_lookup_one");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let total = (*trail.meta_ptr).nwords * 0x40;

                for i in 0x00..total {
                    let a = trail.lookup(0x01).expect("Slot");
                    assert_eq!(a as u64, i);
                }

                assert!(trail.lookup(0x01).is_none());
                assert_eq!((*trail.meta_ptr).free, 0x00);
            }
        }

        #[test]
        fn test_lookup_maps_correctly_to_lookup_n() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_maps_lookup_n");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");

                let start = trail.lookup(0x05).expect("first block");
                assert_eq!(start, 0x00);

                for b in 0x00..0x05 {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    assert_eq!(((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01, 0x01);
                }
            }
        }

        #[test]
        fn test_lookup_wraparound() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_wraparound");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let nwords = (*trail.meta_ptr).nwords as usize;

                // fill 0th word
                assert!(trail.lookup(0x40).is_some());
                assert_eq!((*trail.meta_ptr).cw_idx, 0x00);

                // next region shold start at word at idx 1
                let ix = trail.lookup(0x03).expect("Slot");
                assert_eq!(ix, 0x40);
                assert_eq!((*trail.meta_ptr).cw_idx, 0x01);
            }
        }

        #[test]
        fn test_lookup_returns_none_when_full() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_none_full");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let nwords = (*trail.meta_ptr).nwords as usize;

                // fill entire bmap
                for wi in 0..nwords {
                    (*trail.bmap_ptr.add(wi)).0 = u64::MAX;
                }
                (*trail.meta_ptr).free = 0;

                assert!(trail.lookup(0x01).is_none());
                assert!(trail.lookup(0x05).is_none());
            }
        }

        #[test]
        fn test_lookup_finds_exact_freed_region() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_find_free");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let nwords = (*trail.meta_ptr).nwords as usize;
                let total = nwords * 0x40;

                // fill entire bitmap
                for wi in 0x00..nwords {
                    (*trail.bmap_ptr.add(wi)).0 = u64::MAX;
                }
                (*trail.meta_ptr).free = 0x00;

                // free a block from bmap
                let want = 0x07;
                let start = 0x1E.min(total - want);
                let end = start + want;

                for b in start..end {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    (*trail.bmap_ptr.add(wi)).0 &= !(0x01 << off);
                }
                (*trail.meta_ptr).free = want as u64;
                (*trail.meta_ptr).cw_idx = (start >> 0x06) as u64;

                let got = trail.lookup(want).expect("Slot");
                assert_eq!(got, start, "lookup() must return freed contiguous region");
            }
        }

        #[test]
        fn test_lookup_preserves_meta_free_invariant() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_preserves_meta");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let total = (*trail.meta_ptr).nwords * 64;

                for i in 0x00..total {
                    let before = (*trail.meta_ptr).free;
                    assert!(trail.lookup(0x01).is_some());
                    assert_eq!((*trail.meta_ptr).free, before - 0x01);
                }

                assert!(trail.lookup(0x01).is_none());
                assert_eq!((*trail.meta_ptr).free, 0x00);
            }
        }

        #[test]
        #[cfg(debug_assertions)]
        #[should_panic]
        fn test_lookup_zero_panics_in_debug() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_zero_panic");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let _ = trail.lookup(0x00);
            }
        }

        #[test]
        fn test_lookup_and_extend_remap_cycle_works_correctly() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_w/_ext_rmp");
            cfg = cfg.init_cap(0x80).expect("Update INIT_CAP");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");

                // fill in entire bmap
                for _ in 0x00..0x04 {
                    assert!(trail.lookup(0x20).is_some());
                }

                // no slots left
                assert!(trail.lookup(0x01).is_none());
                assert_eq!((*trail.meta_ptr).free, 0x00);

                // extend to creat space
                assert!(trail.extend_remap().is_ok());
                assert_eq!((*trail.meta_ptr).free, 0x80);

                // again lookup for exact amout
                for _ in 0x00..0x04 {
                    assert!(trail.lookup(0x20).is_some());
                }

                // no slots left
                assert!(trail.lookup(0x01).is_none());
                assert_eq!((*trail.meta_ptr).free, 0x00);
            }
        }

        #[test]
        fn test_lookup_after_extend_remap_returns_correct_next_index() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_after_ext_rmp");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let initial_total = (*trail.meta_ptr).nwords as usize * 0x40; // 128

                // Fill entire bitmap
                for _ in 0x00..initial_total {
                    assert!(trail.lookup(0x01).is_some());
                }

                // sanity checks
                assert_eq!((*trail.meta_ptr).free, 0x00);
                assert!(trail.lookup(0x01).is_none());

                // grow (current + init_cap)
                assert!(trail.extend_remap().is_ok(), "extend_remap must work");

                let new_total = (*trail.meta_ptr).nwords as usize * 0x40;
                assert_eq!(new_total, initial_total * 0x02);

                let got = trail.lookup(0x01).expect("must allocate in extended region");
                assert_eq!(got, initial_total, "lookup() must return first free slot after extend");
            }
        }

        #[test]
        #[ignore]
        fn bench_lookup_with_extend_remap() {
            const INIT_CAP: usize = 0x8000;
            const TARGET_CAP: usize = 0x20_000; // grow until total `1_31_072`
            const MAX_GROWS: usize = TARGET_CAP / INIT_CAP;

            // 50% single slot lookup
            const CHUNKS: [usize; 0x10] = [
                0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x08, 0x0C, 0x10,
            ];
            let sum: usize = CHUNKS.into_iter().sum();
            let iters_to_fill = INIT_CAP / sum;

            let (mut cfg, _tmp) = TurboConfig::test_cfg("[BENCH] Trail::lookup_extend_remap");
            cfg = cfg.init_cap(INIT_CAP).expect("Update init cap");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let meta = &mut *trail.meta_ptr;

                // STEP 1: Cache Warmups

                // NOTE: warmup to eliminate cold cache & cold cpu, and branch predictor effects
                for _ in 0x00..INIT_CAP {
                    let _ = trail.lookup(0x01);
                }

                // HACK: We reset bmap so lookup always does real work
                let nwords = meta.nwords as usize;
                meta.free = meta.nwords * 0x40;
                meta.cw_idx = 0x00;
                for i in 0x00..nwords {
                    (*trail.bmap_ptr.add(i)).0 = 0x00;
                }

                // STEP 2: Benching

                let mut lookup_timings = Vec::<f64>::new();
                let mut grow_timings = Vec::<f64>::new();

                for _ in 0x00..MAX_GROWS {
                    // fill in the entire bmap
                    for _ in 0x00..iters_to_fill {
                        let start = Instant::now();

                        std::hint::black_box(trail.lookup(CHUNKS[0x00]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x01]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x02]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x03]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x04]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x05]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x06]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x07]));

                        std::hint::black_box(trail.lookup(CHUNKS[0x08]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x09]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x0A]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x0B]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x0C]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x0D]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x0E]));
                        std::hint::black_box(trail.lookup(CHUNKS[0x0F]));

                        let elapsed = start.elapsed();
                        let us_op = elapsed.as_nanos() as f64 / CHUNKS.len() as f64;
                        lookup_timings.push(us_op);
                    }

                    // sanity check
                    assert!(trail.lookup(0x01).is_none(), "BMap must be full at this point");

                    let start = Instant::now();
                    assert!(trail.extend_remap().is_ok());
                    let elapsed = start.elapsed();

                    // NOTE: We also measure growing duration
                    grow_timings.push(elapsed.as_millis() as f64);
                }

                // sanity check
                assert!(
                    ((*trail.meta_ptr).nwords * 0x40) >= TARGET_CAP as u64,
                    "Target cap was not reached"
                );

                // STEP 3: Measure results

                let avg_grow_ms = grow_timings.iter().sum::<f64>() / grow_timings.len() as f64;
                let avg_ns = lookup_timings.iter().sum::<f64>() / lookup_timings.len() as f64;
                let lookups_per_grow = (iters_to_fill * CHUNKS.len()) as f64;
                let amort_grow_ns = (avg_grow_ms * 1_00_000.0) / lookups_per_grow;
                let total_us = avg_ns + amort_grow_ns;

                cfg.logger.info(format!("Lookup: {:.3} ns/op", avg_ns));
                cfg.logger.info(format!("Lookup(w/ grow): {:.3} ns/op", total_us));
                cfg.logger.info(format!("Grow: {:.3} ms/grow", avg_grow_ms));

                // STEP 4: Validate

                #[cfg(not(debug_assertions))]
                {
                    let threshold = 0x0C as f64;
                    assert!(avg_ns <= threshold, "lookup_n too slow: {avg_ns} ns/op");
                }
            }
        }
    }

    mod free {
        use super::*;
        use std::time::Instant;

        #[test]
        fn test_free_maps_correctly_to_free_one() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_maps_free_one");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");
                let total = ((*trail.meta_ptr).nwords << 0x06) as usize;

                // fill up entire map
                assert!(trail.lookup(total).is_some());
                assert_eq!((*trail.meta_ptr).free, 0x00);

                // free the last slot
                let free_idx = total - 0x01;
                trail.free(free_idx, 0x01);

                assert_eq!((*trail.meta_ptr).free, 0x01);

                let wi = free_idx >> 0x06;
                let off = free_idx & 0x3F;
                assert_eq!(((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01, 0x00);
            }
        }

        #[test]
        fn test_free_maps_correctly_to_free_n() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_maps_free_n");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");
                let total = ((*trail.meta_ptr).nwords << 0x06) as usize;

                // fill up entire bitmap
                assert!(trail.lookup(total).is_some());
                assert_eq!((*trail.meta_ptr).free, 0x00);

                let start = 0x10;
                let want = 0x0A;
                trail.free(start, want);

                assert_eq!((*trail.meta_ptr).free, want as u64);

                for b in start..(start + want) {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    assert_eq!(((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01, 0x00);
                }
            }
        }

        #[test]
        fn test_free_correctly_wraps_around_word_boundary() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_wraps");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");

                // fill up everything
                let total = ((*trail.meta_ptr).nwords << 0x06) as usize;
                assert!(trail.lookup(total).is_some());

                // free block crossing a 64-bit word boundary
                let start = 0x3C;
                let want = 0x08;
                trail.free(start, want);

                assert_eq!((*trail.meta_ptr).free, want as u64);

                for b in start..(start + want) {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    assert_eq!(((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01, 0x00);
                }
            }
        }

        #[test]
        fn test_free_exactly_ends_on_word_boundary() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_exact_end");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");

                let total = ((*trail.meta_ptr).nwords << 0x06) as usize;
                assert!(trail.lookup(total).is_some());

                // region (start=16, len=48 => end=64)
                let start = 0x10;
                let want = 0x30;

                trail.free(start, want);
                assert_eq!((*trail.meta_ptr).free, want as u64);

                for b in start..(start + want) {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    assert_eq!(((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01, 0x00);
                }
            }
        }

        #[test]
        fn test_free_validates_correctly() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_validates");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");

                let total = ((*trail.meta_ptr).nwords << 0x06) as usize;
                assert!(trail.lookup(total).is_some());

                // free a region
                let start = 0x20;
                let want = 0x0C;
                trail.free(start, want);
                assert_eq!((*trail.meta_ptr).free, want as u64);

                // lookup must return exactly this region
                let got = trail.lookup(want).expect("Slot");
                assert_eq!(got, start);
            }
        }

        #[test]
        #[cfg(debug_assertions)]
        #[should_panic]
        fn test_free_oob_panics() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_panic_oob");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");
                // idx out of bitmap range => must panic
                trail.free(0x200, 0x01);
            }
        }

        #[test]
        #[cfg(debug_assertions)]
        #[should_panic]
        fn test_free_zero_panics() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_zero_panic");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");
                trail.free(0x00, 0x00);
            }
        }

        #[test]
        fn test_free_on_empty_bmap_does_nothing() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_on_empty");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");
                (*trail.meta_ptr).free = 0x00;
                trail.free(0x00, 0x04);

                for b in 0x00..0x04 {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    assert_eq!(((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01, 0x00);
                }
            }
        }

        #[test]
        fn test_free_on_full_bitmap_and_reallocate_all() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_on_full_bm");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Trail");

                let total = ((*trail.meta_ptr).nwords << 0x06) as usize;
                assert!(trail.lookup(total).is_some());
                assert_eq!((*trail.meta_ptr).free, 0x00);

                // free up entire bitmap
                trail.free(0x00, total);
                assert_eq!((*trail.meta_ptr).free, total as u64);

                // reallocate all
                assert!(trail.lookup(total).is_some());
                assert_eq!((*trail.meta_ptr).free, 0x00);
            }
        }

        #[test]
        #[ignore]
        fn bench_free_till_empty() {
            const INIT_CAP: usize = 0x20_000; // cap of `1_31_072`

            // 50% single slot lookup
            const CHUNKS: [usize; 0x10] = [
                0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x08, 0x0C, 0x10,
            ];
            let sum: usize = CHUNKS.into_iter().sum();
            let iters_to_fill = INIT_CAP / sum;

            let (mut cfg, _tmp) = TurboConfig::test_cfg("[BENCH] Trail::free_till_empty");
            cfg = cfg.init_cap(INIT_CAP).expect("Update init cap");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new Trail");
                let meta = &mut *trail.meta_ptr;

                // STEP 1: Warmup
                //
                // NOTE: warmup to eliminate cold cache & cold cpu, and branch predictor effects

                let meta = &mut *trail.meta_ptr;
                let nwords = meta.nwords as usize;
                let total = nwords << 0x06;
                assert!(trail.lookup_n(total).is_some()); // fill up everything
                trail.free(0x00, total); // free up everything

                // STEP 2: Benching

                // fill up entire bitmap
                assert!(trail.lookup_n(total).is_some()); // fill up everything

                let mut lookup_timings = Vec::<f64>::new();
                let mut bitpos = 0x00usize;

                for _ in 0x00..iters_to_fill {
                    let start = Instant::now();
                    for chunk in CHUNKS {
                        trail.free(bitpos, chunk);
                        bitpos += chunk;
                    }
                    let elapsed = start.elapsed();

                    let us_op = elapsed.as_nanos() as f64 / CHUNKS.len() as f64;
                    lookup_timings.push(us_op);
                }

                // sanity check
                assert!(
                    (*trail.meta_ptr).free == total as u64,
                    "BMap must be entirely free at this point"
                );

                // STEP 3: Measure results

                let avg_ns = lookup_timings.iter().sum::<f64>() / lookup_timings.len() as f64;
                cfg.logger.info(format!("Free: {:.3} ns/op", avg_ns));

                // STEP 4: Validate

                #[cfg(not(debug_assertions))]
                {
                    let threshold = 0x0C as f64;
                    assert!(avg_ns <= threshold, "lookup_n too slow: {avg_ns} ns/op");
                }
            }
        }
    }

    mod lookup_one {
        use super::*;
        use std::time::Instant;

        #[test]
        #[cfg(not(debug_assertions))]
        fn test_lookup_one_sequential_filling() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_1_seq_fill");

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
        fn test_lookup_one_wraps_around_correctly() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_1_wraps");

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
        #[cfg(not(debug_assertions))]
        fn test_lookup_one_preserves_meta_free_invariant() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_1_preserves_meta");

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
        fn test_lookup_one_bit_consistency() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_1_bit_consitancy");

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
        #[cfg(not(debug_assertions))]
        fn test_lookup_one_returns_none_when_full() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_1_none_full");
            cfg = cfg.init_cap(0x100).expect("Update INIT_CAP");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new trail");
                let nwords = (*trail.meta_ptr).nwords as usize;

                // fill in the entire map
                for _ in 0x00..0x100 {
                    assert!(trail.lookup_one().is_some());
                }

                // no slots left now
                assert!(trail.lookup_one().is_none());
                assert_eq!((*trail.meta_ptr).free, 0x00);
            }
        }

        #[test]
        fn test_lookup_one_finds_single_freed_slot() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_1_finds_single_free");
            cfg = cfg.init_cap(0x100).expect("Update INIT_CAP");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new trail");
                let nwords = (*trail.meta_ptr).nwords as usize;
                let total_bits = nwords * 0x40;

                // fill in the entire map
                for _ in 0x00..0x100 {
                    assert!(trail.lookup_one().is_some());
                }

                // clear a single bit from the map
                let free_idx = 0x7B;
                let wi = free_idx >> 6;
                let off = free_idx & 0x3F;
                (*trail.bmap_ptr.add(wi)).0 &= !(1u64 << off);
                (*trail.meta_ptr).free = 1;

                // lookup_one now must return the exact freed slot
                let found = trail.lookup_one().expect("should find the freed slot");
                assert_eq!(found, free_idx);
            }
        }

        #[test]
        #[ignore]
        fn bench_lookup_one() {
            const INIT_CAP: usize = 0x80_000; // 524288 bits

            let (mut cfg, _tmp) = TurboConfig::test_cfg("[BENCH] Trail::lookup_one");
            cfg = cfg.init_cap(INIT_CAP).expect("Update init cap");

            unsafe {
                let rounds = 0x0A;
                let iters = INIT_CAP;

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
                    let nwords = meta.nwords as usize;
                    meta.free = meta.nwords * 0x40;
                    meta.cw_idx = 0x00;
                    for i in 0x00..nwords {
                        (*trail.bmap_ptr.add(i)).0 = 0x00;
                    }

                    let start = Instant::now();
                    for _ in 0x00..iters {
                        assert!(trail.lookup_one().is_some());
                    }
                    let elapsed = start.elapsed();
                    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
                    results.push(ns_per_op);
                }

                let avg: f64 = results.iter().sum::<f64>() / results.len() as f64;
                cfg.logger.info(format!("Lookup: {:.3} ns/op", avg));

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

    mod lookup_n {
        use super::*;
        use std::time::Instant;

        #[test]
        #[cfg(not(debug_assertions))]
        fn test_lookup_n_correctly_works() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_works");

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
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_contienous_blocks");

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
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_wraps");

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
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_bit_consistency");
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
        #[cfg(debug_assertions)]
        #[should_panic]
        fn test_lookup_n_zero_panics() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_zero_panics");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("Create new trail");
                let _ = trail.lookup_n(0x00);
            }
        }

        #[test]
        #[cfg(not(debug_assertions))]
        fn test_lookup_n_returns_none_when_full() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_none_full");
            cfg = cfg.init_cap(0x100).expect("Update INIT_CAP");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let nwords = (*trail.meta_ptr).nwords as usize;

                // fill in the entire map
                for _ in 0x00..0x80 {
                    assert!(trail.lookup_n(0x02).is_some());
                }

                assert!(trail.lookup_n(0x02).is_none());
            }
        }

        #[test]
        fn test_lookup_n_finds_freed_block() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_finds_free");
            cfg = cfg.init_cap(0x100).expect("Update INIT_CAP");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let nwords = (*trail.meta_ptr).nwords as usize;
                let total = nwords * 0x40;

                // fill in the entire map
                for _ in 0x00..0x80 {
                    assert!(trail.lookup_n(0x02).is_some());
                }

                // free up some slots
                let want = 0x05;
                let start = 0x2A.min(total - want);
                let end = start + want;
                for b in start..end {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    (*trail.bmap_ptr.add(wi)).0 &= !(0x01 << off);
                }
                (*trail.meta_ptr).free = want as u64;

                let got = trail.lookup_n(want).expect("must find freed block");
                assert_eq!(got, start, "lookup_n must return the freed region start");
            }
        }

        #[test]
        fn test_lookup_n_spans_word_boundary_start_near_end() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_spans_words");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let nwords = (*trail.meta_ptr).nwords as usize;
                let total = nwords * 0x40;

                // fill entire bitmap
                for wi in 0x00..nwords {
                    (*trail.bmap_ptr.add(wi)).0 = u64::MAX;
                }
                (*trail.meta_ptr).free = 0x00;

                // cross word run (60..68)
                let start = 0x3C;
                let want = 0x08;
                for b in start..start + want {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    (*trail.bmap_ptr.add(wi)).0 &= !(0x01 << off);
                }
                (*trail.meta_ptr).free = want as u64;
                (*trail.meta_ptr).cw_idx = (start >> 0x06) as u64;

                let got = trail.lookup_n(want).expect("must find freed run");
                assert_eq!(got, start, "must return exact cross-word start");
            }
        }

        #[test]
        fn test_lookup_n_exactly_ends_on_word_boundary() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_lookup_n_exact_word_boundry");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let nwords = (*trail.meta_ptr).nwords as usize;

                // fill entire bitmap
                for wi in 0x00..nwords {
                    (*trail.bmap_ptr.add(wi)).0 = u64::MAX;
                }
                (*trail.meta_ptr).free = 0x00;

                // start=16, want=48 -> ends at 64
                let start = 0x10;
                let want = 0x30; // start+want == 64
                for b in start..start + want {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    (*trail.bmap_ptr.add(wi)).0 &= !(0x01 << off);
                }
                (*trail.meta_ptr).free = want as u64;
                (*trail.meta_ptr).cw_idx = (start >> 0x06) as u64;

                let got = trail.lookup_n(want).expect("must find block ending at word boundary");
                assert_eq!(got, start, "must return start for block that ends exactly on boundary");
            }
        }

        #[test]
        #[ignore]
        fn bench_lookup_n() {
            const INIT_CAP: usize = 0x80_000; // 524288 bits
            const ROUNDS: usize = 0x14;
            const CHUNKS: [usize; 0x0C] = [0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x14, 0x18, 0x20];

            let sum: usize = CHUNKS.into_iter().sum();
            let iters = INIT_CAP / sum;

            let (mut cfg, _tmp) = TurboConfig::test_cfg("[BENCH] Trail::lookup_n");
            cfg = cfg.init_cap(INIT_CAP).expect("Update init cap");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("New Trail");

                // STEP 1: Warmup

                // NOTE: warmup eliminates cold cache & cold cpu, and branch predictor effects
                let meta = &mut *trail.meta_ptr;
                let nwords = meta.nwords as usize;
                let total = nwords * 0x40;
                for _ in 0x00..total {
                    let _ = trail.lookup_one();
                }

                // STEP 2: Benches

                let mut results = Vec::with_capacity(ROUNDS);
                for r in 0x00..ROUNDS {
                    // HACK: We reset bmap so lookup always does real work
                    let meta = &mut *trail.meta_ptr;
                    let nwords = meta.nwords as usize;
                    meta.free = meta.nwords * 0x40;
                    meta.cw_idx = 0x00;
                    for i in 0x00..nwords {
                        (*trail.bmap_ptr.add(i)).0 = 0x00;
                    }

                    for _ in 0x00..iters {
                        let start = Instant::now();

                        assert!(trail.lookup_n(CHUNKS[0x00]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x01]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x02]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x03]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x04]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x05]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x06]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x07]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x08]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x09]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x0A]).is_some());
                        assert!(trail.lookup_n(CHUNKS[0x0B]).is_some());

                        let elapsed = start.elapsed();
                        let ns_op = elapsed.as_nanos() as f64 / CHUNKS.len() as f64;
                        results.push(ns_op);
                    }
                }

                // STEP 3: Compute results

                let avg: f64 = results.iter().sum::<f64>() / results.len() as f64;
                cfg.logger.info(format!("Lookup: {:.3} ns/op", avg));

                #[cfg(not(debug_assertions))]
                {
                    let threshold = 0x0A as f64;
                    assert!(avg <= threshold, "lookup_n too slow: {avg} ns/op");
                }
            }
        }
    }

    mod free_n {
        use super::*;
        use std::time::Instant;

        #[test]
        fn test_free_n_works_correctly() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_n_works");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let total = (*trail.meta_ptr).nwords as usize * 0x40;

                // fill up entire bitmap
                assert!(trail.lookup_n(total).is_some());
                assert_eq!((*trail.meta_ptr).free, 0x00);

                // free slots [20..28]
                let start = 0x14;
                let want = 0x08;
                trail.free(start, want);

                assert_eq!((*trail.meta_ptr).free, want as u64);

                for b in 0x00..total {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    let bit = ((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01;

                    if b >= start && b < start + want {
                        assert_eq!(bit, 0x00, "bit {b} must be free");
                    } else {
                        assert_eq!(bit, 0x01, "bit {b} must remain allocated");
                    }
                }
            }
        }

        #[test]
        fn test_free_n_correctly_wraps_on_word_boundary() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_n_wraps");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let total = (*trail.meta_ptr).nwords as usize * 0x40;

                assert!(trail.lookup_n(total).is_some());

                // cross-boundary free (63..70)
                let start = 0x3F;
                let want = 0x07;

                trail.free(start, want);
                assert_eq!((*trail.meta_ptr).free, want as u64);

                for b in 0x00..total {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    let bit = ((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01;

                    if b >= start && b < start + want {
                        assert_eq!(bit, 0x00, "bit {b} must be free (cross-boundary)");
                    } else {
                        assert_eq!(bit, 0x01, "bit {b} must remain allocated");
                    }
                }
            }
        }

        #[test]
        fn test_free_n_correctly_ends_on_boundary() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_n_word_boundry");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let nwords = (*trail.meta_ptr).nwords as usize;

                // fill up entire map
                let total = nwords * 0x40;
                assert!(trail.lookup_n(total).is_some());

                // free region ending exactly on word boundary (start=16, len=48 => end=64)
                let start = 0x10;
                let want = 0x30;

                trail.free(start, want);
                assert_eq!((*trail.meta_ptr).free, want as u64);

                for b in 0x00..total {
                    let wi = b >> 0x06;
                    let off = b & 0x3F;
                    let bit = ((*trail.bmap_ptr.add(wi)).0 >> off) & 0x01;

                    if b >= start && b < start + want {
                        assert_eq!(bit, 0x00, "bit {b} must be free");
                    } else {
                        assert_eq!(bit, 0x01, "bit {b} must stay allocated");
                    }
                }
            }
        }

        #[test]
        fn test_free_n_followed_by_lookup_n_returns_exact_block() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_n_fld_lookup_n");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let total = (*trail.meta_ptr).nwords as usize * 0x40;

                // fill up entire map
                assert!(trail.lookup_n(total).is_some());

                // Free first 12 slots
                let start = 0x00;
                let want = 0x0C;
                trail.free(start, want);

                // Now lookup_n must return exactly that region
                let got = trail.lookup_n(want).expect("must find freed block");
                assert_eq!(got, start);
            }
        }

        #[test]
        #[cfg(debug_assertions)]
        #[should_panic]
        fn test_free_n_panics_on_oob_input() {
            let (cfg, _tmp) = TurboConfig::test_cfg("trail_free_n_panics_oob");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("trail");
                let total = (*trail.meta_ptr).nwords as usize * 0x40;

                // allocate all
                assert!(trail.lookup_n(total).is_some());

                // must panic (free across boundry)
                trail.free_n(total - 0x01, 0x04);
            }
        }

        #[test]
        #[ignore]
        fn bench_free_n() {
            const INIT_CAP: usize = 0x80_000; // 524288 bits
            const ROUNDS: usize = 0x14;
            const CHUNKS: [usize; 0x0C] = [0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x14, 0x18, 0x20];

            let sum: usize = CHUNKS.into_iter().sum();
            let iters = INIT_CAP / sum;

            let (mut cfg, _tmp) = TurboConfig::test_cfg("[BENCH] Trail::free_n");
            cfg = cfg.init_cap(INIT_CAP).expect("Update init cap");

            unsafe {
                let mut trail = Trail::new(&cfg).expect("New Trail");

                // STEP 1: Warmup

                let meta = &mut *trail.meta_ptr;
                let nwords = meta.nwords as usize;
                let total = nwords << 0x06;
                assert!(trail.lookup_n(total).is_some()); // fill up everything
                trail.free(0x00, total); // free up everything

                // STEP 2: Benches

                let mut results = Vec::with_capacity(ROUNDS);
                for r in 0x00..ROUNDS {
                    // HACK: We reset bmap so lookup always does real work
                    let meta = &mut *trail.meta_ptr;
                    let nwords = meta.nwords as usize;
                    meta.free = meta.nwords << 0x06;
                    meta.cw_idx = 0x00;
                    for i in 0x00..nwords {
                        (*trail.bmap_ptr.add(i)).0 = 0x00;
                    }

                    // fill up entier bmap
                    assert!(trail.lookup_n(total).is_some());

                    for _ in 0x00..iters {
                        let start = Instant::now();

                        trail.free_n(0x00, CHUNKS[0x00]);
                        trail.free_n(0x20, CHUNKS[0x01]);
                        trail.free_n(0x40, CHUNKS[0x02]);
                        trail.free_n(0x80, CHUNKS[0x03]);
                        trail.free_n(0xC0, CHUNKS[0x04]);
                        trail.free_n(0x100, CHUNKS[0x05]);
                        trail.free_n(0x140, CHUNKS[0x06]);
                        trail.free_n(0x180, CHUNKS[0x07]);
                        trail.free_n(0x1C0, CHUNKS[0x08]);
                        trail.free_n(0x200, CHUNKS[0x09]);
                        trail.free_n(0x240, CHUNKS[0x0A]);
                        trail.free_n(0x280, CHUNKS[0x0B]);

                        let elapsed = start.elapsed();
                        let ns_op = elapsed.as_nanos() as f64 / CHUNKS.len() as f64;
                        results.push(ns_op);
                    }
                }

                // STEP 3: Compute results

                let avg: f64 = results.iter().sum::<f64>() / results.len() as f64;
                cfg.logger.info(format!("Free: {:.3} ns/op", avg));

                #[cfg(not(debug_assertions))]
                {
                    let threshold = 0x07 as f64;
                    assert!(avg <= threshold, "free_n too slow: {avg} ns/op");
                }
            }
        }
    }
}
