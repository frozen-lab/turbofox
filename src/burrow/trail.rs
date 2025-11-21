use super::OS_PAGE_SIZE;
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

const NEXT_PAGE_LINK_SPACE: usize = 0x08; // (u64) page idx
const FREE_SLOTS_IN_PAGE_SPACE: usize = 0x08; // (u64) free slots available
const RESERVED_SPACE_PER_PAGE: usize = NEXT_PAGE_LINK_SPACE + FREE_SLOTS_IN_PAGE_SPACE;

const INIT_OS_PAGES: u64 = 0x02; // (1) BitMap + (1) AdjArr

//
// BitMap (4096) => 8 (next_idx) + 8 (total_free) + 4080 (510 u64's) [i.e. 32640 entries]
//

const BIT_MAP_BITS_PER_PAGE: usize = (OS_PAGE_SIZE - RESERVED_SPACE_PER_PAGE) * 0x08;
const BIT_MAP_WORDS_PER_PAGE: usize = BIT_MAP_BITS_PER_PAGE / 0x40;

#[repr(C, align(0x40))]
#[derive(Debug)]
struct BitMapPtr {
    bits: [u64; BIT_MAP_WORDS_PER_PAGE],
    free: u64,
    next: u64,
}

#[derive(Debug)]
struct BitMap {
    ptr: *mut BitMapPtr,
    bit_idx: usize,
}

// sanity checks
const _: () = assert!(BIT_MAP_BITS_PER_PAGE % 0x40 == 0, "Must be u64 aligned");
const _: () = assert!(std::mem::size_of::<BitMapPtr>() % 0x40 == 0, "Must be 64 bytes aligned");
const _: () = assert!(
    std::mem::size_of::<BitMapPtr>() == OS_PAGE_SIZE,
    "Must align w/ OS_PAGE size"
);
const _: () = assert!(
    (BIT_MAP_BITS_PER_PAGE / 0x08) + RESERVED_SPACE_PER_PAGE == OS_PAGE_SIZE,
    "Correct os page alignment"
);

impl BitMap {
    #[inline(always)]
    fn new(ptr: *mut BitMapPtr) -> Self {
        Self { ptr, bit_idx: 0 }
    }

    /// Lookup for a single slot
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn lookup(&mut self) -> Option<usize> {
        let bitmap_ptr = &mut *self.ptr;

        // sanity check
        debug_assert!(
            bitmap_ptr.free as usize <= BIT_MAP_BITS_PER_PAGE,
            "No of free slots are invalid"
        );

        // no more slots available
        if bitmap_ptr.free == 0x00 {
            return None;
        }

        // NOTE: Fast Path (when the BitMap is new we have all the bits
        // freed up, so we can speed things up)
        if self.bit_idx < BIT_MAP_BITS_PER_PAGE {
            let w = (self.bit_idx >> 0x06) as usize; // same as `bit_idx / 6`
            let off = (self.bit_idx & 0x3F) as usize;
            let word = bitmap_ptr.bits.get_unchecked(w);
            let mask = 0x01u64 << off;

            // we must make sure that the bit is indeed free
            if (word & mask) == 0x00 {
                let bit = self.bit_idx;
                self.bit_idx += 0x01;
                bitmap_ptr.bits[w] = *word | mask;
                bitmap_ptr.free -= 0x01;

                // NOTE: We prefetch next word to avoid cache miss
                {
                    let nxt_w_idx = w + 0x01;
                    if nxt_w_idx < BIT_MAP_WORDS_PER_PAGE {
                        core::arch::x86_64::_mm_prefetch(
                            (bitmap_ptr.bits.as_ptr().add(nxt_w_idx) as *const i8),
                            core::arch::x86_64::_MM_HINT_T0,
                        );
                    }
                }

                return Some(bit);
            }
        }

        // NOTE: Slow Path (when we start again from start while free > 0)
        let mut scanned = 0usize;
        let mut word = {
            let wi = self.bit_idx >> 0x06;

            if wi >= BIT_MAP_WORDS_PER_PAGE {
                0usize
            } else {
                wi
            }
        };

        // NOTE: w/ avx2, we process 4 words per lane
        if is_x86_feature_detected!("avx2") {
            use core::arch::x86_64::{
                __m256i, _mm256_castsi256_pd, _mm256_cmpeq_epi64, _mm256_loadu_si256, _mm256_movemask_pd,
                _mm256_set1_epi64x, _tzcnt_u64,
            };

            let full_mask = _mm256_set1_epi64x(-0x01);

            while scanned < BIT_MAP_WORDS_PER_PAGE {
                // fast path (we process 4 words using `mm256`)
                if (BIT_MAP_WORDS_PER_PAGE - word) >= 0x04 {
                    let ptr = bitmap_ptr.bits.as_ptr().add(word) as *const __m256i;
                    let v = _mm256_loadu_si256(ptr);
                    let cmp = _mm256_cmpeq_epi64(v, full_mask);
                    let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp)) as u32; // 4 bit mask one for each lane

                    // sanity check
                    debug_assert!(mask <= 0x0F, "Only 4 lower bits should be active");

                    // at least one of 4 lane is not full
                    if mask != 0x0F {
                        let not_full = (!mask) & 0x0F;
                        let lane = not_full.trailing_zeros() as usize; // 0..3 (always)
                        let idx = word + lane;

                        // sanity checks
                        debug_assert!(lane < 0x04, "Lane must be between 0..3");
                        debug_assert!(idx < BIT_MAP_WORDS_PER_PAGE, "Idx should be in the bounds");

                        // scalar dive to get the actual free bit from the lane :) (can't optimize this 🥹)
                        let w = bitmap_ptr.bits.get_unchecked(idx);
                        let inv = !*w;
                        let off = _tzcnt_u64(inv) as usize;
                        let bit = idx * 0x40 + off;

                        bitmap_ptr.bits[idx] = *w | (0x01u64 << off);
                        bitmap_ptr.free -= 0x01;
                        self.bit_idx = bit + 0x01;
                        return Some(bit);
                    }

                    // advance by 4 words
                    scanned += 0x04;
                    word += 0x04;
                    if word >= BIT_MAP_WORDS_PER_PAGE {
                        word -= BIT_MAP_BITS_PER_PAGE;
                    }
                    continue;
                }

                for _ in 0..(BIT_MAP_WORDS_PER_PAGE - word) {
                    let w0 = bitmap_ptr.bits.get_unchecked(word);
                    if *w0 != u64::MAX {
                        let inv = !*w0;
                        let off = core::arch::x86_64::_tzcnt_u64(inv) as usize;
                        let bit = word * 0x40 + off;
                        bitmap_ptr.bits[word] = *w0 | (0x01u64 << off);
                        bitmap_ptr.free -= 0x01;
                        self.bit_idx = bit + 0x01;
                        return Some(bit);
                    }

                    scanned += 0x01;
                    word = if word + 0x01 == BIT_MAP_WORDS_PER_PAGE {
                        0x00
                    } else {
                        word + 0x01
                    };
                }
            }

            // NOTE: This may never occur because of the `free` check we do earlier
            return None;
        }

        // NOTE: w/ avx2, we process 4 words per lane
        if is_x86_feature_detected!("sse2") {
            use std::arch::x86_64::_mm_set1_epi64x;

            let full_mask = _mm_set1_epi64x(-0x01);
            while scanned < BIT_MAP_WORDS_PER_PAGE {
                if (BIT_MAP_WORDS_PER_PAGE - word) >= 0x02 {}
            }

            // NOTE: This may never occur because of the `free` check we do earlier
            return None;
        }

        // NOTE: Scalar fallback when avx2/sse2 are not available
        // manually unrolled 4-word scan
        while scanned < BIT_MAP_WORDS_PER_PAGE {
            let w0 = bitmap_ptr.bits.get_unchecked(word);
            if *w0 != u64::MAX {
                let inv = !*w0;
                let off = core::arch::x86_64::_tzcnt_u64(inv) as usize;
                let bit = word * 0x40 + off;
                bitmap_ptr.bits[word] = *w0 | (0x01u64 << off);
                bitmap_ptr.free -= 0x01;
                self.bit_idx = bit + 0x01;
                return Some(bit);
            }

            scanned += 0x01;
            word = if word + 0x01 == BIT_MAP_WORDS_PER_PAGE {
                0x00
            } else {
                word + 0x01
            };
            if scanned >= BIT_MAP_WORDS_PER_PAGE {
                break;
            }

            let w1 = bitmap_ptr.bits.get_unchecked(word);
            if *w1 != u64::MAX {
                let inv = !*w1;
                let off = core::arch::x86_64::_tzcnt_u64(inv) as usize;
                let bit = word * 0x40 + off;
                bitmap_ptr.bits[word] = *w1 | (0x01u64 << off);
                bitmap_ptr.free -= 0x01;
                self.bit_idx = bit + 0x01;
                return Some(bit);
            }

            scanned += 0x01;
            word = if word + 0x01 == BIT_MAP_WORDS_PER_PAGE {
                0x00
            } else {
                word + 0x01
            };
            if scanned >= BIT_MAP_WORDS_PER_PAGE {
                break;
            }

            let w2 = bitmap_ptr.bits.get_unchecked(word);
            if *w2 != u64::MAX {
                let inv = !*w2;
                let off = core::arch::x86_64::_tzcnt_u64(inv) as usize;
                let bit = word * 0x40 + off;
                bitmap_ptr.bits[word] = *w2 | (0x01u64 << off);
                bitmap_ptr.free -= 0x01;
                self.bit_idx = bit + 0x01;
                return Some(bit);
            }

            scanned += 0x01;
            word = if word + 0x01 == BIT_MAP_WORDS_PER_PAGE {
                0x00
            } else {
                word + 0x01
            };
            if scanned >= BIT_MAP_WORDS_PER_PAGE {
                break;
            }

            let w3 = bitmap_ptr.bits.get_unchecked(word);
            if *w3 != u64::MAX {
                let inv = !*w3;
                let off = core::arch::x86_64::_tzcnt_u64(inv) as usize;
                let bit = word * 0x40 + off;
                bitmap_ptr.bits[word] = *w3 | (0x01u64 << off);
                bitmap_ptr.free -= 0x01;
                self.bit_idx = bit + 0x01;
                return Some(bit);
            }

            scanned += 0x01;
            word = if word + 0x01 == BIT_MAP_WORDS_PER_PAGE {
                0x00
            } else {
                word + 0x01
            };
        }

        None
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn lookup_n(&mut self, n: usize) -> Option<(Vec<usize>, bool)> {
        let bitmap_ptr = &mut *self.ptr;

        // sanity checks
        debug_assert!(n > 0x00, "Invalid inputs");
        debug_assert!(n <= BIT_MAP_BITS_PER_PAGE, "N is very large");
        debug_assert!(
            (bitmap_ptr.free as usize) <= BIT_MAP_BITS_PER_PAGE,
            "No of free slots are invalid"
        );

        // not enough slots
        if (bitmap_ptr.free < n as u64) {
            return None;
        }

        let mut out: Vec<usize> = Vec::with_capacity(n);

        // NOTE: Fast path (contagious free blocks)
        let end = self.bit_idx + n;
        if end <= BIT_MAP_BITS_PER_PAGE {
            let mut bit = self.bit_idx;
            while bit < end {
                let w = (bit >> 0x06) as usize;
                let off = (bit & 0x3F) as usize;

                let word = bitmap_ptr.bits.get_unchecked(w);
                let m = 0x01u64 << off;

                // NOTE: we found the occupied bit! So we break fast path and switch to slow one
                if (*word & m) != 0x00 {
                    out.clear();
                    break;
                }

                bitmap_ptr.bits[w] = *word | m;
                out.push(bit);
                bit += 0x01;
            }

            if out.len() == n {
                bitmap_ptr.free -= n as u64;
                self.bit_idx = bit;

                // NOTE: We prefetch next word to avoid cache miss
                {
                    let nxt_w_idx = (bit >> 0x06) + 0x01;
                    if nxt_w_idx < BIT_MAP_WORDS_PER_PAGE {
                        core::arch::x86_64::_mm_prefetch(
                            (bitmap_ptr.bits.as_ptr().add(nxt_w_idx) as *const i8),
                            core::arch::x86_64::_MM_HINT_T0,
                        );
                    }
                }

                return Some((out, true));
            }
        }

        None
    }
}

// NOTE/TODO:
//  - For sequential slots, we could store a u8 in our signs and skip th AdjArr
//  - Use AdjArr only when writes are scattered
//  - We avoid SIMD scans for write, hence increasing lookup speeds for write

//
// AdjArr (4096) => 8 (next_idx) + 8 (total_free) + 36 (288 entries) [u32 * 9] + 4032 (288 * 7 * 2) + 12 (padding)
// --- array => [u16 * 5][next_idx][page_idx], i.e. u16 * 7
//

type AdjArrItemType = u16;
const ADJ_ARR_IDX_SIZE: usize = 0x24; // (36) 288 entries
const ADJ_ARR_ITEMS_PER_ARR: usize = 0x07; // 7 entries per array
const ADJ_ARR_PADDING: usize = 0x0C; // 12 bytes
const ADJ_ARR_TOTAL_ENTRIES: usize = ((OS_PAGE_SIZE - RESERVED_SPACE_PER_PAGE - ADJ_ARR_IDX_SIZE - ADJ_ARR_PADDING)
    / std::mem::size_of::<AdjArrItemType>())
    / ADJ_ARR_ITEMS_PER_ARR;

#[repr(C, align(0x40))]
#[derive(Debug)]
struct AdjArrPtr {
    idx: [u32; ADJ_ARR_IDX_SIZE / 0x04],
    arrays: [[u16; ADJ_ARR_ITEMS_PER_ARR]; ADJ_ARR_TOTAL_ENTRIES],
    _padd: [u16; ADJ_ARR_PADDING / 0x02],
    free: u64,
    next: u64,
}

#[derive(Debug)]
struct AdjArr {
    ptr: *mut AdjArrPtr,
    idx: usize,
}

// sanity checks
const _: () = assert!(ADJ_ARR_IDX_SIZE % 0x04 == 0x00, "Must be u32 aligned");
const _: () = assert!(ADJ_ARR_IDX_SIZE * 0x08 == ADJ_ARR_TOTAL_ENTRIES);
const _: () = assert!(std::mem::size_of::<AdjArrPtr>() % 0x40 == 0, "Must be 64 bytes aligned");
const _: () = assert!(
    std::mem::size_of::<AdjArrPtr>() == OS_PAGE_SIZE,
    "Must be aligned w/ OS_PAGE size"
);

impl AdjArr {
    #[inline(always)]
    fn new(ptr: *mut AdjArrPtr) -> Self {
        Self { ptr, idx: 0 }
    }
}

//
// Meta
//

#[derive(Debug, Copy, Clone)]
#[repr(C, align(0x40))]
struct Meta {
    magic: [u8; 0x04],
    version: u32,
    nbitmap: u16,
    nadjarr: u16,
    // NOTE: Followig pointers are writeops only, for yank and fetch
    // we simply use ephemeral pointers
    bitmap_pidx: u16,
    adjarr_pidx: u16,
    // NOTE: We add this 48 bytes padding to align the [Meta] to 64 bytes
    // so it could fit correctly in a cahce line and be aligned w/ other
    // structs like [BitMapPtr] and [AdjArrPtr]
    _padd: [u8; 0x30],
}

const META_SIZE: usize = std::mem::size_of::<Meta>();

// sanity check
const _: () = assert!(META_SIZE == 0x40, "Must be 64 bytes aligned");

impl Default for Meta {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            nadjarr: 0x01,
            nbitmap: 0x01,
            bitmap_pidx: 0x00, // first page
            adjarr_pidx: 0x01, // second page
            _padd: [0u8; 0x30],
        }
    }
}

//
// Trail
//

const INIT_FILE_LEN: usize = META_SIZE + (OS_PAGE_SIZE * INIT_OS_PAGES as usize);

#[derive(Debug)]
pub(super) struct Trail {
    file: File,
    mmap: MMap,
    adjarr: AdjArr,
    bitmap: BitMap,
    cfg: InternalCfg,
    meta_ptr: *mut Meta,
}

impl Trail {
    /// Creates a new [Trail] file
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn new(cfg: InternalCfg) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);

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
        file.zero_extend(INIT_FILE_LEN)
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

        let mmap = MMap::new(file.0, INIT_FILE_LEN)
            .inspect(|_| {
                cfg.logger
                    .trace(format!("(TRAIL) Mmaped newly created file w/ len={INIT_FILE_LEN}"))
            })
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) Failed to mmap the file({:?}): {e}", path));

                // NOTE: Close + Delete the created file, so new init could work w/o any issues
                //
                // HACK: We ignore error from `close_and_del` as we are already in an errored
                // state, and primary error is more imp then this!
                Self::close_and_del_file(&cfg, &file);

                e
            })?;

        // metadata init & sync
        //
        // NOTE: we use `ms_sync` here to make sure metadata is persisted before
        // any other updates are conducted on the mmap,
        //
        // NOTE: we can afford this syscall here, as init does not come under the fast
        // path. Also it's just one time thing!
        mmap.write(0, &Meta::default());
        mmap.ms_sync().map_err(|e| {
            cfg.logger
                .error(format!("(TRAIL) Failed to write Metadata to mmaped file: {e}"));
            e
        })?;

        let meta_ptr = mmap.read_mut::<Meta>(0);
        let bitmap_ptr = mmap.read_mut::<BitMapPtr>(META_SIZE);
        let adjarr_ptr = mmap.read_mut::<AdjArrPtr>(META_SIZE + OS_PAGE_SIZE);

        cfg.logger.debug("(TRAIL) Created a new file");

        Ok(Self {
            cfg,
            file,
            mmap,
            meta_ptr,
            bitmap: BitMap::new(bitmap_ptr),
            adjarr: AdjArr::new(adjarr_ptr),
        })
    }

    /// Open an existing [Trail] file
    ///
    /// *NOTE*: Returns an [InvalidFile] error when the underlying file is corrupted,
    /// may happen when the file is invalid or tampered with
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn open(cfg: InternalCfg) -> InternalResult<Self> {
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
        if file_len.wrapping_sub(META_SIZE) == 0 || file_len.wrapping_sub(META_SIZE) % OS_PAGE_SIZE != 0 {
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

        let meta_ptr = mmap.ptr_mut() as *mut Meta;

        // metadata validations
        //
        // NOTE/TODO: In future, we need to support the old file versions, if any!
        if (*meta_ptr).magic != MAGIC || (*meta_ptr).version != VERSION {
            cfg.logger.warn("(TRAIL) Existing file has invalid VERSION or MAGIC");
        }

        let bitmap_idx = (*meta_ptr).bitmap_pidx;
        let adjarr_idx = (*meta_ptr).adjarr_pidx;

        // sanity checks
        #[cfg(debug_assertions)]
        {
            let total_pages = (*meta_ptr).nbitmap + (*meta_ptr).nadjarr;
            debug_assert!(bitmap_idx <= total_pages, "BitMap index is out of bounds");
            debug_assert!(adjarr_idx <= total_pages, "AdjArr index is out of bounds");
        }

        let bitmap_ptr = mmap.read_mut::<BitMapPtr>(META_SIZE + (bitmap_idx as usize * OS_PAGE_SIZE));
        let adjarr_ptr = mmap.read_mut::<AdjArrPtr>(META_SIZE + (adjarr_idx as usize * OS_PAGE_SIZE));

        cfg.logger.debug("(TRAIL) Opened an existing file");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            cfg: cfg.clone(),
            bitmap: BitMap::new(bitmap_ptr),
            adjarr: AdjArr::new(adjarr_ptr),
        })
    }

    /// Close & Delete [Trail] file
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline]
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

            let t1 = unsafe { Trail::new(cfg) }.expect("new trail");

            unsafe {
                assert!(t1.file.0 >= 0x00, "File fd must be valid");
                assert!(t1.mmap.len() > 0x00, "Mmap must be non zero");
                assert_eq!((*t1.meta_ptr).magic, MAGIC, "Correct file MAGIC");
                assert_eq!((*t1.meta_ptr).version, VERSION, "Correct file VERSION");
                assert_eq!(
                    (*t1.meta_ptr).nbitmap + (*t1.meta_ptr).nadjarr,
                    INIT_OS_PAGES as u16,
                    "Correct numOf pages"
                );
                assert_eq!((*t1.meta_ptr).bitmap_pidx, 0x00, "Correct ptr for Bits");
                assert_eq!((*t1.meta_ptr).adjarr_pidx, 0x01, "Correct ptr for adjarr");

                let bmap = &*t1.bitmap.ptr;
                assert!(bmap.bits.iter().all(|&b| b == 0x00), "BitMap bits zeroed");
                assert_eq!(bmap.next, 0x00, "BitMap next ptr zeroed");

                let adjarr = &*t1.adjarr.ptr;
                assert!(adjarr.idx.iter().all(|&i| i == 0x00), "AdjArr index zeroed");
                assert!(
                    adjarr.arrays.iter().all(|a| a.iter().all(|&v| v == 0x00)),
                    "AdjArr data zeroed"
                );
                assert_eq!(adjarr.next, 0x00, "AdjArr next ptr zeroed");
            }
        }

        #[test]
        fn test_open_is_valid() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            {
                let t0 = unsafe { Trail::new(cfg.clone()) }.expect("new trail");

                unsafe {
                    let bmap = &mut *t0.bitmap.ptr;
                    let adjarr = &mut *t0.adjarr.ptr;

                    bmap.bits[0xA] = 0xDEADBEEF;
                    (*bmap).next = 0x2A;

                    adjarr.idx[0x05] = 0x07;
                    adjarr.arrays[0x03][0x02] = 0xBEEF;
                    adjarr.next = 0x33;
                }

                drop(t0);
            }

            let t1 = unsafe { Trail::open(cfg) }.expect("open existing");

            unsafe {
                assert!(t1.file.0 >= 0x00, "File fd must be valid");
                assert!(t1.mmap.len() > 0x00, "Mmap must be non zero");
                assert_eq!((*t1.meta_ptr).magic, MAGIC, "Correct file MAGIC");
                assert_eq!((*t1.meta_ptr).version, VERSION, "Correct file VERSION");
                assert_eq!(
                    (*t1.meta_ptr).nbitmap + (*t1.meta_ptr).nadjarr,
                    INIT_OS_PAGES as u16,
                    "Correct noOf pages"
                );
                assert_eq!((*t1.meta_ptr).bitmap_pidx, 0x00, "Correct ptr for Bits");
                assert_eq!((*t1.meta_ptr).adjarr_pidx, 0x01, "Correct ptr for adjarr");

                let bmap = &*t1.bitmap.ptr;
                assert_eq!(bmap.bits[0xA], 0xDEADBEEF, "BitMap persisted bits");
                assert_eq!(bmap.next, 0x2A, "BitMap next persisted");

                let adjarr = &*t1.adjarr.ptr;
                assert_eq!(adjarr.idx[0x05], 0x07, "AdjArr idx persisted");
                assert_eq!(adjarr.arrays[0x03][0x02], 0xBEEF, "AdjArr data persisted");
                assert_eq!(adjarr.next, 0x33, "AdjArr next persisted");
            }
        }

        #[test]
        #[cfg(debug_assertions)]
        #[should_panic]
        fn test_open_panics_on_invalid_metadata_in_file() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            unsafe {
                let t0 = unsafe { Trail::new(cfg.clone()) }.expect("new trail");
                let meta = &mut *t0.meta_ptr;

                // corrupted metadata
                meta.nadjarr = 0x00;
                meta.nbitmap = 0x00;

                drop(t0);
            }

            // should panic
            unsafe { Trail::open(cfg) };
        }
    }
}
