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

//
// BitMap (4096) => 8 (total_free) + 4088 (511 u64's) [i.e. 32704 entries]
//

const BIT_MAP_SPACE_FOR_TOTAL_FREE: usize = 0x08; // (u64) free slots available
const BIT_MAP_BITS_PER_PAGE: usize = (OS_PAGE_SIZE - BIT_MAP_SPACE_FOR_TOTAL_FREE) * 0x08;
const BIT_MAP_WORDS_PER_PAGE: usize = BIT_MAP_BITS_PER_PAGE / 0x40;

#[repr(C, align(0x40))]
#[derive(Debug)]
struct BitMapPtr {
    free: u64,
    bits: [u64; BIT_MAP_WORDS_PER_PAGE],
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
    (BIT_MAP_BITS_PER_PAGE / 0x08) + BIT_MAP_SPACE_FOR_TOTAL_FREE == OS_PAGE_SIZE,
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

        let mut widx = self.bit_idx >> 0x06;
        let mut scanned = 0usize;

        while scanned < BIT_MAP_WORDS_PER_PAGE {
            let word = *bitmap_ptr.bits.get_unchecked(widx);
            if word != u64::MAX {
                let inv = !word;
                let off = core::arch::x86_64::_tzcnt_u64(inv) as usize;
                let bit = (widx << 0x06) | off;

                bitmap_ptr.bits[widx] = word | (0x01u64 << off);
                bitmap_ptr.free -= 0x01;
                self.bit_idx = bit + 0x01;

                return Some(bit);
            }

            let nxt = widx + 0x01;
            scanned += 0x01;
            widx = nxt - ((nxt == BIT_MAP_WORDS_PER_PAGE) as usize) * BIT_MAP_WORDS_PER_PAGE;
        }

        None
    }

    /// Lookup N free slots in [BitMap] (w/ no wraparound and contegious allocations)
    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn lookup_n(&mut self, n: usize) -> Option<Vec<usize>> {
        let bitmap_ptr = &mut *self.ptr;

        // sanity checks
        debug_assert!(n > 0x00, "Invalid inputs");
        debug_assert!(n <= BIT_MAP_BITS_PER_PAGE, "N is very large");
        debug_assert!(self.bit_idx <= BIT_MAP_BITS_PER_PAGE, "BitIdx is out of bounds");
        debug_assert!(
            (bitmap_ptr.free as usize) <= BIT_MAP_BITS_PER_PAGE,
            "No of free slots are invalid"
        );

        // not enough slots
        if bitmap_ptr.free < n as u64 {
            return None;
        }

        // not enough slots (from current idx)
        if self.bit_idx + n > BIT_MAP_BITS_PER_PAGE {
            return None;
        }

        let mut out: Vec<usize> = Vec::with_capacity(n);
        let mut bit_idx = self.bit_idx;

        while bit_idx + n <= BIT_MAP_BITS_PER_PAGE {
            let widx = bit_idx >> 0x06;
            let off = (bit_idx & 0x3F) as usize;

            let word = *bitmap_ptr.bits.get_unchecked(widx);
            let inv = !word & (!0u64 << off);

            // if no free slots (after the `off`) are available, skip to nxt word
            if inv == 0x00 {
                bit_idx += 0x40 - off;
                continue;
            }

            let first_off = core::arch::x86_64::_tzcnt_u64(inv) as usize;
            let first_bit = (widx << 6) | first_off;
            let avail_slots = 0x40 - first_off; // available slots in current word

            if avail_slots >= n {
                let mm = (u64::MAX >> (0x40 - n)) << first_off;
                let p = bitmap_ptr.bits.get_unchecked_mut(widx);

                *p = word | mm;
                bitmap_ptr.free -= n as u64;
                self.bit_idx = first_bit + n;
                out.extend(first_bit..first_bit + n);

                return Some(out);
            }

            let mut slots = avail_slots;
            let mut needed_slots = n - avail_slots;
            let mut word_i = widx + 1;

            while needed_slots > 0 && word_i < BIT_MAP_WORDS_PER_PAGE {
                let widx2 = *bitmap_ptr.bits.get_unchecked(word_i);
                if widx2 == 0 {
                    let slots_taken = needed_slots.min(0x40);
                    slots += slots_taken;
                    needed_slots -= slots_taken;
                    word_i += 0x01;

                    continue;
                }

                // word is full, hence breaks the contegious order of slots
                if widx2 == u64::MAX {
                    break;
                }

                let tz = core::arch::x86_64::_tzcnt_u64(!widx2) as usize;
                let take = needed_slots.min(tz);

                slots += take;
                needed_slots -= take;

                // if true, we cannot extend beyound the `tz` boundry, hence breaks the required order
                if take < 64 && needed_slots > 0 {
                    break;
                }

                word_i += 1;
            }

            if slots >= n {
                let mut remaining = n;
                let mut current = first_bit;

                while remaining > 0 {
                    let cw = current >> 6;
                    let cof = current & 63;

                    let take = remaining.min(64 - cof);
                    let mm = (u64::MAX >> (64 - take)) << cof;

                    let pw = bitmap_ptr.bits.get_unchecked(cw);
                    *bitmap_ptr.bits.get_unchecked_mut(cw) = pw | mm;

                    out.extend(current..current + take);
                    remaining -= take;
                    current += take;
                }

                bitmap_ptr.free -= n as u64;
                self.bit_idx = first_bit + n;
                return Some(out);
            }

            bit_idx = first_bit + 0x01;
        }

        None
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
    // NOTE: Followig pointer are writeops only, for yank and fetch
    // we simply use ephemeral pointers
    bitmap_pidx: u16,
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
            nbitmap: 0x01,
            bitmap_pidx: 0x00, // first page
            _padd: [0u8; 0x30],
        }
    }
}

//
// Trail
//

const INIT_FILE_LEN: usize = META_SIZE + OS_PAGE_SIZE;

#[derive(Debug)]
pub(super) struct Trail {
    file: File,
    mmap: MMap,
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

        cfg.logger.debug("(TRAIL) Created a new file");

        Ok(Self {
            cfg,
            file,
            mmap,
            meta_ptr,
            bitmap: BitMap::new(bitmap_ptr),
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

        // sanity checks
        #[cfg(debug_assertions)]
        {
            let total_pages = (*meta_ptr).nbitmap;
            debug_assert!(bitmap_idx <= total_pages, "BitMap index is out of bounds");
        }

        let bitmap_ptr = mmap.read_mut::<BitMapPtr>(META_SIZE + (bitmap_idx as usize * OS_PAGE_SIZE));

        cfg.logger.debug("(TRAIL) Opened an existing file");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            cfg: cfg.clone(),
            bitmap: BitMap::new(bitmap_ptr),
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::logger::init_test_logger;
//     use tempfile::TempDir;

//     fn temp_dir() -> TempDir {
//         let _ = init_test_logger(None);
//         TempDir::new().expect("temp dir")
//     }

//     mod trail {
//         use super::*;

//         #[test]
//         fn test_new_is_valid() {
//             let tmp = temp_dir();
//             let dir = tmp.path().to_path_buf();
//             let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

//             let t1 = unsafe { Trail::new(cfg) }.expect("new trail");

//             unsafe {
//                 assert!(t1.file.0 >= 0x00, "File fd must be valid");
//                 assert!(t1.mmap.len() > 0x00, "Mmap must be non zero");
//                 assert_eq!((*t1.meta_ptr).magic, MAGIC, "Correct file MAGIC");
//                 assert_eq!((*t1.meta_ptr).version, VERSION, "Correct file VERSION");
//                 assert_eq!(
//                     (*t1.meta_ptr).nbitmap + (*t1.meta_ptr).nadjarr,
//                     INIT_OS_PAGES as u16,
//                     "Correct numOf pages"
//                 );
//                 assert_eq!((*t1.meta_ptr).bitmap_pidx, 0x00, "Correct ptr for Bits");
//                 assert_eq!((*t1.meta_ptr).adjarr_pidx, 0x01, "Correct ptr for adjarr");

//                 let bmap = &*t1.bitmap.ptr;
//                 assert!(bmap.bits.iter().all(|&b| b == 0x00), "BitMap bits zeroed");
//                 assert_eq!(bmap.next, 0x00, "BitMap next ptr zeroed");

//                 let adjarr = &*t1.adjarr.ptr;
//                 assert!(adjarr.idx.iter().all(|&i| i == 0x00), "AdjArr index zeroed");
//                 assert!(
//                     adjarr.arrays.iter().all(|a| a.iter().all(|&v| v == 0x00)),
//                     "AdjArr data zeroed"
//                 );
//                 assert_eq!(adjarr.next, 0x00, "AdjArr next ptr zeroed");
//             }
//         }

//         #[test]
//         fn test_open_is_valid() {
//             let tmp = temp_dir();
//             let dir = tmp.path().to_path_buf();
//             let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

//             {
//                 let t0 = unsafe { Trail::new(cfg.clone()) }.expect("new trail");

//                 unsafe {
//                     let bmap = &mut *t0.bitmap.ptr;
//                     let adjarr = &mut *t0.adjarr.ptr;

//                     bmap.bits[0xA] = 0xDEADBEEF;
//                     (*bmap).next = 0x2A;

//                     adjarr.idx[0x05] = 0x07;
//                     adjarr.arrays[0x03][0x02] = 0xBEEF;
//                     adjarr.next = 0x33;
//                 }

//                 drop(t0);
//             }

//             let t1 = unsafe { Trail::open(cfg) }.expect("open existing");

//             unsafe {
//                 assert!(t1.file.0 >= 0x00, "File fd must be valid");
//                 assert!(t1.mmap.len() > 0x00, "Mmap must be non zero");
//                 assert_eq!((*t1.meta_ptr).magic, MAGIC, "Correct file MAGIC");
//                 assert_eq!((*t1.meta_ptr).version, VERSION, "Correct file VERSION");
//                 assert_eq!(
//                     (*t1.meta_ptr).nbitmap + (*t1.meta_ptr).nadjarr,
//                     INIT_OS_PAGES as u16,
//                     "Correct noOf pages"
//                 );
//                 assert_eq!((*t1.meta_ptr).bitmap_pidx, 0x00, "Correct ptr for Bits");
//                 assert_eq!((*t1.meta_ptr).adjarr_pidx, 0x01, "Correct ptr for adjarr");

//                 let bmap = &*t1.bitmap.ptr;
//                 assert_eq!(bmap.bits[0xA], 0xDEADBEEF, "BitMap persisted bits");
//                 assert_eq!(bmap.next, 0x2A, "BitMap next persisted");

//                 let adjarr = &*t1.adjarr.ptr;
//                 assert_eq!(adjarr.idx[0x05], 0x07, "AdjArr idx persisted");
//                 assert_eq!(adjarr.arrays[0x03][0x02], 0xBEEF, "AdjArr data persisted");
//                 assert_eq!(adjarr.next, 0x33, "AdjArr next persisted");
//             }
//         }

//         #[test]
//         #[cfg(debug_assertions)]
//         #[should_panic]
//         fn test_open_panics_on_invalid_metadata_in_file() {
//             let tmp = temp_dir();
//             let dir = tmp.path().to_path_buf();
//             let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

//             unsafe {
//                 let t0 = unsafe { Trail::new(cfg.clone()) }.expect("new trail");
//                 let meta = &mut *t0.meta_ptr;

//                 // corrupted metadata
//                 meta.nadjarr = 0x00;
//                 meta.nbitmap = 0x00;

//                 drop(t0);
//             }

//             // should panic
//             unsafe { Trail::open(cfg) };
//         }
//     }
// }
