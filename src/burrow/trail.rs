use super::OS_PAGE_SIZE;
use crate::{
    errors::{InternalError, InternalResult},
    linux::{file::File, mmap::MMap},
    logger::Logger,
    InternalCfg,
};

const VERSION: u32 = 0;
const MAGIC: [u8; 4] = *b"trl1";
const PATH: &'static str = "trail";

const RESERVED_SPACE_PER_PAGE: usize = 8;
const BITES_PER_PAGE: usize = (OS_PAGE_SIZE - RESERVED_SPACE_PER_PAGE) * 8;

const ADJ_ARR_IDX_SIZE: usize = 64;
const ADJ_ARR_IDX_PADD: usize = 2;
const ITEMS_PER_ADJ_ARR: usize = 8;
const ADJ_ARR_PADD: usize = 56;
const ADJ_ARR_PER_PAGE: usize = 496;

const INIT_OS_PAGES: usize = 2; // Bits + AdjArr
const INIT_FILE_LEN: usize = META_SIZE + (OS_PAGE_SIZE * INIT_OS_PAGES); // Meta + OS Pages

// sanity checks
const _: () = assert!(META_SIZE % 8 == 0, "Should be 8 bytes aligned");
const _: () = assert!(BITES_PER_PAGE % 64 == 0, "Must be 8 bytes aligned");
const _: () = assert!(RESERVED_SPACE_PER_PAGE % 8 == 0, "Must be 8 bytes aligned");
const _: () = assert!(std::mem::size_of_val(&MAGIC) == 4, "Must be 4 bytes aligned");
const _: () = assert!(std::mem::size_of_val(&VERSION) == 4, "Must be 4 bytes aligned");
const _: () = assert!(INIT_OS_PAGES >= 2, "Must be enough pages for Bits and AdjArr");
const _: () = assert!(
    (ADJ_ARR_IDX_SIZE - ADJ_ARR_IDX_PADD) * 8 == ADJ_ARR_PER_PAGE,
    "Index must be large enough"
);
const _: () = assert!(
    OS_PAGE_SIZE == RESERVED_SPACE_PER_PAGE + (BITES_PER_PAGE / 8),
    "BitMap constants should be valid"
);
const _: () = assert!(
    OS_PAGE_SIZE == RESERVED_SPACE_PER_PAGE + ADJ_ARR_IDX_SIZE + ADJ_ARR_PADD + (ITEMS_PER_ADJ_ARR * ADJ_ARR_PER_PAGE),
    "Adjcent Array Constants should be valid"
);

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct Meta {
    magic: [u8; 4],
    version: u32,
    npages: u64,
    bitptr: u64,
    adjarrptr: u64,
}

const META_SIZE: usize = std::mem::size_of::<Meta>();

// sanity check
const _: () = assert!(META_SIZE % 8 == 0, "Must be 8 bytes aligned");

impl Meta {
    #[inline(always)]
    const fn new() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            npages: INIT_OS_PAGES as u64,
            bitptr: 0u64,    // at first idx
            adjarrptr: 1u64, // at second idx
        }
    }
}

#[derive(Debug)]
pub(super) struct Trail {
    file: File,
    mmap: MMap,
    meta_ptr: *mut Meta,
    bitmap_ptr: *mut BitMap,
    adjarr_ptr: *mut AdjArr,
    cfg: InternalCfg,
}

impl Trail {
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn new(cfg: &InternalCfg) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);

        // file FD
        let file = File::new(&path)
            .inspect(|_| cfg.logger.trace("(TRAIL) Created new file"))
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) Failed to open file({:?}): {e}", path));
                e
            })?;

        // zero init the file
        file.zero_extend(INIT_FILE_LEN)
            .inspect(|_| cfg.logger.trace("(TRAIL) Zero-Extended new file"))
            .map_err(|e| {
                cfg.logger
                    .error(format!("(TRAIL) Failed to zero extend new file({:?}): {e}", path));
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
                e
            })?;

        // metadata init & sync
        //
        // NOTE: we use `ms_sync` here to make sure metadata is persisted before
        // any other updates are conducted on the mmap,
        //
        // NOTE: we can afford this syscall here, as init does not come under the fast
        // path, and its just one time thing!
        mmap.write(0, &Meta::new());
        mmap.ms_sync().map_err(|e| {
            cfg.logger
                .error(format!("(TRAIL) Failed to write Metadata to mmaped file: {e}"));
            e
        })?;

        let meta_ptr = mmap.read_mut::<Meta>(0);
        let bitmap_ptr = mmap.read_mut::<BitMap>(META_SIZE);
        let adjarr_ptr = mmap.read_mut::<AdjArr>(META_SIZE + OS_PAGE_SIZE);

        cfg.logger.debug("(TRAIL) Created a new file");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            bitmap_ptr,
            adjarr_ptr,
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

        // file FD
        let file = File::open(&path)
            .inspect(|_| cfg.logger.trace("(TRAIL) Opened existing file"))
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) Failed to open existing file: {e}"));
                e
            })?;

        // existing file len
        let file_len = file
            .fstat()
            .inspect(|s| cfg.logger.trace(format!("(TRAIL) Existing file has len={}", s.st_size)))
            .map_err(|e| {
                cfg.logger.error(format!("(TRAIL) FStat failed for existing file: {e}"));
                e
            })?
            .st_size as usize;

        // file_len validation (must be page aligned)
        if file_len.wrapping_sub(META_SIZE) == 0 || file_len.wrapping_sub(META_SIZE) % OS_PAGE_SIZE != 0 {
            let err = InternalError::InvalidFile("File is not page aligned".into());
            cfg.logger.error(format!("(TRAIL) Existing file is invalid: {err}"));

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
        if (*meta_ptr).magic != MAGIC || (*meta_ptr).version != VERSION {
            let err = InternalError::InvalidFile("Invalid metadata, file is outdated!".into());
            cfg.logger
                .error(format!("(TRAIL) Existing file has invalid metadata: {err}"));

            return Err(err);
        }

        let bmap_idx = (*meta_ptr).bitptr;
        let adjarr_idx = (*meta_ptr).adjarrptr;

        // sanity checks
        debug_assert!((*meta_ptr).npages < bmap_idx, "BitMap index is out of bounds");
        debug_assert!((*meta_ptr).npages < adjarr_idx, "AdjcentArray index is out of bounds");

        let bitmap_ptr = mmap.read_mut::<BitMap>(META_SIZE + (bmap_idx as usize * OS_PAGE_SIZE));
        let adjarr_ptr = mmap.read_mut::<AdjArr>(META_SIZE + (adjarr_idx as usize * OS_PAGE_SIZE));

        cfg.logger.debug("(TRAIL) Opened an existing file");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            bitmap_ptr,
            adjarr_ptr,
            cfg: cfg.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
struct BitMap {
    bits: [u64; BITES_PER_PAGE / 64],
    next: u64,
}

// sanity check
const _: () = assert!(std::mem::size_of::<BitMap>() == OS_PAGE_SIZE);

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct AdjArr {
    idx: [u8; ADJ_ARR_IDX_SIZE],
    arr: [u64; ADJ_ARR_PER_PAGE],
    padd: [u8; ADJ_ARR_PADD],
    next: u64,
}

// sanity check
const _: () = assert!(std::mem::size_of::<AdjArr>() == OS_PAGE_SIZE);

impl Drop for Trail {
    fn drop(&mut self) {
        unsafe {
            let mut is_err = false;

            // munmap the memory mappings
            self.mmap
                .unmap()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) Unmapped the mmap");
                })
                .map_err(|e| {
                    is_err = true;
                    self.cfg.logger.warn("(TRAIL) Failed to unmap the mmap");
                });

            // close the file descriptor
            self.file
                .close()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) Closed the file fd");
                })
                .map_err(|e| {
                    is_err = true;
                    self.cfg.logger.warn("(TRAIL) Failed to close the file fd");
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

            let t1 = unsafe { Trail::new(&cfg) }.expect("new trail");

            unsafe {
                assert!(t1.file.0 >= 0, "File fd must be valid");
                assert!(t1.mmap.len() > 0, "Mmap must be non zero");
                assert_eq!((*t1.meta_ptr).magic, MAGIC, "Correct file MAGIC");
                assert_eq!((*t1.meta_ptr).version, VERSION, "Correct file VERSION");
                assert_eq!((*t1.meta_ptr).npages, INIT_OS_PAGES as u64, "Correct noOf pages");
                assert_eq!((*t1.meta_ptr).bitptr, 0x00, "Correct ptr for Bits");
                assert_eq!((*t1.meta_ptr).adjarrptr, 0x01, "Correct ptr for adjarr");
            }
        }

        #[test]
        fn test_open_is_valid() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            {
                let t0 = unsafe { Trail::new(&cfg) }.expect("new trail");
                drop(t0);
            }

            let t1 = unsafe { Trail::open(&cfg) }.expect("open existing");

            unsafe {
                assert!(t1.file.0 >= 0, "File fd must be valid");
                assert!(t1.mmap.len() > 0, "Mmap must be non zero");
                assert_eq!((*t1.meta_ptr).magic, MAGIC, "Correct file MAGIC");
                assert_eq!((*t1.meta_ptr).version, VERSION, "Correct file VERSION");
                assert_eq!((*t1.meta_ptr).npages, INIT_OS_PAGES as u64, "Correct noOf pages");
                assert_eq!((*t1.meta_ptr).bitptr, 0x00, "Correct ptr for Bits");
                assert_eq!((*t1.meta_ptr).adjarrptr, 0x01, "Correct ptr for adjarr");
            }
        }
    }
}
