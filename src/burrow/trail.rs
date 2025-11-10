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

const ADJ_ARR_IDX_SIZE: usize = 8;
const ITEMS_PER_ADJ_ARR: usize = 8;
const ADJ_ARR_PER_PAGE: usize = (OS_PAGE_SIZE - RESERVED_SPACE_PER_PAGE - ADJ_ARR_IDX_SIZE) / ITEMS_PER_ADJ_ARR;

const INIT_OS_PAGES: usize = 2; // Bits + AdjArr
const INIT_FILE_LEN: usize = META_SIZE + (OS_PAGE_SIZE * INIT_OS_PAGES); // Meta + OS Pages

// sanity checks
const _: () = assert!(META_SIZE % 8 == 0, "Should be 8 bytes aligned");
const _: () = assert!(BITES_PER_PAGE % 64 == 0, "Must be 8 bytes aligned");
const _: () = assert!(RESERVED_SPACE_PER_PAGE % 8 == 0, "Must be 8 bytes aligned");
const _: () = assert!(std::mem::size_of_val(&MAGIC) == 4, "Must be 4 bytes aligned");
const _: () = assert!(std::mem::size_of_val(&VERSION) == 4, "Must be 4 bytes aligned");
const _: () = assert!(
    ADJ_ARR_PER_PAGE * ITEMS_PER_ADJ_ARR == OS_PAGE_SIZE - RESERVED_SPACE_PER_PAGE - ADJ_ARR_IDX_SIZE,
    "Adjcent Array Constants should be valid"
);

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct Meta {
    magic: [u8; 4],
    version: u32,
    nbitpages: u32,
    naddarrpages: u32,
    bitptr: u64,
    adjptr: u64,
}

const META_SIZE: usize = std::mem::size_of::<Meta>();

// sanity check
const _: () = assert!(META_SIZE % 8 == 0, "Must be 8 bytes aligned");

impl Meta {
    #[inline(always)]
    const fn new() -> Self {
        const N: u32 = INIT_OS_PAGES as u32 / 2;

        // sanity check
        debug_assert!(N > 0, "N must not be zero");

        Self {
            magic: MAGIC,
            version: VERSION,
            nbitpages: N,
            naddarrpages: N,
            bitptr: 0u64, // at first idx
            adjptr: 1u64, // at second idx
        }
    }
}

#[derive(Debug)]
pub(super) struct Trail {
    file: File,
    meta: Meta,
    mmap: MMap,
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

        let meta = Meta::new();

        let meta_ptr = mmap.ptr as *mut Meta;
        std::ptr::write(meta_ptr, meta);
        mmap.ms_sync().map_err(|e| {
            cfg.logger
                .error(format!("(TRAIL) Failed to write Metadata to mmaped file: {e}"));
            e
        })?;

        cfg.logger.debug("(TRAIL) Created a new file");

        Ok(Self {
            file,
            meta,
            mmap,
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

        let meta = *(mmap.ptr as *mut Meta);

        // metadata validations
        if meta.magic != MAGIC || meta.version != VERSION {
            let err = InternalError::InvalidFile("Invalid metadata, file is outdated!".into());
            cfg.logger
                .error(format!("(TRAIL) Existing file has invalid metadata: {err}"));

            return Err(err);
        }

        cfg.logger.debug("(TRAIL) Opened an existing file");

        Ok(Self {
            file,
            meta,
            mmap,
            cfg: cfg.clone(),
        })
    }
}

impl Drop for Trail {
    fn drop(&mut self) {
        unsafe {
            // munmap the memory mappings
            self.mmap
                .unmap()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) Unmapped the mmap");
                })
                .map_err(|e| {
                    self.cfg.logger.warn("(TRAIL) Failed to unmap the mmap");
                });

            // close the file descriptor
            self.file
                .close()
                .inspect(|_| {
                    self.cfg.logger.trace("(TRAIL) Closed the file fd");
                })
                .map_err(|e| {
                    self.cfg.logger.warn("(TRAIL) Failed to close the file fd");
                });
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
        use libc::MAP_FAILED;

        use super::*;

        #[test]
        fn test_new_is_valid() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();
            let cfg = InternalCfg::new(dir).log(true).log_target("Trail");

            let t1 = unsafe { Trail::new(&cfg) }.expect("new trail");

            assert!(t1.file.0 >= 0, "File fd must be valid");
            assert!(t1.mmap.len > 0, "Mmap must be non zero");
            assert_eq!(t1.meta.magic, MAGIC, "Correct file MAGIC");
            assert_eq!(t1.meta.version, VERSION, "Correct file VERSION");
            assert_eq!(t1.meta.nbitpages, INIT_OS_PAGES as u32 / 2);
            assert_eq!(t1.meta.naddarrpages, INIT_OS_PAGES as u32 / 2);
            assert_eq!(t1.meta.bitptr, 0x00, "Correct ptr for Bits");
            assert_eq!(t1.meta.adjptr, 0x01, "Correct ptr for adjarr");
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

            assert!(t1.file.0 >= 0, "File fd must be valid");
            assert!(t1.mmap.len > 0, "Mmap must be non zero");
            assert_eq!(t1.meta.magic, MAGIC, "Correct file MAGIC");
            assert_eq!(t1.meta.version, VERSION, "Correct file VERSION");
            assert_eq!(t1.meta.nbitpages, INIT_OS_PAGES as u32 / 2);
            assert_eq!(t1.meta.naddarrpages, INIT_OS_PAGES as u32 / 2);
            assert_eq!(t1.meta.bitptr, 0x00, "Correct ptr for Bits");
            assert_eq!(t1.meta.adjptr, 0x01, "Correct ptr for adjarr");
        }
    }
}
