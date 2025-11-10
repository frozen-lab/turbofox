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
    const fn new(n: usize) -> Self {
        // sanity check
        debug_assert!(n > 0, "N must not be zero");

        Self {
            magic: MAGIC,
            version: VERSION,
            nbitpages: n as u32,
            naddarrpages: n as u32,
            bitptr: 0u64,
            adjptr: 064,
        }
    }
}

#[derive(Debug)]
pub(super) struct Trail {
    file: File,
    meta: Meta,
    mmap: MMap,
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

        let meta = Meta::new(INIT_OS_PAGES);

        let meta_ptr = mmap.ptr as *mut Meta;
        std::ptr::write(meta_ptr, meta);
        mmap.ms_sync().map_err(|e| {
            cfg.logger
                .error(format!("(TRAIL) Failed to write Metadata to mmaped file: {e}"));
            e
        })?;

        cfg.logger.debug("(TRAIL) Created a new file");

        Ok(Self { file, meta, mmap })
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

        Ok(Self { file, meta, mmap })
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
//         fn test_new_and_open() {
//             let tmp = temp_dir();
//             let dir = tmp.path().to_path_buf();
//             let cfg = InternalCfg::new(dir);

//             let t1 = Trail::open(&cfg).expect("Open existing");
//             assert!(t1.is_none());

//             let t2 = Trail::new(&cfg).expect("Create New");

//             // validate file len
//             let file_len = t2.file.metadata().expect("Meta").len();
//             assert_eq!(file_len, cfg.page_size as u64);

//             // validate mmap
//             assert!(t2.mmap_ptr != std::ptr::null_mut());
//             assert_eq!(t2.mmap_size, cfg.page_size as u64);

//             // validate mmap init
//             assert_eq!(t2.meta.magic, MAGIC);
//             assert_eq!(t2.meta.version, VERSION);
//             assert_eq!(t2.meta.nbits, t2.bmap.nbits as u32);
//             assert_eq!(t2.meta.nadjarr, t2.cfg.init_cap as u32);

//             // NOTE: close the opened Trail instance
//             drop(t2);

//             // validate reopen
//             let t3 = Trail::open(&cfg).expect("Open Existing");
//             assert!(t3.is_some());

//             let t4 = t3.unwrap();

//             // validate mmap on reopen
//             assert_eq!(t4.meta.magic, MAGIC);
//             assert_eq!(t4.meta.version, VERSION);
//             assert_eq!(t4.meta.nbits, t4.bmap.nbits as u32);
//             assert_eq!(t4.meta.nadjarr, t4.cfg.init_cap as u32);

//             drop(t4);
//         }
//     }
// }
