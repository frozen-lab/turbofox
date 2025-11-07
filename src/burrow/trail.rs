use super::PAGE_SIZE;
use crate::{errors::InternalResult, logger::Logger, InternalCfg};
use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::PathBuf,
};

const VERSION: u32 = 0;
const MAGIC: [u8; 4] = *b"trl1";
const PATH: &'static str = "trail";

const RESERVED_PAGE_SPACE: usize = std::mem::size_of::<u64>(); // page_link (u64)
const BITS_PER_PAGE: usize = (PAGE_SIZE - RESERVED_PAGE_SPACE) * 8;
const ITEMS_PER_ADJ_ARR: usize = 7;
const RESERVED_ADJ_ARR_SPACE: usize = 2; // arr_link_idx (u32) & page_idx (u32)
const ADJ_ARR_ITEM_SIZE: usize = std::mem::size_of::<u32>();
const ADJ_ARR_PER_PAGE: usize = (PAGE_SIZE - RESERVED_PAGE_SPACE) / (ADJ_ARR_ITEM_SIZE * ITEMS_PER_ADJ_ARR);

// sanity checks
const _: () = assert!(std::mem::size_of_val(&VERSION) == 4, "Must be 4 bytes aligned");
const _: () = assert!(std::mem::size_of_val(&MAGIC) == 4, "Must be 4 bytes aligned");
const _: () = assert!(BITS_PER_PAGE / 8 == (PAGE_SIZE - RESERVED_PAGE_SPACE));
const _: () = assert!(PAGE_SIZE - RESERVED_PAGE_SPACE == ADJ_ARR_PER_PAGE * ADJ_ARR_ITEM_SIZE * ITEMS_PER_ADJ_ARR);

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct Meta {
    magic: [u8; 4],
    version: u32,
    npages: u32,
    nbits: u32,
    nadjarr: u32,
}

impl Meta {
    #[inline(always)]
    const fn size() -> usize {
        std::mem::size_of::<Self>()
    }

    #[inline(always)]
    const fn new() -> Self {
        // sanity check
        debug_assert!(BITS_PER_PAGE <= u32::MAX as usize);
        debug_assert!(ADJ_ARR_PER_PAGE <= u32::MAX as usize);

        Self {
            magic: MAGIC,
            version: VERSION,
            npages: 2,
            nbits: BITS_PER_PAGE as u32,
            nadjarr: ADJ_ARR_PER_PAGE as u32,
        }
    }

    #[inline(always)]
    const fn from_ptr(ptr: *mut libc::c_void) -> Self {
        unsafe { *(ptr as *const Meta) }
    }
}

// sanity check (meta must be 4 bytes aligned)
const _: () = assert!(Meta::size() % 4 == 0);

#[derive(Debug)]
struct BitMap {
    nbits: usize,
    ptrs: Vec<*mut libc::c_void>,
}

impl BitMap {
    #[inline(always)]
    fn new(nbits: usize, ptrs: Vec<*mut libc::c_void>) -> Self {
        Self { nbits, ptrs }
    }
}

#[derive(Debug)]
struct AdjArr {
    narr: usize,
    ptrs: Vec<*mut libc::c_void>,
}

impl AdjArr {
    #[inline(always)]
    fn new(narr: usize, ptrs: Vec<*mut libc::c_void>) -> Self {
        Self { narr, ptrs }
    }
}

#[derive(Debug)]
pub(super) struct Trail {
    cfg: InternalCfg,
    file: File,
    logger: Logger,
    mmap_size: u64,
    mmap_ptr: *mut libc::c_void,
    bmap: BitMap,
    meta: Meta,
}

impl Trail {
    pub(super) fn new(cfg: &InternalCfg) -> InternalResult<Self> {
        let logger = Logger::new(cfg.logging_enabled, "TurboFox (TRAIL)");
        let path = cfg.dirpath.join(PATH);
        let file_size = Meta::size() + cfg.page_size;

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .inspect(|_| logger.trace("New Trail created"))
            .map_err(|e| {
                logger.error("Unable to create new Trail");
                e
            })?;

        file.set_len(cfg.page_size as u64)
            .inspect(|_| logger.debug(format!("Zero Init trail w/ len={}", cfg.page_size)))
            .map_err(|e| {
                logger.error("Unabele to set length for new Trail");
                Self::_delete_file(&path, &logger);
                e
            })?;

        let fd = file.as_raw_fd();
        let mmap_ptr = unsafe { Self::mmap_file(fd, cfg.page_size, &logger) }?;

        let meta = Meta::new();
        let bmap = BitMap::new(meta.nbits as usize, vec![mmap_ptr]);

        let res = unsafe {
            let meta_ptr = mmap_ptr as *mut Meta;
            std::ptr::write(meta_ptr, meta);
            libc::msync(meta_ptr.cast(), Meta::size(), libc::MS_SYNC)
        };

        if res < 0 {
            let err = std::io::Error::last_os_error();
            logger.error("Unable to set Meta for new Trail");
            Self::_delete_file(&path, &logger);
            return Err(err.into());
        }

        Ok(Self {
            meta,
            file,
            bmap,
            logger,
            mmap_ptr,
            cfg: cfg.clone(),
            mmap_size: cfg.page_size as u64,
        })
    }

    // pub(super) fn open(cfg: &InternalCfg) -> InternalResult<Option<Self>> {
    //     let logger = Logger::new(cfg.logging_enabled, "TurboFox (TRAIL)");
    //     let path = cfg.dirpath.join(PATH);

    //     if !path.exists() {
    //         logger.warn("No existing Trail found.");
    //         return Ok(None);
    //     }

    //     let file = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(false)
    //         .truncate(false)
    //         .open(&path)
    //         .inspect(|_| logger.trace("Opened existing Trail"))
    //         .map_err(|e| {
    //             logger.error("Unable to open existing Trail");
    //             e
    //         })?;

    //     let file_len = file
    //         .metadata()
    //         .map_err(|e| {
    //             logger.error("Unable to read metadata of existing Trail");
    //             e
    //         })?
    //         .len();

    //     // validate file len
    //     if file_len != cfg.page_size as u64 {
    //         logger.error(format!("Trail is invalid and has len={}", file_len));
    //         return Ok(None);
    //     }

    //     let fd = file.as_raw_fd();
    //     let mmap_ptr = unsafe { Self::mmap_file(fd, file_len as usize, &logger) }?;
    //     let meta = unsafe { *(mmap_ptr as *const Meta) };

    //     if meta.magic != MAGIC || meta.version != VERSION {
    //         logger.error("Invalid Trail Meta header");
    //         return Ok(None);
    //     }

    //     let bmap = BitMap::new(cfg.init_cap as usize, vec![mmap_ptr]);

    //     logger.info(format!("Opened Trail w/ \n{:?} \n{:?}", meta, bmap));

    //     Ok(Some(Self {
    //         meta,
    //         file,
    //         bmap,
    //         logger,
    //         mmap_ptr,
    //         cfg: cfg.clone(),
    //         mmap_size: file_len,
    //     }))
    // }

    /// delete created file, so reopen could work
    fn _delete_file(path: &PathBuf, logger: &Logger) {
        match std::fs::remove_file(&path) {
            Ok(_) => logger.warn("Deleted new Trail, due to err: {e}"),
            Err(err) => logger.error(format!("Unable to delete new Trail, due to err: {err}")),
        }
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_file(fd: i32, len: usize, logger: &Logger) -> InternalResult<*mut libc::c_void> {
        // NOTE: Kernel treats `nbytes = 0` as "until EOF", which is what exactly we want!
        let res = libc::sync_file_range(
            fd,
            0,
            0,
            libc::SYNC_FILE_RANGE_WAIT_BEFORE | libc::SYNC_FILE_RANGE_WAIT_AFTER,
        );

        if res < 0 {
            let err = std::io::Error::last_os_error();
            logger.error("Unable to perform data sync on Trail");
            return Err(err.into());
        }

        let ptr = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );

        if ptr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            logger.error(format!("Unable to mmap Trail due to err: {err}"));
            return Err(err.into());
        }

        logger.trace(format!("Mmaped Trace w/ len={len} for fd={fd}"));

        Ok(ptr)
    }
}

impl Drop for Trail {
    fn drop(&mut self) {
        unsafe {
            // unmap mmaped buffer
            let res = libc::munmap(self.mmap_ptr, self.cfg.page_size);

            if res < 0 {
                let err = std::io::Error::last_os_error();
                self.logger
                    .warn(format!("Unable to unmap the buffer due to, res={res} & err={err}"));
            } else {
                self.logger.trace("Unmaped the mapped Trail buffer");
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
