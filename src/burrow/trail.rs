use crate::{errors::InternalResult, logger::Logger};
use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::PathBuf,
};

const PATH: &'static str = "trail";
const PAGE_SIZE: usize = 4096; // 4 KiB
const MAGIC: [u8; 4] = *b"trl1";
const VERSION: u32 = 0;
const META_SIZE: usize = 8;

// sanity check
const _: () = assert!(std::mem::size_of_val(&MAGIC) + std::mem::size_of_val(&VERSION) == META_SIZE);

#[derive(Debug)]
pub(super) struct Trail {
    file: File,
    logger: Logger,
    mmap_size: u64,
    mmap_ptr: *mut libc::c_void,
}

impl Trail {
    pub(super) fn open(logging_enabled: bool, dir: &PathBuf) -> InternalResult<Option<Self>> {
        let logger = Logger::new(logging_enabled, "TurboFox (TRAIL)");
        let path = dir.join(PATH);

        if !path.exists() {
            logger.warn("No existing Trail found.");
            return Ok(None);
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .open(&path)
            .inspect(|_| logger.info("Opened existing Trail"))
            .map_err(|e| {
                logger.error("Unable to open existing Trail");
                e
            })?;

        let file_len = file
            .metadata()
            .map_err(|e| {
                logger.error("Unable to read metadata of existing Trail");
                e
            })?
            .len();

        // validate file len
        if file_len != PAGE_SIZE as u64 {
            logger.error(format!("Trail is invalid and has len={}", file_len));
            return Ok(None);
        }

        let fd = file.as_raw_fd();
        let mmap_ptr = unsafe { Self::mmap_file(fd, file_len as usize, &logger) }?;

        Ok(Some(Self {
            file,
            logger,
            mmap_ptr,
            mmap_size: file_len,
        }))
    }

    pub(super) fn new(logging_enabled: bool, dir: &PathBuf) -> InternalResult<Self> {
        let logger = Logger::new(logging_enabled, "TurboFox (TRAIL)");
        let path = dir.join(PATH);

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

        file.set_len(PAGE_SIZE as u64)
            .inspect(|_| logger.debug(format!("Zero Init trail w/ len={PAGE_SIZE}")))
            .map_err(|e| {
                logger.error("Unabele to set length for new Trail");

                // delete created file, so reopen could work
                match std::fs::remove_file(&path) {
                    Ok(_) => logger.warn("Deleted new Trail, due to err: {e}"),
                    Err(err) => logger.error(format!("Unable to delete new Trail, due to err: {err}")),
                }

                e
            })?;

        let fd = file.as_raw_fd();
        let mmap_ptr = unsafe { Self::mmap_file(fd, PAGE_SIZE, &logger) }?;

        Ok(Self {
            file,
            logger,
            mmap_ptr,
            mmap_size: PAGE_SIZE as u64,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_file(fd: i32, len: usize, logger: &Logger) -> InternalResult<*mut libc::c_void> {
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
            let res = libc::munmap(self.mmap_ptr, PAGE_SIZE);

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
        fn test_new_and_open() {
            let tmp = temp_dir();
            let dir = tmp.path().to_path_buf();

            let t1 = Trail::open(true, &dir).expect("Open existing");
            assert!(t1.is_none());

            let t2 = Trail::new(true, &dir).expect("Create New");

            // validate file len
            let file_len = t2.file.metadata().expect("Meta").len();
            assert_eq!(file_len, PAGE_SIZE as u64);

            // validate mmap
            assert!(t2.mmap_ptr != std::ptr::null_mut());
            assert_eq!(t2.mmap_size, PAGE_SIZE as u64);

            // NOTE: close the opened Trail instance
            drop(t2);

            // validate reopen
            let t3 = Trail::open(true, &dir).expect("Open Existing");
            assert!(t3.is_some());
        }
    }
}
