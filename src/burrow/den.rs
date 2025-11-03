use crate::linux::iouring::{IOUring, NUM_BUFFER_PAGE, SIZE_BUFFER_PAGE};
use crate::{errors::InternalResult, logger::Logger};
use std::{
    fs::{File, OpenOptions},
    os::fd::{AsFd, AsRawFd},
    path::PathBuf,
};

const PATH: &'static str = "deha";

pub(super) struct Deha {
    file: File,
    logger: Logger,
    io_uring: Option<IOUring>,
}

impl Deha {
    pub(super) fn new(logging_enabled: bool, is_new: bool, dir: &PathBuf) -> InternalResult<Self> {
        let logger = Logger::new(logging_enabled, "Deha");
        let path = dir.join(PATH);
        let mut file = Self::open_or_create_file(&path, &logger, is_new)?;

        // validate existing file to be page aligned
        if !is_new {
            let len = file
                .metadata()
                .map_err(|e| {
                    logger.error("Unable to read metadata of Deha file");
                    e
                })?
                .len() as usize;

            if len % SIZE_BUFFER_PAGE != 0 {
                logger.error("Deha file is corrupted or tampered with!");

                std::fs::remove_file(&path).map_err(|e| {
                    logger.error("Unable to delete corrupted Deha file");
                    e
                })?;

                logger.warn("Deleted corrupted Deha file!");

                // create a new Deha file
                file = Self::open_or_create_file(&path, &logger, true)?;
                logger.warn("Created new Deha file!");
            }
        }

        let file_fd = file.as_raw_fd();
        let io_uring = unsafe { IOUring::new(logging_enabled, file_fd, NUM_BUFFER_PAGE, SIZE_BUFFER_PAGE) }?;

        Ok(Self { file, logger, io_uring })
    }

    pub(super) fn write(&self, offset: u64, buffer: &[u8]) -> InternalResult<()> {
        // sanity check
        debug_assert!(
            buffer.len() % SIZE_BUFFER_PAGE == 0 && offset as usize % SIZE_BUFFER_PAGE == 0,
            "Buffer and Offset must be paged correctly"
        );

        // TODO: Impl of sequential I/O for when io_uring is not available
        if let Some(io_uring) = &self.io_uring {
            unsafe { io_uring.write(buffer, offset) }?;
        }

        Ok(())
    }

    pub(super) fn read(&self, offset: u64, buffer: &mut [u8]) -> InternalResult<()> {
        // sanity check
        debug_assert!(
            buffer.len() % SIZE_BUFFER_PAGE == 0 && offset as usize % SIZE_BUFFER_PAGE == 0,
            "Buffer and Offset must be paged correctly"
        );

        Self::pread(&self.file, buffer, offset)?;
        Ok(())
    }

    fn open_or_create_file(path: &PathBuf, logger: &Logger, is_new: bool) -> InternalResult<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(is_new)
            .open(&path)
            .map_err(|e| {
                logger.error("Unable to create Deha file");
                e
            })?;

        if is_new {
            let size = Self::calc_file_size(0);
            file.set_len(size).map_err(|e| {
                logger.error("Unable to set default size for new Deha file");
                e
            })?
        }

        logger.debug("Created Deha file");

        Ok(file)
    }

    #[inline(always)]
    fn calc_file_size(current_size: u64) -> u64 {
        (SIZE_BUFFER_PAGE * NUM_BUFFER_PAGE) as u64 + current_size
    }

    #[cfg(unix)]
    fn pwrite(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
    }

    #[cfg(unix)]
    fn pread(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }
}

impl Drop for Deha {
    fn drop(&mut self) {
        if let Some(io_uring) = self.io_uring.take() {
            drop(io_uring);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logger::init_test_logger;
    use tempfile::TempDir;

    fn create_deha(is_new: bool) -> (Deha, TempDir) {
        let _ = init_test_logger("IOUring");
        let tmp = TempDir::new().expect("tempdir");
        let deha = Deha::new(true, is_new, &tmp.path().to_path_buf()).expect("Create Deha");

        (deha, tmp)
    }

    mod deha {
        use super::*;

        #[test]
        fn test_deha_init_with_new() {
            let (d1, _t1) = create_deha(true);
            let len_new = Deha::calc_file_size(0);

            assert_eq!(
                d1.file.metadata().expect("Meta").len(),
                len_new,
                "New Deha file is initilized w/ pre-defnined file size"
            );
        }

        #[test]
        fn test_deha_init_for_corrupted_file_with_new() {
            let (mut d1, t1) = create_deha(true);
            let len_new = Deha::calc_file_size(0);

            // manually corrupt file len
            let corrupted_len: u64 = 10;
            d1.file.set_len(corrupted_len).expect("Len Update");

            let d2 = Deha::new(true, false, &t1.path().to_path_buf()).expect("Open Deha");
            let d2_len = d2.file.metadata().expect("Meta").len();
            let new_len = Deha::calc_file_size(0);

            assert_ne!(d2_len, corrupted_len, "Corrupted file must be re-inited when opened");
            assert!(
                d2_len == new_len,
                "Corrupted file must be re-inited w/ page aligned file size"
            );
        }

        #[test]
        fn test_file_size_calculations() {
            assert!(
                Deha::calc_file_size(0) as usize % SIZE_BUFFER_PAGE == 0,
                "File size must be aligned w/ page size"
            );

            for i in 1..10 {
                let old_size = SIZE_BUFFER_PAGE * NUM_BUFFER_PAGE * i;

                assert!(
                    Deha::calc_file_size(old_size as u64) as usize % SIZE_BUFFER_PAGE == 0,
                    "New File size must be aligned w/ page size"
                );
            }
        }

        #[test]
        #[cfg(unix)]
        fn test_ops_on_open_or_create_file() {
            let dummy_logger = Logger::new(true, "Deha [File Ops]");
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("test_file");

            let file = Deha::open_or_create_file(&path, &dummy_logger, true).expect("Create File");
            assert!(
                file.metadata().expect("Meta").len() as usize % SIZE_BUFFER_PAGE == 0,
                "File size must be page aligned"
            );

            let file1 = Deha::open_or_create_file(&path, &dummy_logger, false).expect("Open File");
            assert_eq!(
                file.metadata().expect("Meta").len(),
                file1.metadata().expect("Meta").len(),
                "File size must not change on re-open for existing file"
            );

            let new_len: u64 = 10;
            file1.set_len(new_len).expect("Update len");

            let file2 = Deha::open_or_create_file(&path, &dummy_logger, false).expect("Open File");
            assert_eq!(
                file2.metadata().expect("Meta").len(),
                new_len,
                "Opening old file again does not modify len"
            );
        }
    }

    mod p_read_write {
        use super::*;

        #[test]
        #[cfg(unix)]
        fn test_pwrite_pread_cycle() {
            let (deha, _tmp) = create_deha(true);
            let dummy_data = b"Dummy data to write".to_vec();
            let mut buf = vec![0u8; dummy_data.len()];

            Deha::pwrite(&deha.file, &dummy_data, 0).expect("Write");
            Deha::pread(&deha.file, &mut buf, 0).expect("Read");

            assert_eq!(dummy_data, buf, "Read data must match the written data");
        }
    }
}
