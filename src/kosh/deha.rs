use super::iouring::IOUring;
use crate::errors::InternalResult;
use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::PathBuf,
};

// TODO: We shold take `num_buf_page` as config from user, if they insert rapidly,
// queue will overflow then we must block new writes (thread sleep, etc.)
// if no bufs are available to write into

/// No. of page bufs pages registered w/ kernel for `io_uring`
pub(super) const NUM_BUFFER_PAGE: usize = 128;
const _: () = assert!(
    NUM_BUFFER_PAGE > 0 && (NUM_BUFFER_PAGE & (NUM_BUFFER_PAGE - 1)) == 0,
    "NUM_BUFFER_PAGE must be power of 2"
);

// TODO: We shold take `size_buf_page` as config from user, so the dev's could
// optimize for there ideal buf size, so we could avoid resource waste!

/// Size of each page buf registered w/ kernel for `io_uring`
pub(super) const SIZE_BUFFER_PAGE: usize = 128;
const _: () = assert!(
    SIZE_BUFFER_PAGE > 0 && (SIZE_BUFFER_PAGE & (SIZE_BUFFER_PAGE - 1)) == 0,
    "SIZE_BUFFER_PAGE must be power of 2"
);

const PATH: &'static str = "deha";

pub(super) struct Deha {
    file: File,
    iouring: Option<IOUring>,
}

impl Deha {
    pub(super) fn new(log: bool, dir: &PathBuf) -> InternalResult<Self> {
        todo!()
    }

    pub(super) fn open(log: bool, dir: &PathBuf) -> InternalResult<Self> {
        let path = dir.join(PATH);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        let file_fd = file.as_raw_fd();
        let iouring = unsafe { IOUring::new(log, file_fd, NUM_BUFFER_PAGE, SIZE_BUFFER_PAGE) }?;

        // TODO: If file is not paged correctly, we should either
        // - delete and create a new one
        // - throw error to user stating `TurboCache` is corrupted

        Ok(Self { file, iouring })
    }

    pub(super) fn write(&self, offset: u64, buffer: &[u8]) -> InternalResult<()> {
        // sanity check
        debug_assert!(
            buffer.len() % SIZE_BUFFER_PAGE == 0 && offset as usize % SIZE_BUFFER_PAGE == 0,
            "Buffer and Offset must be paged correctly"
        );

        if let Some(io_uring) = &self.iouring {
            unsafe { io_uring.write(buffer, offset) }?;
        } else {
            Self::pwrite(&self.file, buffer, offset)?;
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

    #[cfg(unix)]
    fn pwrite(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
    }

    #[cfg(unix)]
    fn pread(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::logger::init_test_logger;
//     use tempfile::TempDir;

//     fn create_deha() -> (Deha, TempDir) {
//         let _ = init_test_logger("IOUring");
//         let tmp = TempDir::new().expect("tempdir");
//         let deha = Deha::open(true, &tmp.path().to_path_buf()).expect("Deha init");

//         (deha, tmp)
//     }

//     #[test]
//     fn test_open_creates_file_and_persists() {
//         let (deha, tmp) = create_deha();

//         let path = tmp.path().join("deha");
//         assert!(path.exists());
//         assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

//         // Write and reopen to verify persistence
//         let data = vec![0xAA; SIZE_BUFFER_PAGE];
//         deha.write(0, &data).unwrap();

//         drop(deha);
//         let reopened = Deha::open(true, &tmp.path().to_path_buf()).unwrap();

//         let mut buf = vec![0; SIZE_BUFFER_PAGE];
//         reopened.read(0, &mut buf).unwrap();
//         assert_eq!(buf, data);
//     }

//     #[test]
//     #[cfg(debug_assertions)]
//     #[should_panic]
//     fn test_misaligned_buffer_panics() {
//         let dir = tempdir().unwrap();

//         let deha = Deha::open(true, &dir.path().to_path_buf()).unwrap();
//         let data = vec![0; SIZE_BUFFER_PAGE + 1];

//         deha.write(0, &data).unwrap();
//     }

//     #[test]
//     #[cfg(debug_assertions)]
//     #[should_panic]
//     fn test_misaligned_offset_panics() {
//         let dir = tempdir().unwrap();

//         let deha = Deha::open(true, &dir.path().to_path_buf()).unwrap();
//         let data = vec![0; SIZE_BUFFER_PAGE];

//         deha.write(1, &data).unwrap();
//     }

//     #[test]
//     fn test_multi_page_read_write() {
//         let dir = tempdir().unwrap();
//         let deha = Deha::open(true, &dir.path().to_path_buf()).unwrap();

//         let page1 = vec![1; SIZE_BUFFER_PAGE];
//         let page2 = vec![2; SIZE_BUFFER_PAGE];
//         let page3 = vec![3; SIZE_BUFFER_PAGE];

//         deha.write(0, &[page1.clone(), page2.clone(), page3.clone()].concat())
//             .unwrap();

//         let mut buf = vec![0; SIZE_BUFFER_PAGE * 3];
//         deha.read(0, &mut buf).unwrap();

//         assert_eq!(&buf[..SIZE_BUFFER_PAGE], page1);
//         assert_eq!(&buf[SIZE_BUFFER_PAGE..2 * SIZE_BUFFER_PAGE], page2);
//         assert_eq!(&buf[2 * SIZE_BUFFER_PAGE..], page3);
//     }
// }
