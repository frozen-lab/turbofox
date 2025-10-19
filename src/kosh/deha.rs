use crate::errors::InternalResult;
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

pub(super) const PAGE_SIZE: usize = 128;
const PATH: &'static str = "deha";

pub(super) struct Deha {
    file: File,
}

impl Deha {
    pub(super) fn open(dir: &PathBuf) -> InternalResult<Self> {
        let path = dir.join(PATH);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        // TODO: If file is not paged correctly, we should either
        // - delete and create a new one
        // - throw error to user stating `TurboCache` is corrupted

        Ok(Self { file })
    }

    pub(super) fn write(&self, offset: u64, buffer: &[u8]) -> InternalResult<()> {
        // sanity check
        debug_assert!(
            buffer.len() % PAGE_SIZE == 0 && offset as usize % PAGE_SIZE == 0,
            "Buffer and Offset must be paged correctly"
        );

        Self::pwrite(&self.file, buffer, offset)?;
        Ok(())
    }

    pub(super) fn read(&self, offset: u64, buffer: &mut [u8]) -> InternalResult<()> {
        // sanity check
        debug_assert!(
            buffer.len() % PAGE_SIZE == 0 && offset as usize % PAGE_SIZE == 0,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_open_creates_file_and_persists() {
        let dir = tempdir().unwrap();
        let deha = Deha::open(&dir.path().to_path_buf()).unwrap();

        let path = dir.path().join("deha");
        assert!(path.exists());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        // Write and reopen to verify persistence
        let data = vec![0xAA; PAGE_SIZE];
        deha.write(0, &data).unwrap();

        drop(deha);
        let reopened = Deha::open(&dir.path().to_path_buf()).unwrap();

        let mut buf = vec![0; PAGE_SIZE];
        reopened.read(0, &mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn test_misaligned_buffer_panics() {
        let dir = tempdir().unwrap();

        let deha = Deha::open(&dir.path().to_path_buf()).unwrap();
        let data = vec![0; PAGE_SIZE + 1];

        deha.write(0, &data).unwrap();
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn test_misaligned_offset_panics() {
        let dir = tempdir().unwrap();

        let deha = Deha::open(&dir.path().to_path_buf()).unwrap();
        let data = vec![0; PAGE_SIZE];

        deha.write(1, &data).unwrap();
    }

    #[test]
    fn test_multi_page_read_write() {
        let dir = tempdir().unwrap();
        let deha = Deha::open(&dir.path().to_path_buf()).unwrap();

        let page1 = vec![1; PAGE_SIZE];
        let page2 = vec![2; PAGE_SIZE];
        let page3 = vec![3; PAGE_SIZE];

        deha.write(0, &[page1.clone(), page2.clone(), page3.clone()].concat())
            .unwrap();

        let mut buf = vec![0; PAGE_SIZE * 3];
        deha.read(0, &mut buf).unwrap();

        assert_eq!(&buf[..PAGE_SIZE], page1);
        assert_eq!(&buf[PAGE_SIZE..2 * PAGE_SIZE], page2);
        assert_eq!(&buf[2 * PAGE_SIZE..], page3);
    }
}
