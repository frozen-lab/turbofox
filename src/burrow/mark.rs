use crate::{
    core::{TurboFile, TurboMMap},
    errors::{InternalError, InternalResult},
    TurboConfig,
};

const VERSION: u32 = 0x01;
const MAGIC: [u8; 0x04] = *b"mrk1";
const PATH: &'static str = "mark";

//
// Meta
//

#[derive(Debug, Copy, Clone)]
#[repr(C, align(0x20))]
struct Meta {
    magic: [u8; 0x04],
    version: u32,
    free: u32,
    _padd: [u8; 0x04],
    num_rows: u64,
    num_items: u64,
}

impl Meta {
    #[inline]
    const fn new(num_rows: u64, num_items: u64) -> Self {
        Self {
            num_rows,
            num_items,
            magic: MAGIC,
            version: VERSION,
            free: num_items as u32,
            _padd: [0x00; 0x04],
        }
    }

    #[inline]
    const fn incr_num_rows(&mut self, added_count: usize) {
        self.num_rows += added_count as u64;
    }

    #[inline]
    const fn get_num_rows(&self) -> usize {
        self.num_rows as usize
    }
}

const META_SIZE: usize = std::mem::size_of::<Meta>();

// sanity checks
const _: () = assert!(META_SIZE == 0x20, "META must be of 32 bytes!");

//
// Rows
//

const ITEMS_PER_ROW: usize = 0x10;

#[repr(C)]
struct Offsets {
    trail_idx: u32,
    vbuf_slots: u16,
    klen: u16,
    vlen: u16,
    flag: u8,
    _padd: u8,
}

#[repr(C, align(0x20))]
struct Row {
    signs: [u32; ITEMS_PER_ROW],
    offsets: [Offsets; ITEMS_PER_ROW],
}

const ROW_SIZE: usize = std::mem::size_of::<Row>();

// Sanity checks
const _: () = assert!(ROW_SIZE == 0x100, "Row must be of 256 bytes");
const _: () = assert!(std::mem::size_of::<Offsets>() == 0x0C);
const _: () = assert!(std::mem::size_of::<Row>() % (0x04 + 0x0C) == 0x00);

//
// Mark
//

#[derive(Debug)]
pub(super) struct Mark {
    file: TurboFile,
    mmap: TurboMMap,
    cfg: TurboConfig,
    rows_ptr: *mut Row,
    meta_ptr: *mut Meta,
}

impl Mark {
    /// Creates a new [Mark] file
    ///
    /// *NOTE* Returns an [IO] error if something goes wrong
    pub(crate) fn new(cfg: &TurboConfig) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);
        let n_items = cfg.init_cap as u64;
        let n_rows = n_items >> 0x04; // 16 items = 1 row
        let new_file_len = META_SIZE + (ROW_SIZE * n_rows as usize);

        // sanity check
        debug_assert!(
            n_rows * ITEMS_PER_ROW as u64 == n_items,
            "Incorrect row size calculations"
        );

        // new file
        let file = TurboFile::new(&cfg, PATH)?;
        file.zero_extend(new_file_len, true)?;

        let mmap = TurboMMap::new(&cfg, PATH, &file, new_file_len).map_err(|e| {
            // NOTE: Close + Delete the created file, so new init could work w/o any issues
            //
            // HACK: We ignore error from `close` and `del` as we are already in errored state, and primary
            // error is more imp then this!

            // NOTE: We can only delete the file, if file fd is released or closed, e.g. on windows
            if file.close().is_ok() {
                let _ = file.del();
            }

            e
        })?;

        // sanity check
        debug_assert_eq!(mmap.len(), new_file_len, "MMap len must be same as file len");

        let meta = Meta::new(n_rows, n_items);
        mmap.write(0x00, &meta);

        // NOTE: we use `ms_sync` here to make sure metadata is persisted before
        // any other updates are conducted on the mmap,
        //
        // HACK: we can afford this syscall here, as init does not come under the fast path
        mmap.msync()?;

        let meta_ptr = mmap.read_mut::<Meta>(0x00);
        let rows_ptr = mmap.read_mut::<Row>(META_SIZE);

        cfg.logger.debug("(Mark) [new] Created new Mark");

        Ok(Self {
            file,
            mmap,
            cfg: cfg.clone(),
            meta_ptr,
            rows_ptr,
        })
    }

    /// Open an existing [Mark] file
    ///
    /// *NOTE*: Returns an [InvalidFile] error when the underlying file is corrupted,
    /// may happen when the file is invalid or tampered with
    pub(super) fn open(cfg: &TurboConfig) -> InternalResult<Self> {
        let path = cfg.dirpath.join(PATH);

        // file must exists
        if !path.exists() {
            let err = InternalError::InvalidFile("Path does not exists".into());
            cfg.logger.error(format!("(Mark) [open] Invalid path: {err}"));
            return Err(err);
        }

        // open existing file (file handle)
        let file = TurboFile::open(&cfg, PATH)?;
        let file_len = file.len()?;

        // Check if file is rows aligned
        let rows_len = file_len.wrapping_sub(META_SIZE);
        if rows_len == 0x00 || rows_len % 0x10 != 0x00 {
            let err = InternalError::InvalidFile("Mark is not row aligned".into());
            cfg.logger
                .error(format!("(Mark) [open] Existing file is invalid: {err}"));
            return Err(err);
        }

        let mmap = TurboMMap::new(&cfg, PATH, &file, file_len)?;
        let meta_ptr = mmap.read_mut::<Meta>(0x00);
        let rows_ptr = mmap.read_mut::<Row>(META_SIZE);

        // sanity check
        debug_assert_eq!(mmap.len(), file_len, "MMap len must be same as file len");

        // metadata validations
        //
        // NOTE/TODO: In future, we need to support the old file versions, if any!
        unsafe {
            if (*meta_ptr).magic != MAGIC || (*meta_ptr).version != VERSION {
                cfg.logger.warn("(Mark) [open] File has invalid VERSION or MAGIC");
            }
        }

        cfg.logger.debug("(Mark) [open] open is successful");

        Ok(Self {
            file,
            mmap,
            meta_ptr,
            rows_ptr,
            cfg: cfg.clone(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    mod mark {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        #[test]
        fn test_new_works() {
            let (cfg, _tmp) = TurboConfig::test_cfg("mark_new_works");

            let n_rows = cfg.init_cap >> 0x04;
            let m1 = unsafe { Mark::new(&cfg) }.expect("new mark");

            unsafe {
                let meta = *m1.meta_ptr;

                assert!(m1.file.fd() >= 0x00, "File fd must be valid");
                assert!(m1.mmap.len() > 0x00, "Mmap must be non zero");

                assert_eq!(meta.magic, MAGIC, "Correct file MAGIC");
                assert_eq!(meta.version, VERSION, "Correct file VERSION");
                assert_eq!(meta.num_rows, n_rows as u64);
                assert_eq!(meta.num_items, cfg.init_cap as u64);

                assert!(!m1.meta_ptr.is_null());
                assert!(!m1.rows_ptr.is_null());
            }
        }

        #[test]
        fn test_open_works() {
            let (cfg, _tmp) = TurboConfig::test_cfg("mark_open_works");

            unsafe {
                let m0 = Mark::new(&cfg).expect("new mark");

                (*m0.meta_ptr).num_rows = 0x01;
                (*m0.meta_ptr).num_items = 0x10;
                (*m0.meta_ptr).free = 0x0A;

                drop(m0);
            }

            let m1 = unsafe { Mark::open(&cfg) }.expect("open existing");

            unsafe {
                let meta = (*m1.meta_ptr);

                assert!(m1.file.fd() >= 0x00, "File fd must be valid");
                assert!(m1.mmap.len() > 0x00, "Mmap must be non zero");

                assert_eq!(meta.magic, MAGIC, "Correct file MAGIC");
                assert_eq!(meta.version, VERSION, "Correct file VERSION");
                assert_eq!(meta.num_items, 0x10);
                assert_eq!(meta.num_rows, 0x01);
                assert_eq!(meta.free, 0x0A);

                assert!(!m1.meta_ptr.is_null());
                assert!(!m1.rows_ptr.is_null());
            }
        }

        #[test]
        fn test_open_panics_on_invalid_metadata_in_file() {
            let (cfg, _tmp) = TurboConfig::test_cfg("mark_open_panics");

            unsafe {
                let m0 = unsafe { Mark::new(&cfg) }.expect("new mark");
                m0.file.zero_extend(META_SIZE, true).expect("Update file len");
            }

            // should panic
            assert!(unsafe { Mark::open(&cfg) }.is_err());
        }

        #[test]
        fn test_new_fails_when_dir_is_not_writable() {
            let (cfg, _tmp) = TurboConfig::test_cfg("traol_new_fails");
            let dir = _tmp.path().to_path_buf();

            // NOTE: w/ chmod 000 we simulate unwriteable directory
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).expect("Set permission");

            assert!(
                unsafe { Mark::new(&cfg) }.is_err(),
                "Mark::new should fail on unwritable directory"
            );

            // WARN: Must always restore back to avoid shutdown issues
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).expect("Re-Set Permission");
        }

        #[test]
        fn test_open_fails_when_dir_is_not_readable() {
            let (cfg, _tmp) = TurboConfig::test_cfg("mark_open_fails");
            let dir = _tmp.path().to_path_buf();
            let path = dir.join("mark");

            std::fs::write(&path, &[0u8; 64]).expect("Write");
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).expect("Set Permission");

            let res = unsafe { Mark::open(&cfg) };
            assert!(res.is_err(), "Mark::open should fail when directory is unreadable");

            // WARN: Must always restore back to avoid shutdown issues
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).expect("Re-Set Permission");
        }
    }
}
