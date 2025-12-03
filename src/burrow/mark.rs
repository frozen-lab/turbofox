use crate::{
    core::{TurboFile, TurboMMap},
    errors::{InternalError, InternalResult},
    hasher::{Sign, EMPTY_SIGN, TOMBSTONE_SIGN},
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

#[derive(Debug)]
struct MetaPtr(*mut Meta);

impl MetaPtr {
    #[inline]
    const fn new(meta: *mut Meta) -> Self {
        Self(meta)
    }

    #[inline]
    const fn meta(&self) -> Meta {
        unsafe { (*self.0) }
    }

    #[inline]
    const fn meta_mut(&self) -> &mut Meta {
        unsafe { &mut *self.0 }
    }
}

// sanity checks
const _: () = assert!(META_SIZE == 0x20, "META must be of 32 bytes!");

//
// Offsets
//

const BASE_KV_FLAG: u8 = 0x01;
const LIST_FLAG: u8 = 0x02;

const OFFSET_PADDING: u8 = 0x00;

#[repr(C, align(0x04))]
#[derive(Debug, Clone)]
pub(super) struct Offsets {
    trail_idx: u32,
    vbuf_slots: u16,
    klen: u16,
    vlen: u16,
    flag: u8,
    _padd: u8,
}

impl Offsets {
    pub(super) fn new(klen: u16, vlen: u16, vbuf_slots: u16, trail_idx: u32) -> Self {
        Self {
            klen,
            vlen,
            trail_idx,
            vbuf_slots,
            flag: BASE_KV_FLAG,
            _padd: OFFSET_PADDING,
        }
    }
}

//
// Rows
//

const ITEMS_PER_ROW: usize = 0x10;

#[repr(C, align(0x20))]
struct Row {
    signs: [u32; ITEMS_PER_ROW],
    offsets: [Offsets; ITEMS_PER_ROW],
}

const ROW_SIZE: usize = std::mem::size_of::<Row>();

#[derive(Debug)]
struct RowPtr(*mut Row);

impl RowPtr {
    #[inline]
    const fn new(rows_ptr: *mut Row, idx: usize) -> Self {
        unsafe { Self(rows_ptr.add(idx)) }
    }

    #[inline]
    fn sign(&self, idx: usize) -> Sign {
        unsafe { *(*self.0).signs.get_unchecked(idx) }
    }

    #[inline]
    fn sign_mut(&self, slot_idx: usize) -> &mut Sign {
        unsafe { (*self.0).signs.get_unchecked_mut(slot_idx) }
    }

    #[inline]
    fn offset(&self, idx: usize) -> &Offsets {
        unsafe { &*(*self.0).offsets.get_unchecked(idx) }
    }

    #[inline]
    fn offset_mut(&self, slot_idx: usize) -> &mut Offsets {
        unsafe { (*self.0).offsets.get_unchecked_mut(slot_idx) }
    }
}

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
    rows_ptr: RowPtr,
    meta_ptr: MetaPtr,
}

impl Mark {
    /// Creates a new [Mark] file
    ///
    /// *NOTE* Returns an [IO] error if something goes wrong
    pub(super) fn new(cfg: &TurboConfig) -> InternalResult<Self> {
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
            rows_ptr: RowPtr::new(rows_ptr, 0x00),
            meta_ptr: MetaPtr::new(meta_ptr),
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
            cfg: cfg.clone(),
            meta_ptr: MetaPtr::new(meta_ptr),
            rows_ptr: RowPtr::new(rows_ptr, 0x00),
        })
    }

    /// Insert or update a new entry in [Mark]
    pub(super) fn set(&mut self, sign: Sign, ofs: Offsets, upsert: bool) -> InternalResult<Option<()>> {
        let meta = self.meta_ptr.meta_mut();

        // NOTE: This only works if `num_items` is power of 2!
        // This always satisfies as `num_items` is aligned with `init_cap`
        let mut idx = (sign as u64) & (meta.num_items - 0x01);

        // sanity check
        debug_assert_eq!(
            meta.num_items, self.cfg.init_cap as u64,
            "NUM_ITEMS must be aligned with INIT_CAP"
        );

        // TODO: Grow and rehash [Row]'s
        if meta.free == 0x00 {
            return Err(InternalError::Misc("Mark is full".into()));
        }

        // open-address lookup

        let mut rows_left = meta.num_rows;
        let mut row_idx = (idx >> 0x04) as usize; // /16
        let mut slot_idx = (idx & 0x0F) as usize; // %16

        while rows_left > 0x00 {
            let row_ptr = RowPtr::new(self.rows_ptr.0, row_idx);

            for i in slot_idx..ITEMS_PER_ROW {
                let sign_ptr = row_ptr.sign_mut(i);
                let ofs_ptr = row_ptr.offset_mut(i);

                // update existing entry
                if *sign_ptr == sign {
                    if upsert {
                        *ofs_ptr = ofs;
                        return Ok(Some(()));
                    }

                    return Ok(None);
                }

                // insert new entry
                if *sign_ptr == EMPTY_SIGN || *sign_ptr == TOMBSTONE_SIGN {
                    *sign_ptr = sign;
                    *ofs_ptr = ofs;
                    meta.free -= 0x01;
                    return Ok(Some(()));
                }
            }

            rows_left -= 0x01;
            slot_idx = 0x00;
            row_idx += 0x01;

            // idx wrap
            if row_idx >= meta.get_num_rows() {
                row_idx = 0x00;
            }
        }

        // NOTE: This is an unreachable scenerio

        let err = InternalError::Misc("Mark is full and unable to grow".into());
        self.cfg
            .logger
            .error(format!("(Mark) [set] Failed to grow mark: {err}"));
        Err(err)
    }

    /// Fetch [Offsets] for an existing entry from [Mark]
    pub(super) fn get(&mut self, sign: Sign) -> InternalResult<Option<Offsets>> {
        let meta = self.meta_ptr.meta();

        // NOTE: This only works if `num_items` is power of 2!
        // This always satisfies as `num_items` is aligned with `init_cap`
        let mut idx = (sign as u64) & (meta.num_items - 0x01);

        // sanity check
        debug_assert_eq!(
            meta.num_items, self.cfg.init_cap as u64,
            "NUM_ITEMS must be aligned with INIT_CAP"
        );

        // open-address lookup

        let mut rows_left = meta.num_rows;
        let mut row_idx = (idx >> 0x04) as usize; // /16
        let mut slot_idx = (idx & 0x0F) as usize; // %16

        while rows_left > 0x00 {
            let row_ptr = RowPtr::new(self.rows_ptr.0, row_idx);

            for i in slot_idx..ITEMS_PER_ROW {
                let sign_ptr = row_ptr.sign(i);
                let ofs_ptr = row_ptr.offset(i);

                // found existing entry
                if sign_ptr == sign {
                    return Ok(Some(ofs_ptr.clone()));
                }

                // entry not found
                if sign_ptr == EMPTY_SIGN {
                    return Ok(None);
                }
            }

            rows_left -= 0x01;
            slot_idx = 0x00;
            row_idx += 0x01;

            // idx wrap
            if row_idx >= meta.get_num_rows() {
                row_idx = 0x00;
            }
        }

        Ok(None)
    }
    /// Delete [Sign] & [Offsets] for an existing entry from [Mark]
    pub(super) fn del(&mut self, sign: Sign) -> InternalResult<Option<Offsets>> {
        let meta = self.meta_ptr.meta_mut();

        // NOTE: This only works if `num_items` is power of 2!
        // This always satisfies as `num_items` is aligned with `init_cap`
        let mut idx = (sign as u64) & (meta.num_items - 0x01);

        // sanity check
        debug_assert_eq!(
            meta.num_items, self.cfg.init_cap as u64,
            "NUM_ITEMS must be aligned with INIT_CAP"
        );

        // open-address lookup

        let mut rows_left = meta.num_rows;
        let mut row_idx = (idx >> 0x04) as usize; // /16
        let mut slot_idx = (idx & 0x0F) as usize; // %16

        while rows_left > 0x00 {
            let row_ptr = RowPtr::new(self.rows_ptr.0, row_idx);

            for i in slot_idx..ITEMS_PER_ROW {
                let sign_ptr = row_ptr.sign_mut(i);
                let ofs_ptr = row_ptr.offset_mut(i);

                // del existing entry
                //
                // NOTE: We just set the [Sign] to a tombstone! We don't need to update the offset
                // as it'll automatically will get overwritten when new [Sign] is inserted
                if *sign_ptr == sign {
                    *sign_ptr = TOMBSTONE_SIGN;
                    return Ok(Some(ofs_ptr.clone()));
                }

                // no entry found
                if *sign_ptr == EMPTY_SIGN {
                    return Ok(None);
                }
            }

            rows_left -= 0x01;
            slot_idx = 0x00;
            row_idx += 0x01;

            // idx wrap
            if row_idx >= meta.get_num_rows() {
                row_idx = 0x00;
            }
        }

        Ok(None)
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
            let meta = m1.meta_ptr.meta();

            assert!(m1.file.fd() >= 0x00, "File fd must be valid");
            assert!(m1.mmap.len() > 0x00, "Mmap must be non zero");

            assert_eq!(meta.magic, MAGIC, "Correct file MAGIC");
            assert_eq!(meta.version, VERSION, "Correct file VERSION");
            assert_eq!(meta.num_rows, n_rows as u64);
            assert_eq!(meta.num_items, cfg.init_cap as u64);

            assert!(!m1.meta_ptr.0.is_null());
            assert!(!m1.rows_ptr.0.is_null());
        }

        #[test]
        fn test_open_works() {
            let (cfg, _tmp) = TurboConfig::test_cfg("mark_open_works");
            let m0 = Mark::new(&cfg).expect("new mark");

            (m0.meta_ptr.meta_mut()).num_rows = 0x01;
            (m0.meta_ptr.meta_mut()).num_items = 0x10;
            (m0.meta_ptr.meta_mut()).free = 0x0A;

            drop(m0);

            let m1 = unsafe { Mark::open(&cfg) }.expect("open existing");
            let meta = m1.meta_ptr.meta();

            assert!(m1.file.fd() >= 0x00, "File fd must be valid");
            assert!(m1.mmap.len() > 0x00, "Mmap must be non zero");

            assert_eq!(meta.magic, MAGIC, "Correct file MAGIC");
            assert_eq!(meta.version, VERSION, "Correct file VERSION");
            assert_eq!(meta.num_items, 0x10);
            assert_eq!(meta.num_rows, 0x01);
            assert_eq!(meta.free, 0x0A);

            assert!(!m1.meta_ptr.0.is_null());
            assert!(!m1.rows_ptr.0.is_null());
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
