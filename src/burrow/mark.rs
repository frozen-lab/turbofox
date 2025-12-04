use crate::{
    burrow::GROWTH_FACTOR,
    core::{TurboFile, TurboMMap},
    errors::{InternalError, InternalResult},
    hasher::{Sign, EMPTY_SIGN, TOMBSTONE_SIGN},
    TurboConfig,
};

const VERSION: u32 = 0x01;
const MAGIC: [u8; 0x04] = *b"mrk1";
const PATH: &'static str = "mark";
const REHASH_PATH: &'static str = "mark_hash";

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

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy)]
struct RowsPtr(*mut Row);

impl RowsPtr {
    #[inline]
    const fn new(rows_ptr: *mut Row) -> Self {
        Self(rows_ptr)
    }

    #[inline]
    const fn row(&self, idx: usize) -> Self {
        unsafe { Self(self.0.add(idx)) }
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
    rows_ptr: RowsPtr,
    meta_ptr: MetaPtr,
    free_trsh: u64,
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
            meta_ptr: MetaPtr::new(meta_ptr),
            rows_ptr: RowsPtr::new(rows_ptr),
            free_trsh: Self::calc_threshold(n_items),
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

        let meta = MetaPtr::new(meta_ptr).meta();

        // metadata validations
        //
        // NOTE/TODO: In future, we need to support the old file versions, if any!
        if meta.magic != MAGIC || meta.version != VERSION {
            cfg.logger.error("(Mark) [open] File has invalid VERSION or MAGIC");
        }

        cfg.logger.debug("(Mark) [open] open is successful");

        Ok(Self {
            file,
            mmap,
            cfg: cfg.clone(),
            meta_ptr: MetaPtr::new(meta_ptr),
            rows_ptr: RowsPtr::new(rows_ptr),
            free_trsh: Self::calc_threshold(meta.num_items),
        })
    }

    pub(super) fn new_with_rehash(&self) -> InternalResult<Self> {
        let path = self.cfg.dirpath.join(REHASH_PATH);

        // clear up older version
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| {
                self.cfg
                    .logger
                    .error(format!("(Mark) [rehash] Failed to delete older rehash TurboFile"));
                e
            })?;
        }

        let meta = self.meta_ptr.meta();

        let new_nitems = meta.num_items * GROWTH_FACTOR;
        let new_nrows = meta.num_rows * GROWTH_FACTOR;
        let new_file_len = META_SIZE + (ROW_SIZE * new_nrows as usize);

        // sanity check
        debug_assert!(
            new_nrows * ITEMS_PER_ROW as u64 == new_nitems,
            "Incorrect row size calculations"
        );

        // new file
        let file = TurboFile::new(&self.cfg, REHASH_PATH)?;
        file.zero_extend(new_file_len, true)?;

        let mmap = TurboMMap::new(&self.cfg, REHASH_PATH, &file, new_file_len).map_err(|e| {
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

        let meta = Meta::new(new_nrows, new_nitems);
        mmap.write(0x00, &meta);

        // NOTE: we use `ms_sync` here to make sure metadata is persisted before
        // any other updates are conducted on the mmap,
        //
        // HACK: we can afford this syscall here, as init does not come under the fast path
        mmap.msync()?;

        let meta_ptr = MetaPtr::new(mmap.read_mut::<Meta>(0x00));
        let rows_ptr = RowsPtr::new(mmap.read_mut::<Row>(META_SIZE));

        self.cfg.logger.debug("(Mark) [rehash] Created new Mark for rehash");
        let mut new_mark = Self {
            file,
            mmap,
            cfg: self.cfg.clone(),
            rows_ptr,
            meta_ptr,
            free_trsh: Self::calc_threshold(new_nitems),
        };

        // iter & re-hash
        let mut mark_iter = self.iter();
        while let Some((sign, ofs)) = mark_iter.next() {
            new_mark.set(sign, ofs, false)?;
        }

        // persist the new [Mark]
        new_mark.mmap.msync()?;

        Ok(new_mark)
    }

    /// Insert or update a new entry in [Mark]
    #[inline(always)]
    pub(super) fn set(&mut self, sign: Sign, ofs: Offsets, upsert: bool) -> InternalResult<Option<()>> {
        let meta = self.meta_ptr.meta_mut();
        let mut idx = (sign as u64) & (meta.num_items - 0x01); // NOTE: only works when `num_items` is power of 2

        // sanity check
        debug_assert!(
            meta.num_items % self.cfg.init_cap as u64 == 0x00,
            "NUM_ITEMS must be aligned with INIT_CAP"
        );

        // not enough space left
        if self.free_trsh > meta.free as u64 {
            return Err(InternalError::MarkIsFull);
        }

        // lookup

        let mut rows_left = meta.num_rows;
        let mut row_idx = (idx >> 0x04) as usize; // /16
        let mut slot_idx = (idx & 0x0F) as usize; // %16

        while rows_left > 0x00 {
            let row_ptr = self.rows_ptr.row(row_idx);

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
                if *sign_ptr == EMPTY_SIGN {
                    *sign_ptr = sign;
                    *ofs_ptr = ofs;
                    meta.free -= 0x01;
                    return Ok(Some(()));
                }

                // reuse deleted slot
                if *sign_ptr == TOMBSTONE_SIGN {
                    *sign_ptr = sign;
                    *ofs_ptr = ofs;
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
    #[inline(always)]
    pub(super) fn get(&mut self, sign: Sign) -> InternalResult<Option<Offsets>> {
        let meta = self.meta_ptr.meta();
        let mut idx = (sign as u64) & (meta.num_items - 0x01);

        // lookup

        let mut rows_left = meta.num_rows;
        let mut row_idx = (idx >> 0x04) as usize; // /16
        let mut slot_idx = (idx & 0x0F) as usize; // %16

        while rows_left > 0x00 {
            let row_ptr = self.rows_ptr.row(row_idx);

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
    #[inline(always)]
    pub(super) fn del(&mut self, sign: Sign) -> InternalResult<Option<Offsets>> {
        let meta = self.meta_ptr.meta_mut();
        let mut idx = (sign as u64) & (meta.num_items - 0x01);

        // lookup

        let mut rows_left = meta.num_rows;
        let mut row_idx = (idx >> 0x04) as usize; // /16
        let mut slot_idx = (idx & 0x0F) as usize; // %16

        while rows_left > 0x00 {
            let row_ptr = self.rows_ptr.row(row_idx);

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

    #[inline]
    pub(super) fn iter(&self) -> MarkIter {
        MarkIter {
            rows_ptr: self.rows_ptr,
            meta_ptr: self.meta_ptr,
            rows_idx: 0x00,
            slot_idx: 0x00,
            rows_left: self.meta_ptr.meta().num_rows as usize,
        }
    }

    /// Calculate the threshold of *free* w/ `current_cap`
    ///
    /// The threshold is set at *6.25%* of the capacity to avoid frequent collisions
    #[inline]
    const fn calc_threshold(current_cap: u64) -> u64 {
        current_cap >> 0x04
    }
}

pub(super) struct MarkIter {
    rows_ptr: RowsPtr,
    meta_ptr: MetaPtr,
    rows_idx: usize,
    slot_idx: usize,
    rows_left: usize,
}

impl Iterator for MarkIter {
    type Item = (Sign, Offsets);

    fn next(&mut self) -> Option<Self::Item> {
        while self.rows_left > 0x00 {
            let row = self.rows_ptr.row(self.rows_idx);

            while self.slot_idx < ITEMS_PER_ROW {
                let sign = row.sign(self.slot_idx);
                let ofs = row.offset(self.slot_idx);
                self.slot_idx += 0x01;

                // empty slot
                if sign == EMPTY_SIGN || sign == TOMBSTONE_SIGN {
                    continue;
                }

                return Some((sign, ofs.clone()));
            }

            self.slot_idx = 0x00;
            self.rows_idx += 0x01;
            self.rows_left -= 0x01;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hasher::TurboHash;

    #[inline]
    fn mk_sign(i: u8) -> Sign {
        TurboHash::new(&[i])
    }

    #[inline]
    fn mk_ofs(i: u32) -> Offsets {
        Offsets::new(0x04, 0x08, 0x01, i)
    }

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

    mod ops {
        use super::*;

        #[test]
        fn test_set_get_del_flow() {
            let (cfg, _tmp) = TurboConfig::test_cfg("mark_set_get_del");
            let mut mark = Mark::new(&cfg).expect("new mark");

            // validate set ops
            for i in 0x00..0x20 {
                let sign = mk_sign(i);
                let ofs = mk_ofs(i as u32);

                let r = mark.set(sign, ofs.clone(), false);
                assert!(r.is_ok(), "set should succeed");
                assert!(r.expect("set ops").is_some(), "set = inserted");
            }

            // validate get ops
            for i in 0x00..0x20 {
                let sign = mk_sign(i);
                let ofs = mk_ofs(i as u32);
                let got = mark.get(sign).expect("get ok");

                assert!(got.is_some(), "must exist");
                assert_eq!(got.expect("is some").trail_idx, ofs.trail_idx);
            }

            // validate del ops (half)
            for i in 0x00..0x10 {
                let sign = mk_sign(i);
                let del = mark.del(sign).expect("del ok");
                assert!(del.is_some(), "delete must return old value");
                assert_eq!(del.expect("del ok").trail_idx, i as u32);
            }

            // validate del ops are None (on half)
            for i in 0x00..0x10 {
                let sign = mk_sign(i);
                let got = mark.get(sign).expect("get ok");
                assert!(got.is_none(), "deleted entry should not exist");
            }

            // validate non-deleted works correctly
            for i in 0x10..0x20 {
                let sign = mk_sign(i);
                let got = mark.get(sign).expect("get ok");
                assert!(got.is_some(), "existing entry lost after deletes");
                assert_eq!(got.expect("is some").trail_idx, i as u32);
            }
        }

        #[test]
        fn test_set_update_with_upsert() {
            let (cfg, _tmp) = TurboConfig::test_cfg("mark_set_update");
            let mut mark = Mark::new(&cfg).expect("new mark");

            let sign = mk_sign(0x01);
            let ofs0 = mk_ofs(0x0A);
            let ofs1 = mk_ofs(0x63);

            // insert
            let r1 = mark.set(sign, ofs0.clone(), false);
            assert!(r1.expect("set ok").is_some());

            // update (upsert = true)
            let r2 = mark.set(sign, ofs1.clone(), true);
            assert!(r2.expect("set ok").is_some());

            // verify updated
            let got = mark.get(sign).expect("get ok").unwrap();
            assert_eq!(got.trail_idx, ofs1.trail_idx);
        }

        #[test]
        fn test_set_fails_when_full() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_full");
            cfg = cfg.init_cap(0x80).expect("init cap"); // small cap

            let mut mark = Mark::new(&cfg).expect("new mark");

            // Fill until free <= free_trsh
            loop {
                let sign = mk_sign(rand::random());
                let ofs = mk_ofs(1);

                match mark.set(sign, ofs.clone(), false) {
                    Ok(Some(())) => continue,
                    Ok(None) => continue,
                    Err(InternalError::MarkIsFull) => break,
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            }

            // Now set must always fail
            let sign = mk_sign(0x41);
            let ofs = mk_ofs(0x3A);

            let r = mark.set(sign, ofs, false);
            assert!(r.is_err(), "set must fail in full state");
            assert!(matches!(r.unwrap_err(), InternalError::MarkIsFull));
        }

        #[test]
        fn test_del_on_nonexistent_returns_none() {
            let (cfg, _tmp) = TurboConfig::test_cfg("mark_del_nonexistent");
            let mut mark = Mark::new(&cfg).expect("new mark");

            let sign = mk_sign(0x41);
            let r = mark.del(sign).expect("del ok");
            assert!(r.is_none(), "deleting non-existing must return None");
        }
    }

    mod rehash {
        use super::*;

        #[test]
        fn test_rehash_grows_capacity() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_rehash_basic");
            cfg = cfg.init_cap(0x80).expect("new cap");

            let mark = Mark::new(&cfg).expect("new Mark");
            let old_meta = mark.meta_ptr.meta();

            let new_mark = mark.new_with_rehash().expect("rehash ok");
            let new_meta = new_mark.meta_ptr.meta();

            assert_eq!(new_meta.num_items, old_meta.num_items * GROWTH_FACTOR);
            assert_eq!(new_meta.num_rows, old_meta.num_rows * GROWTH_FACTOR);

            // free should be all cap, as no inserts are done!
            assert_eq!(new_meta.free, new_meta.num_items as u32);
        }

        #[test]
        fn test_rehash_preserves_entries() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_rehash_preserve");
            cfg = cfg.init_cap(0x80).expect("new cap");

            let mut mark = Mark::new(&cfg).expect("new");

            for i in 0x00..0x3A {
                let sign = mk_sign(i);
                let ofs = mk_ofs(i as u32);
                assert!(mark.set(sign, ofs.clone(), false).expect("set ok").is_some());
            }

            let mut new_mark = mark.new_with_rehash().expect("rehash ok");

            // All entries must exists after rehash
            for i in 0x00..0x3A {
                let sign = mk_sign(i);
                let got = new_mark.get(sign).expect("get ok");
                assert!(got.is_some());
                assert_eq!(got.expect("is ok").trail_idx, i as u32);
            }
        }

        #[test]
        fn test_rehash_skips_tombstones() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_rehash_tombstones");
            cfg = cfg.init_cap(0x80).expect("new cap");

            let mut mark = Mark::new(&cfg).expect("new");

            // initial inserts
            for i in 0x00..0x2E {
                let sign = mk_sign(i);
                let ofs = mk_ofs(i as u32);
                mark.set(sign, ofs.clone(), false).unwrap();
            }

            // del half (creates tombstones)
            for i in 0x00..0x0F {
                let sign = mk_sign(i);
                assert!(mark.del(sign).expect("is ok").is_some());
            }

            let mut new_mark = mark.new_with_rehash().expect("rehash ok");

            // Deleted ones must NOT reappear
            for i in 0x00..0x0F {
                let sign = mk_sign(i);
                assert!(new_mark.get(sign).expect("is ok").is_none());
            }

            // all others must exists
            for i in 0x0F..0x2E {
                let sign = mk_sign(i);
                let got = new_mark.get(sign).unwrap();
                assert!(got.is_some());
                assert_eq!(got.expect("is ok").trail_idx, i as u32);
            }
        }

        #[test]
        fn test_rehash_no_duplicates() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_rehash_no_duplicates");
            cfg = cfg.init_cap(0x80).expect("new cap");

            let mut mark = Mark::new(&cfg).expect("new");
            let old_meta = mark.meta_ptr.meta();

            for i in 0x00..0x28 {
                let sign = mk_sign(i);
                let ofs = mk_ofs(i as u32);
                mark.set(sign, ofs.clone(), false).expect("is ok");
            }

            let new_mark = mark.new_with_rehash().expect("rehash ok");
            let new_meta = new_mark.meta_ptr.meta();

            let mut count = 0x00;
            for (sign, _) in new_mark.iter() {
                count += 0x01;
            }

            assert_eq!(count, 0x28, "rehash must not duplicate entries");
            assert_eq!(new_meta.free as u64, new_meta.num_items - 0x28);
        }

        #[test]
        fn test_rehash_iter_stability() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_rehash_iter");
            cfg = cfg.init_cap(0x80).expect("new cap");

            let mut mark = Mark::new(&cfg).expect("new mark");
            for i in 0x00..0x20 {
                mark.set(mk_sign(i), mk_ofs(i as u32), false).expect("is ok");
            }

            let mut seen = [false; 0x20];
            let new_mark = mark.new_with_rehash().expect("rehash ok");
            for (sign, ofs) in new_mark.iter() {
                let idx = ofs.trail_idx as usize;
                seen[idx] = true;

                assert!(idx < 0x20);
            }

            assert!(seen.iter().all(|x| *x), "all entries must be present exactly once");
        }

        #[test]
        fn test_rehash_replacement_flow() {
            let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_rehash_replacement_flow");
            cfg = cfg.init_cap(0x80).expect("init cap");

            let mut mark = Mark::new(&cfg).expect("new mark");

            // fill up entier rows
            loop {
                let sign = mk_sign(rand::random());
                let ofs = mk_ofs(rand::random::<u32>() & 0xFF);

                match mark.set(sign, ofs.clone(), false) {
                    Ok(Some(())) => continue,
                    Ok(None) => continue,
                    Err(InternalError::MarkIsFull) => break,
                    Err(e) => panic!("Unexpected error: {e:?}"),
                }
            }

            // Simulate grow + replace
            let mark_old_meta = mark.meta_ptr.meta();
            let new_mark = mark.new_with_rehash().expect("rehash ok");

            // Replace new w/ old
            mark = new_mark;
            let new_meta = mark.meta_ptr.meta();

            // validations (mostly sanity checks)

            assert_eq!(new_meta.num_items, mark_old_meta.num_items * GROWTH_FACTOR);
            assert_eq!(new_meta.num_rows, mark_old_meta.num_rows * GROWTH_FACTOR);

            // old entries exists
            let mut count = 0usize;
            for (sign, ofs) in mark.iter() {
                assert!(mark.get(sign).expect("is ok").is_some());
                count += 0x01;
            }

            assert!(count > 0x00, "rehash must preserve all old entries");

            // set works (cap has grown)
            let sign_new = mk_sign(0xF1);
            let ofs_new = mk_ofs(0xAB);

            let res = mark.set(sign_new, ofs_new.clone(), false);
            assert!(res.is_ok(), "post-rehash insert must succeed");
            assert!(res.expect("is ok").is_some(), "post-rehash insert must be inserted");

            let got = mark.get(sign_new).unwrap();
            assert!(got.is_some(), "new entry must exist");
            assert_eq!(got.expect("is ok").trail_idx, ofs_new.trail_idx);
        }
    }

    mod bench {
        use super::*;
        use std::time::Instant;

        #[ignore]
        #[test]
        fn bench_mark_set() {
            const INIT_CAP: usize = 0x80_000; // 524288 entries
            const ROUNDS: usize = 0x14;

            let threshold = Mark::calc_threshold(INIT_CAP as u64);
            let entries_per_iter = INIT_CAP - (threshold as usize);

            // STEP1: warmup (set + del)
            let niters = 0x10 * ITEMS_PER_ROW;
            let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_rehash_replacement_flow");
            cfg = cfg.init_cap(INIT_CAP).expect("init cap");
            let mut mark = Mark::new(&cfg).expect("new mark");
            for i in 0x00..niters {
                let kbuf = i.to_le_bytes();
                let sign = TurboHash::new(&kbuf);
                let ofs = mk_ofs(i as u32);
                mark.set(sign, ofs, false).expect("set is ok");
            }

            // STEP2: benches

            let mut results = Vec::with_capacity(ROUNDS);
            for _ in 0x00..ROUNDS {
                let (mut cfg, _tmp) = TurboConfig::test_cfg("mark_rehash_replacement_flow");
                cfg = cfg.init_cap(INIT_CAP).expect("init cap");
                let mut mark = Mark::new(&cfg).expect("new mark");
                let mut signs = Vec::with_capacity(entries_per_iter);
                let mut ofs = Vec::with_capacity(entries_per_iter);

                // gen inputs
                for i in 0..entries_per_iter {
                    signs.push(TurboHash::new(&i.to_le_bytes()));
                    ofs.push(mk_ofs(i as u32));
                }

                // bench

                let start = Instant::now();
                for i in 0..entries_per_iter {
                    std::hint::black_box(mark.set(signs[i], ofs[i].clone(), false).expect("set is okay"));
                }
                let elapsed = start.elapsed();

                results.push(elapsed.as_nanos() as f64 / entries_per_iter as f64);
            }

            // STEP3: Compute results

            let avg: f64 = results.iter().sum::<f64>() / results.len() as f64;
            cfg.logger.info(format!("SET: {:.3} ns/op", avg));

            #[cfg(not(debug_assertions))]
            {
                let threshold = 0x40 as f64;
                assert!(avg <= threshold, "set ops are too slow: {avg} ns/op");
            }
        }
    }
}
