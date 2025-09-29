use crate::error::{InternalError, InternalResult};
use memmap2::MmapMut;
use std::sync::atomic::{AtomicU64, Ordering};

/// ----------------------------------------
/// Constants and Types
/// ----------------------------------------

pub(crate) const VERSION: u32 = 3;
pub(crate) const MAGIC: [u8; 4] = *b"TCv3";

pub(crate) type KeyValue = (Vec<u8>, Vec<u8>);
pub(crate) type Key = Vec<u8>;

/// ----------------------------------------
/// Namespaces
/// ----------------------------------------

/// This acts as an id for type of pair stored in [Kosh]
///
/// NOTE: The *index* must start from `0` cause the 0th item,
/// [Base] in this case, acts as an default in the [Patra].
/// Reason, [Patra] has zeroed space at init.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Namespace {
    Base = 0,
}

impl From<Namespace> for u8 {
    fn from(ns: Namespace) -> u8 {
        ns as u8
    }
}

impl TryFrom<u8> for Namespace {
    type Error = InternalError;

    fn try_from(value: u8) -> InternalResult<Namespace> {
        match value {
            0 => Ok(Namespace::Base),
            err_id => Err(InternalError::InvalidEntry(format!(
                "Invalid namespace: {err_id}"
            ))),
        }
    }
}

#[cfg(test)]
mod namespace_tests {
    use super::*;

    const VARIANTS: [Namespace; 1] = [Namespace::Base];

    #[test]
    fn roundtrip_namespace_to_u8_and_back() {
        for &ns in &VARIANTS {
            let val: u8 = ns.into();
            let round = Namespace::try_from(val).expect("roundtrip failed");

            assert_eq!(round, ns, "Namespace {ns:?} did not roundtrip correctly");
        }
    }

    #[test]
    fn invalid_namespace_returns_error() {
        let valid: Vec<u8> = VARIANTS.iter().map(|&ns| ns.into()).collect();

        for v in 0..=u8::MAX {
            if !valid.contains(&v) {
                assert!(Namespace::try_from(v).is_err());
            }
        }
    }

    #[test]
    fn base_namespace_is_zero() {
        assert_eq!(u8::from(VARIANTS[0]), 0, "First value must be zero!");
    }
}

/// ----------------------------------------
/// Pair
/// ----------------------------------------

#[repr(align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pair {
    ns: Namespace,
    klen: u16,
    vlen: u16,
    offset: u64,
}

pub(crate) type PairBytes = [u8; 10];

impl Pair {
    fn to_raw(&self) -> InternalResult<PairBytes> {
        //
        // Overflow check for [self.offset]
        //
        // NOTE: [self.offset] can not grow beyound (2^40 - 1)
        //
        if (self.offset & !((1u64 << 40) - 1)) != 0 {
            return Err(InternalError::BucketOverflow);
        };

        let mut out = [0u8; 10];

        // namespace
        out[0] = self.ns as u8;

        // klen (LE)
        out[1..3].copy_from_slice(&self.klen.to_le_bytes());

        // vlen (LE)
        out[3..5].copy_from_slice(&self.vlen.to_le_bytes());

        // offset (only 5 bytes, LE)
        let offset_bytes = self.offset.to_le_bytes();
        out[5..10].copy_from_slice(&offset_bytes[..5]);

        Ok(out)
    }

    fn from_raw(slice: PairBytes) -> InternalResult<Pair> {
        let ns = Namespace::try_from(slice[0])?;

        let klen = u16::from_le_bytes([slice[1], slice[2]]);
        let vlen = u16::from_le_bytes([slice[3], slice[4]]);

        let offset =
            u64::from_le_bytes([slice[5], slice[6], slice[7], slice[8], slice[9], 0, 0, 0]);

        Ok(Pair {
            ns,
            klen,
            vlen,
            offset,
        })
    }
}

#[cfg(test)]
mod pair_tests {
    use super::{Namespace, Pair};

    #[test]
    fn test_basic_round_trip() {
        let p = Pair {
            ns: Namespace::Base,
            offset: 123456789,
            klen: 100,
            vlen: 200,
        };

        let encoded = p.to_raw().expect("Encode raw pair");
        let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

        assert_eq!(p.ns, decoded.ns);
        assert_eq!(p.offset, decoded.offset);
        assert_eq!(p.klen, decoded.klen);
        assert_eq!(p.vlen, decoded.vlen);
    }

    #[test]
    fn test_boundaries() {
        let p = Pair {
            ns: Namespace::Base,
            offset: 0,
            klen: 0,
            vlen: 0,
        };

        let encoded = p.to_raw().expect("Encode raw pair");
        let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

        assert_eq!(p, decoded);

        let max_off = (1u64 << 40) - 1;
        let max_klen = u16::MAX - 1;
        let max_vlen = u16::MAX - 1;

        let p2 = Pair {
            ns: Namespace::Base,
            offset: max_off,
            klen: max_klen,
            vlen: max_vlen,
        };

        let encoded = p2.to_raw().expect("Encode raw pair");
        let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

        assert_eq!(p2, decoded);
    }

    #[test]
    fn test_randomized_values() {
        for i in 0..100 {
            let offset = (i * 1234567) as u64 & ((1u64 << 40) - 1);
            let klen = (i * 37 % (1 << 16)) as u16;
            let vlen = (i * 91 % (1 << 16)) as u16;

            let r = 0u8;
            let ns = Namespace::try_from(r).expect("Expected valid [Namespace]");

            let p = Pair {
                ns: ns,
                offset,
                klen,
                vlen,
            };

            let encoded = p.to_raw().expect("Encode raw pair");
            let decoded = Pair::from_raw(encoded).expect("Decode raw pair");

            assert_eq!(p, decoded, "Failed at iteration {i}");
        }
    }

    #[test]
    fn test_offset_guard() {
        let valid = Pair {
            ns: Namespace::Base,
            offset: (1u64 << 40) - 1,
            klen: 10,
            vlen: 10,
        };
        assert!(valid.to_raw().is_ok());

        let invalid = Pair {
            ns: Namespace::Base,
            offset: 1u64 << 40,
            klen: 10,
            vlen: 10,
        };
        assert!(invalid.to_raw().is_err());
    }
}

/// ----------------------------------------
/// Meta
/// ----------------------------------------

#[repr(C)]
struct MetaView {
    magic: [u8; 4],
    version: u32,
    inserts: AtomicU64,
    write_pointer: AtomicU64,
}

impl Default for MetaView {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            inserts: AtomicU64::new(0),
            write_pointer: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Meta {
    ptr: *mut MetaView,
}

impl Meta {
    #[inline(always)]
    pub fn new(mmap: &mut MmapMut) -> Self {
        let ptr = mmap.as_mut_ptr() as *mut MetaView;
        Self { ptr }
    }

    #[inline(always)]
    fn meta(&self) -> &MetaView {
        unsafe { &*self.ptr }
    }

    #[inline(always)]
    pub fn get_write_pointer(&self) -> u64 {
        self.meta().write_pointer.load(Ordering::Acquire) as u64
    }

    #[inline(always)]
    pub fn get_insert_count(&self) -> usize {
        self.meta().inserts.load(Ordering::Acquire) as usize
    }

    #[inline(always)]
    pub fn incr_insert_count(&self) {
        self.meta().inserts.fetch_add(1, Ordering::Release);
    }

    #[inline(always)]
    /// NOTE: For AtomicU64 on underflow it wraps around. So we must prevent that!
    pub fn decr_insert_count(&self) {
        // sanity check
        debug_assert!(
            self.meta().inserts.load(Ordering::Relaxed) > 0,
            "Decr must not be called when insert is 0."
        );

        self.meta().inserts.fetch_sub(1, Ordering::Release);
    }

    #[inline(always)]
    pub fn update_write_offset(&self, n: u64) -> u64 {
        self.meta().write_pointer.fetch_add(n, Ordering::Release)
    }

    #[inline(always)]
    pub fn is_current_version(&self) -> bool {
        let meta = self.meta();
        meta.magic == MAGIC && meta.version == VERSION
    }
}

#[cfg(test)]
mod meta_tests {
    use super::*;
    use tempfile::TempDir;

    fn create_dummy_mmap_file() -> (MmapMut, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("meta.bin");

        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("open tmp file");

        let header_size = size_of::<MetaView>();
        file.set_len(header_size as u64).expect("set_len");

        let mut mmap = unsafe { memmap2::MmapOptions::new().len(header_size).map_mut(&file) }
            .expect("map_mut");

        unsafe {
            let meta_ptr = mmap.as_mut_ptr() as *mut MetaView;
            meta_ptr.write(MetaView::default());
        }

        (mmap, tmp)
    }

    #[test]
    fn test_meta_view_default() {
        let (mut mmap, _tmp) = create_dummy_mmap_file();
        let meta = Meta::new(&mut mmap);

        assert_eq!(meta.meta().magic, MAGIC);
        assert_eq!(meta.meta().version, VERSION);
        assert_eq!(meta.meta().write_pointer.load(Ordering::Relaxed), 0);
        assert_eq!(meta.meta().inserts.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn basic_init_and_atomic_ops() {
        let (mut mmap, _tmp) = create_dummy_mmap_file();
        let meta = Meta::new(&mut mmap);

        // init
        assert!(meta.is_current_version(), "meta should match MAGIC/VERSION");
        assert_eq!(meta.get_insert_count(), 0);
        assert_eq!(meta.get_write_pointer(), 0);

        // insert incr
        meta.incr_insert_count();
        meta.incr_insert_count();
        assert_eq!(meta.get_insert_count(), 2);

        // insert decr
        meta.decr_insert_count();
        assert_eq!(meta.get_insert_count(), 1);

        // write offset
        let prev = meta.update_write_offset(64);
        assert_eq!(prev, 0);
    }

    #[test]
    fn update_write_offset_works() {
        let (mut mmap, _tmp) = create_dummy_mmap_file();
        let meta = Meta::new(&mut mmap);

        let prev = meta.update_write_offset(64);
        assert_eq!(prev, 0);

        let prev2 = meta.update_write_offset(16);
        assert_eq!(prev2, 64);

        let wp = unsafe { (&*meta.meta()).write_pointer.load(Ordering::Relaxed) };
        assert_eq!(wp, 80);
    }

    #[test]
    fn version_magic_mismatch_detected() {
        let (mut mmap, _tmp) = create_dummy_mmap_file();

        // manually corrupt magic bytes (first 4 bytes)
        unsafe {
            let view_ptr = mmap.as_mut_ptr() as *mut MetaView;
            (*view_ptr).magic = [0xff, 0xff, 0xff, 0xff];
        }

        let meta = Meta::new(&mut mmap);
        assert!(
            !meta.is_current_version(),
            "corrupted magic should fail version check"
        );

        // manually corrupt version field (4 bytes after first 4 i.e `4..=7`)
        unsafe {
            let view_ptr = mmap.as_mut_ptr() as *mut MetaView;
            (*view_ptr).version = 0xdead_beef;
        }

        let meta2 = Meta::new(&mut mmap);
        assert!(
            !meta2.is_current_version(),
            "corrupted version should fail version check"
        );
    }

    #[test]
    fn multiple_meta_views_reflect_each_other() {
        let (mut mmap, _tmp) = create_dummy_mmap_file();

        // Multiple wrappers w/ same pointer
        let meta_a = Meta::new(&mut mmap);
        let meta_b = Meta::new(&mut mmap);

        // mutate via [A]
        meta_a.incr_insert_count();
        meta_a.incr_insert_count();

        // [B] should observe the changes
        assert_eq!(meta_b.get_insert_count(), 2);

        // update [B] and observe via [A]
        let prev_b = meta_b.update_write_offset(10);
        let prev_a = meta_a.update_write_offset(5);
        assert_eq!(prev_b, 0);
        assert_eq!(prev_a, 10);

        // check final value by reading meta manually
        let final_wp_raw = unsafe { (&*meta_a.meta()).write_pointer.load(Ordering::Relaxed) };
        assert_eq!(final_wp_raw, 15);

        // both [A] & [B] should have same updated write pointer
        assert_eq!(meta_a.get_write_pointer(), meta_b.get_write_pointer());
        assert_eq!(meta_a.get_write_pointer(), 15);
        assert_eq!(meta_b.get_write_pointer(), 15);
    }

    #[test]
    fn increment_and_decrement_edge_behavior() {
        let (mut mmap, _tmp) = create_dummy_mmap_file();
        let meta = Meta::new(&mut mmap);

        (0..3).for_each(|_| meta.incr_insert_count());
        assert_eq!(meta.get_insert_count(), 3);

        (0..3).for_each(|_| meta.decr_insert_count());
        assert_eq!(meta.get_insert_count(), 0);

        // NOTE: For AtomicU64 on underflow it wrapps around. So we must prevent that!
        // HACK: The code uses `debug_assert!` so there won't be any assertion in
        // `--release` mode!
        #[cfg(debug_assertions)]
        assert!(std::panic::catch_unwind(|| meta.decr_insert_count()).is_err());
    }
}
