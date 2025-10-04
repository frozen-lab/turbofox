use crate::{
    debug_error,
    error::{InternalError, InternalResult},
    kosh::{MAGIC, VERSION},
};
use memmap2::MmapMut;
use std::sync::atomic::{AtomicU64, Ordering};

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
            _ => Err(InternalError::InvalidEntry(None)),
        }
    }
}

/// ----------------------------------------
/// Pair
/// ----------------------------------------

#[repr(align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pair {
    pub ns: Namespace,
    pub klen: u16,
    pub vlen: u16,
    pub offset: u64,
}

pub(crate) type PairBytes = [u8; 10];
pub(crate) const EMPTY_PAIR_BYTES: PairBytes = [0u8; 10];

// NOTE: [PairBytes] is `[u8; 10]`, i.e. the alignment = 1, size = 10.
// This assert ensures size is always a multiple of `u8`.
//
// WARN: If [PairBytes] ever has to change, make sure it's size is multiple of u8
// or the read/write on mmap needs to be updated!
const _: () = assert!(size_of::<PairBytes>() % size_of::<u8>() == 0);

impl Pair {
    pub fn new(offset: u64, ns: Namespace, klen: usize, vlen: usize) -> Self {
        Self {
            offset,
            ns,
            klen: klen as u16,
            vlen: vlen as u16,
        }
    }

    pub fn to_raw(&self) -> InternalResult<PairBytes> {
        //
        // Overflow check for [self.offset]
        //
        // NOTE: [self.offset] can not grow beyound (2^40 - 1)
        //
        if (self.offset & !((1u64 << 40) - 1)) != 0 {
            debug_error!("Pair offset ({}) is beyound limit of u40", self.offset);
            return Err(InternalError::BucketOverflow(None));
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

    pub fn from_raw(slice: PairBytes) -> InternalResult<Pair> {
        let ns = Namespace::try_from(slice[0]).map_err(|e| {
            debug_error!("PairBytes contains invalid namespace value ({})", slice[0]);
            e
        })?;

        let klen = u16::from_le_bytes([slice[1], slice[2]]);
        let vlen = u16::from_le_bytes([slice[3], slice[4]]);

        let offset =
            u64::from_le_bytes([slice[5], slice[6], slice[7], slice[8], slice[9], 0, 0, 0]);

        //
        // Overflow check for [self.offset]
        //
        // NOTE: [self.offset] can not grow beyound (2^40 - 1)
        //
        if (offset & !((1u64 << 40) - 1)) != 0 {
            debug_error!("Pair offset ({}) is beyound limit of u40", offset);
            return Err(InternalError::InvalidEntry(None));
        };

        Ok(Pair {
            ns,
            klen,
            vlen,
            offset,
        })
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

        unsafe {
            ptr.write(MetaView::default());
        }

        Self { ptr }
    }

    #[inline(always)]
    pub fn open(mmap: &mut MmapMut) -> Self {
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
            "Decr must not be called when inserts are at 0."
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

    #[inline(always)]
    pub const fn size_of() -> usize {
        std::mem::size_of::<MetaView>()
    }
}

#[cfg(test)]
mod meta_tests {
    use super::*;

    mod namespace {
        use super::*;

        const VARIANTS: [Namespace; 1] = [Namespace::Base];

        #[test]
        fn test_roundtrip_namespace_to_u8_and_back() {
            for &ns in &VARIANTS {
                let val: u8 = ns.into();
                let round = Namespace::try_from(val).expect("roundtrip failed");

                assert_eq!(round, ns, "Namespace {ns:?} did not roundtrip correctly");
            }
        }

        #[test]
        fn test_invalid_namespace_returns_error() {
            let valid: Vec<u8> = VARIANTS.iter().map(|&ns| ns.into()).collect();

            for v in 0..=u8::MAX {
                if !valid.contains(&v) {
                    assert!(Namespace::try_from(v).is_err());
                }
            }
        }

        #[test]
        fn test_base_namespace_is_zero() {
            assert_eq!(u8::from(VARIANTS[0]), 0, "First value must be zero!");
        }
    }

    mod pair {
        use super::*;

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
        fn test_with_max_values() {
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

            let invalid = Pair {
                ns: Namespace::Base,
                offset: 1u64 << 40,
                klen: 10,
                vlen: 10,
            };

            assert!(valid.to_raw().is_ok());
            assert!(invalid.to_raw().is_err());
        }

        #[test]
        fn test_pair_from_raw_invalid_namespace() {
            let mut raw: PairBytes = [0; 10];
            raw[0] = 0xff;

            let res = Pair::from_raw(raw);
            assert!(res.is_err(), "Expected error for invalid namespace");
        }
    }

    mod meta {
        use super::*;
        use std::sync::{Arc, Barrier};
        use std::thread;
        use tempfile::TempDir;

        unsafe impl Send for Meta {}
        unsafe impl Sync for Meta {}

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

            let mmap = unsafe { memmap2::MmapOptions::new().len(header_size).map_mut(&file) }
                .expect("map_mut");

            (mmap, tmp)
        }

        #[test]
        fn test_meta_view_default_init() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();
            let meta = Meta::new(&mut mmap);

            assert_eq!(meta.meta().magic, MAGIC);
            assert_eq!(meta.meta().version, VERSION);
            assert_eq!(meta.meta().write_pointer.load(Ordering::Relaxed), 0);
            assert_eq!(meta.meta().inserts.load(Ordering::Relaxed), 0);
        }

        #[test]
        fn test_basic_init_and_atomic_ops() {
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
        fn test_updating_write_offset_works() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();
            let meta = Meta::new(&mut mmap);

            let prev = meta.update_write_offset(64);
            assert_eq!(prev, 0);

            let prev2 = meta.update_write_offset(16);
            assert_eq!(prev2, 64);

            let wp = (&*meta.meta()).write_pointer.load(Ordering::Relaxed);
            assert_eq!(wp, 80);
        }

        #[test]
        fn test_version_magic_mismatch_detected_on_corrupt() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();

            // manually corrupt magic bytes (first 4 bytes)
            unsafe {
                let view_ptr = mmap.as_mut_ptr() as *mut MetaView;
                (*view_ptr).magic = [0xff, 0xff, 0xff, 0xff];
            }

            let meta = Meta::open(&mut mmap);
            assert!(
                !meta.is_current_version(),
                "corrupted magic should fail version check"
            );

            // manually corrupt version field (4 bytes after first 4 i.e `4..=7`)
            unsafe {
                let view_ptr = mmap.as_mut_ptr() as *mut MetaView;
                (*view_ptr).version = 0xdead_beef;
            }

            let meta2 = Meta::open(&mut mmap);
            assert!(
                !meta2.is_current_version(),
                "corrupted version should fail version check"
            );
        }

        #[test]
        fn test_validate_multiple_meta_views_syncup_on_parallel_update() {
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
            let final_wp_raw = (&*meta_a.meta()).write_pointer.load(Ordering::Relaxed);
            assert_eq!(final_wp_raw, 15);

            // both [A] & [B] should have same updated write pointer
            assert_eq!(meta_a.get_write_pointer(), meta_b.get_write_pointer());
            assert_eq!(meta_a.get_write_pointer(), 15);
            assert_eq!(meta_b.get_write_pointer(), 15);
        }

        #[test]
        fn test_validate_underflow_with_increment_and_decrement_at_edges() {
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

        #[test]
        fn test_initializes_defaults() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();
            let meta = Meta::new(&mut mmap);

            assert_eq!(meta.meta().magic, MAGIC);
            assert_eq!(meta.meta().version, VERSION);
            assert_eq!(meta.get_insert_count(), 0);
            assert_eq!(meta.get_write_pointer(), 0);
        }

        #[test]
        fn test_open_reads_existing() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();

            // Create & initialize
            let meta_a = Meta::new(&mut mmap);
            meta_a.incr_insert_count();
            meta_a.update_write_offset(42);

            // Re-open same mmap
            let meta_b = Meta::open(&mut mmap);

            assert_eq!(meta_b.get_insert_count(), 1);
            assert_eq!(meta_b.get_write_pointer(), 42);
            assert!(meta_b.is_current_version());
        }

        #[test]
        fn test_size_of_matches_metaview() {
            assert_eq!(Meta::size_of(), std::mem::size_of::<MetaView>());
        }

        #[test]
        fn test_atomic_ops_sync_correctly_across_threads() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();

            let meta = Arc::new(Meta::new(&mut mmap));
            let barrier = Arc::new(Barrier::new(2));

            let writer = {
                let meta = Arc::clone(&meta);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for _ in 0..5 {
                        meta.incr_insert_count();
                    }

                    meta.update_write_offset(123);
                })
            };

            let reader = {
                let meta = Arc::clone(&meta);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    let mut seen_count = 0;
                    let mut seen_wp = 0;

                    for _ in 0..10_000 {
                        seen_count = meta.get_insert_count();
                        seen_wp = meta.get_write_pointer();

                        if seen_count == 5 && seen_wp >= 123 {
                            break;
                        }

                        std::thread::yield_now();
                    }

                    assert_eq!(seen_count, 5, "reader must observe 5 inserts");
                    assert_eq!(seen_wp, 123, "reader must observe write offset 123");
                })
            };

            writer.join().unwrap();
            reader.join().unwrap();
        }

        #[test]
        fn test_validate_write_offset_update_for_underflow_overflow() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();
            let meta = Meta::new(&mut mmap);

            assert_eq!(meta.get_write_pointer(), 0);

            let prev = meta.update_write_offset(0);
            assert_eq!(prev, 0);
            assert_eq!(meta.get_write_pointer(), 0);

            meta.update_write_offset(100);
            assert_eq!(meta.get_write_pointer(), 100);

            let prev2 = meta.update_write_offset(u64::MAX - 50);
            assert_eq!(prev2, 100);

            let expected = 100u128.wrapping_add((u64::MAX - 50) as u128) as u64;
            assert_eq!(meta.get_write_pointer(), expected);
        }

        #[test]
        fn test_init_on_corrupted_meta_counters() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();
            let meta = Meta::new(&mut mmap);

            unsafe {
                let view_ptr = mmap.as_mut_ptr() as *mut MetaView;

                (*view_ptr).inserts = AtomicU64::new(u64::MAX - 1);
                (*view_ptr).write_pointer = AtomicU64::new(u64::MAX - 123);
            }

            assert_eq!(meta.get_insert_count(), (u64::MAX - 1) as usize);
            assert_eq!(meta.get_write_pointer(), u64::MAX - 123);
        }

        #[test]
        fn test_explicit_insert_decr_from_multiple_objects() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();
            let meta = Meta::new(&mut mmap);

            for _ in 0..5 {
                meta.incr_insert_count();
            }

            assert_eq!(meta.get_insert_count(), 5);

            for expected in (0..5).rev() {
                meta.decr_insert_count();
                assert_eq!(meta.get_insert_count(), expected);
            }
        }

        #[test]
        fn test_init_open_after_new_without_modifications_are_synced() {
            let (mut mmap, _tmp) = create_dummy_mmap_file();

            let meta_a = Meta::new(&mut mmap);
            let meta_b = Meta::open(&mut mmap);

            assert!(meta_b.is_current_version());
            assert_eq!(meta_a.get_insert_count(), 0);
            assert_eq!(meta_b.get_insert_count(), 0);
            assert_eq!(meta_a.get_write_pointer(), 0);
            assert_eq!(meta_b.get_write_pointer(), 0);
        }
    }
}
