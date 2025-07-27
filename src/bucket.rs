#![allow(dead_code)]

use crate::{
    core::{KVPair, TurboResult, MAGIC, VERSION},
    hash::{EMPTY_SIGN, TOMBSTONE_SIGN},
    TurboError,
};
use core::slice;
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    mem::size_of,
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};

#[repr(C)]
struct Meta {
    version: u32,
    magic: [u8; 4],
    inserts: AtomicU32,
    file_offset: AtomicU32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct PairOffset {
    offset: u32,
    klen: u16,
    vlen: u16,
}

#[derive(Debug)]
struct BucketFile {
    mmap: MmapMut,
    file: File,
    capacity: usize,
    header_size: usize,
    sign_offset: usize,
    po_offset: usize,
}

impl BucketFile {
    fn open<P: AsRef<Path>>(bucket_path: P, capacity: usize) -> TurboResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(bucket_path)?;

        let file_meta = file.metadata()?;
        let header_size = Self::get_header_size(capacity);

        if file_meta.len() < header_size as u64 {
            return Self::create(file, header_size, capacity);
        }

        let sign_offset = size_of::<Meta>();
        let po_offset = sign_offset + capacity * size_of::<u32>();
        let mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;

        let bucket = Self {
            file,
            mmap,
            capacity,
            header_size,
            sign_offset,
            po_offset,
        };

        let meta = bucket.metadata();

        // check if file is invalid (invalid magic, or else)
        if meta.magic != MAGIC || meta.version != VERSION {
            return Err(TurboError::InvalidFile);
        }

        Ok(bucket)
    }

    /// Create a new buffer w/ default state
    fn create(file: File, header_size: usize, capacity: usize) -> TurboResult<Self> {
        file.set_len(header_size as u64)?;

        let sign_offset = size_of::<Meta>();
        let po_offset = sign_offset + capacity * size_of::<u32>();
        let mut mmap = unsafe { MmapOptions::new().len(header_size).map_mut(&file) }?;

        // init w/ zeroed values
        mmap[..].fill(0);

        let bucket = Self {
            file,
            mmap,
            capacity,
            header_size,
            sign_offset,
            po_offset,
        };

        // set metadata
        let meta = bucket.metadata_mut();

        meta.magic = MAGIC;
        meta.version = VERSION;

        Ok(bucket)
    }

    /// Write a [KVPair] to the bucket and get [PairOffset]
    fn write_slot(&self, pair: KVPair) -> TurboResult<PairOffset> {
        let klen = pair.0.len();
        let vlen = pair.1.len();
        let blen = klen + vlen;

        let mut buf = vec![0u8; blen];

        buf[..klen].copy_from_slice(&pair.0);
        buf[klen..].copy_from_slice(&pair.1);

        let offset = self
            .metadata()
            .file_offset
            .fetch_add(blen as u32, Ordering::SeqCst);

        Self::write_all_at(&self.file, &buf, self.header_size as u64 + offset as u64)?;

        Ok(PairOffset {
            klen: klen as u16,
            vlen: vlen as u16,
            offset,
        })
    }

    /// Read a [KVPair] from a given [PairOffset]
    fn read_slot(&self, pair: &PairOffset) -> TurboResult<KVPair> {
        let klen = pair.klen as usize;
        let vlen = pair.vlen as usize;
        let mut buf = vec![0u8; klen + vlen];

        Self::read_exact_at(
            &self.file,
            &mut buf,
            self.header_size as u64 + pair.offset as u64,
        )?;

        let vbuf = buf[klen..(klen + vlen)].to_owned();
        buf.truncate(klen);

        Ok((buf, vbuf))
    }

    fn lookup_insert_slot(
        &self,
        mut idx: usize,
        signs: &[u32],
        sign: u32,
        kbuf: &[u8],
    ) -> TurboResult<(usize, bool)> {
        for _ in 0..self.capacity {
            match signs[idx] {
                EMPTY_SIGN | TOMBSTONE_SIGN => return Ok((idx, true)),
                s if s == sign => {
                    let po = self.get_pair_offset(idx);
                    let (existing_key, _) = self.read_slot(&po)?;
                    if existing_key == kbuf {
                        return Ok((idx, false));
                    }
                }
                _ => {}
            }
            idx = (idx + 1) % self.capacity;
        }

        Err(TurboError::BucketFull)
    }

    /// Returns an immutable reference to [Meta]
    #[inline(always)]
    fn metadata(&self) -> &Meta {
        unsafe { &*(self.mmap.as_ptr() as *const Meta) }
    }

    /// Returns a mutable reference to [Meta]
    #[inline(always)]
    fn metadata_mut(&self) -> &mut Meta {
        unsafe { &mut *(self.mmap.as_ptr() as *mut Meta) }
    }

    /// Returns an immutable reference to signatures slice
    #[inline(always)]
    fn get_signatures(&self) -> &[u32] {
        unsafe {
            let ptr = self.mmap.as_ptr().add(self.sign_offset) as *const u32;
            slice::from_raw_parts(ptr, self.capacity)
        }
    }

    /// Write a new signature into slot `idx`
    #[inline]
    fn set_signature(&mut self, idx: usize, sign: u32) {
        assert!(idx < self.capacity);

        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.sign_offset) as *mut u32;
            *ptr.add(idx) = sign;
        }
    }

    fn get_inserted(&self) -> usize {
        self.metadata()
            .inserts
            .load(std::sync::atomic::Ordering::SeqCst) as usize
    }

    /// Read a single [PairOffset] by index, directly from the mmap
    #[inline(always)]
    fn get_pair_offset(&self, idx: usize) -> PairOffset {
        debug_assert!(idx < self.capacity);
        unsafe {
            let ptr = self.mmap.as_ptr().add(self.po_offset) as *const PairOffset;
            *ptr.add(idx)
        }
    }

    /// Write a new item into [PairOffset] slice
    #[inline]
    fn set_pair_offset(&mut self, idx: usize, po: PairOffset) {
        assert!(idx < self.capacity);

        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(self.po_offset) as *mut PairOffset;
            *ptr.add(idx) = po;
        }
    }

    /// Reads the exact number of bytes at a given offset (`pread`)
    #[cfg(unix)]
    fn read_exact_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }

    /// Writes a buffer to a file at a given offset (`pwrite`)
    #[cfg(unix)]
    fn write_all_at(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
    }

    /// Reads the exact number of bytes at a given offset (`pread`)
    #[cfg(windows)]
    fn read_exact_at(f: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
        while !buf.is_empty() {
            match std::os::windows::fs::FileExt::seek_read(f, buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
        } else {
            Ok(())
        }
    }

    /// Writes a buffer to a file at a given offset (`pwrite`)
    #[cfg(windows)]
    fn write_all_at(f: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
        while !buf.is_empty() {
            match std::os::windows::fs::FileExt::seek_write(f, buf, offset) {
                Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Calculate the size of header based on the capacity of the Buffer
    ///
    /// > Size is calculated as below,
    ///
    /// SIZE = sizeof(META) + sizeof(Stats) + (sizeof(PairOffset) * N) + (sizeof(u32) * N)
    ///
    /// Where,
    ///   N = Capacity of Buffer
    const fn get_header_size(capacity: usize) -> usize {
        let mut n = size_of::<Meta>();

        n += size_of::<u32>() * capacity;
        n += size_of::<PairOffset>() * capacity;

        n
    }
}

#[derive(Debug)]
pub struct Bucket {
    file: BucketFile,
    capacity: usize,
}

impl Bucket {
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> TurboResult<Self> {
        let file = BucketFile::open(path, capacity)?;

        Ok(Self { file, capacity })
    }

    pub fn set(&mut self, pair: KVPair, sign: u32) -> TurboResult<()> {
        let meta = self.file.metadata_mut();
        let signs = self.file.get_signatures();

        let start_idx = sign as usize % self.capacity;

        let (idx, is_new) = self
            .file
            .lookup_insert_slot(start_idx, signs, sign, &pair.0)?;

        if is_new {
            meta.inserts.fetch_add(1, Ordering::SeqCst);
        }

        let po = self.file.write_slot(pair)?;

        self.file.set_signature(idx, sign);
        self.file.set_pair_offset(idx, po);

        return Ok(());
    }

    pub fn get(&self, kbuf: Vec<u8>, sign: u32) -> TurboResult<Option<Vec<u8>>> {
        let signs = self.file.get_signatures();
        let mut idx = sign as usize % self.capacity;

        for _ in 0..self.capacity {
            match signs[idx] {
                EMPTY_SIGN => return Ok(None),
                s if s == sign => {
                    let po = self.file.get_pair_offset(idx);
                    let (k, v) = self.file.read_slot(&po)?;
                    if kbuf == k {
                        return Ok(Some(v));
                    }
                }
                _ => {}
            }
            idx = (idx + 1) % self.capacity;
        }

        Ok(None)
    }

    pub fn del(&mut self, kbuf: Vec<u8>, sign: u32) -> TurboResult<Option<Vec<u8>>> {
        let meta = self.file.metadata_mut();
        let signs = self.file.get_signatures();
        let mut idx = sign as usize % self.capacity;

        for _ in 0..self.capacity {
            match signs[idx] {
                EMPTY_SIGN => return Ok(None),
                s if s == sign => {
                    let po = self.file.get_pair_offset(idx);
                    let (k, v) = self.file.read_slot(&po)?;

                    // found the key
                    if kbuf == k {
                        // update meta and header
                        meta.inserts.fetch_sub(1, Ordering::SeqCst);
                        self.file.set_signature(idx, TOMBSTONE_SIGN);

                        return Ok(Some(v));
                    }
                }
                _ => {}
            }
            idx = (idx + 1) % self.capacity;
        }

        Ok(None)
    }

    pub fn iter(&self, start: &mut usize) -> TurboResult<Option<KVPair>> {
        let signs = self.file.get_signatures();

        while *start < self.capacity {
            let idx = *start;
            *start += 1;

            if signs[idx] != EMPTY_SIGN && signs[idx] != TOMBSTONE_SIGN {
                let p_offset = self.file.get_pair_offset(idx);
                let pair = self.file.read_slot(&p_offset)?;

                return Ok(Some(pair));
            }
        }

        Ok(None)
    }

    pub fn get_inserts(&self) -> usize {
        self.file.get_inserted()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};

    use super::*;
    use crate::hash::TurboHasher;
    use tempfile::tempdir;

    const CAP: usize = 8;

    fn create_bucket() -> Bucket {
        let dir = tempdir().expect("temp directory");
        let path = dir.path().join("bucket.temp");

        Bucket::new(&path, CAP).expect("New bucket instance")
    }

    #[test]
    fn test_set_and_get() {
        let mut bucket = create_bucket();

        let key = b"foo".to_vec();
        let val = b"bar".to_vec();
        let sig = TurboHasher::new(&key).0;

        // empty => None
        assert_eq!(bucket.get(key.clone(), sig).unwrap(), None);

        // insert and get
        bucket.set((key.clone(), val.clone()), sig).unwrap();
        assert_eq!(bucket.get(key.clone(), sig).unwrap(), Some(val.clone()));
    }

    #[test]
    fn test_delete_and_tombstone() {
        let mut bucket = create_bucket();

        let key = b"alpha".to_vec();
        let val = b"beta".to_vec();
        let sig = TurboHasher::new(&key).0;

        bucket.set((key.clone(), val.clone()), sig).unwrap();
        let got = bucket.del(key.clone(), sig).unwrap();

        // check returned value and try fetching again
        assert_eq!(got, Some(val.clone()));
        assert_eq!(bucket.get(key.clone(), sig).unwrap(), None);

        // re-insert same key again
        let newval = b"gamma".to_vec();
        bucket.set((key.clone(), newval.clone()), sig).unwrap();

        assert_eq!(bucket.get(key.clone(), sig).unwrap(), Some(newval));
    }

    #[test]
    fn test_iter_only_live_entries() {
        let mut bucket = create_bucket();
        let mut inserted = Vec::new();

        // insert 3 keys
        for &s in &["one", "two", "three"] {
            let key = s.as_bytes().to_vec();
            let val = (s.to_uppercase()).as_bytes().to_vec();
            let sig = TurboHasher::new(&key).0;

            bucket.set((key.clone(), val.clone()), sig).unwrap();
            inserted.push((key, val));
        }

        // delete "two"
        let key_to_delete = b"two".to_vec();
        let sig2 = TurboHasher::new(&key_to_delete).0;

        let _ = bucket.del(key_to_delete.clone(), sig2).unwrap();

        // collect via iter
        let mut out = Vec::new();
        let mut idx = 0;

        while let Some((k, v)) = bucket.iter(&mut idx).unwrap() {
            out.push((k, v));
        }

        // should contain only "one" and "three"
        let mut expected = inserted;
        expected.retain(|(k, _)| k != &key_to_delete);

        assert_eq!(out.len(), expected.len());

        for pair in expected {
            assert!(out.contains(&pair));
        }
    }

    #[test]
    fn test_collision_and_wraparound() {
        let mut bucket = create_bucket();

        let key1 = b"A".to_vec();
        let key2 = b"B".to_vec();

        // Force keys to same start index
        let base_sig = TurboHasher::new(&key1).0;

        bucket
            .set((key1.clone(), b"v1".to_vec()), base_sig)
            .unwrap();
        bucket
            .set((key2.clone(), b"v2".to_vec()), base_sig)
            .unwrap();

        // both should be retrievable
        assert_eq!(
            bucket.get(key1.clone(), base_sig).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            bucket.get(key2.clone(), base_sig).unwrap(),
            Some(b"v2".to_vec())
        );
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = tempdir().expect("temp directory");
        let path = dir.path().join("bucket.temp");

        let key = b"k".to_vec();
        let val = b"v".to_vec();
        let sig = TurboHasher::new(&key).0;

        // first session
        {
            let mut bucket = Bucket::new(&path, CAP).unwrap();
            bucket.set((key.clone(), val.clone()), sig).unwrap();
        }

        // second session
        {
            let bucket2 = Bucket::new(&path, CAP).unwrap();
            let got = bucket2.get(key.clone(), sig).unwrap();

            assert_eq!(got, Some(b"v".to_vec()));
        }
    }

    #[test]
    fn test_invalid_magic_or_version() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("bucket.dat");

        // create a bucket normally
        {
            let _ = Bucket::new(&path, CAP).unwrap();
        }

        // Corrupt the header: change magic
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff, 0xff, 0xff, 0xff]).unwrap();
        file.flush().unwrap();

        // reopening should now throw an error
        match Bucket::new(&path, CAP) {
            Err(TurboError::InvalidFile) => {}
            other => panic!("expected InvalidFile, got {:?}", other),
        }
    }

    #[test]
    fn test_bucket_full_error() {
        let mut bucket = create_bucket();

        // insert CAP distinct keys
        for i in 0..CAP {
            let k = vec![i as u8];
            let v = vec![i as u8 + 100];
            let sig = TurboHasher::new(&k).0;
            bucket.set((k, v), sig).unwrap();
        }

        // now it’s “full” — next insert should error
        let k = b"overflow".to_vec();
        let sig = TurboHasher::new(&k).0;

        match bucket.set((k, b"!".to_vec()), sig) {
            Err(TurboError::BucketFull) => {}
            other => panic!("expected BucketFull, got {:?}", other),
        }
    }

    #[test]
    fn test_upsert_does_not_increment_and_overwrites() {
        let mut bucket = create_bucket();

        let key = b"dup".to_vec();
        let sig = TurboHasher::new(&key).0;

        bucket.set((key.clone(), b"one".to_vec()), sig).unwrap();
        bucket.set((key.clone(), b"two".to_vec()), sig).unwrap();

        assert_eq!(bucket.get(key.clone(), sig).unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn test_zero_length_keys_and_values() {
        let mut bucket = create_bucket();

        // empty key, non-empty value
        let k1 = vec![];
        let sig1 = TurboHasher::new(&k1).0;

        bucket.set((k1.clone(), b"V".to_vec()), sig1).unwrap();
        assert_eq!(bucket.get(k1.clone(), sig1).unwrap(), Some(b"V".to_vec()));

        // non-empty key, empty value
        let k2 = b"K".to_vec();
        let sig2 = TurboHasher::new(&k2).0;

        bucket.set((k2.clone(), vec![]), sig2).unwrap();
        assert_eq!(bucket.get(k2.clone(), sig2).unwrap(), Some(vec![]));

        // both empty
        let k3 = vec![];
        let sig3 = TurboHasher::new(&k3).0;

        bucket.set((k3.clone(), vec![]), sig3).unwrap();
        assert_eq!(bucket.get(k3.clone(), sig3).unwrap(), Some(vec![]));
    }

    #[test]
    fn test_del_on_nonexistent_returns_none() {
        let mut bucket = create_bucket();

        let key = b"ghost".to_vec();
        let sig = TurboHasher::new(&key).0;

        assert_eq!(bucket.del(key.clone(), sig).unwrap(), None);
        assert_eq!(bucket.get(key, sig).unwrap(), None);
    }

    #[test]
    fn test_file_offset_continues_after_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("offset.temp");

        // first session: write 3 bytes
        {
            let mut bucket = Bucket::new(&path, CAP).unwrap();

            let key1 = b"A".to_vec(); // len = 1
            let val1 = b"111".to_vec(); // len = 3
            let sig1 = TurboHasher::new(&key1).0;

            bucket.set((key1, val1), sig1).unwrap();

            // NOTE: Bucket is dropped! So mmap will be flushed on drop
        }

        // second session: reopen, write 4 more bytes
        {
            let mut bucket = Bucket::new(&path, CAP).unwrap();

            let key2 = b"B".to_vec();
            let val2 = b"2222".to_vec(); // length = 4
            let sig2 = TurboHasher::new(&key2).0;

            // 1) perform the insert
            bucket.set((key2.clone(), val2.clone()), sig2).unwrap();

            // 2) figure out which slot we used (simple linear‑probe from hash idx)
            let mut idx = sig2 as usize % CAP;
            let signs = bucket.file.get_signatures();

            // advance until we find our signature
            while signs[idx] != sig2 {
                idx = (idx + 1) % CAP;
            }

            // 3) now read *after* the set
            let po2 = bucket.file.get_pair_offset(idx);

            assert_eq!(
                po2.offset as usize, 4,
                "expected second write to start at offset=4, got {}",
                po2.offset
            );

            let got = bucket.get(key2, sig2).unwrap().unwrap();
            assert_eq!(got, val2);
        }
    }

    #[test]
    fn test_random_inserts_and_gets() {
        const CAP: usize = 50;
        const NUM: usize = 40; // 80% of CAP

        let dir = tempdir().unwrap();
        let path = dir.path().join("random.temp");
        let mut bucket = Bucket::new(&path, CAP).unwrap();

        // very simple LCG for reproducible “random” bytes
        let mut seed: u32 = 0x1234_5678;

        fn next(seed: &mut u32) -> u32 {
            *seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            *seed
        }

        // generate, insert, and remember all pairs
        let mut entries = Vec::with_capacity(NUM);

        for _ in 0..NUM {
            let knum = next(&mut seed);
            let vnum = next(&mut seed);

            let key = knum.to_be_bytes().to_vec();
            let val = vnum.to_be_bytes().to_vec();
            let sig = TurboHasher::new(&key).0;

            bucket.set((key.clone(), val.clone()), sig).unwrap();
            entries.push((key, val, sig));
        }

        for (key, val, sig) in &entries {
            let got = bucket.get(key.clone(), *sig).unwrap();

            assert_eq!(
                got.as_ref(),
                Some(val),
                "for key={:02X?} expected value={:02X?}, got={:?}",
                key,
                val,
                got
            );
        }

        // a key we never inserted should be absent
        let absent_key = next(&mut seed).to_be_bytes().to_vec();
        let absent_sig = TurboHasher::new(&absent_key).0;

        assert_eq!(
            bucket.get(absent_key, absent_sig).unwrap(),
            None,
            "unexpectedly found a value for a non‐existent key"
        );
    }
}
