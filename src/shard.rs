#![allow(dead_code)]

use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{Seek, Write},
    os::unix::fs::FileExt,
    path::Path,
};

use memmap::{MmapMut, MmapOptions};

use crate::{
    hash::{TurboHash, INVALID_HASH},
    NUM_ROWS, ROW_WIDTH,
};

pub(crate) type Result<T> = anyhow::Result<T>;
pub(crate) type Buf = Vec<u8>;
pub(crate) type KVPair = (Buf, Buf);
pub(crate) const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;

#[repr(C)]
struct ShardRow {
    signs: [u32; ROW_WIDTH],
    offsets: [u64; ROW_WIDTH],
}

#[repr(C)]
struct ShardHeader {
    rows: [ShardRow; NUM_ROWS],
}

pub struct Shard {
    start: u32,
    end: u32,
    file: RefCell<File>,
    mmap: MmapMut,
}

impl Shard {
    pub fn open(dirpath: impl AsRef<Path>, start: u32, end: u32) -> Result<Self> {
        let filepath = dirpath.as_ref().join(format!("{start}-{end}"));
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(filepath)?;

        // create headerspace iif shard is new
        if file.metadata()?.len() < HEADER_SIZE {
            file.set_len(HEADER_SIZE)?;
        }

        file.seek(std::io::SeekFrom::End(0))?;
        let mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;

        Ok(Self {
            start,
            end,
            mmap,
            file: RefCell::new(file),
        })
    }

    pub fn get(&self, hash: TurboHash, kbuf: &[u8]) -> Result<Option<Buf>> {
        let row = self.header_row(hash.row());

        for (i, sign) in row.signs.iter().enumerate() {
            if hash.sign() == *sign {
                let desc = row.offsets[i];

                let (k, v) = self.read(desc)?;

                if k == kbuf {
                    return Ok(Some(v));
                }
            }
        }

        Ok(None)
    }

    pub fn set(&self, hash: TurboHash, kbuf: &[u8], vbuf: &[u8]) -> Result<bool> {
        let row = self.header_row_mut(hash.row());

        for (i, sign) in row.signs.iter_mut().enumerate() {
            if hash.sign() == *sign {
                let desc = row.offsets[i];

                let (k, _) = self.read(desc)?;

                if k == kbuf {
                    row.offsets[i] = self.write(kbuf, vbuf)?;

                    return Ok(true);
                }
            } else if *sign == INVALID_HASH {
                *sign = hash.sign();
                row.offsets[i] = self.write(kbuf, vbuf)?;

                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn remove(&self, hash: TurboHash, kbuf: &[u8]) -> Result<Option<Buf>> {
        let row = self.header_row_mut(hash.row());

        for (i, sign) in row.signs.iter_mut().enumerate() {
            if hash.sign() == *sign {
                let desc = row.offsets[i];

                let (k, v) = self.read(desc)?;

                if k == kbuf {
                    *sign = INVALID_HASH;

                    return Ok(Some(v));
                }
            }
        }

        Ok(None)
    }

    fn read(&self, desc: u64) -> Result<KVPair> {
        let klen = (desc >> 48) as usize;
        let vlen = ((desc >> 32) & 0xffff) as usize;
        let offset: u64 = desc & 0xffff_ffff;

        let mut buf = vec![0u8; klen + vlen];

        let file = self.file.borrow();

        file.read_exact_at(&mut buf, offset)?;

        let val = buf[klen..].to_owned();
        buf.truncate(klen);

        Ok((buf, val))
    }

    fn write(&self, kbuf: &[u8], vbuf: &[u8]) -> Result<u64> {
        let mut file = self.file.borrow_mut();

        let offset = file.stream_position()?;
        let entry_size = kbuf.len() + vbuf.len();
        let mut buf = vec![0u8; entry_size];

        buf[..kbuf.len()].copy_from_slice(kbuf);
        buf[kbuf.len()..].copy_from_slice(vbuf);

        file.write_all(&buf)?;

        Ok(((kbuf.len() as u64) << 48) | ((vbuf.len() as u64) << 32) | offset)
    }

    fn header_row(&self, r: usize) -> &ShardRow {
        &unsafe { &*(self.mmap.as_ptr() as *const ShardHeader) }.rows[r]
    }

    fn header_row_mut(&self, r: usize) -> &mut ShardRow {
        &mut unsafe { &mut *(self.mmap.as_ptr() as *mut ShardHeader) }.rows[r]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_shard() -> Shard {
        let dir = tempdir().unwrap();
        Shard::open(dir.path(), 0, 1).unwrap()
    }

    #[test]
    fn test_operations() {
        let dir = tempdir().unwrap();
        let shard = Shard::open(dir.path(), 0, 1).unwrap();

        let key = b"key1";
        let hash = TurboHash::new(key);

        let val1 = b"value1";
        let val2 = b"value2";

        assert!(
            shard.get(hash, key).unwrap().is_none(),
            "`get()` should return `None` for non existant key",
        );

        assert!(
            shard.set(hash, key, val1).unwrap(),
            "`set()` operation should work correctly",
        );
        assert_eq!(
            shard.get(hash, key).unwrap(),
            Some(val1.to_vec()),
            "`get()` should return correct value for the key",
        );

        assert!(
            shard.set(hash, key, val2).unwrap(),
            "`set()` should correctly update value for pre-existing key",
        );
        assert_eq!(
            shard.get(hash, key).unwrap(),
            Some(val2.to_vec()),
            "`get()` should work correctly after updateing pre-existing value",
        );

        assert_eq!(
            shard.remove(hash, key).unwrap(),
            Some(val2.to_vec()),
            "`remove()` operation should work correctly",
        );
        assert!(
            shard.get(hash, key).unwrap().is_none(),
            "`get()` should work correctly for a deleted kv pair",
        );
    }

    #[test]
    fn remove_and_reinsert() {
        let shard = open_shard();
        let key = b"temp";
        let hash = TurboHash::new(key);

        assert!(shard.set(hash, key, b"v1").unwrap(), "`set()` should work correctly");
        assert_eq!(
            shard.remove(hash, key).unwrap(),
            Some(b"v1".to_vec()),
            "`remove()` should work correctly",
        );
        assert!(
            shard.set(hash, key, b"v2").unwrap(),
            "`set()` should work correctly for previously deleted key",
        );
        assert_eq!(
            shard.get(hash, key).unwrap(),
            Some(b"v2".to_vec()),
            "`get()` should work correctly for re-inserted key after previous deletion",
        );
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = tempdir().unwrap();
        {
            let shard = Shard::open(dir.path(), 0, 1).unwrap();
            let key = b"persistent";
            let val = b"data";

            assert!(
                shard.set(TurboHash::new(key), key, val).unwrap(),
                "`set()` should work correctly",
            );
        }

        let shard = Shard::open(dir.path(), 0, 1).unwrap();

        assert_eq!(
            shard.get(TurboHash::new(b"persistent"), b"persistent").unwrap(),
            Some(b"data".to_vec()),
            "`get()` should work correctly on re-opened shard",
        );
    }

    #[test]
    fn large_key_and_value() {
        let shard = open_shard();

        let key = vec![b'a'; 1024 * 10]; // 10KB key
        let val = vec![b'b'; 1024 * 50]; // 50KB value
        let hash = TurboHash::new(&key);

        assert!(
            shard.set(hash, &key, &val).unwrap(),
            "`set()` should work correctly for large KV pairs",
        );
        assert_eq!(
            shard.get(hash, &key).unwrap().unwrap(),
            val,
            "`get()` should work correctly for large KV pairs",
        );
    }

    #[test]
    fn header_initialization() {
        let dir = tempdir().unwrap();
        let filepath = dir.path().join("0-1");

        assert!(!filepath.exists(), "filepath should not exists before shard creation");

        let _shard = Shard::open(dir.path(), 0, 1).unwrap();
        let meta = std::fs::metadata(&filepath).unwrap();

        assert_eq!(meta.len(), HEADER_SIZE, "header should be properly initialised");
    }
}
