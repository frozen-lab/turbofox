use crate::hasher::{TurboHasher, INVALID_FP};
use memmap::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    io,
    mem::size_of,
    ops::Range,
    path::PathBuf,
    sync::atomic::{AtomicU16, AtomicU32, Ordering},
};

/// Current version of TurboCache shards
pub(crate) const VERSION: u8 = 0;

/// Versioned MAGIC value to help identify shards specific to TurboCache
pub(crate) const MAGIC: [u8; 8] = *b"TURBOv0\0";

pub(crate) const ROWS_NUM: usize = 1024;
pub(crate) const ROWS_WIDTH: usize = 32;
pub(crate) const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    RowFull(usize),
    ShardOutOfRange(u32),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "I/O error: {}", err),
            Error::RowFull(row) => write!(f, "row {} is full", row),
            Error::ShardOutOfRange(shard) => write!(f, "out of range of {}", shard),
        }
    }
}

impl std::error::Error for Error {}

pub type TResult<T> = Result<T, Error>;

#[repr(C, align(4096))]
struct PageAligned<T>(T);

#[derive(Clone, Copy)]
#[repr(C)]
struct ShardSlot {
    offset: u32,
    klen: u16,
    vlen: u16,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct IndexRow {
    fp: [u32; ROWS_WIDTH],
    slots: [ShardSlot; ROWS_WIDTH],
}

impl Default for IndexRow {
    fn default() -> Self {
        Self {
            fp: [INVALID_FP; ROWS_WIDTH],
            slots: [ShardSlot {
                offset: 0,
                klen: 0,
                vlen: 0,
            }; ROWS_WIDTH],
        }
    }
}

#[repr(C)]
struct ShardMeta {
    magic: [u8; 8],
    version: u8,
}

#[repr(C)]
struct ShardStats {
    n_occupied: AtomicU16,
    n_deleted: AtomicU16,
    write_offset: AtomicU32,
}

#[repr(C)]
struct ShardHeader {
    meta: ShardMeta,
    stats: ShardStats,
    index: PageAligned<[IndexRow; ROWS_NUM]>,
}

impl Default for ShardHeader {
    fn default() -> Self {
        Self {
            meta: ShardMeta {
                magic: MAGIC,
                version: VERSION,
            },
            stats: ShardStats {
                n_occupied: AtomicU16::new(0),
                n_deleted: AtomicU16::new(0),
                write_offset: AtomicU32::new(0),
            },
            index: PageAligned([IndexRow::default(); ROWS_NUM]),
        }
    }
}

struct ShardFile {
    file: File,
    mmap: MmapMut,
}

impl ShardFile {
    fn open(path: &PathBuf, truncate: bool) -> TResult<Self> {
        let file = {
            if truncate {
                Self::new(path)?
            } else {
                Self::file(path, truncate)?
            }
        };

        let mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;

        Ok(Self { file, mmap })
    }

    fn new(path: &PathBuf) -> TResult<File> {
        let file = Self::file(path, true)?;
        file.set_len(HEADER_SIZE)?;

        Ok(file)
    }

    fn file(path: &PathBuf, truncate: bool) -> TResult<File> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(truncate)
            .open(path)?;

        Ok(file)
    }

    #[inline(always)]
    fn header(&self) -> &ShardHeader {
        unsafe { &*(self.mmap.as_ptr() as *const ShardHeader) }
    }

    #[inline(always)]
    fn header_mut(&self) -> &mut ShardHeader {
        unsafe { &mut *(self.mmap.as_ptr() as *mut ShardHeader) }
    }

    #[inline(always)]
    fn row(&self, row_idx: usize) -> &IndexRow {
        &self.header().index.0[row_idx]
    }

    #[inline(always)]
    fn row_mut(&self, row_idx: usize) -> &mut IndexRow {
        &mut self.header_mut().index.0[row_idx]
    }

    fn write_slot(&self, kbuf: &[u8], vbuf: &[u8]) -> TResult<ShardSlot> {
        let vlen = vbuf.len();
        let klen = kbuf.len();
        let blen = klen + vlen;

        let mut buf = vec![0u8; blen];
        buf[..klen].copy_from_slice(kbuf);
        buf[klen..].copy_from_slice(vbuf);

        let write_offset: u32 = self
            .header()
            .stats
            .write_offset
            .fetch_add(blen as u32, Ordering::SeqCst) as u32;

        Self::write_all_at(&self.file, &buf, write_offset as u64 + HEADER_SIZE)?;

        Ok(ShardSlot {
            offset: write_offset,
            vlen: vlen as u16,
            klen: klen as u16,
        })
    }

    fn read_kbuf(&self, slot: &ShardSlot) -> TResult<Vec<u8>> {
        let klen = slot.klen as usize;
        let mut buf = vec![0u8; klen];

        Self::read_exact_at(&self.file, &mut buf, slot.offset as u64 + HEADER_SIZE)?;

        Ok(buf)
    }

    fn read_vbuf(&self, slot: &ShardSlot) -> TResult<Vec<u8>> {
        let klen = slot.klen as usize;
        let vlen = slot.vlen as usize;

        let mut buf = vec![0u8; vlen];

        Self::read_exact_at(
            &self.file,
            &mut buf,
            slot.offset as u64 + HEADER_SIZE + klen as u64,
        )?;

        Ok(buf)
    }

    #[cfg(unix)]
    fn read_exact_at(f: &File, buf: &mut [u8], offset: u64) -> TResult<()> {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)?;
        Ok(())
    }

    #[cfg(unix)]
    fn write_all_at(f: &File, buf: &[u8], offset: u64) -> TResult<()> {
        std::os::unix::fs::FileExt::write_all_at(f, buf, offset)?;
        Ok(())
    }
}

pub(crate) struct Shard {
    pub(crate) span: Range<u32>,
    file: ShardFile,
}

impl Shard {
    pub fn open(dirpath: &PathBuf, span: Range<u32>, truncate: bool) -> TResult<Self> {
        let filepath = dirpath.join(format!("shard_{:04x}-{:04x}", span.start, span.end));

        let file = ShardFile::open(&filepath, truncate)?;

        Ok(Self { span, file })
    }

    pub fn set(&self, buf: (&[u8], &[u8]), hash: TurboHasher) -> TResult<()> {
        let (kbuf, vbuf) = buf;
        let row_idx = hash.row_selector() as usize;
        let fp = hash.fingerprint();

        let row = self.file.row_mut(row_idx);

        // incase the fingerprint already exists
        for idx in 0..ROWS_WIDTH {
            if row.fp[idx] == fp {
                let slot = row.slots[idx];
                let key = self.file.read_kbuf(&slot)?;

                if key == kbuf {
                    let new_slot = self.file.write_slot(kbuf, vbuf)?;
                    row.slots[idx] = new_slot;

                    return Ok(());
                }
            }
        }

        // otherwise find the first free slot (fp == 0) to insert
        for idx in 0..ROWS_WIDTH {
            if row.fp[idx] == INVALID_FP {
                let slot = self.file.write_slot(kbuf, vbuf)?;

                row.fp[idx] = fp;
                row.slots[idx] = slot;

                let header = self.file.header_mut();
                header.stats.n_occupied.fetch_add(1, Ordering::SeqCst);

                return Ok(());
            }
        }

        // if we ran out of room in this row
        Err(Error::RowFull(row_idx))
    }

    pub fn get(&self, kbuf: &[u8], hash: TurboHasher) -> TResult<Option<Vec<u8>>> {
        let row_idx = hash.row_selector() as usize;
        let fp = hash.fingerprint();
        let row = self.file.row(row_idx);

        for idx in 0..ROWS_WIDTH {
            if row.fp[idx] == fp {
                let slot = row.slots[idx];
                let existing_key = self.file.read_kbuf(&slot)?;

                if existing_key == kbuf {
                    let vbuf = self.file.read_vbuf(&slot)?;

                    return Ok(Some(vbuf));
                }
            }
        }

        Ok(None)
    }

    pub fn remove(&self, kbuf: &[u8], hash: TurboHasher) -> TResult<bool> {
        let row_idx = hash.row_selector() as usize;
        let fp = hash.fingerprint();
        let row = self.file.row_mut(row_idx);

        for idx in 0..ROWS_WIDTH {
            if row.fp[idx] == fp {
                let slot = row.slots[idx];
                let existing_key = self.file.read_kbuf(&slot)?;

                if existing_key == kbuf {
                    row.fp[idx] = INVALID_FP;
                    row.slots[idx] = ShardSlot {
                        offset: 0,
                        klen: 0,
                        vlen: 0,
                    };

                    let hdr = self.file.header_mut();

                    hdr.stats.n_deleted.fetch_add(1, Ordering::SeqCst);
                    hdr.stats.n_occupied.fetch_sub(1, Ordering::SeqCst);

                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn new_shard(span: Range<u32>) -> TResult<(Shard, TempDir)> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;
        let s = Shard::open(&dir, span, true)?;

        Ok((s, tmp))
    }

    #[test]
    fn set_get_remove_roundtrip() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;
        let key = b"hello";
        let val: Vec<u8> = b"world".to_vec();
        let h = TurboHasher::new(key);

        // initially not present
        assert_eq!(shard.get(key, h)?, None);

        // set then get
        shard.set((key, &val), h)?;

        assert_eq!(shard.get(key, h)?, Some(val));

        // remove => true, then gone
        assert!(shard.remove(key, h)?);
        assert_eq!(shard.get(key, h)?, None);

        // removing again returns false
        assert!(!shard.remove(key, h)?);
        Ok(())
    }

    #[test]
    fn overwrite_existing_value() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;
        let key = b"dup";
        let v1 = b"first".to_vec();
        let v2 = b"second".to_vec();
        let h = TurboHasher::new(key);

        shard.set((key, &v1), h)?;

        assert_eq!(shard.get(key, h)?, Some(v1));

        // overwrite
        shard.set((key, &v2), h)?;

        assert_eq!(shard.get(key, h)?, Some(v2));
        Ok(())
    }

    #[test]
    fn stats_n_occupied_and_n_deleted() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;
        let header = shard.file.header();
        let load = |f: &AtomicU16| f.load(Ordering::SeqCst);

        assert_eq!(load(&header.stats.n_occupied), 0);
        assert_eq!(load(&header.stats.n_deleted), 0);

        // insert two distinct keys
        let k1 = b"k1";
        let k2 = b"k2";
        let h1 = TurboHasher::new(k1);
        let h2 = TurboHasher::new(k2);

        shard.set((k1, b"v1"), h1)?;
        shard.set((k2, b"v2"), h2)?;

        assert_eq!(load(&header.stats.n_occupied), 2);
        assert_eq!(load(&header.stats.n_deleted), 0);

        // remove one
        assert!(shard.remove(k1, h1)?);
        assert_eq!(load(&header.stats.n_occupied), 1);
        assert_eq!(load(&header.stats.n_deleted), 1);

        // remove non‐existent does nothing
        assert!(!shard.remove(b"nope", TurboHasher::new(b"nope"))?);
        assert_eq!(load(&header.stats.n_occupied), 1);
        assert_eq!(load(&header.stats.n_deleted), 1);
        Ok(())
    }

    #[test]
    fn set_returns_row_full_error() -> TResult<()> {
        let (shard, _tmp) = new_shard(0..0x1000)?;

        // Simulate a row being full
        let row_idx = 0;
        let row = shard.file.row_mut(row_idx);
        for i in 0..ROWS_WIDTH {
            row.fp[i] = i as u32 + 1; // Fill with non-zero values
        }

        // Attempt to insert into the full row
        let key = b"another_key";
        let val = b"another_value";

        // Manually create a hash that will map to the full row
        let mut hash_input = Vec::new();
        let h = loop {
            hash_input.push(0);
            let h = TurboHasher::new(&hash_input);
            if h.row_selector() == row_idx {
                break h;
            }
        };

        let result = shard.set((key, val), h);

        assert!(matches!(result, Err(Error::RowFull(idx)) if idx == row_idx));
        Ok(())
    }
}
