use core::hash;
use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{Seek, Write},
    os::unix::fs::FileExt,
    path::Path,
};

use memmap::{MmapMut, MmapOptions};

use crate::{hash::SimHash, Res, ROWS, WIDTH};

type Buf = Vec<u8>;
type KV = (Buf, Buf);

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct ShardDescriptor {
    offset: u32,
    klen: u16,
    vlen: u16,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct ShardRow {
    signs: [u32; WIDTH],
    descs: [ShardDescriptor; WIDTH],
}

#[repr(C)]
struct ShardHeader {
    rows: [ShardRow; ROWS],
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Shard {
    pub start: u32,
    pub end: u32,
    file: RefCell<File>,
    mmap: MmapMut,
}

#[allow(dead_code)]
impl Shard {
    const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;

    pub fn open(dirpath: impl AsRef<Path>, start: u32, end: u32) -> Res<Self> {
        let filepath = dirpath.as_ref().join(format!("{start}-{end}"));
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(filepath)?;

        file.set_len(Self::HEADER_SIZE)?;
        file.seek(std::io::SeekFrom::End(0))?;

        let mmap = unsafe { MmapOptions::new().len(Self::HEADER_SIZE as usize).map_mut(&file) }?;

        Ok(Self {
            start,
            end,
            mmap,
            file: RefCell::new(file),
        })
    }

    pub fn get(&self, hash: SimHash, kbuf: &[u8]) -> Res<Option<Buf>> {
        let row = self.header_row(hash.row(ROWS));

        for (i, sign) in row.signs.iter().enumerate() {
            if hash.sign() == *sign {
                let desc = row.descs[i];
                let (k, v) = self.read(desc)?;

                if k == kbuf {
                    return Ok(Some(v));
                }
            }
        }

        Ok(None)
    }

    pub fn set(&self, hash: SimHash, kbuf: &[u8], vbuf: &[u8]) -> Res<bool> {
        let row = self.header_row_mut(ROWS);

        for (i, sign) in row.signs.iter().enumerate() {
            if hash.sign() == *sign {
                let desc = row.descs[i];
                let (k, _) = self.read(desc)?;

                if k == kbuf {
                    row.descs[i] = self.write(kbuf, vbuf)?;

                    return Ok(true);
                }
            }
        }

        for (i, sign) in row.signs.iter_mut().enumerate() {
            if *sign == SimHash::INVALID_SIGN {
                *sign = hash.sign();
                row.descs[i] = self.write(kbuf, vbuf)?;

                return Ok(true);
            }
        }

        // QUESTION: Can this be reached somehow, if so then how the error
        // should be handled?
        unreachable!()
    }

    pub fn remove(&self, hash: SimHash, kbuf: &[u8]) -> Res<bool> {
        let row = self.header_row_mut(hash.row(ROWS));

        for (i, sign) in row.signs.iter_mut().enumerate() {
            if hash.sign() == *sign {
                let desc = row.descs[i];
                let (k, _) = self.read(desc)?;

                if k == kbuf {
                    *sign = SimHash::INVALID_SIGN;

                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = Res<KV>> + 'a {
        (0..ROWS).map(|r| self.header_row(r)).flat_map(|row| {
            row.signs.iter().enumerate().filter_map(|(i, sig)| {
                if *sig == SimHash::INVALID_SIGN {
                    return None;
                }
                Some(self.read(row.descs[i]))
            })
        })
    }

    fn header_row(&self, r: usize) -> &ShardRow {
        &unsafe { &*(self.mmap.as_ptr() as *const ShardHeader) }.rows[r]
    }

    fn header_row_mut(&self, r: usize) -> &mut ShardRow {
        &mut unsafe { &mut *(self.mmap.as_ptr() as *const ShardHeader as *mut ShardHeader) }.rows[r]
    }

    fn read(&self, descriptor: ShardDescriptor) -> Res<KV> {
        let mut kbuf = vec![0u8; descriptor.klen as usize];
        let mut vbuf = vec![0u8; descriptor.vlen as usize];

        let file = self.file.borrow();

        file.read_exact_at(&mut kbuf, descriptor.offset as u64)?;
        file.read_exact_at(&mut vbuf, (descriptor.offset + descriptor.klen as u32) as u64)?;

        Ok((kbuf, vbuf))
    }

    fn write(&self, kbuf: &[u8], vbuf: &[u8]) -> Res<ShardDescriptor> {
        let mut file = self.file.borrow_mut();
        let offset = file.stream_position()?;

        file.write_all(kbuf)?;
        file.write_all(vbuf)?;

        Ok(ShardDescriptor {
            offset: offset as u32,
            klen: kbuf.len() as u16,
            vlen: vbuf.len() as u16,
        })
    }
}
