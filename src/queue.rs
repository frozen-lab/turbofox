use crate::core::{InternalResult, KVPair};
use byteorder::{ByteOrder, LittleEndian};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    path::Path,
};

pub(crate) struct Queue {
    file: File,
    mmap: MmapMut,
    head: u64,
    tail: u64,
    pending: usize,
}

impl Queue {
    const FLUSH_THRESHOLD: usize = 64 * 64;

    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> InternalResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let meta = file.metadata()?;
        let cap = capacity as u64 + 16;

        if meta.len() < cap {
            file.set_len(cap)?;
        }

        let mmap = unsafe { MmapOptions::new().len(cap as usize).map_mut(&file)? };

        let head = LittleEndian::read_u64(&mmap[0..8]);
        let tail = LittleEndian::read_u64(&mmap[8..16]);

        Ok(Self {
            file,
            mmap,
            head,
            tail,
            pending: 0,
        })
    }

    pub fn push(&mut self, pair: &KVPair) -> InternalResult<()> {
        let record_size = 8 + pair.0.len() as u64 + pair.1.len() as u64;
        let required_size = self.tail + record_size;

        // grow file size if adding new items exceeds current length
        if (required_size as usize) > self.mmap.len() {
            let new_cap = (required_size * 2).max(self.mmap.len() as u64);

            self.file.set_len(new_cap)?;
            self.mmap = unsafe {
                MmapOptions::new()
                    .len(new_cap as usize)
                    .map_mut(&self.file)?
            };
        }

        let mut off = self.tail as usize;

        LittleEndian::write_u32(&mut self.mmap[off..off + 4], pair.0.len() as u32);
        LittleEndian::write_u32(&mut self.mmap[off + 4..off + 8], pair.1.len() as u32);

        off += 8;

        self.mmap[off..off + pair.0.len()].copy_from_slice(&pair.0);
        self.mmap[off + pair.0.len()..off + pair.0.len() + pair.1.len()].copy_from_slice(&pair.1);

        self.tail = required_size;
        LittleEndian::write_u64(&mut self.mmap[8..16], self.tail);

        self.pending += record_size as usize;

        if self.pending >= Self::FLUSH_THRESHOLD {
            self.flush_batch()?;
        }

        Ok(())
    }

    pub fn pop(&mut self) -> InternalResult<Option<KVPair>> {
        if self.head == self.tail {
            return Ok(None);
        }

        let mut off = self.head as usize;
        let key_len = LittleEndian::read_u32(&self.mmap[off..off + 4]) as usize;
        let val_len = LittleEndian::read_u32(&self.mmap[off + 4..off + 8]) as usize;
        off += 8;

        let key = self.mmap[off..off + key_len].to_vec();
        let val = self.mmap[off + key_len..off + key_len + val_len].to_vec();

        let rec_size = 8 + key_len + val_len;

        // advance head
        self.head += rec_size as u64;
        LittleEndian::write_u64(&mut self.mmap[0..8], self.head);

        Ok(Some((key, val)))
    }

    pub fn flush_batch(&mut self) -> InternalResult<()> {
        self.mmap.flush()?;
        self.pending = 0;

        Ok(())
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        // error is absorbed if any
        let _ = self.flush_batch();
    }
}
