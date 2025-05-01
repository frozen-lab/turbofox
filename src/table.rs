use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

const INITIAL_SIZE: usize = 69;
const KEY_SIZE: usize = 32;
const VALUE_SIZE: usize = 256;
const SLOT_SIZE: usize = 1 + KEY_SIZE + VALUE_SIZE; // [status, key, value]

#[derive(Error, Debug)]
pub enum HashError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Hash table full")]
    TableFull,

    #[error("Overflow file error: {0}")]
    OverflowError(String),
}

pub trait Hashable {
    fn hash(&self) -> usize;
}

impl Hashable for [u8; KEY_SIZE] {
    fn hash(&self) -> usize {
        let mut hash: usize = 14695981039346656037;
        for byte in self {
            hash ^= *byte as usize;
            hash = hash.wrapping_mul(1099511628211);
        }
        hash
    }
}

#[derive(Debug)]
pub struct Table {
    mmap: MmapMut,
    capacity: usize,
    curr_size: usize,
    path: PathBuf,
    overflow_file: File,
    free_list_head: u64,
}

impl Table {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, HashError> {
        let path_ref = path.as_ref();
        let mut overflow_path = path_ref.to_path_buf();
        overflow_path.set_extension("overflow");

        // Open main table file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path_ref)?;

        let metadata = file.metadata()?;
        let capacity = if metadata.len() == 0 {
            let new_capacity = INITIAL_SIZE;
            file.set_len((new_capacity * SLOT_SIZE) as u64)?;
            new_capacity
        } else {
            metadata.len() as usize / SLOT_SIZE
        };

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let curr_size = Table::count_occupied(&mmap, capacity);

        // Open or create overflow file
        let mut overflow_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&overflow_path)?;

        // Read free list head from overflow file
        let mut free_list_head = 0;
        let overflow_metadata = overflow_file.metadata()?;
        if overflow_metadata.len() >= 8 {
            overflow_file.seek(SeekFrom::Start(0))?;
            let mut buf = [0u8; 8];
            overflow_file.read_exact(&mut buf)?;
            free_list_head = u64::from_le_bytes(buf);
        } else {
            // Initialize free list head to 0
            overflow_file.set_len(8)?;
            overflow_file.seek(SeekFrom::Start(0))?;
            overflow_file.write_all(&0u64.to_le_bytes())?;
        }

        Ok(Self {
            mmap,
            capacity,
            curr_size,
            path: path_ref.to_path_buf(),
            overflow_file,
            free_list_head,
        })
    }

    fn read_slot(&self, index: usize) -> (u8, [u8; KEY_SIZE], [u8; VALUE_SIZE]) {
        let offset = index * SLOT_SIZE;
        let status = self.mmap[offset];

        let mut key = [0u8; KEY_SIZE];
        let mut value = [0u8; VALUE_SIZE];

        key.copy_from_slice(&self.mmap[offset + 1..offset + 1 + KEY_SIZE]);
        value.copy_from_slice(&self.mmap[offset + 1 + KEY_SIZE..offset + SLOT_SIZE]);

        (status, key, value)
    }

    fn write_slot(
        &mut self,
        index: usize,
        status: u8,
        key: &[u8; KEY_SIZE],
        value: &[u8; VALUE_SIZE],
    ) -> Result<(), HashError> {
        let offset = index * SLOT_SIZE;

        self.mmap[offset] = status;
        self.mmap[offset + 1..offset + 1 + KEY_SIZE].copy_from_slice(key);
        self.mmap[offset + 1 + KEY_SIZE..offset + SLOT_SIZE].copy_from_slice(value);

        self.mmap.flush_async_range(offset, SLOT_SIZE)?;

        Ok(())
    }

    fn count_occupied(mmap: &MmapMut, capacity: usize) -> usize {
        (0..capacity)
            .filter(|i| matches!(Table::slot_status(mmap, *i), 2))
            .count()
    }

    fn slot_status(mmap: &MmapMut, index: usize) -> u8 {
        mmap[index * SLOT_SIZE]
    }

    pub fn insert(&mut self, key: &[u8; KEY_SIZE], value: &[u8]) -> Result<(), HashError> {
        if self.curr_size >= (self.capacity as f64 * 0.75) as usize {
            self.resize()?;
        }

        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            let (status, existing_key, existing_value) = self.read_slot(index);

            match status {
                0 | 1 => {
                    let value_data = if value.len() <= 254 {
                        let mut data = [0u8; VALUE_SIZE];
                        data[0] = 0; // inline flag
                        data[1] = value.len() as u8;
                        data[2..2 + value.len()].copy_from_slice(value);
                        data
                    } else {
                        let offset = self.allocate_overflow(value)?;
                        let mut data = [0u8; VALUE_SIZE];
                        data[0] = 1; // overflow flag
                        data[1..9].copy_from_slice(&offset.to_le_bytes());
                        data[9..17].copy_from_slice(&(value.len() as u64).to_le_bytes());
                        data
                    };

                    self.write_slot(index, 2, key, &value_data)?;
                    self.curr_size += 1;
                    return Ok(());
                }
                2 => {
                    if &existing_key == key {
                        let value_data = if value.len() <= 254 {
                            let mut data = [0u8; VALUE_SIZE];
                            data[0] = 0;
                            data[1] = value.len() as u8;
                            data[2..2 + value.len()].copy_from_slice(value);
                            data
                        } else {
                            // Free existing overflow if present
                            if existing_value[0] == 1 {
                                let offset = u64::from_le_bytes(existing_value[1..9].try_into().unwrap());
                                self.free_overflow(offset)?;
                            }
                            let offset = self.allocate_overflow(value)?;
                            let mut data = [0u8; VALUE_SIZE];
                            data[0] = 1;
                            data[1..9].copy_from_slice(&offset.to_le_bytes());
                            data[9..17].copy_from_slice(&(value.len() as u64).to_le_bytes());
                            data
                        };

                        self.write_slot(index, 2, key, &value_data)?;
                        return Ok(());
                    }

                    index = (index + 1) % self.capacity;
                }
                _ => unreachable!(),
            }
        }

        Err(HashError::TableFull)
    }

    pub fn get(&mut self, key: &[u8; KEY_SIZE]) -> Option<Vec<u8>> {
        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            let (status, existing_key, value_data) = self.read_slot(index);

            match status {
                0 => return None,
                2 if &existing_key == key => {
                    if value_data[0] == 0 {
                        // Inline
                        let len = value_data[1] as usize;
                        return Some(value_data[2..2 + len].to_vec());
                    } else {
                        // Overflow
                        let offset = u64::from_le_bytes(value_data[1..9].try_into().unwrap());
                        let length = u64::from_le_bytes(value_data[9..17].try_into().unwrap());
                        let mut data = vec![0u8; length as usize];
                        self.read_overflow(offset, &mut data).ok()?;
                        return Some(data);
                    }
                }
                _ => index = (index + 1) % self.capacity,
            }
        }

        None
    }

    pub fn delete(&mut self, key: &[u8; KEY_SIZE]) -> Result<Option<Vec<u8>>, HashError> {
        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            let (status, existing_key, value_data) = self.read_slot(index);

            match status {
                0 => return Ok(None),
                2 if &existing_key == key => {
                    let value = if value_data[0] == 0 {
                        let len = value_data[1] as usize;
                        value_data[2..2 + len].to_vec()
                    } else {
                        let offset = u64::from_le_bytes(value_data[1..9].try_into().unwrap());
                        let length = u64::from_le_bytes(value_data[9..17].try_into().unwrap());
                        let mut data = vec![0u8; length as usize];
                        self.read_overflow(offset, &mut data)?;
                        data
                    };

                    // Free overflow if present
                    if value_data[0] == 1 {
                        let offset = u64::from_le_bytes(value_data[1..9].try_into().unwrap());
                        self.free_overflow(offset)?;
                    }

                    // Mark slot as deleted
                    self.write_slot(index, 1, &existing_key, &value_data)?;
                    self.curr_size -= 1;

                    return Ok(Some(value));
                }
                _ => index = (index + 1) % self.capacity,
            }
        }

        Ok(None)
    }

    fn resize(&mut self) -> Result<(), HashError> {
        let new_capacity = (self.capacity * 2) + 1;
        let temp_path = self.path.with_extension("temp");
        let temp_overflow = temp_path.with_extension("overflow"); // fixed extension

        // Create a new temporary table (and its overflow) at the right paths
        let mut new_table = Table::create_new_temp(&temp_path, new_capacity)?;

        // Rehash all entries from the old table into the new one
        for i in 0..self.capacity {
            let (status, key, value_data) = self.read_slot(i);
            if status == 2 {
                // Extract actual value (inline or overflow)
                let value = if value_data[0] == 0 {
                    let len = value_data[1] as usize;
                    value_data[2..2 + len].to_vec()
                } else {
                    let offset = u64::from_le_bytes(value_data[1..9].try_into().unwrap());
                    let length = u64::from_le_bytes(value_data[9..17].try_into().unwrap());
                    let mut buf = vec![0u8; length as usize];
                    self.read_overflow(offset, &mut buf)?;
                    buf
                };
                new_table.insert(&key, &value)?;
            }
        }

        // Replace on-disk files: main table and overflow
        std::fs::rename(&temp_path, &self.path)?;
        std::fs::rename(&temp_overflow, self.overflow_path())?;

        // Re-open ourselves pointing at the new files
        *self = Table::open(&self.path)?;
        Ok(())
    }

    fn create_new_temp<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self, HashError> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        file.set_len((capacity * SLOT_SIZE) as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let curr_size = 0;

        // Create new overflow file
        let overflow_path = path.with_extension("overflow");
        let mut overflow_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(overflow_path)?;
        overflow_file.set_len(8)?;
        overflow_file.write_all(&0u64.to_le_bytes())?;

        Ok(Self {
            mmap,
            capacity,
            curr_size,
            path: path.to_path_buf(),
            overflow_file,
            free_list_head: 0,
        })
    }

    fn allocate_overflow(&mut self, data: &[u8]) -> Result<u64, HashError> {
        let required_size = data.len() as u64;
        let mut current_head = self.free_list_head;
        let mut prev_offset = 0;
        let mut found_block = None;

        // 1) Search the free list as before...
        while current_head != 0 {
            // read header: [size, next_free]
            let mut header_buf = [0u8; 16];
            self.overflow_file.seek(SeekFrom::Start(current_head))?;
            self.overflow_file.read_exact(&mut header_buf)?;
            let header_size = u64::from_le_bytes(header_buf[0..8].try_into().unwrap());
            let next_free = u64::from_le_bytes(header_buf[8..16].try_into().unwrap());

            if header_size >= required_size {
                found_block = Some((current_head, header_size, next_free));
                break;
            }

            prev_offset = current_head;
            current_head = next_free;
        }

        if let Some((block_offset, block_size, next_free)) = found_block {
            // 2) Remove this block from the free list:
            if prev_offset == 0 {
                // we removed the head of the free list
                self.free_list_head = next_free;
            } else {
                // patch previous block’s “next” pointer
                self.overflow_file.seek(SeekFrom::Start(prev_offset + 8))?;
                self.overflow_file.write_all(&next_free.to_le_bytes())?;
            }

            // **PERSIST THE UPDATED HEAD RIGHT AWAY**
            self.overflow_file.seek(SeekFrom::Start(0))?;
            self.overflow_file.write_all(&self.free_list_head.to_le_bytes())?;
            self.overflow_file.flush()?;

            // 3) (Optional) split the block if it’s big enough
            if block_size > required_size + 16 {
                let remaining_size = block_size - required_size - 16;
                let new_free_offset = block_offset + 16 + required_size;
                let mut new_header = Vec::with_capacity(16);
                new_header.extend_from_slice(&remaining_size.to_le_bytes());
                new_header.extend_from_slice(&self.free_list_head.to_le_bytes());

                self.overflow_file.seek(SeekFrom::Start(new_free_offset))?;
                self.overflow_file.write_all(&new_header)?;

                // update in-memory head to point at the new fragment
                self.free_list_head = new_free_offset;

                // **PERSIST AGAIN** after splitting!
                self.overflow_file.seek(SeekFrom::Start(0))?;
                self.overflow_file.write_all(&self.free_list_head.to_le_bytes())?;
                self.overflow_file.flush()?;
            }

            // 4) write out your data chunk
            self.overflow_file.seek(SeekFrom::Start(block_offset + 16))?;
            self.overflow_file.write_all(data)?;
            self.overflow_file.flush()?;

            Ok(block_offset + 16)
        } else {
            // no fit in free list: append at end
            let block_offset = self.overflow_file.seek(SeekFrom::End(0))?;
            let header = [&required_size.to_le_bytes()[..], &0u64.to_le_bytes()[..]].concat();

            self.overflow_file.write_all(&header)?;
            self.overflow_file.write_all(data)?;
            self.overflow_file.flush()?;

            Ok(block_offset + 16)
        }
    }

    fn free_overflow(&mut self, offset: u64) -> Result<(), HashError> {
        let block_offset = offset - 16;
        let mut header_buf = [0u8; 16];
        self.overflow_file.seek(SeekFrom::Start(block_offset))?;
        self.overflow_file.read_exact(&mut header_buf)?;
        let size = u64::from_le_bytes(header_buf[0..8].try_into().unwrap());

        // Update header to add to free list
        let new_header = [size.to_le_bytes(), self.free_list_head.to_le_bytes()].concat();
        self.overflow_file.seek(SeekFrom::Start(block_offset))?;
        self.overflow_file.write_all(&new_header)?;

        // Update free list head
        self.free_list_head = block_offset;
        self.overflow_file.seek(SeekFrom::Start(0))?;
        self.overflow_file.write_all(&self.free_list_head.to_le_bytes())?;
        self.overflow_file.flush()?;

        Ok(())
    }

    fn read_overflow(&mut self, offset: u64, buffer: &mut [u8]) -> Result<(), HashError> {
        self.overflow_file.seek(SeekFrom::Start(offset))?;
        self.overflow_file.read_exact(buffer)?;
        Ok(())
    }

    fn overflow_path(&self) -> PathBuf {
        self.path.with_extension("overflow")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn new_temp_table(capacity: usize) -> Result<Table, HashError> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_path_buf();
        Table::create_new_temp(&path, capacity)
    }

    #[test]
    fn test_insert_get_small_value() -> Result<(), HashError> {
        let mut table = new_temp_table(INITIAL_SIZE)?;
        let key = [42u8; KEY_SIZE];
        let value = vec![1, 2, 3];

        table.insert(&key, &value)?;
        let retrieved = table.get(&key).unwrap();
        assert_eq!(retrieved, value);
        Ok(())
    }

    #[test]
    fn test_insert_get_large_value() -> Result<(), HashError> {
        let mut table = new_temp_table(INITIAL_SIZE)?;
        let key = [42u8; KEY_SIZE];
        let value = vec![123u8; 500]; // Larger than 254 bytes

        table.insert(&key, &value)?;
        let retrieved = table.get(&key).unwrap();
        assert_eq!(retrieved, value);
        Ok(())
    }

    #[test]
    fn test_delete_with_overflow() -> Result<(), HashError> {
        let mut table = new_temp_table(INITIAL_SIZE)?;
        let key = [42u8; KEY_SIZE];
        let value = vec![123u8; 500];

        table.insert(&key, &value)?;
        assert!(table.delete(&key).unwrap().is_some());
        assert!(table.get(&key).is_none());
        Ok(())
    }

    #[test]
    fn test_reopen_persistence() -> Result<(), HashError> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        // Insert data
        {
            let mut table = Table::open(path)?;
            let key = [1u8; KEY_SIZE];
            let value = vec![2u8; 300];
            table.insert(&key, &value)?;
        }

        // Reopen and check
        let mut table = Table::open(path)?;
        let key = [1u8; KEY_SIZE];
        let value = table.get(&key).unwrap();
        assert_eq!(value, vec![2u8; 300]);

        Ok(())
    }
}
