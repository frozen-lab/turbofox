use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::io::{self};
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
}

impl Table {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, HashError> {
        let _path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let metadata = file.metadata()?;
        let capacity = if metadata.len() == 0 {
            let new_capacity = INITIAL_SIZE;
            file.set_len((new_capacity * SLOT_SIZE) as u64)?;
            new_capacity
        } else {
            (metadata.len() as usize / SLOT_SIZE).max(INITIAL_SIZE)
        };

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let curr_size = Table::count_occupied(&mmap, capacity);

        Ok(Self {
            mmap,
            capacity,
            curr_size,
            path: _path,
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

        // update mmap
        self.mmap[offset] = status;
        self.mmap[offset + 1..offset + 1 + KEY_SIZE].copy_from_slice(key);
        self.mmap[offset + 1 + KEY_SIZE..offset + SLOT_SIZE].copy_from_slice(value);

        // Flush the modified slot
        self.mmap.flush_async_range(offset, offset + SLOT_SIZE)?;

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

    pub fn insert(&mut self, key: &[u8; KEY_SIZE], value: &[u8; VALUE_SIZE]) -> Result<(), HashError> {
        if self.curr_size >= (self.capacity as f64 * 0.75) as usize {
            self.resize()?;
        }

        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            let (status, existing_key, _) = self.read_slot(index);

            match status {
                // empty or deleted
                0 | 1 => {
                    self.write_slot(index, 2, key, value)?;
                    self.curr_size += 1;

                    return Ok(());
                }
                // occupied
                2 => {
                    if &existing_key == key {
                        self.write_slot(index, 2, key, value)?;

                        return Ok(());
                    }

                    index = (index + 1) % self.capacity;
                }
                _ => unreachable!(),
            }
        }

        Err(HashError::TableFull)
    }

    pub fn get(&self, key: &[u8; KEY_SIZE]) -> Option<[u8; VALUE_SIZE]> {
        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            let (status, existing_key, value) = self.read_slot(index);

            match status {
                // empty
                0 => return None,
                // occupied
                2 if &existing_key == key => return Some(value),
                // deleted or diff key
                _ => index = (index + 1) % self.capacity,
            }
        }

        None
    }

    pub fn delete(&mut self, key: &[u8; KEY_SIZE]) -> Option<[u8; VALUE_SIZE]> {
        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            let (status, existing_key, value) = self.read_slot(index);

            match status {
                0 => return None,
                2 if &existing_key == key => {
                    self.write_slot(index, 1, &existing_key, &value).ok()?;

                    self.curr_size -= 1;
                    return Some(value);
                }
                _ => index = (index + 1) % self.capacity,
            }
        }

        None
    }

    fn resize(&mut self) -> Result<(), HashError> {
        let new_capacity = (self.capacity * 2) + 1;
        let temp_path = self.path.with_extension("temp");

        // new temp table
        let mut new_table = Table::create_new_temp(&temp_path, new_capacity)?;

        // rehash all entries
        for i in 0..self.capacity {
            let (slot, key, value) = self.read_slot(i);

            if slot == 2 {
                new_table.insert(&key, &value)?;
            }
        }

        // replace old file with new
        std::fs::rename(&temp_path, &self.path)?;
        *self = Table::open(&self.path)?;

        Ok(())
    }

    fn create_new_temp<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self, HashError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        file.set_len((capacity * SLOT_SIZE) as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let curr_size = Table::count_occupied(&mmap, capacity);

        Ok(Self {
            mmap,
            capacity,
            curr_size,
            path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    /// Helper to create a small-capacity table for collision and resize tests
    fn new_temp_table(capacity: usize) -> Result<Table, HashError> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_path_buf();
        // Use the private constructor available within tests
        Table::create_new_temp(&path, capacity)
    }

    #[test]
    fn test_hashable_determinism_and_uniqueness() {
        let mut key1 = [0u8; KEY_SIZE];
        let mut key2 = [0u8; KEY_SIZE];
        key1[0] = 42;
        key2[0] = 43;
        // Same key yields same hash
        assert_eq!(key1.hash(), key1.hash());
        // Different key yields different hash (very unlikely collision for FNV)
        assert_ne!(key1.hash(), key2.hash());
    }

    #[test]
    fn test_nonexistent_get_and_delete_return_none() -> Result<(), HashError> {
        let mut table = Table::open(NamedTempFile::new()?.path())?;
        let key = [5u8; KEY_SIZE];
        // No insertion yet
        assert!(table.get(&key).is_none());
        assert!(table.delete(&key).is_none());
        Ok(())
    }

    #[test]
    fn test_collision_resolution_small_capacity() -> Result<(), HashError> {
        // Find two keys colliding mod capacity=3 by brute as in analysis
        let mut key1 = [0u8; KEY_SIZE];
        let mut key2 = [0u8; KEY_SIZE];
        key1[0] = 1;
        key2[0] = 2; // both hash() % 3 == same

        let mut table = new_temp_table(3)?;
        let value1 = [10u8; VALUE_SIZE];
        let value2 = [20u8; VALUE_SIZE];

        table.insert(&key1, &value1)?;
        table.insert(&key2, &value2)?;

        // Both keys should be retrievable despite same initial slot
        assert_eq!(table.get(&key1), Some(value1));
        assert_eq!(table.get(&key2), Some(value2));
        Ok(())
    }

    #[test]
    fn test_resize_preserves_entries() -> Result<(), HashError> {
        // Small initial capacity to force resize on third insert
        let mut table = new_temp_table(3)?;
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for i in 0u8..3 {
            let mut key = [0u8; KEY_SIZE];
            let mut value = [0u8; VALUE_SIZE];
            key[0] = i;
            value[0] = i + 100;
            table.insert(&key, &value)?;
            keys.push(key);
            values.push(value);
        }

        // After third insert, capacity should have grown and all entries retained
        for (k, v) in keys.iter().zip(values.iter()) {
            assert_eq!(table.get(k), Some(*v));
        }
        Ok(())
    }

    #[test]
    fn test_reopen_persistence_multiple_entries() -> Result<(), HashError> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();
        {
            let mut table = Table::open(path)?;
            for i in 0u8..5 {
                let mut key = [0u8; KEY_SIZE];
                let mut value = [0u8; VALUE_SIZE];
                key[0] = i;
                value[0] = i + 50;
                table.insert(&key, &value)?;
            }
        }
        // Reopen and verify
        let table = Table::open(path)?;
        for i in 0u8..5 {
            let mut key = [0u8; KEY_SIZE];
            let mut expected = [0u8; VALUE_SIZE];
            key[0] = i;
            expected[0] = i + 50;
            assert_eq!(table.get(&key), Some(expected));
        }
        Ok(())
    }

    #[test]
    fn test_delete_and_reuse_slot() -> Result<(), HashError> {
        // Small capacity to ensure reuse within same table
        let mut table = new_temp_table(3)?;
        let mut key1 = [0u8; KEY_SIZE];
        key1[0] = 7;
        let mut key2 = [0u8; KEY_SIZE];
        key2[0] = 8;
        let value1 = [30u8; VALUE_SIZE];
        let value2 = [40u8; VALUE_SIZE];

        // Insert two keys
        table.insert(&key1, &value1)?;
        table.insert(&key2, &value2)?;
        // Delete key1
        assert_eq!(table.delete(&key1), Some(value1));
        assert!(table.get(&key1).is_none());

        // Insert key1 again, should reuse deleted slot
        table.insert(&key1, &value1)?;
        assert_eq!(table.get(&key1), Some(value1));
        assert_eq!(table.get(&key2), Some(value2));
        Ok(())
    }
}
