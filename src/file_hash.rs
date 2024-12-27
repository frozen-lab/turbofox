//!
//! A persistent hash table implementation that stores fixed sized KV pairs in a file.
//!
//! # Features
//!
//! - File-based storage with linear probing for collision resolution
//! - Automatic table resizing when load factor exceeds 75%
//! - Fixed-size keys (8 bytes) and values (56 bytes)
//! - FNV-1a hash function implementation
//!
//! # Example Usage
//!
//! ```rust
//! use turbo_cache::file_hash::FileHash;
//!
//! fn main() -> std::io::Result<()> {
//!     let mut turbo_cache = FileHash::init()?;
//!     
//!     // Store Entries
//!     turbo_cache.set("user_1", "John Doe")?;
//!     turbo_cache.set("user_2", "Jane Smith")?;
//!     
//!     // Fetch Entries
//!     assert_eq!(turbo_cache.get("user_1")?, Some("John Doe".to_string()));
//!     
//!     // Delete Entries
//!     assert_eq!(turbo_cache.del("user_2")?, Some("Jane Smith".to_string()));
//!     assert_eq!(turbo_cache.get("user_2")?, None);
//!     
//!     Ok(())
//! }
//! ```
//!

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
};

const INITIAL_BUCKETS: u64 = 64;

const KEY_SIZE: usize = 8;
const VALUE_SIZE: usize = 56;

const BUCKET_SIZE: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

const METADATA_SIZE: u64 = 16; // 8 bytes for size + 8 bytes for no_of_taken

const FILE_PATH: &str = "hash.tc";

///
/// Trait for types that need to be hashed
///
pub trait Hashable {
    ///
    /// Computes hash for the type being implemented
    ///
    fn hash(&self) -> u64;
}

impl Hashable for &str {
    fn hash(&self) -> u64 {
        // FNV offset basis
        let mut hash: u64 = 14695981039346656037;

        for byte in self.as_bytes() {
            hash ^= *byte as u64;

            // FNV prime
            hash = hash.wrapping_mul(1099511628211);
        }

        hash
    }
}

#[derive(Debug)]
struct HashItem {
    key: [u8; KEY_SIZE],
    value: [u8; VALUE_SIZE],
}

impl HashItem {
    fn new(key: &str, value: &str) -> Option<Self> {
        if key.as_bytes().len() > KEY_SIZE {
            eprintln!("Key must be smaller than {KEY_SIZE} bytes");
            return None;
        }

        if value.as_bytes().len() > VALUE_SIZE {
            eprintln!("Value must be smaller than {VALUE_SIZE} bytes");
            return None;
        }

        let mut key_bytes = [0u8; KEY_SIZE];
        let mut value_bytes = [0u8; VALUE_SIZE];

        key_bytes[..key.as_bytes().len()].copy_from_slice(key.as_bytes());
        value_bytes[..value.as_bytes().len()].copy_from_slice(value.as_bytes());

        Some(Self {
            key: key_bytes,
            value: value_bytes,
        })
    }

    fn to_bytes(&self) -> [u8; BUCKET_SIZE as usize] {
        let mut bytes = [0u8; BUCKET_SIZE as usize];
        bytes[..KEY_SIZE].copy_from_slice(&self.key);
        bytes[KEY_SIZE..].copy_from_slice(&self.value);
        bytes
    }

    fn from_bytes(bytes: [u8; BUCKET_SIZE as usize]) -> Self {
        let mut key = [0u8; KEY_SIZE];
        let mut value = [0u8; VALUE_SIZE];

        key.copy_from_slice(&bytes[..KEY_SIZE]);
        value.copy_from_slice(&bytes[KEY_SIZE..]);

        Self { key, value }
    }

    fn is_empty(&self) -> bool {
        self.key.iter().all(|&b| b == 0)
    }

    fn value_as_string(&self) -> String {
        String::from_utf8_lossy(&self.value).trim_end_matches('\0').to_string()
    }
}

///
/// A persistent KV store
///
pub struct FileHash {
    file: File,
    size: u64,
    no_of_taken: u64,
}

impl FileHash {
    ///
    /// Creates an instance of [FileHash]
    ///
    /// # Errors
    ///
    /// * `io::Error` -> If file operations fail.
    ///
    pub fn init() -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(FILE_PATH)?;

        let file_size = file.metadata()?.len();

        let (size, no_of_taken) = if file_size == 0 {
            // Initialize new file
            let size = INITIAL_BUCKETS;
            let no_of_taken = 0u64;

            // Write metadata
            file.write_all(&no_of_taken.to_le_bytes())?;
            file.write_all(&size.to_le_bytes())?;

            // Initialize buckets
            let empty_bucket = vec![0u8; BUCKET_SIZE as usize];

            for _ in 0..size {
                file.write_all(&empty_bucket)?;
            }

            file.flush()?;

            (size, no_of_taken)
        } else {
            // Read existing metadata
            let mut buffer = [0u8; 8];

            // No of taken
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buffer)?;

            let no_of_taken = u64::from_le_bytes(buffer);

            // Size
            file.seek(SeekFrom::Start(8))?;
            file.read_exact(&mut buffer)?;

            let size = u64::from_le_bytes(buffer);

            (size, no_of_taken)
        };

        Ok(Self {
            file,
            size,
            no_of_taken,
        })
    }

    ///
    /// Inserts or updates a key-value pair in the hash table.
    ///
    /// # Arguments
    ///
    /// * `key` -> String key, must not exceed 8 bytes when UTF-8 encoded
    /// * `value` -> String value, must not exceed 56 bytes when UTF-8 encoded
    ///
    /// # Errors
    ///
    /// * `io::Error` -> If file operations fail.
    ///
    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        // Check load factor and extend if necessary
        if (self.no_of_taken as f64 / self.size as f64) >= 0.75 {
            self.extend()?;
        }

        let hash_item = match HashItem::new(key, value) {
            Some(item) => item,
            None => return Ok(()),
        };

        let mut index = self.get_hash_index(key);
        let start_index = index;

        loop {
            let offset = METADATA_SIZE + (index * BUCKET_SIZE);
            self.file.seek(SeekFrom::Start(offset))?;

            let mut bucket = vec![0u8; BUCKET_SIZE as usize];
            self.file.read_exact(&mut bucket)?;
            let current_item = HashItem::from_bytes(bucket.try_into().unwrap());

            if current_item.is_empty() {
                // Found empty slot
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.write_all(&hash_item.to_bytes())?;
                self.increment_no_of_taken()?;

                self.file.flush()?;
                return Ok(());
            }

            if current_item.key == hash_item.key {
                // Update existing key
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.write_all(&hash_item.to_bytes())?;

                self.file.flush()?;
                return Ok(());
            }

            index = (index + 1) % self.size;
            
            if index == start_index {
                return Err(io::Error::new(io::ErrorKind::Other, "Hash table is full"));
            }
        }
    }

    ///
    /// Retrieves the value associated with a key.
    ///
    /// # Arguments
    ///
    /// * `key` -> The key to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Some(String))` -> Value if key exists
    /// * `Ok(None)` -> If key doesn't exist or exceeds size limit
    /// * `Err(io::Error)` -> If file operations fail
    ///
    pub fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        if key.as_bytes().len() > KEY_SIZE {
            return Ok(None);
        }

        let mut key_bytes = [0u8; KEY_SIZE];
        key_bytes[..key.as_bytes().len()].copy_from_slice(key.as_bytes());

        let mut index = self.get_hash_index(key);
        let start_index = index;

        loop {
            let offset = METADATA_SIZE + (index * BUCKET_SIZE);
            self.file.seek(SeekFrom::Start(offset))?;

            let mut bucket = vec![0u8; BUCKET_SIZE as usize];
            self.file.read_exact(&mut bucket)?;
            let item = HashItem::from_bytes(bucket.try_into().unwrap());

            if item.is_empty() {
                return Ok(None);
            }

            if item.key == key_bytes {
                return Ok(Some(item.value_as_string()));
            }

            index = (index + 1) % self.size;
            if index == start_index {
                return Ok(None);
            }
        }
    }

    /// 
    /// Removes a key-value pair from the hash table.
    ///
    /// # Arguments
    /// 
    /// * `key` -> The key to remove
    ///
    /// # Returns
    /// 
    /// * `Ok(Some(String))` -> Removed value if key existed
    /// * `Ok(None)` -> If key doesn't exist or exceeds size limit
    /// * `Err(io::Error)` -> If file operations fail
    /// 
    pub fn del(&mut self, key: &str) -> io::Result<Option<String>> {
        if key.as_bytes().len() > KEY_SIZE {
            return Ok(None);
        }

        let mut key_bytes = [0u8; KEY_SIZE];
        key_bytes[..key.as_bytes().len()].copy_from_slice(key.as_bytes());

        let mut index = self.get_hash_index(key);
        let start_index = index;

        loop {
            let offset = METADATA_SIZE + (index * BUCKET_SIZE);
            self.file.seek(SeekFrom::Start(offset))?;

            let mut bucket = vec![0u8; BUCKET_SIZE as usize];
            self.file.read_exact(&mut bucket)?;
            let item = HashItem::from_bytes(bucket.try_into().unwrap());

            if item.is_empty() {
                return Ok(None);
            }

            if item.key == key_bytes {
                let value = item.value_as_string();

                // Clear the bucket
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.write_all(&vec![0u8; BUCKET_SIZE as usize])?;
                self.decrement_no_of_taken()?;

                self.file.flush()?;
                return Ok(Some(value));
            }

            index = (index + 1) % self.size;
            if index == start_index {
                return Ok(None);
            }
        }
    }

    fn extend(&mut self) -> io::Result<()> {
        let old_size = self.size;
        let new_size = old_size * 2;

        // Create temporary storage for all items
        let mut items = Vec::new();

        // Read all existing items
        for i in 0..old_size {
            let offset = METADATA_SIZE + (i * BUCKET_SIZE);
            self.file.seek(SeekFrom::Start(offset))?;

            let mut bucket = vec![0u8; BUCKET_SIZE as usize];
            self.file.read_exact(&mut bucket)?;
            let item = HashItem::from_bytes(bucket.try_into().unwrap());

            if !item.is_empty() {
                items.push(item);
            }
        }

        // Truncate and reinitialize file with new size
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;

        // Write metadata
        self.file.write_all(&0u64.to_le_bytes())?; // Reset no_of_taken
        self.file.write_all(&new_size.to_le_bytes())?;

        // Initialize new buckets
        let empty_bucket = vec![0u8; BUCKET_SIZE as usize];
        for _ in 0..new_size {
            self.file.write_all(&empty_bucket)?;
        }

        // Update struct
        self.size = new_size;
        self.no_of_taken = 0;

        // Reinsert all items
        for item in items {
            let key = String::from_utf8_lossy(&item.key).trim_end_matches('\0').to_string();
            let value = String::from_utf8_lossy(&item.value).trim_end_matches('\0').to_string();

            self.set(&key, &value)?;
        }

        Ok(())
    }

    fn get_hash_index(&self, key: &str) -> u64 {
        key.hash() % self.size
    }

    fn increment_no_of_taken(&mut self) -> io::Result<()> {
        self.no_of_taken += 1;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.no_of_taken.to_le_bytes())?;

        Ok(())
    }

    fn decrement_no_of_taken(&mut self) -> io::Result<()> {
        if self.no_of_taken > 0 {
            self.no_of_taken -= 1;
            self.file.seek(SeekFrom::Start(0))?;
            self.file.write_all(&self.no_of_taken.to_le_bytes())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn cleanup() {
        let _ = fs::remove_file(FILE_PATH);
    }

    #[test]
    fn test_init() {
        cleanup();
        let hash = FileHash::init().unwrap();
        assert_eq!(hash.size, INITIAL_BUCKETS);
        assert_eq!(hash.no_of_taken, 0);
        cleanup();
    }

    #[test]
    fn test_basic_set_get() {
        cleanup();
        let mut hash = FileHash::init().unwrap();
        hash.set("key1", "value1").unwrap();
        assert_eq!(hash.get("key1").unwrap(), Some("value1".to_string()));
        cleanup();
    }

    #[test]
    fn test_update_existing() {
        cleanup();
        let mut hash = FileHash::init().unwrap();
        hash.set("key1", "value1").unwrap();
        hash.set("key1", "value2").unwrap();
        assert_eq!(hash.get("key1").unwrap(), Some("value2".to_string()));
        cleanup();
    }

    #[test]
    fn test_delete() {
        cleanup();
        let mut hash = FileHash::init().unwrap();
        hash.set("key1", "value1").unwrap();
        assert_eq!(hash.del("key1").unwrap(), Some("value1".to_string()));
        assert_eq!(hash.get("key1").unwrap(), None);
        cleanup();
    }

    #[test]
    fn test_collision_handling() {
        cleanup();
        let mut hash = FileHash::init().unwrap();
        // Force collision by filling specific buckets
        hash.set("a", "value1").unwrap();
        hash.set("b", "value2").unwrap();
        hash.set("c", "value3").unwrap();

        assert_eq!(hash.get("a").unwrap(), Some("value1".to_string()));
        assert_eq!(hash.get("b").unwrap(), Some("value2".to_string()));
        assert_eq!(hash.get("c").unwrap(), Some("value3".to_string()));
        cleanup();
    }

    #[test]
    fn test_size_limits() {
        cleanup();
        let mut hash = FileHash::init().unwrap();

        // Test key size limit
        let long_key = "a".repeat(KEY_SIZE + 1);
        hash.set(&long_key, "value").unwrap();
        assert_eq!(hash.get(&long_key).unwrap(), None);

        // Test value size limit
        let long_value = "a".repeat(VALUE_SIZE + 1);
        hash.set("key", &long_value).unwrap();
        assert_eq!(hash.get("key").unwrap(), None);
        cleanup();
    }

    #[test]
    fn test_auto_resize() {
        cleanup();
        let mut hash = FileHash::init().unwrap();
        let initial_size = hash.size;

        // Fill up to trigger resize
        for i in 0..((INITIAL_BUCKETS as f64 * 0.8) as i32) {
            hash.set(&format!("key{}", i), &format!("value{}", i)).unwrap();
        }

        assert!(hash.size > initial_size);
        cleanup();
    }

    #[test]
    fn test_delete_nonexistent() {
        cleanup();
        let mut hash = FileHash::init().unwrap();
        assert_eq!(hash.del("nonexistent").unwrap(), None);
        cleanup();
    }

    #[test]
    fn test_unicode_handling() {
        cleanup();
        let mut hash = FileHash::init().unwrap();
        hash.set("🦀", "rust").unwrap();
        hash.set("키", "값").unwrap();

        assert_eq!(hash.get("🦀").unwrap(), Some("rust".to_string()));
        assert_eq!(hash.get("키").unwrap(), Some("값".to_string()));
        cleanup();
    }
}
