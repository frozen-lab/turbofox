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

pub trait Hashable {
    fn hash(&self) -> u64;
}

impl Hashable for &str {
    fn hash(&self) -> u64 {
        // FNV-1a hash function for better distribution
        let mut hash: u64 = 14695981039346656037; // FNV offset basis
        for byte in self.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(1099511628211); // FNV prime
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

pub struct FileHash {
    file: File,
    size: u64,
    no_of_taken: u64,
}

impl FileHash {
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

            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buffer)?;
            let no_of_taken = u64::from_le_bytes(buffer);

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
                return Ok(());
            }

            if current_item.key == hash_item.key {
                // Update existing key
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.write_all(&hash_item.to_bytes())?;
                return Ok(());
            }

            index = (index + 1) % self.size;
            if index == start_index {
                return Err(io::Error::new(io::ErrorKind::Other, "Hash table is full"));
            }
        }
    }

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
