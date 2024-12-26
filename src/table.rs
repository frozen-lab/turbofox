const MAX_KEY_SIZE: usize = 8;
const MAX_VALUE_SIZE: usize = 56;
const MAX_BUCKET_SIZE: usize = MAX_KEY_SIZE + MAX_VALUE_SIZE;

const INITIAL_ITEMS: usize = 64;
const INITIAL_SIZE: usize = INITIAL_ITEMS * MAX_BUCKET_SIZE; // (4096) 4 KiB

pub trait Hashable {
    fn hash(&self) -> usize;
}

impl Hashable for &str {
    fn hash(&self) -> usize {
        let mut result: usize = 5381;

        for c in self.chars() {
            result = ((result << 5).wrapping_add(result)).wrapping_add(c as usize);
        }

        result
    }
}

struct HashItem {
    key: [u8; MAX_KEY_SIZE],
    value: [u8; MAX_VALUE_SIZE],
}

impl HashItem {
    fn new(key: &str, value: &str) -> Option<Self> {
        let mut key_bytes = Vec::from(key.as_bytes());
        let mut value_bytes = Vec::from(value.as_bytes());

        if key_bytes.len() > MAX_KEY_SIZE {
            eprintln!("KEY should be smaller then {MAX_KEY_SIZE}");
            return None;
        }

        if value_bytes.len() > MAX_VALUE_SIZE {
            eprintln!("Value should be smaller then {MAX_VALUE_SIZE}");
            return None;
        }

        key_bytes.resize(MAX_KEY_SIZE, b'\0');
        value_bytes.resize(MAX_VALUE_SIZE, b'\0');

        Some(Self {
            key: key_bytes.try_into().unwrap(),
            value: value_bytes.try_into().unwrap(),
        })
    }

    fn get_key_bytes(key: &str) -> Option<[u8; MAX_KEY_SIZE]> {
        let mut key_bytes = Vec::from(key.as_bytes());

        if key_bytes.len() > MAX_KEY_SIZE {
            eprintln!("KEY should be smaller then {MAX_KEY_SIZE}");
            return None;
        }

        key_bytes.resize(MAX_KEY_SIZE, b'\0');

        Some(key_bytes.try_into().unwrap())
    }

    fn value_from_bytes(value_bytes: [u8; MAX_VALUE_SIZE]) -> String {
        String::from_utf8_lossy(&value_bytes).trim_end_matches('\0').to_string()
    }

    fn key_from_bytes(key_bytes: [u8; MAX_KEY_SIZE]) -> String {
        String::from_utf8_lossy(&key_bytes).trim_end_matches('\0').to_string()
    }

    fn empty() -> [u8; MAX_BUCKET_SIZE] {
        vec![b'\0'; MAX_BUCKET_SIZE].try_into().unwrap()
    }
}

pub struct HashTable {
    kvs: Vec<u8>,
    size: usize,
    no_of_taken: usize,
}

impl HashTable {
    pub fn new() -> Self {
        Self {
            kvs: vec![b'\0'; INITIAL_SIZE],
            size: INITIAL_ITEMS,
            no_of_taken: 0,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        let load_factor = (self.size as f64 * 0.75) as usize;

        // extend the table upon reaching 75% of capacity
        if self.no_of_taken >= load_factor {
            self.extend();
        }

        match HashItem::new(key, value) {
            Some(hash_item) => {
                let mut index = self.get_hash_index(&key);

                for _ in 0..self.size {
                    let offset = index * MAX_BUCKET_SIZE;

                    if self.kvs[offset] == b'\0' {
                        let bucket: Vec<u8> = hash_item.key.iter().chain(hash_item.value.iter()).copied().collect();

                        self.kvs[offset..(offset + MAX_BUCKET_SIZE)].copy_from_slice(&bucket);
                        self.no_of_taken += 1;

                        return;
                    }

                    let stored_key_bytes: [u8; 8] = self.kvs[offset..(offset + MAX_KEY_SIZE)].try_into().unwrap();

                    // update existing value
                    if stored_key_bytes == hash_item.key {
                        self.kvs[(offset + MAX_KEY_SIZE)..(offset + MAX_BUCKET_SIZE)].copy_from_slice(&hash_item.value);

                        return;
                    }

                    index = (index + 1) % self.size;
                }

                eprintln!("No spot found to store the bucket");
            }
            None => {
                return;
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match HashItem::get_key_bytes(key) {
            Some(key_bytes) => {
                let index = self.get_hash_index(&key);

                match self.get_index(index, key_bytes) {
                    Some(i) => {
                        let offset = i * MAX_BUCKET_SIZE;

                        let value_bytes: [u8; MAX_VALUE_SIZE] = self.kvs
                            [(offset + MAX_KEY_SIZE)..(offset + MAX_BUCKET_SIZE)]
                            .try_into()
                            .unwrap();

                        return Some(HashItem::value_from_bytes(value_bytes));
                    }
                    None => {
                        return None;
                    }
                }
            }
            None => None,
        }
    }

    pub fn del(&mut self, key: &str) -> Option<String> {
        match HashItem::get_key_bytes(key) {
            Some(key_bytes) => {
                let index = self.get_hash_index(&key);

                match self.get_index(index, key_bytes) {
                    Some(i) => {
                        let offset = i * MAX_BUCKET_SIZE;

                        let value_bytes: [u8; MAX_VALUE_SIZE] = self.kvs
                            [(offset + MAX_KEY_SIZE)..(offset + MAX_BUCKET_SIZE)]
                            .try_into()
                            .unwrap();

                        self.kvs[offset..(offset + MAX_BUCKET_SIZE)].copy_from_slice(&HashItem::empty());

                        self.no_of_taken -= 1;

                        return Some(HashItem::value_from_bytes(value_bytes));
                    }
                    None => {
                        return None;
                    }
                }
            }
            None => None,
        }
    }

    fn get_index(&self, index: usize, key_bytes: [u8; MAX_KEY_SIZE]) -> Option<usize> {
        let mut index = index;

        for _ in 0..self.size {
            let offset = index * MAX_BUCKET_SIZE;

            // pair does not exists
            if self.kvs[offset] == b'\0' {
                return None;
            }

            let stored_key_bytes: [u8; 8] = self.kvs[offset..(offset + MAX_KEY_SIZE)].try_into().unwrap();

            if stored_key_bytes == key_bytes {
                return Some(index);
            }

            index = (index + 1) % self.size;
        }

        return None;
    }

    fn extend(&mut self) {
        let new_size = self.size * 2;

        let mut new_self = HashTable {
            kvs: vec![b'\0'; new_size * MAX_BUCKET_SIZE],
            size: new_size,
            no_of_taken: 0,
        };

        for i in 0..self.size {
            let offset = i * MAX_BUCKET_SIZE;

            if self.kvs[offset] == b'\0' {
                continue;
            }

            let stored_key: [u8; MAX_KEY_SIZE] = self.kvs[offset..(offset + MAX_KEY_SIZE)].try_into().unwrap();
            let stored_value: [u8; MAX_VALUE_SIZE] = self.kvs[(offset + MAX_KEY_SIZE)..(offset + MAX_BUCKET_SIZE)]
                .try_into()
                .unwrap();

            let key = HashItem::key_from_bytes(stored_key);
            let value = HashItem::value_from_bytes(stored_value);

            new_self.set(&key, &value);
        }

        *self = new_self;
    }

    fn get_hash_index(&self, key: &str) -> usize {
        key.hash() % self.size
    }
}
