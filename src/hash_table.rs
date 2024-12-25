const INITIAL_SIZE: usize = 4096; // 4 KiB
const MAX_KEY_SIZE: usize = 8;
const MAX_VALUE_SIZE: usize = 56;
const MAX_BUCKET_SIZE: usize = MAX_KEY_SIZE + MAX_VALUE_SIZE;

pub trait Hashable {
    fn hash(&self) -> usize;
}

impl Hashable for &str {
    fn hash(&self) -> usize {
        let mut result: usize = 5381;

        for c in self.chars() {
            result =
                ((result << 5).wrapping_add(result)).wrapping_add(c as usize);
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

    fn get_key(key: &str) -> Option<[u8; MAX_KEY_SIZE]> {
        let mut key_bytes = Vec::from(key.as_bytes());

        if key_bytes.len() > MAX_KEY_SIZE {
            eprintln!("KEY should be smaller then {MAX_KEY_SIZE}");
            return None;
        }

        key_bytes.resize(MAX_KEY_SIZE, b'\0');

        Some(key_bytes.try_into().unwrap())
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
            size: INITIAL_SIZE,
            no_of_taken: 0,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        let load_factor = (self.size as f64 * 0.75) as usize;

        if self.no_of_taken >= load_factor {
            // TODO: Extend the kvs
        }

        match HashItem::new(key, value) {
            None => {
                return;
            }
            Some(bucket) => {
                let mut index = self.get_hash_index(&key);

                for _ in 0..self.size {
                    let offset = index * 64;

                    let stored_key_bytes: [u8; 8] = self.kvs
                        [offset..(offset + MAX_KEY_SIZE)]
                        .try_into()
                        .unwrap();

                    // If the bucket is empty, replace empty bytes w/ bucket
                    if stored_key_bytes[0] == b'\0' {
                        let joined: Vec<u8> = bucket
                            .key
                            .iter()
                            .chain(bucket.value.iter())
                            .copied()
                            .collect();

                        self.kvs[offset..(offset + MAX_BUCKET_SIZE)]
                            .copy_from_slice(&joined);

                        self.no_of_taken += 1;

                        return;
                    }

                    // update value if key already exists
                    if stored_key_bytes == bucket.key {
                        self.kvs[(offset + MAX_KEY_SIZE)
                            ..(offset + MAX_BUCKET_SIZE)]
                            .copy_from_slice(&bucket.value);

                        return;
                    }

                    index = (index + 1) % self.size;
                }

                eprintln!("No spot found to store the bucket");
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match HashItem::get_key(key) {
            Some(key_bytes) => {
                let mut index = self.get_hash_index(&key);

                for _ in 0..self.size {
                    let offset = index * 64;

                    let stored_key_bytes: [u8; 8] = self.kvs
                        [offset..(offset + MAX_KEY_SIZE)]
                        .try_into()
                        .unwrap();

                    // If the bucket is empty, return None
                    if stored_key_bytes[0] == b'\0' {
                        return None;
                    }

                    if stored_key_bytes == key_bytes {
                        let value_bytes: [u8; MAX_VALUE_SIZE] =
                            self.kvs[(offset + MAX_KEY_SIZE)
                                ..(offset + MAX_BUCKET_SIZE)]
                                .try_into()
                                .unwrap();

                        return Some(
                            String::from_utf8_lossy(&value_bytes)
                                .trim_end_matches('\0')
                                .to_string(),
                        );
                    }

                    index = (index + 1) % self.size;
                }

                None
            }

            None => None,
        }
    }

    pub fn del(&mut self, key: &str) -> Option<String> {
        match HashItem::get_key(key) {
            Some(key_bytes) => {
                let mut index = self.get_hash_index(&key);

                for _ in 0..self.size {
                    let offset = index * 64;

                    let stored_key_bytes: [u8; 8] = self.kvs
                        [offset..(offset + MAX_KEY_SIZE)]
                        .try_into()
                        .unwrap();

                    // If the bucket is empty, return None
                    if stored_key_bytes[0] == b'\0' {
                        return None;
                    }

                    if stored_key_bytes == key_bytes {
                        let value_bytes: [u8; MAX_VALUE_SIZE] =
                            self.kvs[(offset + MAX_KEY_SIZE)
                                ..(offset + MAX_BUCKET_SIZE)]
                                .try_into()
                                .unwrap();

                        self.kvs[offset..(offset + MAX_BUCKET_SIZE)]
                            .copy_from_slice(&HashItem::empty());

                        return Some(
                            String::from_utf8_lossy(&value_bytes)
                                .trim_end_matches('\0')
                                .to_string(),
                        );
                    }

                    index = (index + 1) % self.size;
                }

                None
            }

            None => None,
        }
    }

    fn get_hash_index(&self, key: &str) -> usize {
        key.hash() % self.size
    }
}
