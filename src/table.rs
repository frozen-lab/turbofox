const INITIAL_SIZE: usize = 69;

pub trait Hashable {
    fn hash(&self) -> usize;
}

impl Hashable for &str {
    fn hash(&self) -> usize {
        let mut hash: usize = 14695981039346656037;

        for byte in self.as_bytes() {
            hash ^= *byte as usize;

            hash = hash.wrapping_mul(1099511628211);
        }

        hash
    }
}

#[derive(Clone)]
struct HashItem {
    key: String,
    value: String,
}

#[derive(Clone)]
enum Slot {
    Empty,
    Deleted,
    Occupied(HashItem),
}

pub struct Table {
    items: Vec<Slot>,
    curr_size: usize,
    capacity: usize,
}

impl Table {
    pub fn init() -> Self {
        let items: Vec<Slot> = vec![Slot::Empty; INITIAL_SIZE];

        Self {
            items,
            curr_size: 0,
            capacity: INITIAL_SIZE,
        }
    }

    pub fn insert(&mut self, key: &str, value: &str) -> bool {
        if self.curr_size >= (self.capacity as f64 * 0.75) as usize {
            self.extend();
        }

        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            match &mut self.items[index] {
                Slot::Empty => {
                    let item = HashItem {
                        key: key.to_string(),
                        value: value.to_string(),
                    };

                    self.items[index] = Slot::Occupied(item);
                    self.curr_size += 1;
                    return true;
                }
                Slot::Occupied(item) if item.key == key => {
                    item.value = value.to_string();
                    return true;
                }
                _ => {
                    index = (index + 1) % self.capacity;
                }
            }
        }

        false
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            match &mut self.items[index] {
                Slot::Empty => return None,
                Slot::Occupied(item) if item.key == key => {
                    return Some(item.value.clone());
                }
                _ => {
                    index = (index + 1) % self.capacity;
                }
            }
        }

        None
    }

    pub fn delete(&mut self, key: &str) -> Option<String> {
        let hash = key.hash();
        let mut index = hash % self.capacity;

        for _ in 0..self.capacity {
            match &mut self.items[index] {
                Slot::Empty => return None,
                Slot::Occupied(item) if item.key == key => {
                    let value = item.value.clone();
                    self.items[index] = Slot::Deleted;

                    self.curr_size -= 1;

                    return Some(value);
                }
                _ => {
                    index = (index + 1) % self.capacity;
                }
            }
        }

        None
    }

    fn extend(&mut self) {
        let new_capacity = (self.capacity * 2) + 1;

        let mut new_self = Self {
            items: vec![Slot::Empty; new_capacity],
            curr_size: 0,
            capacity: new_capacity,
        };

        for index in 0..self.capacity {
            if let Slot::Occupied(item) = &mut self.items[index] {
                new_self.insert(&item.key, &item.value);
            }
        }

        *self = new_self;
    }
}

#[cfg(test)]
mod tests {
    use super::{Hashable, Table, INITIAL_SIZE};

    #[test]
    fn test_hashable_consistency_and_uniqueness() {
        // Same &str always hashes the same
        let a = "consistent";
        assert_eq!(a.hash(), a.hash());

        // Different strings generally hash differently
        let x = "foo";
        let y = "bar";
        assert_ne!(x.hash(), y.hash());
    }

    #[test]
    fn test_insert_returns_true_and_get() {
        let mut table = Table::init();
        // insert should return true
        assert!(table.insert("apple", "red"));
        assert!(table.insert("banana", "yellow"));

        // get should return the inserted values
        assert_eq!(table.get("apple"), Some("red".to_string()));
        assert_eq!(table.get("banana"), Some("yellow".to_string()));
        // missing key
        assert_eq!(table.get("pear"), None);
    }

    #[test]
    fn test_update_value_on_duplicate_insert() {
        let mut table = Table::init();
        assert!(table.insert("key", "v1"));
        // inserting same key should overwrite
        assert!(table.insert("key", "v2"));
        assert_eq!(table.get("key"), Some("v2".to_string()));
    }

    #[test]
    fn test_delete_and_get() {
        let mut table = Table::init();
        assert!(table.insert("delete_me", "gone"));
        // delete returns the value
        assert_eq!(table.delete("delete_me"), Some("gone".to_string()));
        // now it's gone
        assert_eq!(table.get("delete_me"), None);
        // deleting again returns None
        assert_eq!(table.delete("delete_me"), None);
    }

    #[test]
    fn test_collision_handling_and_delete_in_chain() {
        let mut table = Table::init();

        // We precompute two distinct keys that collide:
        // "r" and "aa" both hash to the same slot modulo INITIAL_SIZE == 69.
        let k1 = "r";
        let k2 = "aa";
        assert_eq!(k1.hash() % INITIAL_SIZE, k2.hash() % INITIAL_SIZE);

        // Insert both
        assert!(table.insert(k1, "first"));
        assert!(table.insert(k2, "second"));

        // Both must be retrievable
        assert_eq!(table.get(k1), Some("first".to_string()));
        assert_eq!(table.get(k2), Some("second".to_string()));

        // Delete the first, ensure second is still reachable via probing
        assert_eq!(table.delete(k1), Some("first".to_string()));
        assert_eq!(table.get(k1), None);
        assert_eq!(table.get(k2), Some("second".to_string()));
    }

    #[test]
    fn test_extend_and_retrieve_many() {
        let mut table = Table::init();
        let n = 100;
        // Insert more than 75% of INITIAL_SIZE to force an extend
        for i in 0..n {
            let key = format!("key{}", i);
            assert!(table.insert(&key, "val"));
        }

        // After extension, all must still be present
        for i in 0..n {
            let key = format!("key{}", i);
            assert_eq!(table.get(&key), Some("val".to_string()));
        }
    }
}
