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
