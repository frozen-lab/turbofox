use turbo_cache::table::HashTable;

fn main() {
    let mut table = HashTable::new();

    for i in 0..5 {
        let key = format!("{i}");

        table.set(&key, &key);
    }

    for i in 0..7 {
        let key = format!("{i}");

        match table.get(&key) {
            Some(val) => {
                println!("Value: {val}")
            }
            None => {
                eprintln!("Value: NaN")
            }
        }
    }
}
