use std::{io, time::Instant};

use turbo_cache::file_hash::FileHash;

fn main() -> io::Result<()> {
    let mut table = FileHash::init()?;

    // benchmark set operation
    let start = Instant::now();

    for i in 0..100000 {
        let key = i.to_string();

        table.set(&key, &key)?;
    }

    let duration = start.elapsed();

    println!("Time taken for inserting 100K items: {:?}", duration);

    // benchmark get operation
    let start = Instant::now();

    let mut not_found = 0;

    for i in 0..100000 {
        let key = i.to_string();

        match table.get(&key)? {
            Some(val) => {
                if &val != &key {
                    eprintln!("[ERR] Value mismatched, {key}:{val}");

                    not_found += 1;
                }
            }
            None => {
                not_found += 1;
            }
        }
    }

    let duration = start.elapsed();

    if not_found > 0 {
        eprintln!("[ERR] 404:{not_found}");
    }

    println!("Time taken for retrieval of 100K items: {:?}", duration);

    Ok(())
}
