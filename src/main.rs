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

    for i in 0..100000 {
        let key = i.to_string();

        match table.get(&key)? {
            Some(val) => {
                if &val != &key {
                    eprintln!("[ERR] Value mismatched, {key}:{val}");
                }
            }
            None => {}
        }
    }

    let duration = start.elapsed();

    println!("Time taken for retrieval of 100K items: {:?}", duration);

    // benchmark del operation
    let start = Instant::now();

    for i in 0..100000 {
        let key = i.to_string();

        match table.del(&key)? {
            Some(val) => {
                if &val != &key {
                    eprintln!("[ERR] Value mismatched, {key}:{val}");
                }
            }
            None => {}
        }
    }

    let duration = start.elapsed();

    println!("Time taken for deletion of 100K items: {:?}", duration);

    Ok(())
}
