# TurboCache

A persistent, high-performance, disk-backed Key-Value store w/ a novel sharding algorithm.

## Usage

```rust
use core::str;
use tempfile::tempdir;
use turbocache::TurboCache;

fn main() -> std::io::Result<()> {
    let dir = tempdir().unwrap();
    let mut db = TurboCache::open(dir.path())?;

    println!("{:?}", db.get(b"mykey")?); // None

    db.set(b"mykey", b"myval")?;
    println!("{:?}", db.get(b"mykey")?); // Some([109, 121, 118, 97, 108])

    println!("{:?}", db.remove(b"mykey")?); // Some([109, 121, 118, 97, 108])
    println!("{:?}", db.remove(b"mykey")?); // None

    println!("{:?}", db.get(b"mykey")?); // None

    for i in 0..10 {
        db.set(&format!("mykey{i}").into_bytes(), &format!("myval{i}").into_bytes())?;
    }
    for res in db.iter() {
        let (k, v) = res?;
        println!("{} = {}", str::from_utf8(&k).unwrap(), str::from_utf8(&v).unwrap());
    }

    Ok(())
}
```

## Benchmarks

- **OS**: Windows 64-bit (`WSL2 Ubuntu 24.04.1 LTS`)
- **CPU**: Intel Core i5-10300H @ 2.50GHz
- **Architecture**: x86/64
- **Pool Size**: 1000000

| Command | Avg Time (µs)   |
|:-------:|:---------------:|
| `SET`   | 20              |
| `GET`   | 2000            |
| `DEL`   | 15              |

