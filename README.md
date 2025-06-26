# TurboCache

A persistent, high-performance, disk-backed Key-Value store w/ a novel sharding algorithm.

## Usage

```rust
use tempfile::tempdir;
use turbocache::{Result, TurboCache};

fn main() -> Result<()> {
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

    Ok(())
}
```

## Benchmarks

- **OS**: Windows 64-bit (`WSL2 NixOS 24.11 (Vicuna)`)
- **Kernel**: Linux 6.6.87.2-microsoft-standard-WSL2
- **CPU**: Intel Core i5-10300H @ 2.50GHz
- **Architecture**: x86/64
- **Pool Size**: 100000

| Operation      | Fastest   | Slowest   | Median    | Samples | Iterations |
|----------------|-----------|-----------|-----------|---------|------------|
| `bench_get`    | 226 µs    | 443.6 µs  | 234 µs    | 100     | 100000     |
| `bench_remove` | 225.7 µs  | 255.7 µs  | 230.3 µs  | 100     | 100000     |
| `bench_set`    | 224.9 µs  | 246.8 µs  | 229.8 µs  | 100     | 100000     |

