# TurboCache

A persistent and efficient embedded KV database.

## Quickstart

Add to your `Cargo.toml`,

```toml
[dependencies]
turbocache = "0.0.3"
```

Or install using `cargo`,

```sh
cargo add turbocache
```

## Usage Example

```rust
use tempfile::TempDir;
use turbocache::TurboCache;

fn main() -> anyhow::Result<()> {
    let tmp = TempDir::new()?;
    let mut cache = TurboCache::new(tmp.path(), 16)?;

    // set
    cache.set(b"apple".to_vec(), b"red".to_vec())?;
    cache.set(b"banana".to_vec(), b"yellow".to_vec())?;

    // get
    assert_eq!(cache.get(b"apple".to_vec())?, Some(b"red".to_vec()));
    assert!(cache.get(b"pear".to_vec())?.is_none());

    // del
    let removed = cache.del(b"banana".to_vec())?;
    assert_eq!(removed, Some(b"yellow".to_vec()));

    // Iterate all live entries
    for entry in cache.iter() {
        let (k, v) = entry?;

        println!("key={:?}, val={:?}", k, v);
    }

    // Get total inserts so far
    println!("Total inserts: {}", cache.get_inserts());

    Ok(())
}
```

## Benchmarks

* **OS**: Windows 64-bit (`WSL2 NixOS 24.11 (Vicuna)`)
* **Kernel**: Linux 6.6.87.2-microsoft-standard-WSL2
* **CPU**: Intel Core i5-10300H @ 2.50GHz
* **Architecture**: x86/64

| Operation  | Latency \[p50]          | Throughput                    | Outliers          |
| ---------- | ----------------------- | ----------------------------- | ----------------- |
| **set**    | 1.2950 µs               | 0.7718 Million pairs/s        | 2.50%             |
| **get**    | 502.52 ns               | 1.9900 Million pairs/s        | 17.70%            |
| **del**    | 63.101 ns               | 15.848 Million pairs/s        | 12.70%            |

