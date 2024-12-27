# TurboCache 🚀📦

A persistent, file-based Key-Value store implementation in Rust. 

It supports fixed-size KV storage with automatic resizing.

## Features

- File-based storage with linear probing for collision resolution
- Automatic table resizing when load factor exceeds 75%
- Fixed-size keys (8 bytes) and values (56 bytes)
- FNV-1a hash function implementation

## Usage

```rust
use turbo_cache::file_hash::FileHash;

fn main() -> std::io::Result<()> {
    let mut turbo_cache = FileHash::init()?;

    // Store Entries
    turbo_cache.set("user_1", "John Doe")?;
    turbo_cache.set("user_2", "Jane Smith")?;

    // Fetch Entries
    assert_eq!(turbo_cache.get("user_1")?, Some("John Doe".to_string()));

    // Delete Entries
    assert_eq!(turbo_cache.del("user_2")?, Some("Jane Smith".to_string()));
    assert_eq!(turbo_cache.get("user_2")?, None);

    Ok(())
}
```

## API

- `FileHash::init() -> io::Result<FileHash>`

    Creates or opens hash table stored in `hash.tc`.

- `set(&mut self, key: &str, value: &str) -> io::Result<()>`
    
    Inserts or updates a key-value pair.

- `get(&mut self, key: &str) -> io::Result<Option<String>>`
    
    Retrieves value for given key.

- `del(&mut self, key: &str) -> io::Result<Option<String>>`
    
    Removes key-value pair and returns removed value.

## Performance

Benchmarks conducted on:

- CPU: Intel Core i5-10300H @ 2.50GHz
- RAM: 16GB
- OS: Windows 64-bit (WSL2 Ubuntu 24.04.1 LTS)

| Operation       | Time      |
| --------------- | --------- |
| 100K insertions | ~174.79ms |
| 100K retrievals | ~70.21ms  |
| 100K deletions  | ~161.62ms |
