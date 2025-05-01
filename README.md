# TurboCache

A persistent, high-performance, disk-backed Key-Value store in Rust.

## Usage

```rust
use turbocache::table::Table;
use std::path::Path;

fn main() -> std::io::Result<()> {
    let mut table = Table::open(Path::new("data.tbl"))?;

    let key = [42u8; KEY_SIZE];
    let value = vec![1, 2, 3];

    table.insert(&key, &value)?;

    let retrieved = table.get(&key).unwrap();
    
    assert_eq!(retrieved, value);

    Ok(())
}
```

