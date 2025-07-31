[![Linux X86_64](https://github.com/frozen-lab/turbocache/actions/workflows/linux_x86_64_tests.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_x86_64_tests.yml)
[![Linux ARM_64](https://github.com/frozen-lab/turbocache/actions/workflows/linux_arm_64_tests.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_arm_64_tests.yml)
[![WIN 11 x86_64](https://github.com/frozen-lab/turbocache/actions/workflows/windows_x86_64_tests.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/windows_x86_64_tests.yml)
[![WIN 11 ARM_64](https://github.com/frozen-lab/turbocache/actions/workflows/windows_arm_64_tests.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/windows_arm_64_tests.yml)

# TurboCache

A persistent and efficient embedded KV database.

## Quickstart

Add to your `Cargo.toml`,

```toml
[dependencies]
turbocache = "0.0.4"
```

Or install using `cargo`,

```sh
cargo add turbocache
```

## Usage Example

```rust
use turbocache::{TurboCache, TurboResult};

const INITIAL_CAP: usize = 1024;

fn main() -> TurboResult<()> {
    let path = std::env::temp_dir().join("cache-dir");
    let mut cache = TurboCache::new(path, INITIAL_CAP).unwrap();

    for i in 0..5 {
        cache.set(vec![i], vec![i * 10]).unwrap();
    }

    assert_eq!(cache.get(vec![3]).unwrap(), Some(vec![30]));
    assert_eq!(cache.del(vec![3]).unwrap(), Some(vec![30]));

    Ok(())
}
```

Refer [here](https://docs.rs/turbocache/0.0.3/turbocache/index.html) for API Docs!

## Benchmarks

* **OS**: Windows 64-bit (`WSL2 NixOS 24.11 (Vicuna)`)
* **Kernel**: Linux 6.6.87.2-microsoft-standard-WSL2
* **CPU**: Intel Core i5-10300H @ 2.50GHz
* **Architecture**: x86/64

| Operation      | Latency \[p50]          | Throughput                     | Outliers (total)            |
| -------------- | ----------------------- | ------------------------------ | --------------------------- |
| **set**        | 39.997 µs               | 25.002 Thousand pairs/s        | 00.20%                      |
| **get**        | 23.636 µs               | 42.309 Thousand pairs/s        | 18.70%                      |
| **del**        | 19.980 µs               | 50.049 Thousand pairs/s        | 15.20%                      |

