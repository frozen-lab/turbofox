[![Crates.io](https://img.shields.io/crates/v/turbocache.svg)](https://crates.io/crates/turbocache)
[![Last Commits](https://img.shields.io/github/last-commit/frozen-lab/turbocache?logo=git&logoColor=white)](https://github.com/frozen-lab/turbocache/commits/main)
[![Pull Requests](https://img.shields.io/github/issues-pr/frozen-lab/turbocache?logo=github&logoColor=white)](https://github.com/frozen-lab/turbocache/pulls)
[![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/frozen-lab/turbocache?logo=github&logoColor=white)](https://github.com/frozen-lab/turbocache/issues)
[![License](https://img.shields.io/github/license/frozen-lab/turbocache?logo=open-source-initiative&logoColor=white)](https://github.com/frozen-lab/turbocache/blob/master/LICENSE)

[![Linux (ARM)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_arm.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_arm.yml)
[![Linux (x86)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_x86.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_x86.yml)
[![WIN (ARM)](https://github.com/frozen-lab/turbocache/actions/workflows/win_arm.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/win_arm.yml)
[![WIN (x86)](https://github.com/frozen-lab/turbocache/actions/workflows/win_x86.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/win_x86.yml)

# TurboCache

A persistant and embedded KV Database built for on-device caching.

### Overview

- [Installation](#installation)
- [API](#api)
- [Performance](#performance)
- [Architecture](#architecture)
- [Memory Usage](#memory-usage)

## Installation

Install **Turbocache** using `cargo`,

```sh
cargo add turbocache
```

## API

Example usage of all public methods,

```rust
use turbocache::TurboCache;

fn main() {
    const INITIAL_CAPACITY: usize = 1024;
    let path = std::env::temp_dir().join("cache-dir");
    let cache = TurboCache::new(path, INITIAL_CAPACITY).unwrap();

    // inserts 5 kev-value pairs into the cache
    for i in 0..5 {
        cache.set(&vec![i], &vec![i * 10]).unwrap();
    }

    // fetch key(3) from cache
    assert_eq!(cache.get(&vec![3]).unwrap(), Some(vec![30]));

    // delete key(3) from cache
    assert_eq!(cache.del(&vec![3]).unwrap(), Some(vec![30]));

    let mut keys = Vec::new();

    // iterate over all keys inserted in cache
    for res in cache.iter().unwrap() {
        let (k, _) = res.unwrap();

        keys.push(k);
    }

    // match keys vector's length w/ total number for keys in cache
    assert_eq!(keys.len(), cache.total_count().unwrap());
}
```

## Performance

### Common Benchmarks

* **OS**: Windows 64-bit (`WSL2 NixOS 24.11 (Vicuna)`)
* **Kernel**: Linux 6.6.87.2-microsoft-standard-WSL2
* **CPU**: Intel Core i5-10300H @ 2.50GHz
* **Architecture**: x86/64

| Operation           | Latency (p50)                 | Throughput (p50)                   | Outliers (Total)            |
| ------------------- | ----------------------------- | ---------------------------------- | --------------------------- |
| **set**             | ~ 1.43 µs                     | ~ 0.695 Million pairs/s            | ~ 20%                       |
| **get**             | ~ 0.49 µs                     | ~ 2.034 Million pairs/s            | ~ 07%                       |
| **del**             | ~ 0.11 µs                     | ~ 8.347 Million pairs/s            | ~ 02%                       |

*NOTE*: Benchmarks are derived from 256 samples collected from millions of iterations per opeation.

## Architecture

## Memory Usage

