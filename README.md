[![Latest Version](https://img.shields.io/crates/v/turbofox.svg)](https://crates.io/crates/turbofox)
[![License](https://img.shields.io/github/license/frozen-lab/turbofox?logo=open-source-initiative&logoColor=white)](https://github.com/frozen-lab/turbofox/blob/master/LICENSE)
[![Tests](https://github.com/frozen-lab/turbofox/actions/workflows/tests.yaml/badge.svg)](https://github.com/frozen-lab/turbofox/actions/workflows/tests.yaml)

# TurboFox 🚀🦊

A persistant and embedded KV Database built for on-device caching

> [!IMPORTANT]
> The `turbofox` db is still in a proof-of-concept stage of it's development. Currently use is
> discouraged, but not prohibited, **given you assume all the risks**.

## Usage

Add following to your `Cargo.toml`,

```toml
[dependencies]
turbofox = { version = "0.0.1" }

```

> [!NOTE]
> Current version of `turbofox` requires Rust 1.86 or later.

## Benchmarks

Environment used for benching:

* OS: NixOS (WSL2)
* Architecture: x86_64
* Memory: 8 GiB RAM (DDR4)
* Rust: rustc 1.86.0 w/ cargo 1.86.0
* Kernel: Linux 6.6.87.2-microsoft-standard-WSL2
* CPU: Intel® Core™ i5-10300H @ 2.50GHz (4C / 8T)

**Write Latency:**

Observed measurements for 1,048,576 batched operations,

| Metric  | Single TX (µs) | Multi TX (µs) |
|:--------|:---------------|:--------------|
| P50     | 0.2750         | 0.7340        |
| P90     | 0.5500         | 1.6500        |
| P99     | 0.9170         | 12.6550       |
| MEAN    | 2.7639         | 10.8486       |
| MAX     | 16891.9030     | 23986.1750    |

**Read Latency:**

Observed measurements for 262,144 operations,

| Metric | Single TX (µs) | Multi TX (µs) |
|:-------|:---------------|:--------------|
| P50    | 0.9160         | 1.0090        |
| P90    | 1.0090         | 1.3750        |
| P99    | 1.6500         | 2.0160        |
| MEAN   | 0.9406         | 1.1210        |
| MAX    | 128.2550       | 577.0230      |

**Delete Latency:**

Observed measurements for 262,144 operations,

| Metric | Single TX (µs) | Multi TX (µs) |
|:-------|:---------------|:--------------|
| P50    | 1.2830         | 1.6500        |
| P90    | 1.4670         | 2.1090        |
| P99    | 2.0170         | 2.8430        |
| MEAN   | 14.7996        | 38.7403       |
| MAX    | 24035.3270     | 161742.8470   |

## Example

```rs
use turbofox::{TurboFox, TurboFoxCfg, BufferSize};
use std::time::Duration;

let dir = tempfile::tempdir().unwrap();
let cfg = TurboFoxCfg {
    path: dir.path().to_path_buf(),
    buffer_size: BufferSize::S64,
    initial_available_buffers: 0x1000,
    flush_duration: Duration::from_millis(2),
    max_memory: 0x400 * 0x400 * 0x40, // 64 MB
};

let db = TurboFox::new(cfg).unwrap();

let key = b"my_key";
let value = b"hello world, fire and forget semantics!";

let ticket = db.write(key, value).unwrap();
ticket.wait().unwrap(); // Wait for sync

let data = db.read(key).unwrap().unwrap();
assert_eq!(value.as_slice(), data.as_slice());

db.delete(key).unwrap();
```
