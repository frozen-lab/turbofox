# TurboCache

A persistant and embedded KV Database built for on-device caching.

## Current Benchmarks

* **OS**: Windows 64-bit (`WSL2 NixOS 24.11 (Vicuna)`)
* **Kernel**: Linux 6.6.87.2-microsoft-standard-WSL2
* **CPU**: Intel Core i5-10300H @ 2.50GHz
* **Architecture**: x86/64

| Operation      | Latency \[p50]          | Throughput                     | Outliers (total)            |
| -------------- | ----------------------- | ------------------------------ | --------------------------- |
| **set**        | 39.997 µs               | 25.002 Thousand pairs/s        | 00.20%                      |
| **get**        | 23.636 µs               | 42.309 Thousand pairs/s        | 18.70%                      |
| **del**        | 19.980 µs               | 50.049 Thousand pairs/s        | 15.20%                      |

