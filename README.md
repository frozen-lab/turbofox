[![Crates.io](https://img.shields.io/crates/v/turbocache.svg)](https://crates.io/crates/turbocache)
[![Linux (ARM)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_arm.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_arm.yml)
[![Linux (x86)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_x86.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/linux_x86.yml)
[![WIN (ARM)](https://github.com/frozen-lab/turbocache/actions/workflows/win_arm.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/win_arm.yml)
[![WIN (x86)](https://github.com/frozen-lab/turbocache/actions/workflows/win_x86.yml/badge.svg)](https://github.com/frozen-lab/turbocache/actions/workflows/win_x86.yml)

# TurboCache

A persistant and embedded KV Database built for on-device caching.

## Benchmarks

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
