# TurboCache

A persistent and efficient embedded KV database.

## Benchmarks

*   **OS**: Windows 64-bit (`WSL2 NixOS 24.11 (Vicuna)`)
*   **Kernel**: Linux 6.6.87.2-microsoft-standard-WSL2
*   **CPU**: Intel Core i5-10300H @ 2.50GHz
*   **Architecture**: x86/64
*   **Sample Size**: 1_000 kv pairs per iteration (about 5M iterations)

| Operation  | Latency \[p50]    | Throughput          | Outliers     |
| -----------| ------------------| --------------------| ------------ |
| **set**    | 1.4070 µs         | 0.710 M pairs/s     | 2.50%        |
| **get**    | 488.37 ns         | 0.204 M pairs/s     | 17.70%       |
| **del**    | 63.262 ns         | 1.580 M pairs/s     | 12.70%       |

* **Latency** columns show the low, median, and high bounds reported by Criterion.
* **Throughput** columns are in thousands‑elements per second.
* **Outliers** gives the total count and percentage of measurements flagged above the noise threshold.

