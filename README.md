# TurboCache

A persistent, high-performance, disk-backed Key-Value store w/ a novel sharding algorithm.

## Benchmarks

* **OS**: Windows 64-bit (`WSL2 NixOS 24.11 (Vicuna)`)
* **Kernel**: Linux 6.6.87.2-microsoft-standard-WSL2
* **CPU**: Intel Core i5-10300H @ 2.50GHz
* **Architecture**: x86/64
* **Pool Size**: 10_000 keys per iteration

| Operation      | Fastest   | Slowest   | Median    | Samples | Iterations |
| -------------- | --------- | --------- | --------- | ------- | ---------- |
| `bench_set`    | 11.993 ms | 12.222 ms | 12.103 ms | 1000    | 10_000     |
| `bench_get`    | 9.0864 ms | 9.3700 ms | 9.2260 ms | 1000    | 10_000     |
| `bench_remove` | 381.15 µs | 385.87 µs | 383.44 µs | 1000    | 10_000     |

* **Samples**: total measurement samples collected by Criterion
* **Iterations**: number of operations per sample (i.e., N\_KEYS = 10_000 per `iter()` call)
