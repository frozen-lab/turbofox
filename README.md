# TurboCache

A persistent, high-performance, disk-backed Key-Value store w/ a novel sharding algorithm.

## Benchmarks

*   **OS**: Windows 64-bit (`WSL2 NixOS 24.11 (Vicuna)`)
*   **Kernel**: Linux 6.6.87.2-microsoft-standard-WSL2
*   **CPU**: Intel Core i5-10300H @ 2.50GHz
*   **Architecture**: x86/64
*   **Pool Size**: 10_000 keys per iteration

| Operation      | Fastest   | Slowest   | Median    | Samples | Iterations |
| -------------- | --------- | --------- | --------- | ------- | ---------- |
| `bench_set`    | 11.993 ms | 12.222 ms | 12.103 ms | 1000    | 10_000     |
| `bench_get`    | 9.0864 ms | 9.3700 ms | 9.2260 ms | 1000    | 10_000     |
| `bench_remove` | 381.15 µs | 385.87 µs | 383.44 µs | 1000    | 10_000     |

*   **Samples**: total measurement samples collected by Criterion
*   **Iterations**: number of operations per sample (i.e., N_KEYS = 10_000 per `iter()` call)

---

## TODO

List of critical issues to be addressed:

### Critical Bugs

-   [ ] **Ensure Atomic Shard Splitting**
    -   **Issue**: The `split` operation is not atomic. A crash during a split can leave the
    database in an inconsistent state, leading to data loss.
    -   **Recommendation**: Implement a more robust, multi-phase split process:
        1.  Create the new shard files with temporary names (e.g., `shard_..._new`).
        2.  Write all the data to the new shards.
        3.  `fsync` the new shard files to ensure they are written to disk.
        4.  Atomically rename the new shard files to their final names.
        5.  Delete the old shard file.

