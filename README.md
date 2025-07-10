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

### Robustness and Error Handling

-   [ ] **Improve Library Error Handling**
    -   **Issue**: The `load` function in `router.rs` prints errors to `stderr` instead of returning them,
    which is not ideal for a library.
    -   **Recommendation**: Return a `TResult` with a specific error type that can be handled by the caller.

### Performance and Portability

-   [ ] **Improve `get`/`set` Performance**
    -   **Issue**: High hash collision rates can lead to multiple slow disk reads for a single `get` or
    `set` operation.
    -   **Recommendation**: Consider adding Bloom filters to each row or storing in-memory key prefixes
    to reduce unnecessary disk access.

-   [ ] **Make File I/O Portable**
    -   **Issue**: The code uses Unix-specific file I/O (`FileExt`), making it non-portable to Windows.
    -   **Recommendation**: Use conditional compilation (`#[cfg(...)]`) to provide separate, portable
    implementations for different operating systems.
