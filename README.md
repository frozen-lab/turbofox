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

-   [ ] **Fix Data Loss During Cascading Shard Splits**

    -   **Issue**: When redistributing items during a `split`, if a row in a *new* shard is also full,
    the item that fails to be inserted is silently ignored, leading to data loss. The `split` function
    has its own insertion logic that does not handle a row becoming full.

    -   **Plan**: Refactor the insertion and split logic to use a centralized "work queue" model.
    This ensures that all items (both new and redistributed) are processed by the same logic that can
    handle cascading splits correctly.

        -   **Step 1: Refactor `Shard::split`**: Modify `Shard::split` to return the two new shards and
        a `Vec` containing all the key-value pairs from the original shard.

            ```rust
            // In src/shard.rs
            pub fn split(&self, dirpath: &PathBuf) -> TResult<((Shard, Shard), Vec<(Vec<u8>, Vec<u8>)>)> {
                // ...
            }
            ```

        -   **Step 2: Refactor `Router::set`**: Implement a work queue in `Router::set` to manage the
        entire insertion process, including handling splits and re-inserting data. Remove the
        `set_recursive` function.

            ```rust
            // In src/router.rs
            pub fn set(&self, kbuf: &[u8], vbuf: &[u8]) -> TResult<()> {
                let mut work_queue = vec![(kbuf.to_vec(), vbuf.to_vec())];

                while let Some((key, value)) = work_queue.pop() {
                    let hash = TurboHasher::new(&key);
                    let s = hash.shard_selector();
                    let mut shards = self.shards.lock().or_else(|e| e.into_inner()).unwrap();

                    let shard_idx = match shards.iter().position(|sh| sh.span.contains(&s)) {
                        Some(idx) => idx,
                        None => return Err(TError::ShardOutOfRange(s)),
                    };

                    match shards[shard_idx].set((&key, &value), hash) {
                        Ok(()) => {
                            // Item was inserted successfully, continue to the next.
                            continue;
                        }
                        Err(TError::RowFull(_)) => {
                            // The shard is full, so we must split it.
                            let old_shard = std::mem::replace(
                                &mut shards[shard_idx],
                                Shard::open(&self.dirpath, 0..0, true)? // Dummy shard
                            );

                            // The new split function gives us the new shards and all the data.
                            let ((left, right), items_to_reinsert) = old_shard.split(&self.dirpath)?;

                            // Replace the dummy shard with the two new ones.
                            shards.remove(shard_idx);
                            shards.insert(shard_idx, right);
                            shards.insert(shard_idx, left);

                            // Add all items from the old shard back into the work queue.
                            work_queue.extend(items_to_reinsert);

                            // IMPORTANT: Add the item that *caused* the split back to the queue to be retried.
                            work_queue.push((key, value));
                        }
                        Err(err) => return Err(err),
                    }
                }

                Ok(())
            }
            ```

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

-   [ ] **Prevent Mutex Poisoning**

    -   **Issue**: Using `.unwrap()` on a `Mutex` lock can cause the application to panic if a
    thread panics while holding the lock.
    -   **Recommendation**: Use `lock().or_else(|e| e.into_inner())` to handle poisoned mutexes gracefully.

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
