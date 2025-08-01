use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::TempDir;
use turbocache::{TurboCache, TurboResult};

const N_THREADS: usize = 8;
const OPS_PER_THREAD: usize = 3000;
const KEY_LEN: usize = 32;
const VAL_LEN: usize = 128;
const SEED: u64 = 42;
const INIT_CAP: usize = 1024;

fn gen_dataset(num_entries: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(SEED);

    (0..num_entries)
        .map(|_| {
            let key = (0..KEY_LEN).map(|_| rng.random()).collect();
            let val = (0..VAL_LEN).map(|_| rng.random()).collect();

            (key, val)
        })
        .collect()
}

#[test]
fn test_concurrent_operations() -> TurboResult<()> {
    let tmp = TempDir::new().unwrap();
    let cache = TurboCache::new(tmp.path().to_path_buf(), INIT_CAP)?;

    let dataset = Arc::new(gen_dataset(N_THREADS * OPS_PER_THREAD));
    let mut handles = vec![];

    // Keep track of keys that are expected to be in the cache
    let expected_keys = Arc::new(Mutex::new(HashSet::new()));

    for i in 0..N_THREADS {
        let mut cache_clone = cache.clone();
        let dataset_clone = Arc::clone(&dataset);
        let expected_keys_clone = Arc::clone(&expected_keys);

        let handle = thread::spawn(move || -> TurboResult<()> {
            let mut rng = StdRng::seed_from_u64(SEED + i as u64);

            for j in 0..OPS_PER_THREAD {
                let idx = (i * OPS_PER_THREAD + j) % dataset_clone.len();
                let (key, value) = dataset_clone[idx].clone();

                // 0 for set, 1 for get, 2 for del
                let op_type = rng.random_range(0..3);

                match op_type {
                    // Set operation
                    0 => {
                        cache_clone.set(key.clone(), value.clone())?;

                        expected_keys_clone.lock().unwrap().insert(key);
                    }
                    // Get operation
                    1 => {
                        let _ = cache_clone.get(key.clone())?;
                    }
                    // Delete operation
                    2 => {
                        if cache_clone.del(key.clone())?.is_some() {
                            expected_keys_clone.lock().unwrap().remove(&key);
                        }
                    }
                    _ => unreachable!(),
                }
            }

            Ok(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    let final_expected_keys = expected_keys.lock().unwrap();
    let mut actual_keys_in_cache = HashSet::new();

    for (key, _) in dataset.iter() {
        if cache.get(key.clone())?.is_some() {
            actual_keys_in_cache.insert(key.clone());
        }
    }

    assert_eq!(actual_keys_in_cache, *final_expected_keys);
    assert_eq!(cache.get_inserts()?, final_expected_keys.len());

    Ok(())
}

#[test]
fn test_crash_during_swap_with_staging() -> TurboResult<()> {
    let tmp_dir = TempDir::new().unwrap();
    let cache_path = tmp_dir.path().to_path_buf();
    let num_entries = 100;
    let dataset = gen_dataset(num_entries);

    // Phase 1: Populate cache and simulate crash (by dropping cache)
    {
        let mut cache = TurboCache::new(cache_path.clone(), INIT_CAP)?;

        for (key, value) in &dataset {
            cache.set(key.clone(), value.clone())?;
        }

        // Cache is dropped here, which should trigger a flush and swap_with_staging
        // This simulates a crash where the process terminates after writing to staging
        // but before the final swap is complete, or during the swap itself.
    }

    // Phase 2: Restart cache and verify recovery
    {
        let cache = TurboCache::new(cache_path.clone(), INIT_CAP)?;

        for (key, expected_value) in &dataset {
            let actual_value = cache.get(key.clone())?;

            assert!(
                actual_value.is_some(),
                "Key not found after recovery: {:?}",
                key
            );
            assert_eq!(
                actual_value.unwrap(),
                *expected_value,
                "Value mismatch for key: {:?}",
                key
            );
        }

        assert_eq!(
            cache.get_inserts()?,
            num_entries,
            "Incorrect number of inserts after recovery"
        );
    }

    Ok(())
}
