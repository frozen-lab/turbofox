use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{thread, vec};

use tempfile::TempDir;
use turbocache::TurboCache;

const INITIAL_CAP: usize = 1000;
const PREPOP: usize = 799;
const BATCH_SIZE: usize = 50;
const NUM_BATCHES: usize = 64;
const BG_INSERT_RATE_MS: u64 = 0;
const RNG_SEED: u64 = 42;
const KEY_LEN: usize = 16;
const VAL_LEN: usize = 32;

fn make_key(id: u64) -> Vec<u8> {
    let mut key = vec![0u8; KEY_LEN];
    key[..8].copy_from_slice(&id.to_be_bytes());

    key
}

fn make_val_from_rng(rng: &mut StdRng) -> Vec<u8> {
    (0..VAL_LEN).map(|_| rng.random()).collect()
}

fn create_csv(path: &std::path::Path) -> std::io::Result<File> {
    let mut f = File::create(path)?;

    writeln!(
        f,
        "batch_index,batch_ops,batch_ns,batch_ops_per_sec,cumulative_ops"
    )?;

    Ok(f)
}

fn append_csv_line(
    f: &mut File,
    batch_index: usize,
    batch_ops: usize,
    batch_ns: u128,
    cumulative_ops: usize,
) -> std::io::Result<()> {
    let secs = (batch_ns as f64) / 1_000_000_000.0;
    let ops_per_sec = (batch_ops as f64) / secs.max(1e-12);

    writeln!(
        f,
        "{},{},{},{:.2},{}",
        batch_index, batch_ops, batch_ns, ops_per_sec, cumulative_ops
    )?;

    Ok(())
}

fn bench_set_with_migration(out_path: &std::path::Path) {
    println!("bench_set_with_migration -> CSV: {}", out_path.display());

    let tmp = TempDir::new().expect("tempdir");
    let cache = TurboCache::new(tmp.path(), INITIAL_CAP).expect("create cache");

    for id in 0..(PREPOP as u64) {
        let key = make_key(id);
        let mut rng = StdRng::seed_from_u64(RNG_SEED + id);
        let val = make_val_from_rng(&mut rng);

        cache.set(&key, &val).expect("pre-insert");
    }

    let mut csv = create_csv(out_path).expect("create csv");
    let mut id_counter: u64 = PREPOP as u64;
    let mut rng = StdRng::seed_from_u64(RNG_SEED + 9999);
    let mut cumulative_ops = 0usize;

    for batch in 0..NUM_BATCHES {
        let start = Instant::now();

        for _ in 0..BATCH_SIZE {
            let key = make_key(id_counter);
            let val = make_val_from_rng(&mut rng);

            cache.set(&key, &val).expect("set should succeed (bench)");
            id_counter += 1;
        }

        let elapsed = start.elapsed();
        let batch_ns = elapsed.as_nanos();

        cumulative_ops += BATCH_SIZE;

        append_csv_line(&mut csv, batch, BATCH_SIZE, batch_ns, cumulative_ops).expect("write csv");

        println!(
            "set: batch {:03} ops={} time_ms={:.3}",
            batch,
            BATCH_SIZE,
            (batch_ns as f64) / 1_000_000.0
        );
    }
}

fn bench_get_during_migration(out_path: &std::path::Path) {
    println!("bench_get_during_migration -> CSV: {}", out_path.display());

    let tmp = TempDir::new().expect("tempdir");
    let cache = Arc::new(TurboCache::new(tmp.path(), INITIAL_CAP).expect("create cache"));
    let measured_key_count = 5000usize;

    for id in 0..measured_key_count {
        let key = make_key(id as u64);
        let mut rng = StdRng::seed_from_u64(RNG_SEED + id as u64);
        let val = make_val_from_rng(&mut rng);

        cache.set(&key, &val).expect("pre-insert");
    }

    // Start a background inserter to drive migration.
    let bg_counter = Arc::new(AtomicUsize::new(measured_key_count));
    let bg_cache = cache.clone();
    let bg_counter_clone = bg_counter.clone();

    let bg_inserts = BATCH_SIZE * NUM_BATCHES * 3;

    let bg_handle = thread::spawn(move || {
        let mut local_rng = StdRng::seed_from_u64(RNG_SEED + 7777);

        for _ in 0..bg_inserts {
            let id = bg_counter_clone.fetch_add(1, Ordering::SeqCst) as u64;
            let key = make_key(id);
            let val = make_val_from_rng(&mut local_rng);
            let _ = bg_cache.set(&key, &val);

            if BG_INSERT_RATE_MS > 0 {
                thread::sleep(Duration::from_millis(BG_INSERT_RATE_MS));
            }
        }
    });

    let mut csv = create_csv(out_path).expect("create csv");
    let mut rng = StdRng::seed_from_u64(RNG_SEED + 2222);
    let mut cumulative_ops = 0usize;

    // For each measured batch, pick random keys from 0..measured_key_count
    for batch in 0..NUM_BATCHES {
        let start = Instant::now();

        for _ in 0..BATCH_SIZE {
            let idx = rng.random_range(0..measured_key_count) as u64;
            let key = make_key(idx);

            let _ = cache.get(&key).expect("get");
        }

        let elapsed = start.elapsed();
        let batch_ns = elapsed.as_nanos();

        cumulative_ops += BATCH_SIZE;

        append_csv_line(&mut csv, batch, BATCH_SIZE, batch_ns, cumulative_ops).expect("write csv");

        println!(
            "get: batch {:03} ops={} time_ms={:.3}",
            batch,
            BATCH_SIZE,
            (batch_ns as f64) / 1_000_000.0
        );
    }

    bg_handle.join().expect("bg join");
}

fn bench_del_during_migration(out_path: &std::path::Path) {
    println!("bench_del_during_migration -> CSV: {}", out_path.display());

    let tmp = TempDir::new().expect("tempdir");
    let cache = Arc::new(TurboCache::new(tmp.path(), INITIAL_CAP).expect("create cache"));
    let measured_del_count = BATCH_SIZE * NUM_BATCHES;

    for id in 0..measured_del_count {
        let key = make_key(id as u64);
        let mut rng = StdRng::seed_from_u64(RNG_SEED + id as u64);
        let val = make_val_from_rng(&mut rng);

        cache.set(&key, &val).expect("pre-insert for delete");
    }

    let bg_counter = Arc::new(AtomicUsize::new(measured_del_count));
    let bg_cache = cache.clone();
    let bg_counter_clone = bg_counter.clone();
    let bg_inserts = BATCH_SIZE * NUM_BATCHES * 3;

    let bg_handle = thread::spawn(move || {
        let mut local_rng = StdRng::seed_from_u64(RNG_SEED + 3333);

        for _ in 0..bg_inserts {
            let id = bg_counter_clone.fetch_add(1, Ordering::SeqCst) as u64;
            let key = make_key(id);
            let val = make_val_from_rng(&mut local_rng);
            let _ = bg_cache.set(&key, &val);

            if BG_INSERT_RATE_MS > 0 {
                thread::sleep(Duration::from_millis(BG_INSERT_RATE_MS));
            }
        }
    });

    let mut csv = create_csv(out_path).expect("create csv");
    let mut cumulative_ops = 0usize;

    for batch in 0..NUM_BATCHES {
        let start_id = batch * BATCH_SIZE;
        let start = Instant::now();

        for j in 0..BATCH_SIZE {
            let id = (start_id + j) as u64;
            let key = make_key(id);

            let _ = cache.del(&key).expect("del");
        }

        let elapsed = start.elapsed();
        let batch_ns = elapsed.as_nanos();

        cumulative_ops += BATCH_SIZE;

        append_csv_line(&mut csv, batch, BATCH_SIZE, batch_ns, cumulative_ops).expect("write csv");

        println!(
            "del: batch {:03} ops={} time_ms={:.3}",
            batch,
            BATCH_SIZE,
            (batch_ns as f64) / 1_000_000.0
        );
    }

    bg_handle.join().expect("bg join");
}

fn main() {
    bench_set_with_migration(std::path::Path::new("bench_set.csv"));
    bench_get_during_migration(std::path::Path::new("bench_get.csv"));
    bench_del_during_migration(std::path::Path::new("bench_del.csv"));

    println!("CSV files written");
}
