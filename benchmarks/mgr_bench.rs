use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{thread, vec};

use tempfile::TempDir;
use turbocache::TurboCache;

const INITIAL_CAP: usize = 128;
const PREPOP: usize = 102;
const BATCH_SIZE: usize = 128;
const NUM_BATCHES: usize = 32;
const BG_INSERT_RATE_MS: u64 = 1;
const RNG_SEED: u64 = 42;
const KEY_LEN: usize = 16;
const VAL_LEN: usize = 32;

fn make_key(id: u64) -> Vec<u8> {
    let mut key = vec![0u8; KEY_LEN];
    key[..8].copy_from_slice(&id.to_be_bytes());
    key
}

fn make_val_from_rng(rng: &mut StdRng) -> Vec<u8> {
    // generate VAL_LEN random bytes
    let mut v = Vec::with_capacity(VAL_LEN);
    for _ in 0..VAL_LEN {
        v.push(rng.random::<u8>());
    }
    v
}

/// CSV header now includes:
/// db_size,batch_index,batch_ops,batch_wall_ns,total_sample_ns,mean_ns,p50_ns,p95_ns,ops_per_sec,cumulative_ops
fn create_csv(path: &std::path::Path) -> std::io::Result<BufWriter<File>> {
    let f = File::create(path)?;
    let mut bw = BufWriter::new(f);
    writeln!(
        bw,
        "db_size,batch_index,batch_ops,batch_wall_ns,total_sample_ns,mean_ns,p50_ns,p95_ns,ops_per_sec,cumulative_ops"
    )?;
    Ok(bw)
}

fn append_csv_line(
    bw: &mut BufWriter<File>,
    db_size: usize,
    batch_index: usize,
    batch_ops: usize,
    batch_wall_ns: u128,
    total_sample_ns: u128,
    mean_ns: u128,
    p50_ns: u128,
    p95_ns: u128,
    ops_per_sec: f64,
    cumulative_ops: usize,
) -> std::io::Result<()> {
    writeln!(
        bw,
        "{},{},{},{},{},{},{},{},{:.2},{}",
        db_size,
        batch_index,
        batch_ops,
        batch_wall_ns,
        total_sample_ns,
        mean_ns,
        p50_ns,
        p95_ns,
        ops_per_sec,
        cumulative_ops
    )?;
    // We rely on BufWriter; don't flush each line for speed. Caller can drop/flush at end.
    Ok(())
}

fn compute_stats(mut samples: Vec<u128>) -> (u128, u128, u128, u128) {
    // returns (total_ns, mean_ns, p50_ns, p95_ns)
    if samples.is_empty() {
        return (0, 0, 0, 0);
    }
    samples.sort_unstable();
    let total: u128 = samples.iter().sum();
    let mean = total / (samples.len() as u128);
    let p50 = samples[samples.len() / 2];
    // integer-safe p95 index:
    // e.g., for len=1 -> ((1*95+99)/100)-1 = (194/100)-1 = 1-1 = 0
    let p95_idx = ((samples.len() * 95 + 99) / 100).saturating_sub(1);
    let p95 = samples[p95_idx];
    (total, mean, p50, p95)
}

fn bench_set_with_migration(out_path: &std::path::Path) {
    println!("bench_set_with_migration -> CSV: {}", out_path.display());

    let tmp = TempDir::new().expect("tempdir");
    let cache = TurboCache::new(tmp.path(), INITIAL_CAP).expect("create cache");

    // Pre-populate PREPOP keys (we had PREPOP constant but never used it before)
    {
        let mut rng = StdRng::seed_from_u64(RNG_SEED);
        for id in 0..PREPOP {
            let key = make_key(id as u64);
            let val = make_val_from_rng(&mut rng);
            cache.set(&key, &val).expect("pre-insert (bench_set)");
        }
    }

    let mut csv = create_csv(out_path).expect("create csv");
    let mut id_counter: u64 = PREPOP as u64;
    let mut rng = StdRng::seed_from_u64(RNG_SEED + 9999);
    let mut cumulative_ops = 0usize;

    for batch in 0..NUM_BATCHES {
        // measure per-op latencies for this batch
        let mut samples: Vec<u128> = Vec::with_capacity(BATCH_SIZE);
        let batch_start = Instant::now();

        let db_size_before = id_counter as usize;

        for _ in 0..BATCH_SIZE {
            let key = make_key(id_counter);
            let val = make_val_from_rng(&mut rng);

            let op_start = Instant::now();
            cache.set(&key, &val).expect("set should succeed (bench)");
            let elapsed = op_start.elapsed().as_nanos();
            samples.push(elapsed);

            id_counter += 1;
        }

        let batch_wall_ns = batch_start.elapsed().as_nanos();
        cumulative_ops += BATCH_SIZE;

        let (total_sample_ns, mean_ns, p50_ns, p95_ns) = compute_stats(samples);

        // compute ops/sec using wall-clock batch time
        let secs = (batch_wall_ns as f64) / 1_000_000_000.0;
        let ops_per_sec = (BATCH_SIZE as f64) / secs.max(1e-12);

        append_csv_line(
            &mut csv,
            db_size_before,
            batch,
            BATCH_SIZE,
            batch_wall_ns,
            total_sample_ns,
            mean_ns,
            p50_ns,
            p95_ns,
            ops_per_sec,
            cumulative_ops,
        )
        .expect("write csv");

        println!(
            "set: batch {:03} db_size={} ops={} wall_ms={:.3} mean_ns={} p50_ns={} p95_ns={} ops/s={:.2}",
            batch,
            db_size_before,
            BATCH_SIZE,
            (batch_wall_ns as f64) / 1_000_000.0,
            mean_ns,
            p50_ns,
            p95_ns,
            ops_per_sec
        );
    }

    // ensure csv is flushed
    csv.flush().expect("flush csv");
}

fn bench_get_during_migration(out_path: &std::path::Path) {
    println!("bench_get_during_migration -> CSV: {}", out_path.display());

    let tmp = TempDir::new().expect("tempdir");
    let cache = Arc::new(TurboCache::new(tmp.path(), INITIAL_CAP).expect("create cache"));
    let measured_key_count = 5000usize;

    // pre-insert measured_key_count keys deterministically
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
        let mut samples: Vec<u128> = Vec::with_capacity(BATCH_SIZE);
        let batch_start = Instant::now();

        // read db size from the bg counter (gives approximate current size)
        let db_size_now = bg_counter.load(Ordering::SeqCst);

        for _ in 0..BATCH_SIZE {
            let idx = rng.random_range(0..measured_key_count) as u64;
            let key = make_key(idx);

            let op_start = Instant::now();
            let _ = cache.get(&key).expect("get");
            let elapsed = op_start.elapsed().as_nanos();

            samples.push(elapsed);
        }

        let batch_wall_ns = batch_start.elapsed().as_nanos();
        cumulative_ops += BATCH_SIZE;

        let (total_sample_ns, mean_ns, p50_ns, p95_ns) = compute_stats(samples);

        let secs = (batch_wall_ns as f64) / 1_000_000_000.0;
        let ops_per_sec = (BATCH_SIZE as f64) / secs.max(1e-12);

        append_csv_line(
            &mut csv,
            db_size_now,
            batch,
            BATCH_SIZE,
            batch_wall_ns,
            total_sample_ns,
            mean_ns,
            p50_ns,
            p95_ns,
            ops_per_sec,
            cumulative_ops,
        )
        .expect("write csv");

        println!(
            "get: batch {:03} db_size={} ops={} wall_ms={:.3} mean_ns={} p50_ns={} p95_ns={} ops/s={:.2}",
            batch,
            db_size_now,
            BATCH_SIZE,
            (batch_wall_ns as f64) / 1_000_000.0,
            mean_ns,
            p50_ns,
            p95_ns,
            ops_per_sec
        );
    }

    bg_handle.join().expect("bg join");
    csv.flush().expect("flush csv");
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
        let batch_start = Instant::now();

        // db_size at start of batch (approx)
        let db_size_now = bg_counter.load(Ordering::SeqCst);

        let mut samples: Vec<u128> = Vec::with_capacity(BATCH_SIZE);
        for j in 0..BATCH_SIZE {
            let id = (start_id + j) as u64;
            let key = make_key(id);

            let op_start = Instant::now();
            let _ = cache.del(&key).expect("del");
            let elapsed = op_start.elapsed().as_nanos();
            samples.push(elapsed);
        }

        let batch_wall_ns = batch_start.elapsed().as_nanos();
        cumulative_ops += BATCH_SIZE;

        let (total_sample_ns, mean_ns, p50_ns, p95_ns) = compute_stats(samples);

        let secs = (batch_wall_ns as f64) / 1_000_000_000.0;
        let ops_per_sec = (BATCH_SIZE as f64) / secs.max(1e-12);

        append_csv_line(
            &mut csv,
            db_size_now,
            batch,
            BATCH_SIZE,
            batch_wall_ns,
            total_sample_ns,
            mean_ns,
            p50_ns,
            p95_ns,
            ops_per_sec,
            cumulative_ops,
        )
        .expect("write csv");

        println!(
            "del: batch {:03} db_size={} ops={} wall_ms={:.3} mean_ns={} p50_ns={} p95_ns={} ops/s={:.2}",
            batch,
            db_size_now,
            BATCH_SIZE,
            (batch_wall_ns as f64) / 1_000_000.0,
            mean_ns,
            p50_ns,
            p95_ns,
            ops_per_sec
        );
    }

    bg_handle.join().expect("bg join");
    csv.flush().expect("flush csv");
}

fn main() {
    bench_set_with_migration(std::path::Path::new("bench_set.csv"));
    bench_get_during_migration(std::path::Path::new("bench_get.csv"));
    bench_del_during_migration(std::path::Path::new("bench_del.csv"));

    println!("CSV files written");
}
