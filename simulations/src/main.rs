use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;
use turbocache::{TResult, TurboCache};

struct TrialStats {
    inserts: usize,
    failed_row: usize,
    avg_klen: f64,
    avg_vlen: f64,
}

fn run_one_trial(seed: u64) -> TResult<TrialStats> {
    let mut rng = StdRng::seed_from_u64(seed);
    let tmp = TempDir::new().unwrap();
    let cache = TurboCache::new(tmp.path().to_path_buf())?;

    let mut count = 0;
    let mut total_k = 0usize;
    let mut total_v = 0usize;

    loop {
        let klen = rng.random_range(4..=64);
        let vlen = rng.random_range(8..=256);
        let key: Vec<u8> = (0..klen).map(|_| rng.random()).collect();
        let val: Vec<u8> = (0..vlen).map(|_| rng.random()).collect();

        if let Err(e) = cache.set(&key, &val) {
            let msg = e.to_string();
            if let Some(rest) = msg.strip_prefix("row ") {
                let row_idx = rest
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(usize::MAX);
                let avg_k = total_k as f64 / count as f64;
                let avg_v = total_v as f64 / count as f64;
                return Ok(TrialStats {
                    inserts: count,
                    failed_row: row_idx,
                    avg_klen: avg_k,
                    avg_vlen: avg_v,
                });
            }
            return Err(e);
        } else {
            total_k += klen;
            total_v += vlen;
            count += 1;
        }
    }
}

fn main() -> TResult<()> {
    const TRIALS: usize = 30;
    let mut stats = Vec::new();

    for t in 0..TRIALS {
        let s = run_one_trial(t as u64)?;
        println!(
            "Trial {:>2}: full after {:>4} inserts on row {:>2}; avg k/v len = {:.1}/{:.1}",
            t + 1,
            s.inserts,
            s.failed_row,
            s.avg_klen,
            s.avg_vlen
        );
        stats.push(s);
    }

    let inserts: Vec<_> = stats.iter().map(|s| s.inserts).collect();
    let avg_k: Vec<_> = stats.iter().map(|s| s.avg_klen).collect();
    let avg_v: Vec<_> = stats.iter().map(|s| s.avg_vlen).collect();

    let stats = insert_stats(&inserts);

    println!("\nSummary:");
    println!(
        "  Inserts before full: min = {}, max = {}, avg = {:.1}",
        stats.0, stats.1, stats.2,
    );
    println!(
        "  Avg key len = {:.1}, Avg val len = {:.1}",
        avg_stats(&avg_k),
        avg_stats(&avg_v)
    );

    Ok(())
}

fn insert_stats(xs: &[usize]) -> (usize, usize, f64) {
    let min = *xs.iter().min().unwrap();
    let max = *xs.iter().max().unwrap();
    let avg = xs.iter().sum::<usize>() as f64 / xs.len() as f64;
    (min, max, avg)
}

fn avg_stats(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}
