use std::time::Instant;
use tempfile::TempDir;
use turbocache::TurboCache;

fn create_cache(capacity: usize, name: &'static str) -> TurboCache {
    let tmp = TempDir::new().unwrap();
    TurboCache::new(tmp.path().to_path_buf(), name, capacity).unwrap()
}

fn fetch_system_info() {
    let os = std::env::consts::OS;
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").expect("Failed to read /proc/cpuinfo");

    let mut cpu = "Unknown CPU".to_string();
    let mut flags = String::new();

    for line in cpuinfo.lines() {
        if line.starts_with("model name") && cpu == "Unknown CPU" {
            if let Some(pos) = line.find(':') {
                cpu = line[(pos + 1)..].trim().to_string();
            }
        }

        if line.starts_with("flags") && flags.is_empty() {
            if let Some(pos) = line.find(':') {
                flags = line[(pos + 1)..].trim().to_string();
            }
        }
    }

    let isa: String = {
        let f = flags.split_whitespace().collect::<Vec<_>>();

        if f.contains(&"avx2") {
            "avx2".to_string()
        } else if f.contains(&"avx") {
            "avx".to_string()
        } else {
            "sse2".to_string()
        }
    };

    let meminfo = std::fs::read_to_string("/proc/meminfo").expect("Failed to read /proc/meminfo");
    let mut total_mem_kb: u64 = 0;

    for line in meminfo.lines() {
        if line.starts_with("MemTotal:") {
            let parts: Vec<&str> = line.split_whitespace().collect();

            if parts.len() >= 2 {
                total_mem_kb = parts[1].parse::<u64>().expect("Failed to parse MemTotal");
            }

            break;
        }
    }

    let total_mem_gb = total_mem_kb as f64 / (1024.0 * 1024.0);

    println!("\n");
    println!("| System Info     | Value                          |");
    println!("|:---------------:|:------------------------------:|");
    println!("| OS              | {:<30}   |", os);
    println!("| CPU             | {:<30}   |", cpu);
    println!("| SIMD ISA        | {:<30}   |", isa);
    println!("| RAM (GB)        | {:>28.2} |", total_mem_gb);
}

fn bench_operations<F>(mut func: F, iter: usize, ops: usize) -> f64
where
    F: FnMut() -> (),
{
    let mut results = Vec::with_capacity(iter);

    // Warm-up
    for _ in 0..(ops.wrapping_div(2)) {
        func();
    }

    for _ in 0..iter {
        let start = Instant::now();

        for _ in 0..ops {
            func();
        }

        let elapsed_us = start.elapsed().as_secs_f64() * 1e6;
        let us_per_op = elapsed_us / ops as f64;
        results.push(us_per_op);
    }

    results.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // median
    results[iter / 2]
}

fn bench_set() -> f64 {
    let mut cache = create_cache(2_500, "set_ops");
    let bench = bench_operations(
        || {
            let _ = cache.set(&[1], &[2]);
        },
        50,
        10000,
    );

    bench
}

fn bench_get() -> f64 {
    let mut cache = create_cache(2_500, "get_ops");
    let bench = bench_operations(
        || {
            let _ = cache.get(&[1]);
        },
        50,
        10000,
    );

    bench
}

fn bench_del() -> f64 {
    let mut cache = create_cache(2_500, "del_ops");
    let bench = bench_operations(
        || {
            let _ = cache.del(&[1]);
        },
        50,
        10000,
    );

    bench
}

fn main() {
    println!("## Benchmarks");
    fetch_system_info();

    let set_us = bench_set();
    let get_us = bench_get();
    let del_us = bench_del();

    println!("\n");
    println!("| API        | Throughput (µs/ops) |");
    println!("|:----------:|:--------------------:|");
    println!("| set        | {:>20.2} |", set_us,);
    println!("| get        | {:>20.2} |", get_us,);
    println!("| del        | {:>20.2} |", del_us,);
}
