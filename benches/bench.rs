use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tempfile::tempdir;
use turbocache::TurboCache;

fn bench_cache_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_set");
    for &n in &[1_000u32, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &count| {
            b.iter(|| {
                let dir = tempdir().unwrap();
                let mut cache = TurboCache::open(dir.path()).unwrap();
                for i in 0..count {
                    cache.set(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
                }

                drop(cache);
            });
        });
    }
    group.finish();
}

fn bench_cache_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_get");
    for &n in &[1_000u32, 10_000, 100_000] {
        // prepopulate once per group
        let dir = tempdir().unwrap();
        {
            let mut cache = TurboCache::open(dir.path()).unwrap();
            for i in 0..n {
                cache.set(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
            }

            drop(cache);
        }

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &count| {
            // re-open for each bench iteration
            b.iter(|| {
                let cache = TurboCache::open(dir.path()).unwrap();
                for key in 0..count {
                    let _ = cache.get(&key.to_be_bytes()).unwrap();
                }
            });
        });
    }

    group.finish();
}

fn bench_cache_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_remove");
    for &n in &[1_000u32, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &count| {
            b.iter(|| {
                let dir = tempdir().unwrap();
                let mut cache = TurboCache::open(dir.path()).unwrap();

                for i in 0..count {
                    cache.set(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
                }

                for i in 0..count {
                    cache.remove(&i.to_be_bytes()).unwrap();
                }

                drop(cache);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_cache_set, bench_cache_get, bench_cache_remove);
criterion_main!(benches);
