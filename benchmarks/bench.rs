use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::time::Duration;
use tempfile::TempDir;
use turbocache::TurboCache;

const N_KEYS: usize = 1_000;
const INIT_CAP: usize = 128 * 5; // 5 Kib
const KEY_LEN: usize = 16;
const VAL_LEN: usize = 32;
const SEED: u64 = 42;

fn gen_dataset() -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(SEED);

    (0..N_KEYS)
        .map(|_| {
            let key = (0..KEY_LEN).map(|_| rng.random()).collect();
            let val = (0..VAL_LEN).map(|_| rng.random()).collect();

            (key, val)
        })
        .collect()
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("turbocache_set");
    group.throughput(Throughput::Elements(N_KEYS as u64));

    let data = gen_dataset();
    let tmp = TempDir::new().expect("tmp");

    let mut cache = TurboCache::new(tmp.path().to_path_buf(), INIT_CAP).unwrap();

    group.bench_function("set_all", |b| {
        b.iter(|| {
            for (k, v) in data.iter() {
                cache
                    .set(black_box(k.clone()), black_box(v.clone()))
                    .unwrap();
            }
        })
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("turbocache_get");
    group.throughput(Throughput::Elements(N_KEYS as u64));

    let data = gen_dataset();
    let tmp = TempDir::new().expect("tmp");
    let mut cache = TurboCache::new(tmp.path().to_path_buf(), INIT_CAP).unwrap();

    for (k, v) in &data {
        cache.set(k.clone(), v.clone()).unwrap();
    }

    group.bench_function("get_all", |b| {
        b.iter(|| {
            for (k, _) in &data {
                let _ = cache.get(black_box(k.clone())).unwrap();
            }
        })
    });

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("turbocache_remove");
    group.throughput(Throughput::Elements(N_KEYS as u64));

    let data = gen_dataset();
    let tmp = TempDir::new().expect("tmp");
    let mut cache = TurboCache::new(tmp.path().to_path_buf(), INIT_CAP).unwrap();

    for (k, v) in &data {
        cache.set(k.clone(), v.clone()).unwrap();
    }

    group.bench_function("remove_all", |b| {
        b.iter(|| {
            for (k, _) in &data {
                let _ = cache.del(black_box(k.clone())).unwrap();
            }
        })
    });

    group.finish();
}

fn configured_criterion() -> Criterion {
    Criterion::default()
        .configure_from_args()
        .sample_size(1000)
        .measurement_time(Duration::from_secs(15))
        .noise_threshold(0.05)
        .with_plots()
}

criterion_group! {
    name = benches;
    config = configured_criterion();
    targets = bench_set, bench_get, bench_remove
}
criterion_main!(benches);
