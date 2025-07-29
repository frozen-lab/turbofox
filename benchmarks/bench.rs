use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;
use turbocache::TurboCache;

const SAMPLE: usize = 1_000;
const INIT_CAP: usize = 1024 * 5; // 5 Kib
const KEY_LEN: usize = 128;
const VAL_LEN: usize = 256;
const SEED: u64 = 42;

fn gen_pair() -> (Vec<u8>, Vec<u8>) {
    let mut rng = StdRng::seed_from_u64(SEED);

    let key = (0..KEY_LEN).map(|_| rng.random()).collect();
    let val = (0..VAL_LEN).map(|_| rng.random()).collect();

    (key, val)
}

fn create_db() -> TurboCache<PathBuf> {
    let mut path = std::env::temp_dir();
    path.push("tc-bench");

    let cache = TurboCache::new(path, INIT_CAP).unwrap();

    cache
}

fn set(c: &mut Criterion) {
    let mut group = c.benchmark_group("turbocache_set");
    group.throughput(Throughput::Elements(1));

    let (k, v) = gen_pair();
    let mut cache = create_db();

    group.bench_function("set", |b| {
        b.iter(|| {
            cache
                .set(black_box(k.clone()), black_box(v.clone()))
                .unwrap();
        })
    });

    group.finish();
}

fn get(c: &mut Criterion) {
    let mut group = c.benchmark_group("turbocache_get");
    group.throughput(Throughput::Elements(1));

    let (k, v) = gen_pair();
    let mut cache = create_db();

    cache.set(k.clone(), v.clone()).unwrap();

    group.bench_function("get", |b| {
        b.iter(|| {
            let _ = cache.get(black_box(k.clone())).unwrap();
        })
    });

    group.finish();
}

fn del(c: &mut Criterion) {
    let mut group = c.benchmark_group("turbocache_remove");
    group.throughput(Throughput::Elements(1));

    let (k, v) = gen_pair();
    let mut cache = create_db();

    cache.set(k.clone(), v.clone()).unwrap();

    group.bench_function("del", |b| {
        b.iter(|| {
            let _ = cache.del(black_box(k.clone())).unwrap();
        })
    });

    group.finish();
}

fn configured_criterion() -> Criterion {
    Criterion::default()
        .configure_from_args()
        .sample_size(SAMPLE)
        .measurement_time(Duration::from_secs(15))
        .warm_up_time(Duration::from_secs(5))
        .noise_threshold(0.05)
        .with_plots()
}

criterion_group! {
    name = benches;
    config = configured_criterion();
    targets = set, get, del
}
criterion_main!(benches);
