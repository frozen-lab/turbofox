use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;
use turbocache::TurboCache;

const SAMPLE: usize = 2_048;
const INIT_CAP: usize = 1024 * 5;
const KEY_LEN: usize = 128;
const VAL_LEN: usize = 256;
const SEED: u64 = 42;

fn gen_pair() -> (Vec<u8>, Vec<u8>) {
    let mut rng = StdRng::seed_from_u64(SEED);

    let key = (0..KEY_LEN).map(|_| rng.random()).collect();
    let val = (0..VAL_LEN).map(|_| rng.random()).collect();

    (key, val)
}

fn create_db(erase_old: bool) -> TurboCache<PathBuf> {
    let path = std::env::temp_dir().join("tc-bench");

    if erase_old {
        std::fs::remove_dir_all(&path).unwrap();
    }

    TurboCache::new(path, INIT_CAP).unwrap()
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");
    group.throughput(Throughput::Elements(1)); // calculating ops/sec

    group.bench_function("set", |b| {
        let mut cache = create_db(true);

        b.iter_batched(
            || {
                let pair = gen_pair();

                pair
            },
            |pair| {
                black_box(cache.set(pair.0, pair.1).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1)); // calculating ops/sec

    // pre-populate cache
    let mut cache = create_db(true);

    for _ in 0..SAMPLE {
        let (k, v) = gen_pair();

        cache.set(k.clone(), v).unwrap();
    }

    group.bench_function("get", |b| {
        let cache = create_db(false);

        b.iter_batched(
            || {
                let (k, _) = gen_pair();

                k
            },
            |k| {
                black_box(cache.get(k).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_del(c: &mut Criterion) {
    let mut group = c.benchmark_group("del");
    group.throughput(Throughput::Elements(1));

    // pre-populate cache
    let mut cache = create_db(true);

    for _ in 0..SAMPLE {
        let (k, v) = gen_pair();

        cache.set(k.clone(), v).unwrap();
    }

    group.bench_function("del", |b| {
        let mut cache = create_db(false);

        b.iter_batched(
            || {
                let (k, _) = gen_pair();

                k
            },
            |k| {
                black_box(cache.del(k).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn configured_criterion() -> Criterion {
    Criterion::default()
        .configure_from_args()
        .sample_size(SAMPLE)
        .measurement_time(Duration::from_secs(20))
        .warm_up_time(Duration::from_secs(5))
        .noise_threshold(0.05)
        .with_plots()
}

criterion_group! {
    name = benches;
    config = configured_criterion();
    targets = bench_set, bench_get, bench_del,
}
criterion_main!(benches);
