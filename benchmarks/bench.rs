use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;
use turbocache::TurboCache;

const SAMPLE: usize = 1_000;
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

fn create_db() -> TurboCache<PathBuf> {
    let path = std::env::temp_dir().join("tc-bench");

    TurboCache::new(path, INIT_CAP).unwrap()
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");
    group.throughput(Throughput::Elements(1)); // calculating ops/sec

    group.bench_function("set", |b| {
        let mut cache = create_db();

        b.iter_batched(
            || gen_pair(),
            |(k, v)| {
                black_box(cache.set(k, v).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1)); // calculating ops/sec

    group.bench_function("get", |b| {
        b.iter_batched(
            || {
                let mut cache = create_db();
                let (k, v) = gen_pair();

                cache.set(k.clone(), v).unwrap();

                (cache, k)
            },
            |(cache, k)| {
                black_box(cache.get(k).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_del(c: &mut Criterion) {
    let mut group = c.benchmark_group("del");
    group.throughput(Throughput::Elements(1)); // calculating ops/sec

    group.bench_function("del", |b| {
        b.iter_batched(
            || {
                let mut cache = create_db();
                let (k, v) = gen_pair();

                cache.set(k.clone(), v).unwrap();

                (cache, k)
            },
            |(mut cache, k)| {
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
        .measurement_time(Duration::from_secs(15))
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
