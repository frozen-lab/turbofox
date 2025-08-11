use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::time::Duration;
use turbocache::TurboCache;

const SAMPLE: usize = 256;
const RNG_SEED: u64 = 42;
const KEY_LEN: usize = 64;
const VAL_LEN: usize = 128;

const INIT_CAP: usize = 1 << 20; // 1,048,576

fn gen_pair_from_rng(rng: &mut StdRng) -> (Vec<u8>, Vec<u8>) {
    let key: Vec<u8> = (0..KEY_LEN).map(|_| rng.random()).collect();
    let val: Vec<u8> = (0..VAL_LEN).map(|_| rng.random()).collect();

    (key, val)
}

fn create_db_with_cap(erase_old: bool, cap: usize) -> TurboCache {
    let path = std::env::temp_dir().join("tc-bench");

    if erase_old {
        let _ = std::fs::remove_dir_all(&path);
    }

    TurboCache::new(path, cap).unwrap()
}

fn configured_criterion() -> Criterion {
    Criterion::default()
        .configure_from_args()
        .sample_size(SAMPLE)
        .measurement_time(Duration::from_secs(15))
        .warm_up_time(Duration::from_secs(3))
        .noise_threshold(0.05)
        .with_plots()
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");
    group.throughput(Throughput::Elements(1));

    let cache = create_db_with_cap(true, INIT_CAP);

    group.bench_function("insert_unique", |b| {
        let mut rng = StdRng::seed_from_u64(RNG_SEED);
        b.iter_batched(
            || gen_pair_from_rng(&mut rng),
            |(k, v)| {
                black_box(cache.set(&k, &v).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));

    let cache = create_db_with_cap(true, INIT_CAP);
    let mut rng = StdRng::seed_from_u64(RNG_SEED);

    const PREPOP_KEYS: usize = 10_000;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(PREPOP_KEYS);

    for _ in 0..PREPOP_KEYS {
        let (k, v) = gen_pair_from_rng(&mut rng);

        cache.set(&k, &v).unwrap();
        keys.push(k);
    }

    // Reset RNG for reproducible random picks in the per-iteration setup
    let mut rnd_picker = StdRng::seed_from_u64(RNG_SEED + 1);

    group.bench_function("get_random", |b| {
        b.iter_batched(
            || {
                let idx = rnd_picker.random_range(0..keys.len());

                keys[idx].clone()
            },
            |k| {
                black_box(cache.get(&k).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_del(c: &mut Criterion) {
    let mut group = c.benchmark_group("del");
    group.throughput(Throughput::Elements(1));

    let cache = create_db_with_cap(true, INIT_CAP);
    let mut rng = StdRng::seed_from_u64(RNG_SEED + 2);

    group.bench_function("del_then_reinsert", |b| {
        b.iter_batched(
            || {
                let (k, v) = gen_pair_from_rng(&mut rng);
                cache.set(&k, &v).unwrap();

                k
            },
            |k| {
                black_box(cache.del(&k).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = configured_criterion();
    targets = bench_set, bench_get, bench_del,
}
criterion_main!(benches);
