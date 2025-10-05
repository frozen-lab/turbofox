use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use sphur::Sphur;
use std::{path::PathBuf, time::Duration};
use turbocache::{TurboCache, TurboCfg};

const SEED1: u64 = 0x1234567890ABCDEF;
const SEED2: u64 = 0xFEDCBA9876543210;
const SEED3: u64 = 0x0F1E2D3C4B5A6978;
const SAMPLE: usize = 64;
const KV_MIN: usize = 8;
const K_MAX: usize = 128;
const V_MAX: usize = 256;

fn gen_random_kv(sphur: &mut Sphur) -> (Vec<u8>, Vec<u8>) {
    let ksize = sphur.gen_range((KV_MIN as u64)..(K_MAX as u64)) as usize;
    let vsize = sphur.gen_range((KV_MIN as u64)..(V_MAX as u64)) as usize;

    let mut k = Vec::with_capacity(ksize);
    let mut v = Vec::with_capacity(vsize);

    while k.len() < ksize {
        k.extend_from_slice(&sphur.gen_u64().to_ne_bytes());
    }

    while v.len() < vsize {
        v.extend_from_slice(&sphur.gen_u64().to_ne_bytes());
    }

    k.truncate(ksize);
    v.truncate(vsize);

    (k, v)
}

fn get_path(flag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("tc-bench/{flag}"));
    let _ = std::fs::remove_dir_all(&path);

    path
}

fn setup_cache(flag: &str) -> TurboCache {
    let path = get_path(flag);
    let cfg = TurboCfg::default().rows(128);

    TurboCache::new(path, cfg).expect("Failed to create TurboCache")
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");
    let mut sphur = Sphur::new_seeded(SEED1);

    group.bench_function("variable_kv", |b| {
        b.iter_batched(
            || {
                let cache = setup_cache("set");
                let (k, v) = gen_random_kv(&mut sphur);

                (cache, k, v)
            },
            |(mut cache, k, v)| {
                // HACK: This is needed cause inserts beyound 80% of cap are not allowed
                // and will throw an error! This'll change in future!
                if let Err(_) = cache.set(&k, &v) {
                    return;
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

///
/// Bench w/ approx 80% hit and 20% miss
///
fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    let mut sphur = Sphur::new_seeded(SEED2);

    group.bench_function("hit80_miss20", |b| {
        b.iter_batched(
            || {
                let mut cache = setup_cache("get");
                let mut keys = Vec::new();

                // Add 80% hit keys
                for _ in 0..80 {
                    let (k, v) = gen_random_kv(&mut sphur);
                    cache.set(&k, &v).unwrap();
                    keys.push(k);
                }

                // Add 20% miss
                for _ in 0..20 {
                    let (k, _) = gen_random_kv(&mut sphur);
                    keys.push(k);
                }

                (cache, keys)
            },
            |(mut cache, keys)| {
                keys.iter().for_each(|k| {
                    let _ = cache.get(k);
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

///
/// Bench w/ 80% hit and 20% miss
///
fn bench_del(c: &mut Criterion) {
    let mut group = c.benchmark_group("del");
    let mut sphur = Sphur::new_seeded(SEED3);

    group.bench_function("hit80_miss20", |b| {
        b.iter_batched(
            || {
                let mut cache = setup_cache("del");
                let mut keys = Vec::with_capacity(100);

                // Add 80% hit keys
                for _ in 0..80 {
                    let (k, v) = gen_random_kv(&mut sphur);
                    cache.set(&k, &v).unwrap();
                    keys.push(k);
                }

                // 20% misses
                for _ in 0..20 {
                    let (k, _) = gen_random_kv(&mut sphur);
                    keys.push(k);
                }

                (cache, keys)
            },
            |(mut cache, keys)| {
                keys.iter().for_each(|k| {
                    let _ = cache.del(k);
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn configured_criterion() -> Criterion {
    Criterion::default()
        .configure_from_args()
        .sample_size(SAMPLE)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(2))
        .noise_threshold(0.05)
        .with_plots()
}

criterion_group! {
    name = benches;
    config = configured_criterion();
    targets = bench_set, bench_get, bench_del,
}
criterion_main!(benches);
