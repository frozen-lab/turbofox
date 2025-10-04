use divan::Bencher;
use sphur::Sphur;
use std::path::PathBuf;
use turbocache::{TurboCache, TurboCfg};

fn gen_random_kv(ksize: usize, vsize: usize) -> (Vec<u8>, Vec<u8>) {
    let mut sphur = Sphur::new();
    let mut k = Vec::with_capacity(ksize);
    let mut v = Vec::with_capacity(vsize);

    while k.len() < ksize {
        k.extend_from_slice(&sphur.gen_u64().to_ne_bytes());
    }

    while v.len() < vsize {
        v.extend_from_slice(&sphur.gen_u64().to_ne_bytes());
    }

    // trim to exact size (avoid overshot)
    k.truncate(ksize);
    v.truncate(vsize);

    (k, v)
}

fn get_path(flag: String) -> PathBuf {
    let path = std::env::temp_dir().join(format!("tc-bench/{flag}"));

    match std::fs::remove_dir_all(&path) {
        Ok(_) => {}
        Err(_) => {}
    }

    path
}

fn setup_cache(flag: String) -> TurboCache {
    let path = get_path(flag);
    let cfg = TurboCfg::default().rows(128);

    TurboCache::new(path, cfg).unwrap()
}

#[divan::bench]
fn set_small(b: Bencher) {
    let mut cache = setup_cache("set_small".into());
    let (k, v) = gen_random_kv(8, 8);

    b.bench_local(move || {
        cache.set(&k, &v).unwrap();
    });
}

#[divan::bench]
fn set_large(b: Bencher) {
    let mut cache = setup_cache("set_large".into());
    let (k, v) = gen_random_kv(256, 4096);

    b.bench_local(move || {
        cache.set(&k, &v).unwrap();
    });
}

#[divan::bench]
fn get_hit(b: Bencher) {
    let mut cache = setup_cache("get_hit".into());

    let (k, v) = gen_random_kv(8, 8);
    cache.set(&k, &v).expect("Insert pair into cache");

    b.bench_local(move || {
        cache.get(&k).expect("Unable to fetch key from cache");
    });
}

#[divan::bench]
fn get_miss(b: Bencher) {
    let mut cache = setup_cache("get_miss".into());
    let (k, _) = gen_random_kv(8, 8);

    b.bench_local(move || {
        cache.get(&k).unwrap();
    });
}

#[divan::bench]
fn del_hit(b: Bencher) {
    let mut cache = setup_cache("del_hit".into());

    let (k, v) = gen_random_kv(8, 8);
    cache.set(&k, &v).expect("Insert pair into cache");

    b.bench_local(move || {
        cache.del(&k).unwrap();
    });
}

#[divan::bench]
fn del_miss(b: Bencher) {
    let mut cache = setup_cache("del_miss".into());
    let (k, _) = gen_random_kv(8, 8);

    b.bench_local(move || {
        cache.del(&k).unwrap();
    });
}

fn main() {
    divan::main();
}
