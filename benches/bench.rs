use divan::black_box;
use tempfile::tempdir;
use turbocache::TurboCache;

fn main() {
    divan::main();
}

fn make_db() -> TurboCache {
    let dir = tempdir().unwrap();
    TurboCache::open(dir.path()).unwrap()
}

#[divan::bench_group(sample_count = 100, sample_size = 1000)]
mod db_ops {
    use super::*;

    #[divan::bench]
    fn bench_set() {
        let db = make_db();
        black_box(db.set(b"mykey", b"myval").unwrap());
    }

    #[divan::bench]
    fn bench_get() {
        let db = make_db();
        db.set(b"mykey", b"myval").unwrap();
        black_box(db.get(b"mykey").unwrap());
    }

    #[divan::bench]
    fn bench_remove() {
        let db = make_db();
        db.set(b"mykey", b"myval").unwrap();
        black_box(db.remove(b"mykey").unwrap());
    }
}
