use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::NamedTempFile;
use turbocache::table::Table;

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    for &n in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            // Create a fresh table with capacity = n
            let temp = NamedTempFile::new().unwrap();
            let mut table = Table::create_new_temp(temp.path(), n).unwrap();

            // Pre-generate (key, value) pairs
            let data: Vec<([u8; 32], Vec<u8>)> = (0..n)
                .map(|i| {
                    let mut key = [0u8; 32];
                    key[..8].copy_from_slice(&i.to_le_bytes());
                    let value = vec![0u8; 128];
                    (key, value)
                })
                .collect();

            b.iter(|| {
                for (key, val) in data.iter() {
                    table.insert(key, val).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let n = 100_000;
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(n as u64));

    // Build a table and insert n elements once
    let temp = NamedTempFile::new().unwrap();
    let mut table = Table::create_new_temp(temp.path(), n).unwrap();
    let keys: Vec<[u8; 32]> = (0..n)
        .map(|i| {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&i.to_le_bytes());
            table.insert(&key, &[0u8; 128]).unwrap();
            key
        })
        .collect();

    group.bench_function("get_existing", |b| {
        b.iter(|| {
            for key in keys.iter() {
                table.get(key).unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_insert, bench_get);
criterion_main!(benches);
