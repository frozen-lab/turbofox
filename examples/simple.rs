use turbocache::TurboCache;

fn main() {
    let path = std::env::temp_dir().join("cache-dir");
    let cache = TurboCache::new(path, 1024).unwrap();

    for i in 0..5 {
        cache.set(&vec![i], &vec![i * 10]).unwrap();
    }

    assert_eq!(cache.get(&vec![3]).unwrap(), Some(vec![30]));
    assert_eq!(cache.del(&vec![3]).unwrap(), Some(vec![30]));
}
