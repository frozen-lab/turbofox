use turbocache::TurboCache;

fn main() {
    const INITIAL_CAPACITY: usize = 1024;
    let path = std::env::temp_dir().join("cache-dir");
    let cache = TurboCache::new(path, INITIAL_CAPACITY).unwrap();

    // inserts 5 kev-value pairs into the cache
    for i in 0..5 {
        cache.set(&vec![i], &vec![i * 10]).unwrap();
    }

    // fetch key(3) from cache
    assert_eq!(cache.get(&vec![3]).unwrap(), Some(vec![30]));

    // delete key(3) from cache
    assert_eq!(cache.del(&vec![3]).unwrap(), Some(vec![30]));

    let mut keys = Vec::new();

    // iterate over all keys inserted in cache
    for res in cache.iter().unwrap() {
        let (k, _) = res.unwrap();

        keys.push(k);
    }

    // match keys vector's length w/ total number for keys in cache
    assert_eq!(keys.len(), cache.total_count().unwrap());
}
