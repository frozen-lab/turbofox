use turbocache::{TurboCache, TurboResult};

const CACHE_INITIAL_CAP: usize = 1024;

fn main() -> TurboResult<()> {
    let path = std::env::temp_dir().join("cache-dir");
    let mut cache = TurboCache::new(path, CACHE_INITIAL_CAP).unwrap();

    for i in 0..5 {
        cache.set(vec![i], vec![i * 10]).unwrap();
    }

    assert_eq!(cache.get(vec![3]).unwrap(), Some(vec![30]));
    assert_eq!(cache.del(vec![3]).unwrap(), Some(vec![30]));

    Ok(())
}
