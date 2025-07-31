use std::time::Duration;
use turbocache::{TurboCache, TurboResult};

const CACHE_INITIAL_CAP: usize = 1024;

fn main() -> TurboResult<()> {
    let path = std::env::temp_dir().join("cache-dir-m-ex");
    let cache = TurboCache::new(path, CACHE_INITIAL_CAP)?;

    let mut c1 = cache.clone();
    let t1 = std::thread::spawn(move || -> TurboResult<()> {
        for i in 0..10 {
            let key = vec![i as u8];
            let value = vec![(i * 10) as u8];

            c1.set(key, value).unwrap();

            std::thread::sleep(Duration::from_millis(1));
        }

        Ok(())
    });

    let mut c2 = cache.clone();
    let t2 = std::thread::spawn(move || -> TurboResult<()> {
        for i in 10..100 {
            let key = vec![i as u8];
            let value = vec![(i * 10) as u8];

            c2.set(key, value).unwrap();

            std::thread::sleep(Duration::from_millis(1));
        }

        Ok(())
    });

    t1.join().unwrap()?;
    t2.join().unwrap()?;

    // read all the values all at once
    for i in 0..100 {
        let key = vec![i as u8];
        let expected_value = vec![(i * 10) as u8];

        assert_eq!(cache.get(key).unwrap(), Some(expected_value));
    }

    Ok(())
}
