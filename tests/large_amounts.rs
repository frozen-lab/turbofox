use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;
use turbocache::TurboCache;

const N_KEYS: usize = 10_000;
const KEY_LEN: usize = 32;
const VAL_LEN: usize = 128;
const SEED: u64 = 42;
const INIT_CAP: usize = 1024 * 5; // 5 Kib

fn gen_dataset() -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(SEED);

    (0..N_KEYS)
        .map(|_| {
            let key = (0..KEY_LEN).map(|_| rng.random()).collect();
            let val = (0..VAL_LEN).map(|_| rng.random()).collect();

            (key, val)
        })
        .collect()
}

#[test]
fn test_with_large_amounts() {
    let tmp = TempDir::new().unwrap();
    let dataset = gen_dataset();

    let mut cache = TurboCache::new(tmp.path(), INIT_CAP).unwrap();

    // set all the items
    for (k, v) in &dataset {
        cache.set(k.clone(), v.clone()).unwrap();
    }

    // check if inserts matches the size of [dataset]
    assert_eq!(cache.get_inserts(), N_KEYS);

    // check if all items are retrived correctly
    for (k, v) in &dataset {
        let vbuf = cache.get(k.clone()).unwrap();

        assert_eq!(vbuf, Some(v.clone()));
    }

    // check if all items are retrived correctly on delete
    for (k, v) in &dataset {
        let vbuf = cache.del(k.clone()).unwrap();

        assert_eq!(vbuf, Some(v.clone()));
    }
}
