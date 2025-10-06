use log::{error, info, warn};
use turbocache::{TurboCache, TurboCfg};

fn main() {
    // Logger init! To get log set `RUST_LOG=info`, e.g.
    // `RUST_LOG=info cargo run --example basic`
    env_logger::init();

    // for rng's
    let mut sphur = sphur::Sphur::new();

    let path = "/tmp/turbocache_examples/default/";
    let cfg = TurboCfg::default().rows(1); // i.e. capacity of 12 pairs

    info!("Creating cache at {path}...");

    let mut cache = match TurboCache::new(path, cfg) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to initialize TurboCache: {e}");
            return;
        }
    };

    // ---
    // set & get
    // ---

    if let Err(e) = cache.set(b"foo", b"bar") {
        error!("Failed to set key 'foo': {e}");
    } else {
        info!("Successfully set key 'foo'");
    }

    match cache.get(b"foo") {
        Ok(Some(val)) => info!("Got value from default bucket: {:?}", val),
        Ok(None) => warn!("Key 'foo' not found!"),
        Err(e) => error!("Failed to get key 'foo': {e}"),
    }

    // ---
    // del
    // ---

    if let Err(e) = cache.del(b"foo") {
        error!("Failed to delete key 'foo': {e}");
    } else {
        info!("Deleted key 'foo'");
    }

    match cache.get(b"foo") {
        Ok(Some(val)) => warn!("Key 'foo' still exists: {:?}", val),
        Ok(None) => info!("Confirmed deletion of 'foo'"),
        Err(e) => error!("Error verifying deletion: {e}"),
    }

    // ---
    // Fill in till entire cap
    // ---

    let mut i = 0;

    loop {
        let key = format!("key_{}", sphur.gen_u32());

        match cache.set(key.as_bytes(), &[]) {
            Ok(_) => {
                if i % 100 == 0 {
                    info!("Inserted {i} keys...");
                }

                i += 1;
            }

            Err(e) => {
                warn!("Cache insertion failed at {i} keys: {e}");
                info!("Example complete — inserted {i} keys total.");
                break;
            }
        }
    }
}
