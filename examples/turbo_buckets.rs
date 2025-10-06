use log::{error, info, warn};
use turbocache::{BucketCfg, TurboCache, TurboCfg};

fn main() {
    // Logger init! To get log set `RUST_LOG=info`, e.g.
    // `RUST_LOG=info cargo run --example turbo_buckets`
    env_logger::init();

    let path = "/tmp/turbocache_examples/turbo_buckets/";
    let cfg = TurboCfg::default().rows(64);

    info!("Creating TurboCache at {path}...");

    let mut cache = match TurboCache::new(path, cfg) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to initialize TurboCache: {e}");
            return;
        }
    };

    // Create a named bucket w/ custom configuration
    let bucket_cfg = BucketCfg::default().rows(10).growable(false);
    let mut users = cache.bucket("users", Some(bucket_cfg));

    info!("Performing operations on bucket 'users'...");

    // ---
    // Insert users
    // ---

    for (id, name) in [(b"id:1", b"Alice"), (b"id:2", b"Bobby")] {
        if let Err(e) = users.set(id, name) {
            error!("Failed to insert {:?}: {e}", name);
        } else {
            info!("Inserted user {:?} with key {:?}", name, id);
        }
    }

    // ---
    // Fetch users
    // ---

    for id in [b"id:1", b"id:2"] {
        match users.get(id) {
            Ok(Some(v)) => info!("User {:?} => {:?}", id, v),
            Ok(None) => warn!("User {:?} not found", id),
            Err(e) => error!("Failed to fetch {:?}: {e}", id),
        }
    }

    // ---
    // Yank user
    // ---

    if let Err(e) = users.del(b"id:1") {
        error!("Failed to delete user id:1: {e}");
    } else {
        info!("Deleted user id:1");
    }

    match users.get(b"id:1") {
        Ok(Some(v)) => warn!("User id:1 still exists: {:?}", v),
        Ok(None) => info!("Confirmed deletion of user id:1"),
        Err(e) => error!("Error verifying deletion: {e}"),
    }

    info!("'users' bucket operations successful.");
}
