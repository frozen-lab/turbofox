use turbocache::{TurboCache, TurboConfig, TurboResult};

fn main() -> TurboResult<()> {
    // Global default config
    let mut cache = TurboCache::new(
        "/tmp/turbocache_examples/custom/",
        TurboConfig::default().capacity(1024),
    )?;

    // Create a named bucket with custom capacity/growable
    let mut users = cache.bucket(
        "users",
        Some(TurboConfig::default().capacity(5000).growable(false)),
    );

    // Operations on custom bucket
    users.set(b"id:1", b"Alice")?;
    users.set(b"id:2", b"Bob")?;

    println!("User 1: {:?}", users.get(b"id:1")?);
    println!("User 2: {:?}", users.get(b"id:2")?);

    users.del(b"id:1")?;
    println!("User 1 after delete: {:?}", users.get(b"id:1")?);

    Ok(())
}
