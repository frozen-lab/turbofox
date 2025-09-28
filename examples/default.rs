use turbocache::{TurboCache, TurboConfig, TurboResult};

fn main() -> TurboResult<()> {
    let mut cache = TurboCache::new(
        "/tmp/turbocache_examples/default/",
        TurboConfig::default().capacity(1024),
    )?;

    cache.set(b"foo", b"bar")?;
    let val = cache.get(b"foo")?;
    println!("Got value from default bucket: {:?}", val);

    cache.del(b"foo")?;
    let val = cache.get(b"foo")?;
    println!("After delete: {:?}", val);

    Ok(())
}
