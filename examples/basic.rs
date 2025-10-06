use turbocache::{TurboCache, TurboCfg, TurboResult};

fn main() -> TurboResult<()> {
    let mut cache = TurboCache::new(
        "/tmp/turbocache_examples/default/",
        TurboCfg::default().rows(16),
    )?;

    cache.set(b"foo_2", b"bar")?;
    let val = cache.get(b"foo")?;
    println!("Got value from default bucket: {:?}", val);

    cache.del(b"foo")?;
    let val = cache.get(b"foo")?;
    println!("After delete: {:?}", val);

    Ok(())
}
