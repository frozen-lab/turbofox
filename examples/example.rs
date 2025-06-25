use tempfile::tempdir;
use turbocache::{Result, TurboCache};

fn main() -> Result<()> {
    let dir = tempdir().unwrap();
    let mut db = TurboCache::open(dir.path())?;

    println!("{:?}", db.get(b"mykey")?); // None

    db.set(b"mykey", b"myval")?;
    println!("{:?}", db.get(b"mykey")?); // Some([109, 121, 118, 97, 108])

    println!("{:?}", db.remove(b"mykey")?); // Some([109, 121, 118, 97, 108])
    println!("{:?}", db.remove(b"mykey")?); // None

    println!("{:?}", db.get(b"mykey")?); // None

    for i in 0..10 {
        db.set(&format!("mykey{i}").into_bytes(), &format!("myval{i}").into_bytes())?;
    }

    Ok(())
}
