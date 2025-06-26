use core::str;
use std::{sync::Arc, time::Duration};
use tempfile::tempdir;
use turbocache::{Result, TurboCache};

fn main() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Arc::new(TurboCache::open(dir.path())?);

    // clone db and spawn thread 1
    let db1 = db.clone();
    let h1 = std::thread::spawn(move || -> Result<()> {
        for i in 0..100 {
            db1.set(format!("key{i}").as_bytes(), b"thread 1")?;
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    });

    // clone db and spawn thread 2
    let db2 = db.clone();
    let h2 = std::thread::spawn(move || -> Result<()> {
        for i in 0..100 {
            db2.set(format!("key{i}").as_bytes(), b"thread 2")?;
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    });

    h1.join().unwrap()?;
    h2.join().unwrap()?;

    for i in 0..100 {
        let k = format!("key{i}");
        let v = db.get(k.as_bytes())?;

        println!(
            "{} = {}",
            k,
            str::from_utf8(&v.unwrap()).unwrap()
        );
    }

    Ok(())
}
