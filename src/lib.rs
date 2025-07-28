mod bucket;
mod core;
mod hash;
mod router;

use core::TurboConfig;
use router::Router;
use std::path::Path;

pub use core::{TurboError, TurboResult};

pub struct TurboCache<P: AsRef<Path>> {
    router: Router<P>,
}

impl<P: AsRef<Path>> TurboCache<P> {
    pub fn new(dirpath: P, initial_capacity: usize) -> TurboResult<Self> {
        let config = TurboConfig {
            initial_capacity,
            dirpath,
        };

        let router = Router::new(config)?;

        Ok(Self { router })
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> TurboResult<()> {
        self.router.set((key, value))
    }

    pub fn get(&self, key: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        self.router.get(key)
    }

    pub fn del(&mut self, key: Vec<u8>) -> TurboResult<Option<Vec<u8>>> {
        self.router.del(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = TurboResult<(Vec<u8>, Vec<u8>)>> + '_ {
        self.router.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn collect_pairs<P: AsRef<std::path::Path>>(
        cache: &TurboCache<P>,
    ) -> HashSet<(Vec<u8>, Vec<u8>)> {
        cache.iter().map(|res| res.expect("iter error")).collect()
    }

    #[test]
    fn basic_set_get_del() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path(), 16).unwrap();

        assert!(cache.get(b"foo".to_vec()).unwrap().is_none());

        cache.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert_eq!(cache.get(b"foo".to_vec()).unwrap(), Some(b"bar".to_vec()));

        let deleted = cache.del(b"foo".to_vec()).unwrap();

        assert_eq!(deleted, Some(b"bar".to_vec()));
        assert!(cache.get(b"foo".to_vec()).unwrap().is_none());
    }

    #[test]
    fn iter_on_empty_cache() {
        let tmp = TempDir::new().unwrap();
        let cache = TurboCache::new(tmp.path(), 8).unwrap();

        // empty cache → iter yields nothing
        assert!(cache.iter().next().is_none());
    }

    #[test]
    fn iter_after_simple_inserts() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path(), 100).unwrap();

        let inputs = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        for pair in &inputs {
            cache.set(pair.0.clone(), pair.1.clone()).unwrap();
        }

        let got: HashSet<_> = collect_pairs(&cache);
        let want: HashSet<_> = inputs.into_iter().collect();

        assert_eq!(got, want);
    }

    #[test]
    fn iter_through_staging_and_swap() {
        let tmp = TempDir::new().unwrap();

        // small capacity so that `threshold = (cap * 4) / 5` floors to 1
        let mut cache = TurboCache::new(tmp.path(), 2).unwrap();

        // Insert 5 items → forces staging, partial migrations, and at least one final swap
        let all: Vec<_> = (0..5).map(|i| (vec![i], vec![i + 100])).collect();

        for (k, v) in &all {
            cache.set(k.clone(), v.clone()).unwrap();
        }

        // confirm get() sees everything
        for (k, v) in &all {
            assert_eq!(cache.get(k.clone()).unwrap(), Some(v.clone()));
        }

        // confirm iter() sees everything (order-agnostic)
        let got = collect_pairs(&cache);
        let want: HashSet<_> = all.into_iter().collect();

        assert_eq!(got, want);
    }

    #[test]
    fn delete_via_public_api_and_iter() {
        let tmp = TempDir::new().unwrap();
        let mut cache = TurboCache::new(tmp.path(), 4).unwrap();

        cache.set(b"x".to_vec(), b"10".to_vec()).unwrap();
        cache.set(b"y".to_vec(), b"20".to_vec()).unwrap();
        cache.set(b"z".to_vec(), b"30".to_vec()).unwrap();

        let deleted = cache.del(b"y".to_vec()).unwrap().unwrap();
        assert_eq!(deleted, b"20".to_vec());

        let got = collect_pairs(&cache);
        let want: HashSet<_> = vec![
            (b"x".to_vec(), b"10".to_vec()),
            (b"z".to_vec(), b"30".to_vec()),
        ]
        .into_iter()
        .collect();

        assert_eq!(got, want);
    }

    #[test]
    fn persistence_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let mut cache1 = TurboCache::new(&path, 8).unwrap();

            cache1.set(b"keep".to_vec(), b"alive".to_vec()).unwrap();
            cache1.set(b"swap".to_vec(), b"me".to_vec()).unwrap();

            for i in 0..10 {
                cache1.set(vec![i], vec![i]).unwrap();
            }
        }

        let cache2 = TurboCache::new(&path, 8).unwrap();

        assert_eq!(
            cache2.get(b"keep".to_vec()).unwrap(),
            Some(b"alive".to_vec())
        );
        assert_eq!(cache2.get(b"swap".to_vec()).unwrap(), Some(b"me".to_vec()));

        let got = collect_pairs(&cache2);

        assert!(got.contains(&(b"keep".to_vec(), b"alive".to_vec())));
        assert!(got.contains(&(b"swap".to_vec(), b"me".to_vec())));
    }
}
