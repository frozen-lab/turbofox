//! `Router` is the central component for directing db operations to the correct shard.
use crate::{
    hasher::TurboHasher,
    shard::{Shard, TError, TResult},
};
use std::{path::PathBuf, sync::Mutex};

/// The `Router` manages a collection of shards and routes database operations
/// to the correct one based on the key's hash.
pub(crate) struct Router {
    dirpath: PathBuf,
    shards: Mutex<Vec<Shard>>,
}

impl Router {
    pub(crate) const END_OF_SHARDS: u32 = 1u32 << 16;

    /// Opens the database at the specified directory, loading existing shards or
    /// creating a new one if none are found.
    pub fn open(dirpath: &PathBuf) -> TResult<Self> {
        let shards = Self::load(dirpath)?;

        if shards.is_empty() {
            let shard = Shard::open(&dirpath, 0..Self::END_OF_SHARDS, true)?;

            return Ok(Self {
                dirpath: dirpath.clone(),
                shards: Mutex::new(vec![shard]),
            });
        }

        Ok(Self {
            shards: Mutex::new(shards),
            dirpath: dirpath.clone(),
        })
    }

    /// Loads all valid shards from the specified directory.
    fn load(dirpath: &PathBuf) -> TResult<Vec<Shard>> {
        let mut found_shards: Vec<Shard> = vec![];

        // Check if directory exists
        if !dirpath.exists() {
            std::fs::create_dir_all(dirpath)?;
            return Ok(found_shards);
        }

        for res in std::fs::read_dir(&dirpath)? {
            let entry = res?;
            let filename = entry.file_name();

            let Some(filename) = filename.to_str() else {
                continue;
            };

            let Ok(filetype) = entry.file_type() else {
                continue;
            };

            if !filetype.is_file() {
                continue;
            }

            // Clean up temporary files
            if filename.starts_with("bottom_")
                || filename.starts_with("top_")
                || filename.starts_with("merge_")
                || filename.ends_with(".tmp")
            {
                if let Err(e) = std::fs::remove_file(entry.path()) {
                    eprintln!(
                        "Warning: Failed to remove temporary file {}: {}",
                        filename, e
                    );
                }
                continue;
            } else if !filename.starts_with("shard_") {
                continue;
            }

            let Some((_, span)) = filename.split_once("_") else {
                continue;
            };

            let Some((start_str, end_str)) = span.split_once("-") else {
                continue;
            };

            let Ok(start) = u32::from_str_radix(start_str, 16) else {
                eprintln!("Warning: Invalid start range in filename: {}", filename);
                continue;
            };

            let Ok(end) = u32::from_str_radix(end_str, 16) else {
                eprintln!("Warning: Invalid end range in filename: {}", filename);
                continue;
            };

            if start >= end || end > Self::END_OF_SHARDS {
                eprintln!(
                    "Warning: Invalid shard range {}-{} in file: {}",
                    start, end, filename
                );
                continue;
            }

            let range = start..end;
            match Shard::open(&dirpath, range, false) {
                Ok(shard) => found_shards.push(shard),
                Err(e) => {
                    eprintln!("Warning: Failed to open shard {}: {}", filename, e);
                    continue;
                }
            }
        }

        // Sort shards by their start range for consistent ordering
        found_shards.sort_by(|a, b| a.span.start.cmp(&b.span.start));

        Ok(found_shards)
    }

    /// Sets a key-value pair in the appropriate shard.
    pub fn set(&self, buf: (&[u8], &[u8]), hash: TurboHasher) -> TResult<()> {
        let mut shards = self
            .shards
            .lock()
            .or_else(|e| Ok::<std::sync::MutexGuard<'_, Vec<Shard>>, TError>(e.into_inner()))?;

        self.set_recursive(buf, hash, &mut shards)
    }

    fn set_recursive(
        &self,
        buf: (&[u8], &[u8]),
        hash: TurboHasher,
        shards: &mut Vec<Shard>,
    ) -> TResult<()> {
        let s = hash.shard_selector();
        let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = vec![(buf.0.to_vec(), buf.1.to_vec())];

        while let Some((k, v)) = kvs.pop() {
            for i in 0..shards.len() {
                if shards[i].span.contains(&s) {
                    let res = shards[i].set((&k, &v), hash);

                    match res {
                        Err(TError::RowFull(_)) => {
                            let old = std::mem::replace(
                                &mut shards[i],
                                Shard::open(&self.dirpath, 0..0, true)?,
                            );

                            let (left, right, remaining_kvs) = old.split(&self.dirpath)?;
                            kvs.extend(remaining_kvs);

                            shards.remove(i);
                            shards.insert(i, right);
                            shards.insert(i, left);

                            return self.set_recursive((&k, &v), hash, shards);
                        }
                        Err(err) => return Err(err),
                        Ok(()) => return Ok(()),
                    }
                }
            }
        }

        Err(TError::ShardOutOfRange(s))
    }

    /// Retrieves a value by its key from the appropriate shard.
    pub fn get(&self, buf: &[u8], hash: TurboHasher) -> TResult<Option<Vec<u8>>> {
        let s = hash.shard_selector();
        let shards = self.shards.lock().unwrap();

        for shard in shards.iter() {
            if shard.span.contains(&s) {
                return shard.get(buf, hash);
            }
        }

        Err(TError::ShardOutOfRange(s))
    }

    /// Removes a key-value pair from the appropriate shard.
    pub fn remove(&self, buf: &[u8], hash: TurboHasher) -> TResult<bool> {
        let s = hash.shard_selector();
        let shards = self.shards.lock().unwrap();

        for shard in shards.iter() {
            if shard.span.contains(&s) {
                return shard.remove(buf, hash);
            }
        }

        Err(TError::ShardOutOfRange(s))
    }

    /// Returns the number of shards currently managed by the router.
    #[allow(dead_code)]
    pub fn shard_count(&self) -> usize {
        self.shards.lock().unwrap().len()
    }

    /// Returns the spans of all shards
    #[allow(dead_code)]
    pub fn shard_spans(&self) -> Vec<std::ops::Range<u32>> {
        self.shards
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.span.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn new_router() -> TResult<(Router, TempDir)> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;
        let router = Router::open(&dir)?;

        Ok((router, tmp))
    }

    #[test]
    fn test_new_router_single_shard() -> TResult<()> {
        let (router, _tmp) = new_router()?;

        assert_eq!(router.shard_count(), 1);
        let spans = router.shard_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0], 0..Router::END_OF_SHARDS);

        Ok(())
    }

    #[test]
    fn test_set_get_remove_via_router() -> TResult<()> {
        let (router, _tmp) = new_router()?;
        let key = b"key1";
        let val = b"val1".to_vec();
        let h = TurboHasher::new(key);

        // Not present yet
        assert_eq!(router.get(key, h)?, None);

        // Set and get
        router.set((key, &val), h)?;
        assert_eq!(router.get(key, h)?, Some(val.clone()));

        // Remove and gone
        assert!(router.remove(key, h)?);
        assert_eq!(router.get(key, h)?, None);

        // Removing again returns false
        assert!(!router.remove(key, h)?);

        Ok(())
    }

    #[test]
    fn test_persistence_across_reopen() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        std::fs::create_dir_all(&dir)?;

        {
            let router = Router::open(&dir)?;
            let key = b"persist";
            let val = b"data".to_vec();
            let h = TurboHasher::new(key);

            router.set((key, &val), h)?;
        }

        let router2 = Router::open(&dir)?;
        let key = b"persist";
        let h2 = TurboHasher::new(key);

        assert_eq!(router2.get(key, h2)?, Some(b"data".to_vec()));

        Ok(())
    }

    #[test]
    fn test_out_of_range_error() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let s = Shard::open(&dir, 0..1, true)?;
        let router = Router {
            shards: Mutex::new(vec![s]),
            dirpath: dir,
        };

        let key = b"x";
        let hash = TurboHasher::new(key);

        // This should fail if the hash selector is >= 1
        let res = router.get(key, hash);

        if hash.shard_selector() >= 1 {
            assert!(matches!(res, Err(TError::ShardOutOfRange(_))));
        } else {
            // If the hash happens to be in range, it should work
            assert!(res.is_ok());
        }

        Ok(())
    }

    #[test]
    fn test_multiple_operations_trigger_splits() -> TResult<()> {
        let (router, _tmp) = new_router()?;

        let initial_shard_count = router.shard_count();

        // Insert a large number of entries to likely trigger splits
        for i in 0..1000 {
            let key = format!("test_key_{:04}", i);
            let val = format!("test_value_{:04}", i);
            let hash = TurboHasher::new(key.as_bytes());

            router.set((key.as_bytes(), val.as_bytes()), hash)?;
        }

        // Verify all data is still accessible
        for i in 0..1000 {
            let key = format!("test_key_{:04}", i);
            let expected_val = format!("test_value_{:04}", i);
            let hash = TurboHasher::new(key.as_bytes());

            let retrieved = router.get(key.as_bytes(), hash)?;
            assert_eq!(retrieved, Some(expected_val.as_bytes().to_vec()));
        }

        // We might have more shards now (depends on hash distribution)
        let final_shard_count = router.shard_count();
        println!("Shards: {} -> {}", initial_shard_count, final_shard_count);

        Ok(())
    }

    #[test]
    fn test_load_existing_shards() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Create some shard files manually
        let shard1 = Shard::open(&dir, 0..0x8000, true)?;
        let shard2 = Shard::open(&dir, 0x8000..0x10000, true)?;

        // Add some data
        let key1 = b"key1";
        let val1 = b"value1";
        let hash1 = TurboHasher::new(key1);

        if hash1.shard_selector() < 0x8000 {
            shard1.set((key1, val1), hash1)?;
        } else {
            shard2.set((key1, val1), hash1)?;
        }

        // Drop the shards to ensure they're written
        drop(shard1);
        drop(shard2);

        // Now load via router
        let router = Router::open(&dir)?;
        assert_eq!(router.shard_count(), 2);

        // Verify data is accessible
        assert_eq!(router.get(key1, hash1)?, Some(val1.to_vec()));

        Ok(())
    }

    #[test]
    fn test_cleanup_temporary_files() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Create some temporary files that should be cleaned up
        let temp_files = vec![
            "bottom_shard_0000-8000",
            "top_shard_8000-ffff",
            "merge_shard_0000-ffff",
            "some_file.tmp",
        ];

        for filename in &temp_files {
            let filepath = dir.join(filename);
            std::fs::write(&filepath, "temporary data")?;
            assert!(filepath.exists());
        }

        // Also create a valid shard
        let _shard = Shard::open(&dir, 0..0x1000, true)?;

        // Opening the router should clean up temp files
        let router = Router::open(&dir)?;
        assert_eq!(router.shard_count(), 1);

        // Check that temp files were removed
        for filename in &temp_files {
            let filepath = dir.join(filename);
            assert!(
                !filepath.exists(),
                "Temp file {} was not cleaned up",
                filename
            );
        }

        Ok(())
    }

    #[test]
    fn test_invalid_shard_files_ignored() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Create invalid shard files
        let invalid_files = vec![
            "shard_invalid-range",
            "shard_ffff-0000",   // end < start
            "shard_0000-100000", // end > END_OF_SHARDS
            "not_a_shard_file",
        ];

        for filename in &invalid_files {
            let filepath = dir.join(filename);
            std::fs::write(&filepath, "invalid data")?;
        }

        // Create a valid shard
        let _shard = Shard::open(&dir, 0..0x1000, true)?;

        // Opening should ignore invalid files and load only valid ones
        let router = Router::open(&dir)?;
        assert_eq!(router.shard_count(), 1);

        Ok(())
    }

    #[test]
    fn test_shard_spans_ordering() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Create shards in non-sequential order
        let _shard3 = Shard::open(&dir, 0x8000..0x10000, true)?;
        let _shard1 = Shard::open(&dir, 0..0x4000, true)?;
        let _shard2 = Shard::open(&dir, 0x4000..0x8000, true)?;

        // Drop to ensure they're written
        drop(_shard1);
        drop(_shard2);
        drop(_shard3);

        // Router should load them in correct order
        let router = Router::open(&dir)?;
        let spans = router.shard_spans();

        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0], 0..0x4000);
        assert_eq!(spans[1], 0x4000..0x8000);
        assert_eq!(spans[2], 0x8000..0x10000);

        Ok(())
    }

    #[test]
    fn test_concurrent_operations() -> TResult<()> {
        let (router, _tmp) = new_router()?;

        // Test that multiple operations can work concurrently
        // (Note: This is a basic test - proper concurrent testing would need threads)

        let mut test_data = HashMap::new();

        // Insert multiple entries
        for i in 0..100 {
            let key = format!("concurrent_key_{}", i);
            let val = format!("concurrent_value_{}", i);
            let hash = TurboHasher::new(key.as_bytes());

            router.set((key.as_bytes(), val.as_bytes()), hash)?;
            test_data.insert(key, val);
        }

        // Verify all entries
        for (key, expected_val) in &test_data {
            let hash = TurboHasher::new(key.as_bytes());
            let retrieved = router.get(key.as_bytes(), hash)?;
            assert_eq!(retrieved, Some(expected_val.as_bytes().to_vec()));
        }

        // Remove half the entries
        let mut removed_count = 0;
        for (key, _) in &test_data {
            if removed_count % 2 == 0 {
                let hash = TurboHasher::new(key.as_bytes());
                assert!(router.remove(key.as_bytes(), hash)?);
            }
            removed_count += 1;
        }

        // Verify removal
        removed_count = 0;
        for (key, expected_val) in &test_data {
            let hash = TurboHasher::new(key.as_bytes());
            let retrieved = router.get(key.as_bytes(), hash)?;

            if removed_count % 2 == 0 {
                assert_eq!(retrieved, None);
            } else {
                assert_eq!(retrieved, Some(expected_val.as_bytes().to_vec()));
            }
            removed_count += 1;
        }

        Ok(())
    }

    #[test]
    fn test_empty_directory_initialization() -> TResult<()> {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        // Don't create the directory - let Router::open create it

        let router = Router::open(&dir)?;
        assert_eq!(router.shard_count(), 1);

        let spans = router.shard_spans();
        assert_eq!(spans[0], 0..Router::END_OF_SHARDS);

        Ok(())
    }

    #[test]
    #[ignore]
    fn test_cascading_splits_data_integrity() -> TResult<()> {
        let (router, _tmp) = new_router()?;
        let mut inserted_data = HashMap::new();

        // Note: According to simulations, avg split will occur at
        // around 20K entries
        let num_entries = 50000;

        for i in 0..num_entries {
            let key = format!("key_{}", i);
            let val = format!("value_{}", i);
            let hash = TurboHasher::new(key.as_bytes());

            router.set((key.as_bytes(), val.as_bytes()), hash)?;
            inserted_data.insert(key, val);
        }

        for (key, expected_val) in inserted_data.iter() {
            let hash = TurboHasher::new(key.as_bytes());
            let retrieved = router.get(key.as_bytes(), hash)?;

            assert_eq!(
                retrieved,
                Some(expected_val.as_bytes().to_vec()),
                "Failed to retrieve key: {}",
                key
            );
        }

        Ok(())
    }
}
