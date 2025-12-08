use crate::{
    errors::{InternalError, InternalResult},
    hasher::{TurboHash, SIGN_SIZE},
    TurboConfig,
};

mod den;
mod mark;
mod trail;

pub(crate) const DEFAULT_INIT_CAP: usize = 0x400; // 1024
pub(crate) const DEFAULT_PAGE_SIZE: usize = 0x80; // 128
pub(crate) const GROWTH_FACTOR: u64 = 0x02; // must preserve power of 2

// sanity checks
const _: () = assert!(
    (DEFAULT_INIT_CAP & (DEFAULT_INIT_CAP - 0x01)) == 0x00,
    "Default init capacity must be power of 2"
);
const _: () = assert!(
    (DEFAULT_PAGE_SIZE & (DEFAULT_PAGE_SIZE - 0x01)) == 0x00,
    "Default page size must be power of 2"
);
const _: () = assert!(
    (0x400 * GROWTH_FACTOR) & ((0x400 * GROWTH_FACTOR) - 0x01) == 0x00,
    "GROWTH_FACTOR must preserve power of 2 nature of values"
);

const FILE_PATHS: [&'static str; 0x04] = [mark::PATH, mark::REHASH_PATH, den::PATH, trail::PATH];

pub(crate) struct Burrow {
    cfg: TurboConfig,
    den: den::Den,
    mark: mark::Mark,
    trail: trail::Trail,
}

impl Burrow {
    pub(crate) fn new(cfg: &TurboConfig) -> InternalResult<Self> {
        let mark = Self::get_mark(cfg)?;
        let trail = Self::get_trail(cfg)?;
        let den = Self::get_den(cfg)?;

        if mark.is_none() || trail.is_none() || den.is_none() {
            cfg.logger.warn("(Burrow) [new] Incomplete burrow detected!");

            let dir = cfg.dirpath.as_ref();
            for res in std::fs::read_dir(dir)? {
                let Ok(entry) = res else {
                    continue;
                };

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

                if FILE_PATHS.contains(&filename) {
                    std::fs::remove_file(&entry.path())
                        .inspect(|_| {
                            cfg.logger
                                .warn(format!("(Burrow) [new] Deleted {filename} from target dir"))
                        })
                        .map_err(|e| {
                            cfg.logger.error(format!(
                                "(Burrow) [new] Failed to delete {filename} from target dir: {e}"
                            ));
                            e
                        })?;
                }
            }

            return Ok(Self {
                den: den::Den::new(&cfg)?,
                mark: mark::Mark::new(&cfg)?,
                trail: trail::Trail::new(&cfg)?,
                cfg: cfg.clone(),
            });
        }

        cfg.logger.debug("(Burrow) [new] Burrow init (existing)");

        Ok(Self {
            den: den.unwrap(),
            mark: mark.unwrap(),
            trail: trail.unwrap(),
            cfg: cfg.clone(),
        })
    }

    fn grow(&mut self) -> InternalResult<()> {
        let curr_cap = self.trail.current_cap();
        let new_cap = curr_cap * GROWTH_FACTOR as usize;

        // grow extend remap
        self.trail.extend_remap()?;

        // migrate to new mark
        let new_mark = self.mark.new_with_rehash()?;
        self.mark = new_mark;
        let old_path = self.cfg.dirpath.join(mark::PATH);
        std::fs::remove_file(&old_path)?;

        // extend den
        self.den.zero_extend(new_cap * DEFAULT_PAGE_SIZE)?;

        Ok(())
    }

    pub(crate) fn set(&mut self, key: &[u8], value: &[u8]) -> InternalResult<()> {
        let sign = TurboHash::new(key);
        let klen = key.len();
        let vlen = value.len();

        let min_len = SIGN_SIZE + klen + vlen;
        let buf_size = Self::align_to_page_size(min_len);
        let n_bufs = buf_size / DEFAULT_PAGE_SIZE;

        // sanity check
        debug_assert!(buf_size % DEFAULT_PAGE_SIZE == 0x00, "Must be page aligned");

        // layout: [ sign ][ key ][ value ]
        let mut buf = vec![0u8; buf_size];
        buf[..SIGN_SIZE].copy_from_slice(&sign.to_le_bytes());
        buf[SIGN_SIZE..SIGN_SIZE + klen].copy_from_slice(key);
        buf[SIGN_SIZE + klen..SIGN_SIZE + klen + vlen].copy_from_slice(value);

        // if no space left, so we need to grow, [Mark], [Trail] as well as [Den]
        let mut start_idx = self.trail.lookup(n_bufs);
        if start_idx.is_none() {
            self.grow()?;
            start_idx = self.trail.lookup(n_bufs);

            if start_idx.is_none() {
                let err = InternalError::Misc("Unable to grow trail".into());
                self.cfg
                    .logger
                    .error(format!("(Burrow) [set] Unable to fetch free slots: {err}"));
                return Err(err);
            }
        }

        self.den.write(&buf, start_idx.unwrap())?;

        let ofs = mark::Offsets::new(klen as u16, vlen as u16, n_bufs as u16, start_idx.unwrap() as u32);
        let res = self.mark.set(sign, ofs, true)?;

        if res.is_none() {
            let err = InternalError::Misc("Unable to set in mark".into());
            self.cfg
                .logger
                .error(format!("(Burrow) [set] Unable to insert into Mark: {err}"));
            return Err(err);
        }

        Ok(())
    }

    pub(crate) fn get(&self, key: &[u8]) -> InternalResult<Option<Vec<u8>>> {
        const RETRIES: usize = 32;
        const RETRY_DELAY_US: u64 = 200; // 0.2ms -> total worst case ~6.4ms

        let sign = TurboHash::new(key);
        let klen = key.len();

        let Some(ofs) = self.mark.get(sign)? else {
            return Ok(None);
        };

        // sanity check
        if klen != ofs.klen as usize {
            self.cfg.logger.warn("Klen mismatch!");
            return Ok(None);
        }

        for _ in 0..RETRIES {
            let buf = self.den.read(ofs.trail_idx as usize, ofs.vbuf_slots as usize)?;

            // extract sign
            let mut arr = [0u8; SIGN_SIZE];
            arr.copy_from_slice(&buf[..SIGN_SIZE]);
            let saved_sign = u32::from_le_bytes(arr);

            if saved_sign == sign {
                let start = SIGN_SIZE + ofs.klen as usize;
                let end = start + ofs.vlen as usize;
                return Ok(Some(buf[start..end].to_vec()));
            }

            std::thread::sleep(std::time::Duration::from_micros(RETRY_DELAY_US));
        }

        self.cfg.logger.warn("saved sign mismatch after retries!");
        Ok(None)
    }

    pub(crate) fn del(&mut self, key: &[u8]) -> InternalResult<Option<()>> {
        let sign = TurboHash::new(key);
        let klen = key.len();

        let Some(ofs) = self.mark.del(sign)? else {
            return Ok(None);
        };

        self.trail.free(ofs.trail_idx as usize, ofs.vbuf_slots as usize);
        Ok(Some(()))
    }

    fn get_mark(cfg: &TurboConfig) -> InternalResult<Option<mark::Mark>> {
        let dirpath = cfg.dirpath.as_ref();
        let mark_path = dirpath.join(mark::PATH);
        let mark_rehash_path = dirpath.join(mark::REHASH_PATH);

        let mark_exists = mark_path.exists();
        let mark_rehash_exists = mark_rehash_path.exists();

        // [Mark] does not exists
        if !mark_exists && !mark_rehash_exists {
            return Ok(None);
        }

        // remove unfinsihed rehash of [Mark]
        if mark_rehash_exists && mark_exists {
            std::fs::remove_file(&mark_rehash_path)
                .inspect(|_| {
                    cfg.logger
                        .warn(format!("(Burrow) [new] Deleted {} from target dir", mark::REHASH_PATH))
                })
                .map_err(|e| {
                    cfg.logger.error(format!(
                        "(Burrow) [new] Failed to delete {} from target dir: {e}",
                        mark::REHASH_PATH,
                    ));
                    e
                })?;
        }

        // promote rehash to active path, if not already
        if mark_rehash_exists && !mark_exists {
            std::fs::rename(&mark_rehash_path, &mark_path)
                .inspect(|_| {
                    cfg.logger.warn(format!(
                        "(Burrow) [new] Renamed {} to {}",
                        mark::REHASH_PATH,
                        mark::PATH
                    ))
                })
                .map_err(|e| {
                    cfg.logger
                        .error(format!("(Burrow) [new] Failed to rename {}: {e}", mark::REHASH_PATH,));
                    e
                })?;
        }

        let mark = mark::Mark::open(&cfg)?;
        Ok(Some(mark))
    }

    fn get_trail(cfg: &TurboConfig) -> InternalResult<Option<trail::Trail>> {
        let dirpath = cfg.dirpath.as_ref();
        let trail_path = dirpath.join(trail::PATH);

        // [Trail] does not exists
        if !trail_path.exists() {
            return Ok(None);
        }

        let trail = trail::Trail::open(&cfg)?;
        Ok(Some(trail))
    }

    fn get_den(cfg: &TurboConfig) -> InternalResult<Option<den::Den>> {
        let dirpath = cfg.dirpath.as_ref();
        let den_path = dirpath.join(den::PATH);

        // [Den] does not exists
        if !den_path.exists() {
            return Ok(None);
        }

        let den = den::Den::open(&cfg)?;
        Ok(Some(den))
    }

    #[inline]
    const fn align_to_page_size(x: usize) -> usize {
        // NOTE: only works if page_size is power of 2
        (x + (DEFAULT_PAGE_SIZE - 1)) & !(DEFAULT_PAGE_SIZE - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn touch<P: AsRef<Path>>(p: P) {
        fs::write(p, &[0u8]).expect("write works");
    }

    mod init {
        use super::*;

        #[test]
        fn test_init_works() {
            let (cfg, dir) = TurboConfig::test_cfg("Burrow");

            // Fresh dir
            let burrow = Burrow::new(&cfg);
            assert!(burrow.is_ok());

            for file in FILE_PATHS {
                let p = dir.path().join(file);
                if file != mark::REHASH_PATH {
                    assert!(p.exists(), "Expected {file} to be created");
                }
            }
        }

        #[test]
        fn test_partial_init_correctly_cleans_directory() {
            let (cfg, dir) = TurboConfig::test_cfg("Burrow");

            mark::Mark::new(&cfg).expect("mark creation");
            assert!(dir.path().join(mark::PATH).exists());

            // must now cleanup and re-init due to missing files
            let _ = Burrow::new(&cfg).expect("new Burrow");

            assert!(dir.path().join(mark::PATH).exists());
            assert!(dir.path().join(den::PATH).exists());
            assert!(dir.path().join(trail::PATH).exists());
            assert!(!dir.path().join(mark::REHASH_PATH).exists());
        }

        #[test]
        fn test_rehash_file_is_deleted_when_mark_exists() {
            let (cfg, dir) = TurboConfig::test_cfg("Burrow");

            let mark = mark::Mark::new(&cfg).expect("mark creation");
            let _rehash = mark.new_with_rehash().expect("rehash creation");
            assert!(dir.path().join(mark::REHASH_PATH).exists());

            // now it should delete rehash because mark exists
            let _ = Burrow::new(&cfg).expect("new Burrow");
            assert!(!dir.path().join(mark::REHASH_PATH).exists());
        }

        #[test]
        fn test_rehash_is_promoted_when_mark_missing() {
            let (cfg, dir) = TurboConfig::test_cfg("Burrow");

            let mark = mark::Mark::new(&cfg).expect("mark creation");
            let _rehash = mark.new_with_rehash().expect("rehash creation");

            std::fs::remove_file(dir.path().join(mark::PATH)).unwrap();
            assert!(!dir.path().join(mark::PATH).exists());
            assert!(dir.path().join(mark::REHASH_PATH).exists());

            let _ = Burrow::new(&cfg).expect("new Burrow");
            assert!(dir.path().join(mark::PATH).exists());
            assert!(!dir.path().join(mark::REHASH_PATH).exists());
        }

        #[test]
        fn test_new_works_on_valid_dir() {
            let (cfg, dir) = TurboConfig::test_cfg("Burrow");

            mark::Mark::new(&cfg).expect("mark creation");
            den::Den::new(&cfg).expect("den creation");
            trail::Trail::new(&cfg).expect("trail creation");

            let _ = Burrow::new(&cfg).expect("new Burrow");

            // Must not delete anything
            assert!(dir.path().join(mark::PATH).exists());
            assert!(dir.path().join(den::PATH).exists());
            assert!(dir.path().join(trail::PATH).exists());
        }

        #[test]
        fn test_new_ignores_unrelated_files() {
            let (cfg, dir) = TurboConfig::test_cfg("Burrow");

            // random files
            touch(dir.path().join("junk.bin"));
            assert!(dir.path().join("junk.bin").exists());

            // Missing Mark/Den/Trail => cleanup + rebuild
            let _ = Burrow::new(&cfg).expect("new Burrow");

            // junk file must not be touched
            assert!(dir.path().join("junk.bin").exists());
        }
    }

    mod ops {
        use super::*;

        #[test]
        fn test_set_get_del_ops_cycle() {
            let (mut cfg, _dir) = TurboConfig::test_cfg("Burrow");
            cfg = cfg.init_cap(0x80).expect("new cap");

            let mut burrow = Burrow::new(&cfg).expect("new Burrow");

            let kbuf = [0x0Au8; 0x20];
            let vbuf = [0x0Fu8; 0x40];

            assert!(burrow.set(&kbuf, &vbuf).is_ok());
            std::thread::sleep(std::time::Duration::from_millis(0x0A));
            assert_eq!(burrow.get(&kbuf).expect("get works"), Some(vbuf.to_vec()));
            assert!(burrow.del(&kbuf).is_ok());
            assert_eq!(burrow.get(&kbuf).expect("get works"), None);
        }

        #[test]
        fn test_overwrite_same_key() {
            let (mut cfg, _dir) = TurboConfig::test_cfg("Burrow");
            cfg = cfg.init_cap(0x80).unwrap();
            let mut burrow = Burrow::new(&cfg).unwrap();

            let key = [0x42u8; 8];

            for i in 0..20 {
                let val = vec![i as u8; 64];
                burrow.set(&key, &val).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(5));

                let got = burrow.get(&key).unwrap().unwrap();
                assert_eq!(got, val, "Mismatch on iteration {i}");
            }

            burrow.del(&key).unwrap();
            assert!(burrow.get(&key).unwrap().is_none());
        }

        #[test]
        fn test_collision_handling() {
            let (mut cfg, _dir) = TurboConfig::test_cfg("Burrow");
            cfg = cfg.init_cap(0x80).unwrap();
            let mut burrow = Burrow::new(&cfg).unwrap();

            // craft keys that differ but use same TurboHash sign
            // (cheap trick: same prefix, high entropy suffix — TurboHash collapses by design)
            let base = vec![0x11; 16];

            let keys: Vec<Vec<u8>> = (0..20)
                .map(|i| {
                    let mut k = base.clone();
                    k.extend_from_slice(&[i as u8, (i * 3) as u8, 0xAA, 0xBB]);
                    k
                })
                .collect();

            let vals: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8; 32]).collect();

            for (k, v) in keys.iter().zip(vals.iter()) {
                burrow.set(k, v).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(1));
            }

            for (k, v) in keys.iter().zip(vals.iter()) {
                let got = burrow.get(k).unwrap().unwrap();
                assert_eq!(got, *v, "Collision lookup failed");
            }
        }

        #[test]
        fn test_growth_mechanism_end_to_end() {
            let (mut cfg, _dir) = TurboConfig::test_cfg("Burrow");
            cfg = cfg.init_cap(0x80).unwrap();

            let mut burrow = Burrow::new(&cfg).unwrap();

            // ------- generate enough KV pairs to force growth -------
            // each value = 256B → 2 pages per entry → few inserts force extend_remap
            let mut keys = Vec::new();
            let mut vals = Vec::new();

            for i in 0..200 {
                let mut k = vec![0xAB; 16];
                k.extend_from_slice(&(i as u32).to_le_bytes());
                let v = vec![i as u8; 256];

                keys.push(k);
                vals.push(v);
            }

            // ------- batch writes -------
            for (k, v) in keys.iter().zip(vals.iter()) {
                burrow.set(k, v).unwrap();
            }

            // barrier: IO writes may still be in-flight
            std::thread::sleep(std::time::Duration::from_millis(20));

            // ------- fsync all 3 files -------
            burrow.den.file.sync().expect("sync file");

            // ------- verify correctness -------
            for (idx, (k, v)) in keys.iter().zip(vals.iter()).enumerate() {
                let got = burrow.get(k).unwrap().unwrap();
                assert_eq!(got, *v, "Value mismatch post-growth at index {idx}");
            }

            // ------- delete everything -------
            for k in keys.iter() {
                burrow.del(k).unwrap();
            }

            // ------- validate deletion -------
            for (idx, k) in keys.iter().enumerate() {
                let got = burrow.get(k).unwrap();
                assert!(got.is_none(), "Key not deleted at index {idx}");
            }
        }
    }
}
