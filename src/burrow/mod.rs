use crate::{errors::InternalResult, TurboConfig};

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
        }

        cfg.logger.debug("(Burrow) [new] Burrow init (existing)");

        Ok(Self {
            den: den.unwrap(),
            mark: mark.unwrap(),
            trail: trail.unwrap(),
        })
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
}
