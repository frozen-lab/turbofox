#![allow(unused)]

mod burrow;
mod errors;
mod linux;
mod logger;

#[derive(Debug, Clone, Copy)]
pub struct TurboFox;

#[derive(Debug, Clone)]
pub(crate) struct InternalCfg {
    pub(crate) init_cap: usize,
    pub(crate) page_size: usize,
    pub(crate) dirpath: std::path::PathBuf,
    pub(crate) logger: crate::logger::Logger,
}

impl InternalCfg {
    #[inline]
    pub(crate) fn new(dirpath: std::path::PathBuf) -> Self {
        Self {
            dirpath: dirpath,
            init_cap: crate::burrow::DEFAULT_CAP,
            page_size: crate::burrow::DEFAULT_PAGE_SIZE,
            logger: crate::logger::Logger {
                enabled: false,
                target: "TurboFox",
            },
        }
    }

    #[inline]
    pub(crate) fn log(mut self, logging_enabled: bool) -> Self {
        self.logger.enabled = logging_enabled;
        self
    }

    #[inline]
    pub(crate) fn cap(mut self, cap: usize) -> Self {
        self.init_cap = cap;
        self
    }

    #[inline]
    pub(crate) fn page(mut self, size: usize) -> Self {
        // sanity checks
        assert!(size >= 128, "Buffer Size must be equal to or greater then 128 bytes");
        assert!((size & (size - 1)) == 0, "Buffer Size value must be power of 2");

        self.page_size = size;
        self
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn log_target(mut self, target: &'static str) -> Self {
        self.logger.target = target;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    mod internal_cfg {
        use super::*;
        use std::path::Path;

        const DEFAULT_CAP: usize = crate::burrow::DEFAULT_CAP;
        const DEFAULT_PAGE: usize = crate::burrow::DEFAULT_PAGE_SIZE;

        #[test]
        fn test_builder_pattern_and_default_values() {
            let dir = TempDir::new().expect("Tempdir");
            let path = dir.path().to_path_buf();
            let cfg = InternalCfg::new(path.clone());

            assert_eq!(cfg.dirpath, path);
            assert_eq!(cfg.init_cap, DEFAULT_CAP);
            assert_eq!(cfg.page_size, DEFAULT_PAGE);
            assert!(!cfg.logger.enabled);
            assert_eq!(cfg.logger.target, "TurboFox");
        }

        #[test]
        fn test_chained_builder_updates() {
            let dir = TempDir::new().expect("Tempdir");
            let path = dir.path().to_path_buf();
            let cfg = InternalCfg::new(path.clone()).log(true).cap(512).page(4096);

            assert!(cfg.logger.enabled);
            assert_eq!(cfg.init_cap, 512);
            assert_eq!(cfg.page_size, 4096);
            assert_eq!(cfg.dirpath, path);
        }
    }
}
