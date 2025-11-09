#![allow(unused)]

mod burrow;
mod errors;
mod linux;
mod logger;

#[derive(Debug, Clone, Copy)]
pub struct TurboFox;

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalCfg<'p> {
    pub(crate) init_cap: usize,
    pub(crate) page_size: usize,
    pub(crate) dirpath: &'p std::path::Path,
    pub(crate) logger: crate::logger::Logger,
}

impl<'p> InternalCfg<'p> {
    #[inline]
    pub(crate) fn new(dirpath: &'p std::path::Path) -> Self {
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
    pub(crate) fn init_cap(mut self, cap: usize) -> Self {
        self.init_cap = cap;
        self
    }

    #[inline]
    pub(crate) fn page_size(mut self, size: usize) -> Self {
        self.page_size = size;
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
            let path = dir.path();
            let cfg = InternalCfg::new(path);

            assert_eq!(cfg.dirpath, path);
            assert_eq!(cfg.init_cap, DEFAULT_CAP);
            assert_eq!(cfg.page_size, DEFAULT_PAGE);
            assert!(!cfg.logger.enabled);
            assert_eq!(cfg.logger.target, "TurboFox");
        }

        #[test]
        fn test_chained_builder_updates() {
            let dir = TempDir::new().expect("Tempdir");
            let path = dir.path();
            let cfg = InternalCfg::new(path).log(true).init_cap(512).page_size(4096);

            assert!(cfg.logger.enabled);
            assert_eq!(cfg.init_cap, 512);
            assert_eq!(cfg.page_size, 4096);
            assert_eq!(cfg.dirpath, path);
        }
    }
}
