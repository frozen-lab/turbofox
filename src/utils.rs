/// empty function used as a placeholder to influence branch prediction
#[cold]
#[inline]
const fn cold_fn() {}

/// Hint for branch predictor that given branch condition is *likely* to be `true`
#[inline]
pub(crate) const fn likely(b: bool) -> bool {
    if !b {
        cold_fn();
    }
    b
}

#[test]
fn test_sanity_check_for_likely() {
    assert!(likely(true), "true should be true");
    assert!(!likely(false), "false should be false");
}

/// Hint for branch predictor that given branch condition is *unlikely* to be `true`
#[inline]
pub(crate) const fn unlikely(b: bool) -> bool {
    if b {
        cold_fn();
    }
    b
}

#[test]
fn test_sanity_check_for_unlikely() {
    assert!(unlikely(true), "true should be true");
    assert!(!unlikely(false), "false should be false");
}

/// Prepares directory at given dirpath for use in `TurboFox`
pub(crate) fn prep_directory(
    dirpath: &std::path::PathBuf,
    logger: &crate::logger::Logger,
) -> crate::error::InternalResult<()> {
    use crate::{error::InternalError, logger::LogCtx};

    if dirpath.exists() {
        if unlikely(!dirpath.is_dir()) {
            logger.error(
                LogCtx::Dir,
                format!(
                    "Failed to create/open directory for TurboFox as given Path=[{:?}] is not a directory",
                    dirpath
                ),
            );

            return Err(InternalError::IO(format!("Path=[{:?}] is not a directory", dirpath)));
        }

        logger.trace(LogCtx::Dir, format!("Created new dir at path=[{:?}]", dirpath));
        return Ok(());
    }

    // create dir if missing (w/ fast fail)
    std::fs::create_dir_all(&dirpath)
        .inspect(|_| {
            logger.info(
                LogCtx::Dir,
                format!("New directory for TurboFox at path=[{:?}]", dirpath),
            );
        })
        .map_err(|e| {
            logger.error(
                LogCtx::Dir,
                format!(
                    "Unable to create new directory for TurboFox at path=[{:?}] due to error: {e}",
                    dirpath
                ),
            );
            e
        })?;

    logger.trace(LogCtx::Dir, format!("Successful"));
    Ok(())
}

#[cfg(test)]
mod test_prep_directory {
    use super::prep_directory;
    use crate::{error::InternalError, logger::test_logger};
    use tempfile::{NamedTempFile, TempDir};

    #[test]
    fn test_creates_if_missing() {
        let logger = test_logger("CreateDir");
        let tmp = TempDir::new().expect("New temp dir");
        let dir = tmp.path().join("dummy");

        assert!(
            prep_directory(&dir, &logger).is_ok(),
            "Should create a new directory when missing"
        );

        // sanity checks for validity
        assert!(dir.exists(), "New directory should be created");
        assert!(dir.is_dir(), "New directory must be a directory");
    }

    #[test]
    fn test_correctly_opens_existing_dir() {
        let logger = test_logger("CreateDir");
        let tmp = TempDir::new().expect("New temp dir");
        let dir = tmp.path().join("dummy");
        std::fs::create_dir_all(&dir).expect("create new directory");

        assert!(prep_directory(&dir, &logger).is_ok(), "Should open existing dir");

        // sanity checks for validity
        assert!(dir.exists(), "New directory should be created");
        assert!(dir.is_dir(), "New directory must be a directory");
    }

    #[test]
    fn test_fails_when_path_is_file() {
        let logger = test_logger("CreateDir");
        let invalid_dir = NamedTempFile::new().expect("new temp file");

        match prep_directory(&invalid_dir.path().to_path_buf(), &logger) {
            Ok(_) => panic!("must throw error when path is a file"),
            Err(e) => match e {
                InternalError::IO(_) => {}
                _ => panic!("expected InvalidPath error"),
            },
        }
    }
}
