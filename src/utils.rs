/// Check if `x` is power of 2 or not
#[inline]
pub(crate) const fn is_pow_of_2(x: usize) -> bool {
    (x & (x - 1)) == 0
}

#[test]
fn test_correctness_of_is_pow_of_2() {
    assert!(!is_pow_of_2(0x1C), "28 is not pow of 2");
    assert!(is_pow_of_2(0x20), "32 is pow of 2");
}

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

    // create dir if missing (w/ fast fail)
    if !dirpath.exists() {
        std::fs::create_dir_all(&dirpath)
            .inspect(|_| {
                logger.info(
                    LogCtx::Cfg,
                    format!("New directory for TurboFox at path=[{:?}]", dirpath),
                );
            })
            .map_err(|e| {
                logger.error(
                    LogCtx::Cfg,
                    format!(
                        "Unable to create new directory for TurboFox at path=[{:?}] due to error: {e}",
                        dirpath
                    ),
                );
                e
            })?;
    }

    if !dirpath.is_dir() {
        logger.error(
            LogCtx::Cfg,
            format!(
                "Failed to create/open directory for TurboFox as given Path=[{:?}] is not a directory",
                dirpath
            ),
        );

        return Err(InternalError::InvalidPath(format!(
            "Path=[{:?}] is not a directory",
            dirpath
        )));
    }

    // NOTE: We must have read permission to the directory
    std::fs::read_dir(&dirpath).map_err(|e| {
        let err = InternalError::PermissionDenied(format!("{e}"));
        logger.error(
            LogCtx::Cfg,
            format!("Failed to read from path=[{:?}] due to error: {err}", dirpath),
        );
        err
    })?;

    // NOTE: we must have write permission to the directory
    let test_file = dirpath.join(".turbofox_perm_test");
    match std::fs::File::create(&test_file) {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
        }
        Err(e) => {
            let err = InternalError::PermissionDenied(format!("{e}"));
            logger.error(
                LogCtx::Cfg,
                format!("Failed to write into path=[{:?}] due to error: {err}", dirpath),
            );
            return Err(err);
        }
    }

    Ok(())
}

#[cfg(test)]
mod test_prep_directory {
    use super::prep_directory;
    use crate::{error::InternalError, logger::test_logger};
    use std::os::unix::fs::PermissionsExt;
    use std::{
        fs::{set_permissions, Permissions},
        path::PathBuf,
    };
    use tempfile::{NamedTempFile, TempDir};

    #[cfg(target_os = "linux")]
    fn chmod(dirpath: &PathBuf, mode: u32) {
        let perms = Permissions::from_mode(mode);
        set_permissions(dirpath, perms).expect("Set directory permissions");
    }

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
                InternalError::InvalidPath(_) => {}
                _ => panic!("expected InvalidPath error"),
            },
        }
    }

    #[test]
    fn test_fails_on_no_read_permission() {
        let logger = test_logger("CreateDir");
        let tmp = TempDir::new().expect("New temp dir");
        let dir = tmp.path().join("dummy");

        // new dir w/ restricted perms
        std::fs::create_dir_all(&dir).expect("create new directory");
        chmod(&dir, 0o300);

        match prep_directory(&dir, &logger) {
            Ok(_) => panic!("must throw error when failed to get read permission"),
            Err(e) => match e {
                InternalError::PermissionDenied(_) => {}
                _ => panic!("expected PermissionDenied error"),
            },
        }
    }

    #[test]
    fn test_fails_on_no_write_permission() {
        let logger = test_logger("CreateDir");
        let tmp = TempDir::new().expect("New temp dir");
        let dir = tmp.path().join("dummy");

        // new dir w/ restricted perms
        std::fs::create_dir_all(&dir).expect("create new directory");
        chmod(&dir, 0o500);

        match prep_directory(&dir, &logger) {
            Ok(_) => panic!("must throw error when failed to get write permission"),
            Err(e) => match e {
                InternalError::PermissionDenied(_) => {}
                _ => panic!("expected PermissionDenied error"),
            },
        }
    }

    #[test]
    fn test_fails_on_no_execute_permission() {
        let logger = test_logger("CreateDir");
        let tmp = TempDir::new().expect("New temp dir");
        let dir = tmp.path().join("dummy");

        // new dir w/ restricted perms
        std::fs::create_dir_all(&dir).expect("create new directory");
        chmod(&dir, 0o600);

        match prep_directory(&dir, &logger) {
            Ok(_) => panic!("must throw error when failed to get execute permission"),
            Err(e) => match e {
                InternalError::PermissionDenied(_) => {}
                _ => panic!("expected PermissionDenied error"),
            },
        }
    }
}
