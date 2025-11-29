#[cfg(target_os = "linux")]
pub(crate) mod mmap;

#[cfg(target_os = "linux")]
pub(crate) mod file;
