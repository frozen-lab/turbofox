#[cfg(target_os = "linux")]
pub(crate) mod iouring;

#[cfg(target_os = "linux")]
pub(crate) mod mmap;
