#[cfg(target_os = "linux")]
mod mmap;

#[cfg(target_os = "linux")]
mod file;

#[cfg(target_os = "linux")]
mod iouring;

#[cfg(target_os = "linux")]
pub(crate) use mmap::MMap;

#[cfg(target_os = "linux")]
pub(crate) use file::File;

#[cfg(target_os = "linux")]
pub(crate) use iouring::IOUring;
