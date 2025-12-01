#![allow(unused)]

mod burrow;
mod cfg;
mod core;
mod errors;
mod hasher;
mod linux;
mod logger;

// -----------------------------------------------------------------------------
// Compile guard!
// TurboFox only supports Linux 64-bit architectures (x86_64 and AArch64).
// -----------------------------------------------------------------------------
#[cfg(not(all(
    any(target_arch = "x86_64", target_arch = "aarch64"),
    any(target_os = "linux", target_os = "windows")
)))]
compile_error!(
    "[ERROR]: TurboFox requires 64-bit Linux/Windows (x86_64 or AArch64). MacOS targets, and Linux/Windows 32-bit targets (i386/armv7) are not supported."
);

pub use crate::cfg::TurboConfig;
pub use crate::logger::TurboLogLevel;

#[derive(Debug, Clone, Copy)]
pub struct TurboFox;
