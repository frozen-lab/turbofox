use core::mem::zeroed;
use libc::{c_void, syscall, SYS_io_uring_setup};

use crate::errors::{InternalError, InternalResult};

const QUEUE_DEPTH: u32 = 1024; // ~80 KiB of memory overhead
const IORING_SETUP_SQPOLL: u32 = 0x02;

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
struct io_sqring_offsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    resv1: u32,
    user_addr: u64,
}

impl Default for io_sqring_offsets {
    fn default() -> Self {
        unsafe { zeroed() }
    }
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
struct io_cqring_offsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: u32,
    cqes: u32,
    flags: u32,
    resv1: u32,
    user_addr: u64,
}

impl Default for io_cqring_offsets {
    fn default() -> Self {
        unsafe { zeroed() }
    }
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
struct io_uring_params {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    wq_fd: u32,
    resv: [u32; 3],
    sq_off: io_sqring_offsets,
    cq_off: io_cqring_offsets,
}

impl Default for io_uring_params {
    fn default() -> Self {
        unsafe { zeroed() }
    }
}

pub(super) struct IoUring {
    params: io_uring_params,
    rfd: i32,
}

impl IoUring {
    pub(super) fn new() -> InternalResult<Self> {
        let mut params: io_uring_params = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };

        params.flags = IORING_SETUP_SQPOLL;
        params.sq_entries = QUEUE_DEPTH;
        params.sq_thread_idle = 2000;

        let rfd = unsafe {
            syscall(
                SYS_io_uring_setup,
                QUEUE_DEPTH,
                &mut params as *mut io_uring_params as *mut c_void,
            ) as i32
        };

        if rfd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(Self { params, rfd })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_uring_setup() {
        let io_ring = IoUring::new().expect("Failed to create io_uring");

        assert!(io_ring.rfd >= 0, "Ring fd must be non-negative");
        assert!(
            io_ring.params.sq_off.array != 0,
            "SQ array offset must be set by kernel"
        );
        assert!(
            io_ring.params.cq_off.cqes != 0,
            "CQes offset must be set by kernel"
        );

        println!("RFD is {}", io_ring.rfd);

        // NOTE: This is imp to prevent FD exhaustion!
        unsafe { libc::close(io_ring.rfd) };
    }
}
