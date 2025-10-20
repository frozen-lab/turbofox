use super::deha::PAGE_SIZE;
use crate::errors::InternalResult;
use libc::*;
use std::fs::File;
use std::os::unix::io::AsRawFd;

const QUEUE_DEPTH: usize = 128;
const BUFFER_SIZE: usize = 64;

#[allow(non_camel_case_types)]
#[repr(C)]
struct io_uring_sqe {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: u64,
    addr: u64,
    len: u32,
    rw_flags: i32,
    user_data: u64,
    _pad: [u64; 3],
}

#[allow(non_camel_case_types)]
#[repr(C)]
struct io_uring_cqe {
    user_data: u64,
    res: i32,
    flags: u32,
}

pub(super) struct IoUring {
    file_fd: i32,
    ring_fd: i32,
    sqes: *mut io_uring_sqe,
    cqes: *mut io_uring_cqe,
}

impl IoUring {
    pub(super) fn new(file: &File) -> InternalResult<()> {
        let file_fd = file.as_raw_fd();
        let ring_fd = unsafe {
            syscall(
                SYS_io_uring_setup,
                QUEUE_DEPTH,
                std::ptr::null_mut::<libc::c_void>(),
            ) as i32
        };

        Ok(())
    }
}
