use crate::errors::InternalResult;
use libc::{c_void, mmap, munmap, off_t, syscall, SYS_io_uring_setup, MAP_SHARED, PROT_READ, PROT_WRITE};
use std::{
    fs::File,
    mem::zeroed,
    os::{fd::AsRawFd, raw::c_int},
    ptr::null_mut,
};

const IOURING_SETUP_SQPOLL: u32 = 1 << 0;
const IOURING_SETUP_SQ_AFF: u32 = 1 << 1;
const IOURING_FEAT_SINGLE_MMAP: u32 = 1 << 0;

const QUEUE_DEPTH: u32 = 0x400; // 1024 i.e. ~80 KiB of memory overhead
const IOURING_POLL_THREAD_IDLE: u32 = 200; // 200 ms
const IOURING_CPU_THREAD_ID: u32 = 0x01; // pinned to a single CPU for predictable cache locality

const IOURING_OFF_SQ_RING: off_t = 0;
const IOURING_OFF_CQ_RING: off_t = 0x8000000;
const IOURING_OFF_SQES: off_t = 0x10000000;

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct SqringOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    user_addr: u64,
}

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct CqringOffsets {
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

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct IoUringSqe {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: u64,
    addr: u64,
    len: u32,
    union1: u32,
    user_data: u64,
    union2: [u64; 3],
}

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct IOUringParams {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    resv: [u32; 4],
    sq_off: SqringOffsets,
    cq_off: CqringOffsets,
}

struct IOUringRings {
    sq_ptr: *mut c_void,
    cq_ptr: *mut c_void,
    sqes_ptr: *mut c_void,
}

pub(super) struct IOUring {
    ring_fd: i32,
    file_fd: i32,
    rings: IOUringRings,
    params: IOUringParams,
}

impl std::fmt::Debug for IOUring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IOUring")
            .field("ring_fd", &self.ring_fd)
            .field("file_fd", &self.file_fd)
            .finish()
    }
}

impl IOUring {
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn new(file: &File) -> InternalResult<Self> {
        let mut params: IOUringParams = std::mem::zeroed();

        params.flags = IOURING_SETUP_SQPOLL;
        params.sq_thread_idle = IOURING_POLL_THREAD_IDLE;
        params.sq_thread_cpu = IOURING_CPU_THREAD_ID;

        let ring_fd = syscall(SYS_io_uring_setup, QUEUE_DEPTH, &mut params as *mut IOUringParams) as c_int;
        let file_fd = file.as_raw_fd();

        if ring_fd < 0 || file_fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        let rings = Self::mmap_rings(ring_fd, &params)?;

        Ok(Self {
            ring_fd,
            file_fd,
            rings,
            params,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_rings(ring_fd: i32, params: &IOUringParams) -> InternalResult<IOUringRings> {
        let sq_size = params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>();
        let cq_size = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();

        let sq_ptr = mmap(
            null_mut(),
            sq_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            ring_fd,
            IOURING_OFF_SQ_RING,
        );
        let cq_ptr = mmap(
            null_mut(),
            cq_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            ring_fd,
            IOURING_OFF_CQ_RING,
        );
        let sqes_ptr = mmap(
            null_mut(),
            params.sq_entries as usize * std::mem::size_of::<IoUringSqe>(),
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            ring_fd,
            IOURING_OFF_SQES,
        );

        if sq_ptr == libc::MAP_FAILED || cq_ptr == libc::MAP_FAILED || sqes_ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(IOUringRings {
            sq_ptr,
            cq_ptr,
            sqes_ptr,
        })
    }
}

impl Drop for IOUring {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.ring_fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::OpenOptions, io::Read};
    use tempfile::TempDir;

    #[test]
    fn test_iouring_init() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("temp_io_uring");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .expect("file");

        let io_ring = unsafe { IOUring::new(&file).expect("Failed to create io_uring") };

        assert!(io_ring.ring_fd >= 0, "Ring fd must be non-negative");
        assert!(io_ring.file_fd >= 0, "File fd must be non-negative");

        assert!(io_ring.params.sq_off.array != 0, "SQE's offset must be set by kernel");
        assert!(io_ring.params.cq_off.cqes != 0, "CQE's offset must be set by kernel");

        assert!(!io_ring.rings.sq_ptr.is_null(), "SQ pointer must be valid");
        assert!(!io_ring.rings.cq_ptr.is_null(), "CQ pointer must be valid");
        assert!(!io_ring.rings.sqes_ptr.is_null(), "SQEs pointer must be valid");

        drop(io_ring);
    }
}
