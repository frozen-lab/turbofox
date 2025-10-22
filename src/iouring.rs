use std::{fs::File, os::fd::AsRawFd, sync::atomic::AtomicI64};

use crate::errors::InternalResult;
use libc::{
    c_void, mmap, munmap, syscall, SYS_io_uring_setup, MAP_FAILED, MAP_POPULATE, MAP_SHARED, PROT_READ, PROT_WRITE,
};

const QUEUE_DEPTH: u32 = 0x400; // 1024 i.e. ~80 KiB of memory overhead
const IOURING_SETUP_SQPOLL: u32 = 0x02;

const IOURING_OFF_SQ_RING: i64 = 0x00;
const IOURING_OFF_SQES: i64 = 0x10000000;
const IOURING_OFF_CQ_RING: i64 = 0x8000000;

const IOURING_POLL_THREAD_IDLE: u32 = 0xC8; // 200 ms

const IORING_OP_WRITE: u8 = 0x01;
const IORING_OP_FSYNC: u8 = 0x23;
const IOSQE_IO_LINK: u8 = 0x01;

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
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

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
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

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
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

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct io_uring_sqe {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: u64,
    addr: u64,
    len: u32,
    rw_flags: u32,
    user_data: u64,
    buf_index: u16,
    personality: u16,
    splice_fd_in: i32,
    __pad2: [u64; 2],
}

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct io_uring_cqe {
    user_data: u64,
    res: i32,
    flags: u32,
}

struct IOUringReq {
    buf: Box<[u8]>,
    result: AtomicI64,
}

impl IOUringReq {
    fn new(buf: Box<[u8]>) -> Box<Self> {
        Box::new(Self {
            buf,
            result: AtomicI64::new(i64::MIN),
        })
    }
}

pub(super) struct IOUring {
    ring_fd: i32,
    file_fd: i32,
    cq_ptr: *mut c_void,
    sq_ptr: *mut c_void,
    sqe_ptr: *mut c_void,
    params: io_uring_params,
}

impl IOUring {
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(super) unsafe fn new(file: &File) -> InternalResult<Self> {
        let file_fd = file.as_raw_fd();

        //
        // io_uring init syscall
        //

        let mut params: io_uring_params = std::mem::MaybeUninit::zeroed().assume_init();

        params.flags = IOURING_SETUP_SQPOLL;
        params.sq_entries = QUEUE_DEPTH;
        params.sq_thread_idle = IOURING_POLL_THREAD_IDLE;

        let ring_fd = syscall(
            SYS_io_uring_setup,
            QUEUE_DEPTH,
            &mut params as *mut io_uring_params as *mut c_void,
        ) as i32;

        if ring_fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        // mmap pointers for ring buffers
        let (sq_ptr, cq_ptr, sqe_ptr) = Self::create_mmaped_io_rings(ring_fd, &params)?;

        Ok(Self {
            ring_fd,
            file_fd,
            cq_ptr,
            sq_ptr,
            sqe_ptr,
            params,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn create_mmaped_io_rings(
        ring_fd: i32,
        params: &io_uring_params,
    ) -> InternalResult<(*mut c_void, *mut c_void, *mut c_void)> {
        //
        // SQ ring mmap
        //

        let sq_ring_size = params.sq_off.array as usize + (params.sq_entries as usize * std::mem::size_of::<u32>());
        let sq_ptr = mmap(
            std::ptr::null_mut(),
            sq_ring_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            ring_fd,
            IOURING_OFF_SQ_RING,
        );

        if sq_ptr == MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }

        //
        // CQ ring mmap
        //

        let cq_ring_size = params.cq_off.cqes as usize + (params.cq_entries as usize * std::mem::size_of::<u32>());
        let cq_ptr = mmap(
            std::ptr::null_mut(),
            cq_ring_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            ring_fd,
            IOURING_OFF_CQ_RING,
        );

        if cq_ptr == MAP_FAILED {
            // NOTE: We free or unmap the SQ ring mmap 'cause the
            // entire process is being exited
            munmap(sq_ptr, sq_ring_size);

            return Err(std::io::Error::last_os_error().into());
        }

        //
        // SQE (Submission Queue Entry) memory map
        //

        let sqe_size = std::mem::size_of::<io_uring_sqe>();
        let sqe_region_size = params.sq_entries as usize * sqe_size;

        let sqe_ptr = mmap(
            std::ptr::null_mut(),
            sqe_region_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            ring_fd,
            IOURING_OFF_SQES,
        );

        if sqe_ptr == MAP_FAILED {
            // NOTE: We free or unmap the SQ and CQ ring mmaps 'cause the entire
            // process is being exited
            munmap(sq_ptr, sq_ring_size);
            munmap(cq_ptr, cq_ring_size);

            return Err(std::io::Error::last_os_error().into());
        }

        Ok((sq_ptr, cq_ptr, sqe_ptr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
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
        assert!(
            io_ring.params.sq_off.array != 0,
            "SQ array offset must be set by kernel"
        );
        assert!(io_ring.params.cq_off.cqes != 0, "CQes offset must be set by kernel");

        // NOTE: This is imp to prevent FD exhaustion!
        unsafe { libc::close(io_ring.ring_fd) };
    }

    #[test]
    fn test_iouring_mmap_init_is_valid_and_is_io_ready() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("temp_io_uring");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .expect("file");

        let io_ring = unsafe { IOUring::new(&file).expect("Failed to init io_uring") };

        assert!(!io_ring.sq_ptr.is_null(), "SQ ring mmap failed");
        assert!(!io_ring.cq_ptr.is_null(), "CQ ring mmap failed");
        assert!(!io_ring.sqe_ptr.is_null(), "SQE mmap failed");

        // NOTE: Here we try touching first byte of each mmaped region! This is to ensure mapping is
        // I/O ready!
        unsafe {
            //
            // SQ
            //

            let sq_head_ptr = (io_ring.sq_ptr as *mut u8).add(io_ring.params.sq_off.head as usize) as *mut u32;
            let sq_tail_ptr = (io_ring.sq_ptr as *mut u8).add(io_ring.params.sq_off.tail as usize) as *mut u32;

            assert_eq!(*sq_head_ptr, 0, "SQ head not initialized to 0");
            assert_eq!(*sq_tail_ptr, 0, "SQ tail not initialized to 0");

            //
            // CQ
            //

            let cq_head_ptr = (io_ring.cq_ptr as *mut u8).add(io_ring.params.cq_off.head as usize) as *mut u32;
            let cq_tail_ptr = (io_ring.cq_ptr as *mut u8).add(io_ring.params.cq_off.tail as usize) as *mut u32;

            assert_eq!(*cq_head_ptr, 0, "CQ head not initialized to 0");
            assert_eq!(*cq_tail_ptr, 0, "CQ tail not initialized to 0");
        }

        // NOTE: This is imp to prevent FD exhaustion!
        unsafe { libc::close(io_ring.ring_fd) };
    }
}
