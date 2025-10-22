use crate::errors::{InternalError, InternalResult};
use core::mem::zeroed;
use libc::{c_void, syscall, SYS_io_uring_setup};
use libc::{mmap, munmap, MAP_FAILED, MAP_POPULATE, MAP_SHARED, PROT_READ, PROT_WRITE};

const QUEUE_DEPTH: u32 = 1024; // ~80 KiB of memory overhead
const IORING_SETUP_SQPOLL: u32 = 0x02;

const IORING_OFF_SQ_RING: i64 = 0;
const IORING_OFF_SQES: i64 = 0x10000000;
const IORING_OFF_CQ_RING: i64 = 0x8000000;

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
#[derive(Copy, Clone)]
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

#[derive(Copy, Clone, Debug)]
pub(super) struct IoUring {
    rfd: i32,
    sq_ptr: *mut c_void,
    sqe_ptr: *mut c_void,
    cq_ptr: *mut c_void,
    params: io_uring_params,
}

impl IoUring {
    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(super) unsafe fn new() -> InternalResult<Self> {
        let mut params: io_uring_params = std::mem::MaybeUninit::zeroed().assume_init();

        params.flags = IORING_SETUP_SQPOLL;
        params.sq_entries = QUEUE_DEPTH;
        params.sq_thread_idle = 2000;

        let rfd = syscall(
            SYS_io_uring_setup,
            QUEUE_DEPTH,
            &mut params as *mut io_uring_params as *mut c_void,
        ) as i32;

        if rfd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        let (sq_ptr, sqe_ptr, cq_ptr) = Self::init_mapped_rings(rfd, &params)?;

        Ok(Self {
            params,
            rfd,
            sq_ptr,
            sqe_ptr,
            cq_ptr,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn init_mapped_rings(
        rfd: i32,
        params: &io_uring_params,
    ) -> InternalResult<(*mut c_void, *mut c_void, *mut c_void)> {
        const U32SIZE: usize = std::mem::size_of::<u32>();

        //
        // SQ ring
        //

        let sq_ring_size = params.sq_off.array as usize + params.sq_entries as usize * U32SIZE;
        let sq_ptr = mmap(
            std::ptr::null_mut(),
            sq_ring_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            rfd,
            IORING_OFF_SQ_RING,
        );

        if sq_ptr == MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }

        //
        // CQ ring
        //

        let cq_ring_size = params.cq_off.cqes as usize + params.cq_entries as usize * U32SIZE;
        let cq_ptr = mmap(
            std::ptr::null_mut(),
            cq_ring_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            rfd,
            IORING_OFF_CQ_RING,
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
            rfd,
            IORING_OFF_SQES,
        );

        if sqe_ptr == MAP_FAILED {
            // NOTE: We free or unmap the SQ and CQ ring mmaps 'cause the entire
            // process is being exited
            munmap(sq_ptr, sq_ring_size);
            munmap(cq_ptr, cq_ring_size);

            return Err(std::io::Error::last_os_error().into());
        }

        Ok((sq_ptr, sqe_ptr, cq_ptr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iouring_init() {
        let io_ring = unsafe { IoUring::new().expect("Failed to create io_uring") };

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

    #[test]
    fn test_iouring_mmap_init_is_valid_and_is_io_ready() {
        let io_ring = unsafe { IoUring::new().expect("Failed to init io_uring") };

        assert!(!io_ring.sq_ptr.is_null(), "SQ ring mmap failed");
        assert!(!io_ring.cq_ptr.is_null(), "CQ ring mmap failed");
        assert!(!io_ring.sqe_ptr.is_null(), "SQE mmap failed");

        // NOTE: Here we try touching first byte of each mmaped region! This is to ensure mapping is
        // I/O ready!
        unsafe {
            //
            // SQ
            //

            let sq_head_ptr =
                (io_ring.sq_ptr as *mut u8).add(io_ring.params.sq_off.head as usize) as *mut u32;
            let sq_tail_ptr =
                (io_ring.sq_ptr as *mut u8).add(io_ring.params.sq_off.tail as usize) as *mut u32;

            assert_eq!(*sq_head_ptr, 0, "SQ head not initialized to 0");
            assert_eq!(*sq_tail_ptr, 0, "SQ tail not initialized to 0");

            //
            // CQ
            //

            let cq_head_ptr =
                (io_ring.cq_ptr as *mut u8).add(io_ring.params.cq_off.head as usize) as *mut u32;
            let cq_tail_ptr =
                (io_ring.cq_ptr as *mut u8).add(io_ring.params.cq_off.tail as usize) as *mut u32;

            assert_eq!(*cq_head_ptr, 0, "CQ head not initialized to 0");
            assert_eq!(*cq_tail_ptr, 0, "CQ tail not initialized to 0");
        }

        // NOTE: This is imp to prevent FD exhaustion!
        unsafe { libc::close(io_ring.rfd) };
    }
}
