use crate::errors::InternalResult;
use libc::{c_void, mmap, munmap, syscall, SYS_io_uring_setup, MAP_SHARED, PROT_READ, PROT_WRITE};
use std::{
    fs::File,
    mem::zeroed,
    os::{fd::AsRawFd, raw::c_int},
    ptr::null_mut,
};

pub(super) const PAGE_SIZE: usize = 128;

/// No. of page bufs we can store into mem for write syscalls
///
/// TODO: We must take `max_value` as config from user, and it
/// should match this, so we will not have race conditions, cause
/// we must block new writes (thread sleep, etc.) if no bufs are
/// available to write into
pub(super) const PAGE_BUF_SIZE: usize = 128;

const IOURING_SETUP_SQPOLL: u32 = 1 << 0;
const IOURING_SETUP_SQ_AFF: u32 = 1 << 1;
const IOURING_FEAT_SINGLE_MMAP: u32 = 1 << 0;

const QUEUE_DEPTH: u32 = 0x400; // 1024 i.e. ~80 KiB of memory overhead
const IOURING_POLL_THREAD_IDLE: u32 = 200; // 200 ms

const IOURING_OFF_SQ_RING: libc::off_t = 0;
const IOURING_OFF_CQ_RING: libc::off_t = 0x8000000;
const IOURING_OFF_SQES: libc::off_t = 0x10000000;

const IOURING_REGISTER_FILES: u32 = 1;
const IOURING_REGISTER_FILES_UPDATE: u32 = 2;
const IOURING_REGISTER_BUFFERS: u32 = 3;

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

#[derive(Copy, Clone)]
#[repr(C)]
pub struct IoUringSqe {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: OffUnion,
    addr: AddrUnion,
    len: u32,
    user_data: u64,
    union2: [u64; 3],
}

#[derive(Copy, Clone)]
#[repr(C)]
union OffUnion {
    off: u64,
    addr2: u64,
}

#[derive(Copy, Clone)]
#[repr(C)]
union AddrUnion {
    addr: u64,
    splice_off_in: u64,
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
    bufs: Vec<Vec<u8>>,
    rings: IOUringRings,
    params: IOUringParams,
    active_buf_idx: usize,
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

        let ring_fd = syscall(SYS_io_uring_setup, QUEUE_DEPTH, &mut params as *mut IOUringParams) as c_int;
        let file_fd = file.as_raw_fd();

        if ring_fd < 0 || file_fd < 0 {
            eprintln!("Either ring or file fd's are invalid!");
            return Err(std::io::Error::last_os_error().into());
        }

        let rings = Self::mmap_rings(ring_fd, &params)?;
        Self::register_files(ring_fd, file_fd)?;

        //
        // register bufs for write syscalls
        //
        let mut bufs: Vec<Vec<u8>> = vec![vec![0u8; PAGE_SIZE]; PAGE_BUF_SIZE];
        let iovecs: Vec<libc::iovec> = bufs
            .iter_mut()
            .map(|buf| libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut c_void,
                iov_len: buf.len(),
            })
            .collect();

        Self::register_buffers(ring_fd, iovecs.as_ptr(), iovecs.len() as u32)?;

        Ok(Self {
            ring_fd,
            file_fd,
            rings,
            params,
            bufs,
            active_buf_idx: 0,
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
            eprintln!("MAP failed error in mmap");
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(IOUringRings {
            sq_ptr,
            cq_ptr,
            sqes_ptr,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn register_files(ring_fd: i32, file_fd: i32) -> InternalResult<()> {
        let fds = [file_fd];

        let ret = libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_FILES_UPDATE as libc::c_ulong,
            fds.as_ptr(),
            fds.len() as libc::c_uint,
        ) as libc::c_int;

        if ret < 0 {
            eprintln!("Unable to register file!");
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn register_buffers(ring_fd: i32, iovecs: *const libc::iovec, nr: u32) -> InternalResult<()> {
        let ret = syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_BUFFERS as libc::c_ulong,
            iovecs,
            nr as libc::c_uint,
        ) as libc::c_int;

        if ret < 0 {
            eprintln!("Unable to register bufs!");
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(())
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

        assert!(!io_ring.bufs.is_empty());
        assert_eq!(io_ring.bufs.len(), PAGE_BUF_SIZE);

        drop(io_ring);
    }
}
