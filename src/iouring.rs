use crate::errors::InternalResult;
use libc::{c_void, mmap, munmap, syscall, SYS_io_uring_setup, MAP_SHARED, PROT_READ, PROT_WRITE};
use std::{
    fs::File,
    mem::zeroed,
    os::{fd::AsRawFd, raw::c_int},
    ptr::null_mut,
};

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

const IOURING_REGISTER_BUFFERS: u32 = 0;
const IOURING_UNREGISTER_BUFFERS: u32 = 1;
const IOURING_REGISTER_FILES: u32 = 2;
const IOURING_UNREGISTER_FILES: u32 = 3;

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
    buf_base: *mut u8,
    rings: IOUringRings,
    params: IOUringParams,
    buf_page_size: usize,
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
        let active_buf_idx = 0;
        let buf_page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;

        //
        // io_uring init
        //

        let mut params: IOUringParams = std::mem::zeroed();

        params.flags = IOURING_SETUP_SQPOLL;
        params.sq_thread_idle = IOURING_POLL_THREAD_IDLE;

        let ring_fd = syscall(SYS_io_uring_setup, QUEUE_DEPTH, &mut params as *mut IOUringParams) as c_int;
        let file_fd = file.as_raw_fd();

        if ring_fd < 0 || file_fd < 0 {
            eprintln!("Either ring or file fd's are invalid!");
            return Err(std::io::Error::last_os_error().into());
        }

        //
        // mmap shared rings (SQ, CQ, etc.)
        //

        let rings = Self::mmap_rings(ring_fd, &params).map_err(|e| {
            libc::close(ring_fd);
            e
        })?;

        //
        // Register file descriptor
        //

        if let Err(e) = Self::register_files(ring_fd, file_fd) {
            libc::close(ring_fd);
            return Err(e);
        }

        //
        // allocate (page align) and register buffer to be used for write syscalls
        //

        let (iovecs, buf_base) = Self::allocate_bufs(buf_page_size).map_err(|e| {
            libc::close(ring_fd);
            e
        })?;

        if let Err(e) = Self::register_buffers(ring_fd, iovecs.as_ptr(), iovecs.len()) {
            libc::close(ring_fd);
            return Err(e);
        }

        Ok(Self {
            ring_fd,
            file_fd,
            rings,
            params,
            buf_base,
            active_buf_idx,
            buf_page_size,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn allocate_bufs(page_size: usize) -> InternalResult<(Vec<libc::iovec>, *mut u8)> {
        let total_size = page_size.checked_mul(PAGE_BUF_SIZE).ok_or_else(|| {
            eprintln!("Mult error while calculating total_size in alloc_bufs!");
            std::io::Error::from_raw_os_error(libc::EINVAL)
        })?;

        let layout = std::alloc::Layout::from_size_align(total_size, page_size).map_err(|_| {
            eprintln!("unable to create layout in alloc_bufs!");
            std::io::Error::from_raw_os_error(libc::EINVAL)
        })?;

        let base_ptr = std::alloc::alloc_zeroed(layout);

        if base_ptr.is_null() {
            eprintln!("base_ptr is null in alloc_bufs!");
            return Err(std::io::Error::last_os_error().into());
        }

        let mut iovecs = Vec::with_capacity(PAGE_BUF_SIZE);

        for i in 0..PAGE_BUF_SIZE {
            let ptr = base_ptr.add(i * page_size);

            iovecs.push(libc::iovec {
                iov_base: ptr as *mut c_void,
                iov_len: page_size,
            });
        }

        Ok((iovecs, base_ptr))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_rings(ring_fd: i32, params: &IOUringParams) -> InternalResult<IOUringRings> {
        let sq_ring_sz = params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>();
        let cq_ring_sz = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();

        let single_mmap = params.features & IOURING_FEAT_SINGLE_MMAP != 0;
        let ring_sz = std::cmp::max(sq_ring_sz, cq_ring_sz);

        // helper to create mmap
        let do_mmap = |len: usize, offset: libc::off_t| -> InternalResult<*mut c_void> {
            let ptr = mmap(null_mut(), len, PROT_READ | PROT_WRITE, MAP_SHARED, ring_fd, offset);

            if ptr == libc::MAP_FAILED {
                let err = std::io::Error::last_os_error();
                eprintln!("Unable to create mmap => {:?}", err);
                return Err(err.into());
            }

            Ok(ptr)
        };

        // helper for cleanup on failure (unmap mapped objects)
        let cleanup = |sq: *mut c_void, cq: *mut c_void, sqes: *mut c_void| {
            if !sqes.is_null() && sqes != libc::MAP_FAILED {
                munmap(sqes, params.sq_entries as usize * std::mem::size_of::<IoUringSqe>());
            }

            if !cq.is_null() && cq != libc::MAP_FAILED && cq != sq {
                munmap(cq, cq_ring_sz);
            }

            if !sq.is_null() && sq != libc::MAP_FAILED {
                munmap(sq, if single_mmap { ring_sz } else { sq_ring_sz });
            }
        };

        // SQ ring map
        let sq_ptr = do_mmap(if single_mmap { ring_sz } else { sq_ring_sz }, IOURING_OFF_SQ_RING)?;

        // CQ ring map
        let cq_ptr = if single_mmap {
            sq_ptr
        } else {
            match do_mmap(cq_ring_sz, IOURING_OFF_CQ_RING) {
                Ok(ptr) => ptr,

                Err(e) => {
                    cleanup(sq_ptr, null_mut(), null_mut());
                    return Err(e.into());
                }
            }
        };

        // SQEs array map
        let sqes_sz = params.sq_entries as usize * std::mem::size_of::<IoUringSqe>();

        let sqes_ptr = match do_mmap(sqes_sz, IOURING_OFF_SQES) {
            Ok(ptr) => ptr,

            Err(e) => {
                cleanup(sq_ptr, cq_ptr, null_mut());
                return Err(e.into());
            }
        };

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
            IOURING_REGISTER_FILES as libc::c_ulong,
            fds.as_ptr(),
            fds.len() as libc::c_uint,
        ) as libc::c_int;

        if ret < 0 {
            eprintln!("Unable to register file! ring_fd={ring_fd} & file_fd={file_fd}");
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn register_buffers(ring_fd: i32, iovecs: *const libc::iovec, iovecs_len: usize) -> InternalResult<()> {
        let ret = libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_BUFFERS as libc::c_ulong,
            iovecs,
            iovecs_len as u32 as libc::c_uint,
        ) as libc::c_int;

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!(
                "Unable to register bufs! ring_fd={ring_fd}, ret={}, errno={:?}",
                ret, err
            );
            return Err(err.into());
        }

        Ok(())
    }
}

impl Drop for IOUring {
    fn drop(&mut self) {
        unsafe {
            //
            // clear up buffer alocated for write ops
            //
            if !self.buf_base.is_null() {
                let total_size = self.buf_page_size * PAGE_BUF_SIZE;

                match std::alloc::Layout::from_size_align(total_size, self.buf_page_size) {
                    Ok(layout) => std::alloc::dealloc(self.buf_base, layout),
                    Err(e) => eprintln!("Unable to drop layout => {:?}", e),
                }
            }

            //
            // Unmap the SQE's array map
            //
            if !self.rings.sqes_ptr.is_null() {
                munmap(
                    self.rings.sqes_ptr,
                    self.params.sq_entries as usize * std::mem::size_of::<IoUringSqe>(),
                );
            }

            //
            // Unmap the CQ map
            //
            if !self.rings.cq_ptr.is_null() && self.rings.cq_ptr != self.rings.sq_ptr {
                let cq_size =
                    self.params.cq_off.cqes as usize + self.params.cq_entries as usize * std::mem::size_of::<u64>();

                munmap(self.rings.cq_ptr, cq_size);
            }

            //
            // Unmap the SQ map
            //
            if !self.rings.sq_ptr.is_null() {
                let sq_size = if self.params.features & IOURING_FEAT_SINGLE_MMAP != 0 {
                    std::cmp::max(
                        self.params.sq_off.array as usize
                            + self.params.sq_entries as usize * std::mem::size_of::<u32>(),
                        self.params.cq_off.cqes as usize + self.params.cq_entries as usize * std::mem::size_of::<u64>(),
                    )
                } else {
                    self.params.sq_off.array as usize + self.params.sq_entries as usize * std::mem::size_of::<u32>()
                };

                munmap(self.rings.sq_ptr, sq_size);
            }

            libc::close(self.ring_fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::OpenOptions, io::Read};
    use tempfile::TempDir;

    fn create_iouring() -> (IOUring, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("temp_io_uring");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .expect("file");

        let io_ring = unsafe { IOUring::new(&file).expect("Failed to create io_uring") };

        (io_ring, tmp)
    }

    #[test]
    fn test_iouring_init() {
        let (io_ring, _) = create_iouring();

        assert!(io_ring.ring_fd >= 0, "Ring fd must be non-negative");
        assert!(io_ring.file_fd >= 0, "File fd must be non-negative");

        assert!(io_ring.params.sq_off.array != 0, "SQE's offset must be set by kernel");
        assert!(io_ring.params.cq_off.cqes != 0, "CQE's offset must be set by kernel");

        assert!(!io_ring.rings.sq_ptr.is_null(), "SQ pointer must be valid");
        assert!(!io_ring.rings.cq_ptr.is_null(), "CQ pointer must be valid");
        assert!(!io_ring.rings.sqes_ptr.is_null(), "SQEs pointer must be valid");

        assert!(io_ring.buf_page_size > 0, "Buf page size must not be zero");
        assert_eq!(io_ring.active_buf_idx, 0, "Active buf idx must start from 0");
        assert!(io_ring.buf_base != null_mut(), "Base buf ptr must not be 0");

        drop(io_ring);
    }

    #[test]
    fn test_allocate_bufs() {
        unsafe {
            let page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;
            let (iovecs, base_ptr) = IOUring::allocate_bufs(page_size).expect("alloc failed");

            assert_eq!(iovecs.len(), PAGE_BUF_SIZE);
            assert!(!base_ptr.is_null());

            for i in 0..PAGE_BUF_SIZE {
                let ptr = base_ptr.add(i * page_size);

                assert_eq!(iovecs[i].iov_base, ptr as *mut c_void, "Each iovec must be zeroed");
                assert_eq!(iovecs[i].iov_len, page_size, "Each iovec must be of PAGE_SIZE");
            }

            // cleanup to avoid resource leak
            let layout = std::alloc::Layout::from_size_align(page_size * PAGE_BUF_SIZE, page_size).unwrap();
            std::alloc::dealloc(base_ptr, layout);
        }
    }

    #[test]
    fn test_register_files_invalid_fd() {
        unsafe {
            let res = IOUring::register_files(-1, -1);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_register_buffers_invalid_ptr() {
        unsafe {
            let res = IOUring::register_buffers(-1, null_mut(), 1);
            assert!(res.is_err());
        }
    }
}
