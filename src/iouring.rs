use crate::errors::InternalResult;
use std::os::fd::AsRawFd;

/// No. of page bufs we can store into mem for write syscalls
///
/// TODO: We must take `max_value` as config from user, and it
/// should match this, so we will not have race conditions, cause
/// we must block new writes (thread sleep, etc.) if no bufs are
/// available to write into
pub(super) const NUM_BUFFER_PAGES: usize = 128;

/// SQE array size. It's 1024 i.e. ~80 KiB of memory overhead
const QUEUE_DEPTH: u32 = 0x400;

const IOURING_FEAT_SINGLE_MMAP: u32 = 1 << 0;

const IOURING_REGISTER_FILES: u32 = 2;
const IOURING_UNREGISTER_FILES: u32 = 3;

const IOURING_REGISTER_BUFFERS: u32 = 0;
const IOURING_UNREGISTER_BUFFERS: u32 = 1;

const IOURING_OFF_SQ_RING: libc::off_t = 0;
const IOURING_OFF_CQ_RING: libc::off_t = 0x8000000;
const IOURING_OFF_SQES: libc::off_t = 0x10000000;

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct SQringOffset {
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
struct CQringOffset {
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
pub struct IOUringSQE {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: SQEOffUnion,
    addr: SQEAddrUnion,
    len: u32,
    user_data: u64,
    union2: [u64; 3],
}

#[derive(Copy, Clone)]
#[repr(C)]
union SQEOffUnion {
    off: u64,
    addr2: u64,
}

#[derive(Copy, Clone)]
#[repr(C)]
union SQEAddrUnion {
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
    sq_off: SQringOffset,
    cq_off: CQringOffset,
}

#[derive(Debug, Copy, Clone)]
struct RingPtrs {
    sq_ptr: *mut libc::c_void,
    cq_ptr: *mut libc::c_void,
    sqes_ptr: *mut libc::c_void,
}

pub(super) struct IOUring {
    ring_fd: i32,
    file_fd: i32,
    rings: RingPtrs,
    buf_page_size: usize,
    active_buf_idx: usize,
    params: IOUringParams,
    iovecs: Vec<libc::iovec>,
    buf_base_ptr: *mut libc::c_void,
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
    pub(super) unsafe fn new(file: &std::fs::File) -> InternalResult<Self> {
        // TODO: Add sanity check to ensure min size in file
        // debug_assert!(file.metadata().expect("Metadata").len() >= ?, "File must atleast be of size X");

        let active_buf_idx = 0;
        let buf_page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;

        //
        // io_uring init
        //

        let mut params: IOUringParams = std::mem::zeroed();

        params.flags = 0x00;
        params.sq_thread_idle = 0x00;
        params.sq_thread_cpu = 0x00;

        let file_fd = file.as_raw_fd();
        let ring_fd =
            libc::syscall(libc::SYS_io_uring_setup, QUEUE_DEPTH, &mut params as *mut IOUringParams) as libc::c_int;

        if ring_fd < 0 || file_fd < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("Invalid file descriptor! ring={ring_fd} file={file_fd}\n ERR => {err}");

            return Err(err.into());
        }

        //
        // NOTE: Kernel fills the sq_off and cq_off fields, in case it's not filled, the logic
        // will fall apart!
        //
        // For now follwing assertion is a sanity check!
        //
        debug_assert!(
            params.sq_off.array != 0 && params.cq_off.cqes != 0,
            "Kernel did not fill SQ/CQ offsets"
        );

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
            clear_ring_mmaps_on_err(&rings, &params);
            libc::close(ring_fd);

            return Err(e);
        }

        //
        // Register buffers
        //
        let (iovecs, buf_base_ptr) = Self::register_buffers(ring_fd, buf_page_size).map_err(|e| {
            libc::close(ring_fd);
            clear_ring_mmaps_on_err(&rings, &params);

            e
        })?;

        Ok(Self {
            rings,
            params,
            ring_fd,
            file_fd,
            active_buf_idx,
            buf_page_size,
            iovecs,
            buf_base_ptr,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_rings(ring_fd: i32, params: &IOUringParams) -> InternalResult<RingPtrs> {
        let sq_ring_sz = params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>();
        let cq_ring_sz = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();

        let single_mmap = params.features & IOURING_FEAT_SINGLE_MMAP != 0;
        let ring_sz = std::cmp::max(sq_ring_sz, cq_ring_sz);

        // helper to create mmap
        let do_mmap = |len: usize, offset: libc::off_t| -> InternalResult<*mut libc::c_void> {
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                ring_fd,
                offset,
            );

            if ptr == libc::MAP_FAILED {
                let err = std::io::Error::last_os_error();
                eprintln!("Unable to create mmap => {:?}", err);
                return Err(err.into());
            }

            Ok(ptr)
        };

        // helper for cleanup on failure (unmap mapped objects)
        let cleanup = |sq: *mut libc::c_void, cq: *mut libc::c_void, sqes: *mut libc::c_void| {
            if !sqes.is_null() && sqes != libc::MAP_FAILED {
                libc::munmap(sqes, params.sq_entries as usize * std::mem::size_of::<IOUringSQE>());
            }

            if !cq.is_null() && cq != libc::MAP_FAILED && cq != sq {
                libc::munmap(cq, cq_ring_sz);
            }

            if !sq.is_null() && sq != libc::MAP_FAILED {
                libc::munmap(sq, if single_mmap { ring_sz } else { sq_ring_sz });
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
                    cleanup(sq_ptr, std::ptr::null_mut(), std::ptr::null_mut());
                    return Err(e.into());
                }
            }
        };

        // SQEs array map
        let sqes_sz = params.sq_entries as usize * std::mem::size_of::<IOUringSQE>();

        let sqes_ptr = match do_mmap(sqes_sz, IOURING_OFF_SQES) {
            Ok(ptr) => ptr,

            Err(e) => {
                cleanup(sq_ptr, cq_ptr, std::ptr::null_mut());
                return Err(e.into());
            }
        };

        Ok(RingPtrs {
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
            let err = std::io::Error::last_os_error();
            eprintln!("Unable to register file! ERROR => {err}");

            return Err(err.into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn register_buffers(
        ring_fd: i32,
        page_size: usize,
    ) -> InternalResult<(Vec<libc::iovec>, *mut libc::c_void)> {
        let total_size = NUM_BUFFER_PAGES * page_size;
        let base_ptr = libc::mmap(
            std::ptr::null_mut(),
            total_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );

        if base_ptr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            eprintln!("register_buffers() failed cause base_ptr mmap failed: {err}");

            return Err(err.into());
        }

        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(NUM_BUFFER_PAGES);

        for i in 0..NUM_BUFFER_PAGES {
            iovecs.push(libc::iovec {
                iov_base: (base_ptr as *mut u8).add(i * page_size) as *mut libc::c_void,
                iov_len: page_size,
            });
        }

        let ret = libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_BUFFERS as libc::c_ulong,
            iovecs.as_ptr(),
            iovecs.len() as libc::c_uint,
        );

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("register_buffers() failed on registration syscall w/ res({ret}): {err}");

            return Err(err.into());
        }

        Ok((iovecs, base_ptr))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn next_sqe_index(&self) -> (u32, u32) {
        let tail_ptr = self.sq_tail_ptr();
        let tail = core::ptr::read_volatile(tail_ptr);
        let mask = self.sq_mask();
        let idx = tail & mask;

        (tail, idx)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn sq_head_ptr(&self) -> *mut u32 {
        (self.rings.sq_ptr as *mut u8).add(self.params.sq_off.head as usize) as *mut u32
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn sq_tail_ptr(&self) -> *mut u32 {
        (self.rings.sq_ptr as *mut u8).add(self.params.sq_off.tail as usize) as *mut u32
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn sq_mask(&self) -> u32 {
        *((self.rings.sq_ptr as *mut u8).add(self.params.sq_off.ring_mask as usize) as *const u32)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn sq_array_ptr(&self) -> *mut u32 {
        (self.rings.sq_ptr as *mut u8).add(self.params.sq_off.array as usize) as *mut u32
    }
}

#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn clear_ring_mmaps_on_err(rings: &RingPtrs, params: &IOUringParams) {
    //
    // Unmap the SQE's array map
    //
    if !rings.sqes_ptr.is_null() {
        let res = libc::munmap(
            rings.sqes_ptr,
            params.sq_entries as usize * std::mem::size_of::<IOUringSQE>(),
        );

        if res < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("Unable to unmap SQE array: {err}");
        }
    }

    //
    // Unmap the CQ map
    //
    if !rings.cq_ptr.is_null() && rings.cq_ptr != rings.sq_ptr {
        let cq_size = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();

        let res = libc::munmap(rings.cq_ptr, cq_size);

        if res < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("Unable to unmap CQ: {err}");
        }
    }

    //
    // Unmap the SQ map
    //
    if !rings.sq_ptr.is_null() {
        let sq_size = if params.features & IOURING_FEAT_SINGLE_MMAP != 0 {
            std::cmp::max(
                params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>(),
                params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>(),
            )
        } else {
            params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>()
        };

        let res = libc::munmap(rings.sq_ptr, sq_size);

        if res < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("Unable to unmap SQ: {err}");
        }
    }
}

impl Drop for IOUring {
    fn drop(&mut self) {
        unsafe {
            clear_ring_mmaps_on_err(&self.rings, &self.params);

            //
            // unmap the buffer alocated space
            //

            if !self.buf_base_ptr.is_null() {
                let total_size = NUM_BUFFER_PAGES * self.buf_page_size;
                let res = libc::munmap(self.buf_base_ptr, total_size);

                if res < 0 {
                    let err = std::io::Error::last_os_error();
                    eprintln!("Unable to unmap SQE array: {err}");
                }
            }

            //
            // unregister registered file
            //
            let res = libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IOURING_UNREGISTER_FILES as libc::c_ulong,
                std::ptr::null::<libc::c_void>(),
                0u32,
            );

            if res < 0 {
                let err = std::io::Error::last_os_error();
                eprintln!("Unable to unregister file: {err}");
            }

            //
            // unregister registered buffers
            //
            let res = libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IOURING_UNREGISTER_BUFFERS as libc::c_ulong,
                std::ptr::null::<libc::c_void>(),
                0u32,
            );

            if res < 0 {
                let err = std::io::Error::last_os_error();
                eprintln!("Unable to unregister buffers: {err}");
            }

            libc::close(self.ring_fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::File, fs::OpenOptions, io::Read, path::PathBuf};
    use tempfile::TempDir;

    fn create_iouring() -> (IOUring, PathBuf, File, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("temp_io_uring");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("file");

        let io_ring = unsafe { IOUring::new(&file).expect("Failed to create io_uring") };

        (io_ring, path, file, tmp)
    }

    #[test]
    fn test_iouring_init() {
        let (io_ring, _path, _file, _tmp) = create_iouring();

        assert!(io_ring.ring_fd >= 0, "Ring fd must be non-negative");
        assert!(io_ring.file_fd >= 0, "File fd must be non-negative");

        assert!(io_ring.params.sq_off.array != 0, "SQE's offset must be set by kernel");
        assert!(io_ring.params.cq_off.cqes != 0, "CQE's offset must be set by kernel");

        assert!(!io_ring.rings.sq_ptr.is_null(), "SQ pointer must be valid");
        assert!(!io_ring.rings.cq_ptr.is_null(), "CQ pointer must be valid");
        assert!(!io_ring.rings.sqes_ptr.is_null(), "SQEs pointer must be valid");

        assert!(io_ring.buf_page_size > 0, "Buf page size must not be zero");
        assert_eq!(io_ring.active_buf_idx, 0, "Active buf idx must start from 0");

        assert!(!io_ring.buf_base_ptr.is_null(), "Base buf ptr must not be null");
        assert_eq!(
            io_ring.iovecs.len(),
            NUM_BUFFER_PAGES,
            "IOVEC lane must match with constant"
        );
        assert!(
            io_ring.buf_base_ptr != std::ptr::null_mut(),
            "Base buf ptr must not be 0"
        );

        drop(io_ring);
    }
}
