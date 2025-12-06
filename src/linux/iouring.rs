use crate::{
    burrow::DEFAULT_PAGE_SIZE,
    errors::{InternalError, InternalResult},
};
use libc::{c_int, c_uint, c_void, iovec, mmap, off_t, sigset_t, SYS_io_uring_setup};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
};

const NUM_BUFFER_PAGE: usize = 0x80; // No. of page bufs pages registered w/ kernel for `io_uring`
const QUEUE_DEPTH: u32 = NUM_BUFFER_PAGE as u32 / 0x02; // 64 SQE entries, which is ~5 KiB of memory

// sanity checks
const _: () = assert!(
    NUM_BUFFER_PAGE > 0x00 && (NUM_BUFFER_PAGE & (NUM_BUFFER_PAGE - 0x01)) == 0x00,
    "NUM_BUFFER_PAGE must be power of 2"
);

const IOURING_FEAT_SINGLE_MMAP: u32 = 0x01;

const IOURING_REGISTER_BUFFERS: u32 = 0x00;
const IOURING_UNREGISTER_BUFFERS: u32 = 0x01;
const IOURING_REGISTER_FILES: u32 = 0x02;
const IOURING_UNREGISTER_FILES: u32 = 0x03;

const IOURING_OFF_SQ_RING: off_t = 0x00;
const IOURING_OFF_CQ_RING: off_t = 0x8000000;
const IOURING_OFF_SQES: off_t = 0x10000000;

const IOURING_FYSNC_USER_DATA: u64 = 0xFFFF_FFFF_FFFF_FFFF;
const IOURING_FIXED_FILE_IDX: i32 = 0x00; // as we only register one file, it's stored at 0th index.

/// Maps the io_uring operation codes mirroring the original `io_uring_op` enum.
/// For reference, <https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h#L234>
enum IOUringOP {
    FSYNC = 0x03,
    WRITEFIXED = 0x05,
}

/// Maps the io_uring sqe flag bits mirroring original constants.
/// For reference, <https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h#L141>
enum IOUringSQEFlags {
    FIXEDFILE = 0x01 << 0x00,
    IOLINK = 0x01 << 0x02,
}

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

#[allow(unused)]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
struct IOUringCQE {
    user_data: u64,
    res: i32,
    flags: u32,
}

#[allow(unused)]
#[derive(Copy, Clone)]
#[repr(C)]
struct IOUringSQE {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: SQEOffUnion,
    addr: SQEAddrUnion,
    len: u32,
    user_data: u64,
    union2: [u64; 0x03],
}

const IOURING_SQE_SIZE: usize = std::mem::size_of::<IOUringSQE>();

#[allow(unused)]
#[derive(Copy, Clone)]
#[repr(C)]
union SQEOffUnion {
    off: u64,
    addr2: u64,
}

#[allow(unused)]
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
    resv: [u32; 0x04],
    sq_off: SQringOffset,
    cq_off: CQringOffset,
}

#[derive(Debug, Copy, Clone)]
struct RingPtrs {
    sq_ptr: *mut c_void,
    cq_ptr: *mut c_void,
    sqes_ptr: *mut c_void,
}

pub(crate) struct IOUring {
    ring_fd: i32,
    file_fd: i32,
    rings: RingPtrs,
    params: IOUringParams,
    iovecs: Vec<iovec>,
    buf_base_ptr: *mut c_void,
    cq_poll_tx: Option<JoinHandle<()>>,
    cq_poll_shutdown_flag: Arc<AtomicBool>,
    buf_pool: Arc<BufPool>,
}

unsafe impl Send for IOUring {}
unsafe impl Sync for IOUring {}

impl IOUring {
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn new(file_fd: i32) -> InternalResult<Self> {
        let mut params: IOUringParams = std::mem::zeroed();

        params.flags = 0x00;
        params.sq_thread_idle = 0x00;
        params.sq_thread_cpu = 0x00;

        let ring_fd = libc::syscall(SYS_io_uring_setup, QUEUE_DEPTH, &mut params as *mut IOUringParams) as c_int;
        if ring_fd < 0x00 {
            let errno = *libc::__errno_location();

            // check for `io_uring` support
            if errno == libc::ENOSYS {
                return Err(InternalError::Misc("IOUring is not supported".into()));
            }

            let err = std::io::Error::last_os_error();
            return Err(err.into());
        }

        // Sanity check
        debug_assert!(
            params.sq_off.array != 0 && params.cq_off.cqes != 0,
            "Kernel did not fill SQ/CQ offsets"
        );

        let rings = Self::mmap_rings(ring_fd, &params).map_err(|e| {
            libc::close(ring_fd);
            e
        })?;

        if let Err(e) = Self::register_files(ring_fd, file_fd) {
            Self::clear_ring_mmaps_on_err(&rings, &params);
            libc::close(ring_fd);
            return Err(e);
        }

        let (iovecs, buf_base_ptr) = Self::register_buffers(ring_fd).map_err(|e| {
            libc::close(ring_fd);
            Self::clear_ring_mmaps_on_err(&rings, &params);
            e
        })?;

        let buf_pool = Arc::new(BufPool::new(NUM_BUFFER_PAGE));
        let cqes_ptr = (rings.cq_ptr as *mut u8).add(params.cq_off.cqes as usize) as usize;
        let cq_tail_ptr = (rings.cq_ptr as *mut u8).add(params.cq_off.tail as usize) as usize;
        let cq_head_ptr = (rings.cq_ptr as *mut u8).add(params.cq_off.head as usize) as usize;
        let cq_mask_ptr = *((rings.cq_ptr as *mut u8).add(params.cq_off.ring_mask as usize) as *const u32);

        let (cq_poll_tx, cq_poll_shutdown_flag) =
            Self::spawn_cq_poll_tx(buf_pool.clone(), cq_head_ptr, cq_tail_ptr, cq_mask_ptr, cqes_ptr);

        Ok(Self {
            ring_fd,
            file_fd,
            rings,
            params,
            iovecs,
            buf_base_ptr,
            buf_pool,
            cq_poll_shutdown_flag,
            cq_poll_tx: Some(cq_poll_tx),
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn register_files(ring_fd: i32, file_fd: i32) -> InternalResult<()> {
        let fds = [file_fd];
        let ret = libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_FILES as libc::c_ulong,
            fds.as_ptr(),
            fds.len() as c_uint,
        ) as libc::c_int;

        if ret < 0x00 {
            let err = std::io::Error::last_os_error();
            return Err(err.into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn submit_write_and_fsync(&self, range: BufRange, write_offset: u64) -> InternalResult<()> {
        // sanity check
        debug_assert!(
            (range.start + range.len) <= NUM_BUFFER_PAGE,
            "buf_range is out of bounds"
        );

        // SQE prep for WRITE_FIXED
        let start = range.start;
        let len_bytes = (range.len * DEFAULT_PAGE_SIZE) as u32;
        let (tail, sqe_idx) = self.next_sqe_index();
        let iov_base = self.iovecs[start].iov_base;
        let sqe_ptr = (self.rings.sqes_ptr as *mut IOUringSQE).add(sqe_idx as usize);
        std::ptr::write_bytes(sqe_ptr as *mut u8, 0x00, IOURING_SQE_SIZE); // zero init

        // NOTE: we pack start (low 32) and len (high 32) so CQE can free exactly
        // the allocation correctly
        let packed = ((range.len as u64) << 32) | (start as u64);

        (*sqe_ptr).opcode = IOUringOP::WRITEFIXED as u8;
        (*sqe_ptr).flags = IOUringSQEFlags::IOLINK as u8 | IOUringSQEFlags::FIXEDFILE as u8;
        (*sqe_ptr).fd = IOURING_FIXED_FILE_IDX;
        (*sqe_ptr).len = len_bytes;
        (*sqe_ptr).user_data = packed;
        (*sqe_ptr).addr = SQEAddrUnion { addr: iov_base as u64 };
        (*sqe_ptr).off = SQEOffUnion { off: write_offset };
        (*sqe_ptr).union2[0] = packed;

        // SQE prep for (Linked) FSYNC
        let tail2 = tail + 1;
        let sqe_idx2 = tail2 & self.sq_mask();
        let sqe2_ptr = (self.rings.sqes_ptr as *mut IOUringSQE).add(sqe_idx2 as usize);

        (*sqe2_ptr).opcode = IOUringOP::FSYNC as u8;
        (*sqe2_ptr).flags = IOUringSQEFlags::FIXEDFILE as u8;
        (*sqe2_ptr).fd = IOURING_FIXED_FILE_IDX;
        (*sqe2_ptr).user_data = IOURING_FYSNC_USER_DATA;

        // Submit SQE's to SQ

        let sq_array = self.sq_array_ptr();
        let mask = self.sq_mask();
        let pos1 = (tail & mask) as usize;
        let pos2 = (tail2 & mask) as usize;

        std::ptr::write_volatile(sq_array.add(pos1), sqe_idx);
        std::ptr::write_volatile(sq_array.add(pos2), sqe_idx2);

        // NOTE: This fence ensures SQE and array writes are visible to kernel before updating the tail.
        // It's imp cause, it prevents reordering of SQE entries.
        std::sync::atomic::fence(Ordering::Release);
        std::ptr::write_volatile(self.sq_tail_ptr(), tail.wrapping_add(2)); // new tail

        // Submit SQE (both) w/ `io_uring_enter` syscall
        let ret = libc::syscall(
            libc::SYS_io_uring_enter,
            self.ring_fd,
            2u32, // submit both entires
            0u32, // nonblocking
            0u32,
            std::ptr::null::<sigset_t>(),
        ) as c_int;

        if ret < 0x00 {
            let err = std::io::Error::last_os_error();
            return Err(err.into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn write(&self, buf: &[u8], write_offset: u64) -> InternalResult<()> {
        // sanity checks
        debug_assert!(!buf.is_empty(), "Input buffer must not be empty");

        let needed = (buf.len() + DEFAULT_PAGE_SIZE - 0x01) / DEFAULT_PAGE_SIZE;
        let range = loop {
            if let Some(r) = self.buf_pool.alloc(needed) {
                break r;
            }
            // no busy wait → small sleep
            std::thread::park_timeout(std::time::Duration::from_micros(4));
        };

        // copy into sequential pages
        let base = self.buf_base_ptr.add(range.start * DEFAULT_PAGE_SIZE);
        std::ptr::copy_nonoverlapping(buf.as_ptr(), base as *mut u8, buf.len());
        self.submit_write_and_fsync(range, write_offset)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_rings(ring_fd: i32, params: &IOUringParams) -> InternalResult<RingPtrs> {
        let sq_ring_sz = params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>();
        let cq_ring_sz = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();
        let single_mmap = params.features & IOURING_FEAT_SINGLE_MMAP != 0x00;
        let ring_sz = std::cmp::max(sq_ring_sz, cq_ring_sz);

        // helper to create mmap
        let create_mmap = |len: usize, offset: off_t| -> InternalResult<*mut c_void> {
            let ptr = mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                ring_fd,
                offset,
            );

            if ptr == libc::MAP_FAILED {
                let err = std::io::Error::last_os_error();
                return Err(err.into());
            }

            Ok(ptr)
        };

        // helper for cleanup on failure (unmap mapped objects)
        let cleanup = |sq: *mut libc::c_void, cq: *mut libc::c_void, sqes: *mut libc::c_void| {
            if !sqes.is_null() && sqes != libc::MAP_FAILED {
                let _res = libc::munmap(sqes, params.sq_entries as usize * std::mem::size_of::<IOUringSQE>());
            }

            if !cq.is_null() && cq != libc::MAP_FAILED && cq != sq {
                let _res = libc::munmap(cq, cq_ring_sz);
            }

            if !sq.is_null() && sq != libc::MAP_FAILED {
                let _res = libc::munmap(sq, if single_mmap { ring_sz } else { sq_ring_sz });
            }
        };

        // SQ ring map
        let sq_ptr = create_mmap(if single_mmap { ring_sz } else { sq_ring_sz }, IOURING_OFF_SQ_RING)?;

        // CQ ring map
        let cq_ptr = if single_mmap {
            sq_ptr
        } else {
            match create_mmap(cq_ring_sz, IOURING_OFF_CQ_RING) {
                Ok(ptr) => ptr,
                Err(e) => {
                    cleanup(sq_ptr, std::ptr::null_mut(), std::ptr::null_mut());
                    return Err(e);
                }
            }
        };

        // SQEs array map
        let sqes_sz = params.sq_entries as usize * std::mem::size_of::<IOUringSQE>();
        let sqes_ptr = match create_mmap(sqes_sz, IOURING_OFF_SQES) {
            Ok(ptr) => ptr,
            Err(e) => {
                cleanup(sq_ptr, cq_ptr, std::ptr::null_mut());
                return Err(e);
            }
        };

        Ok(RingPtrs {
            sq_ptr,
            cq_ptr,
            sqes_ptr,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn register_buffers(ring_fd: i32) -> InternalResult<(Vec<iovec>, *mut c_void)> {
        let total_size = NUM_BUFFER_PAGE * DEFAULT_PAGE_SIZE;
        let base_ptr = libc::mmap(
            std::ptr::null_mut(),
            total_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -0x01,
            0x00,
        );

        if base_ptr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            return Err(err.into());
        }

        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(NUM_BUFFER_PAGE);
        for i in 0x00..NUM_BUFFER_PAGE {
            iovecs.push(libc::iovec {
                iov_base: (base_ptr as *mut u8).add(i * DEFAULT_PAGE_SIZE) as *mut c_void,
                iov_len: DEFAULT_PAGE_SIZE,
            });
        }

        let ret = libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_BUFFERS as libc::c_ulong,
            iovecs.as_ptr(),
            iovecs.len() as libc::c_uint,
        );

        if ret < 0x00 {
            let err = std::io::Error::last_os_error();
            return Err(err.into());
        }

        Ok((iovecs, base_ptr))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn spawn_cq_poll_tx(
        pool: Arc<BufPool>,
        cq_head_ptr: usize,
        cq_tail_ptr: usize,
        cq_mask_ptr: u32,
        cqes_ptr: usize,
    ) -> (JoinHandle<()>, Arc<AtomicBool>) {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        let tx = std::thread::spawn(move || {
            loop {
                let cq_head = cq_head_ptr as *mut u32;
                let cq_tail = cq_tail_ptr as *mut u32;
                let cqes = cqes_ptr as *mut IOUringCQE;

                let head = std::ptr::read_volatile(cq_head);
                let tail = std::ptr::read_volatile(cq_tail);

                if head == tail {
                    // thread shutdown signal
                    if shutdown_flag_clone.load(Ordering::Acquire) {
                        break;
                    }

                    std::thread::park_timeout(std::time::Duration::from_micros(10));
                    continue;
                }

                let idx = head & cq_mask_ptr;
                let cqe = core::ptr::read_volatile(cqes.add(idx as usize));

                // NOTE: Even if the write/fsync op has failed we still free up the buffer as its
                // free to be used for next op's!
                if cqe.res < 0x00 {
                    let _err = std::io::Error::last_os_error();
                }

                // NOTE: We only assign buf for write, so we must skip the fsync calls!
                if cqe.user_data != IOURING_FYSNC_USER_DATA {
                    let packed = cqe.user_data;
                    let start = (packed & 0xFFFF_FFFF) as usize;
                    let len = (packed >> 0x20) as usize;

                    if len == 0x00 || start >= NUM_BUFFER_PAGE || start + len > NUM_BUFFER_PAGE {
                        // corrupted meta, SKIP!
                    } else {
                        pool.free_range(BufRange::new(start, len));
                    }
                }

                core::ptr::write_volatile(cq_head, head.wrapping_add(1));
            }
        });

        (tx, shutdown_flag)
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

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn cq_head_ptr(&self) -> *mut u32 {
        (self.rings.cq_ptr as *mut u8).add(self.params.cq_off.head as usize) as *mut u32
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn cq_tail_ptr(&self) -> *mut u32 {
        (self.rings.cq_ptr as *mut u8).add(self.params.cq_off.tail as usize) as *mut u32
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn cq_mask(&self) -> u32 {
        *((self.rings.cq_ptr as *mut u8).add(self.params.cq_off.ring_mask as usize) as *const u32)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn cqes_ptr(&self) -> *mut IOUringCQE {
        (self.rings.cq_ptr as *mut u8).add(self.params.cq_off.cqes as usize) as *mut IOUringCQE
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn clear_ring_mmaps_on_err(rings: &RingPtrs, params: &IOUringParams) {
        // Unmap the SQE's array map
        if !rings.sqes_ptr.is_null() {
            let _res = libc::munmap(
                rings.sqes_ptr,
                params.sq_entries as usize * std::mem::size_of::<IOUringSQE>(),
            );
        }

        // Unmap the CQ map
        if !rings.cq_ptr.is_null() && rings.cq_ptr != rings.sq_ptr {
            let cq_size = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();
            let _res = libc::munmap(rings.cq_ptr, cq_size);
        }

        // Unmap the SQ map
        if !rings.sq_ptr.is_null() {
            let sq_size = if params.features & IOURING_FEAT_SINGLE_MMAP != 0x00 {
                std::cmp::max(
                    params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>(),
                    params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>(),
                )
            } else {
                params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>()
            };

            let _res = libc::munmap(rings.sq_ptr, sq_size);
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct BufRange {
    start: usize,
    len: usize,
}

impl BufRange {
    fn new(start: usize, len: usize) -> Self {
        Self { start, len }
    }
}

#[derive(Debug)]
struct BufPool {
    size: usize,
    free: Mutex<Vec<BufRange>>,
}

impl BufPool {
    fn new(size: usize) -> Self {
        Self {
            size,
            free: Mutex::new(vec![BufRange::new(0x00, size)]),
        }
    }

    /// Alloc contiguous range of pages.
    fn alloc(&self, n: usize) -> Option<BufRange> {
        let mut free = self.free.lock().unwrap();

        for i in 0..free.len() {
            let bfrng = free[i];
            if bfrng.len >= n {
                let out = BufRange {
                    start: bfrng.start,
                    len: n,
                };

                if bfrng.len == n {
                    free.remove(i);
                } else {
                    free[i] = BufRange::new(bfrng.start + n, bfrng.len - n);
                }

                return Some(out);
            }
        }

        None
    }

    /// Free a previously allocated range.
    fn free_range(&self, r: BufRange) {
        let mut free = self.free.lock().unwrap();
        let mut out: Vec<BufRange> = Vec::with_capacity(free.len());
        free.push(r);
        free.sort_by_key(|x| x.start);

        for seg in free.drain(..) {
            if let Some(last) = out.last_mut() {
                if last.start + last.len >= seg.start {
                    let end = std::cmp::max(last.start + last.len, seg.start + seg.len);
                    last.len = end - last.start;
                    continue;
                }
            }
            out.push(seg);
        }

        *free = out;
    }
}

impl Drop for IOUring {
    fn drop(&mut self) {
        unsafe {
            // stop CQ poller
            if let Some(tx) = self.cq_poll_tx.take() {
                self.cq_poll_shutdown_flag.store(true, Ordering::Release);
                tx.thread().unpark();
                let _ = tx.join();
            }

            // unmap ring mmaps
            Self::clear_ring_mmaps_on_err(&self.rings, &self.params);

            // unmap registered buffer region
            if !self.buf_base_ptr.is_null() {
                let total_size = NUM_BUFFER_PAGE * DEFAULT_PAGE_SIZE;
                let _ = libc::munmap(self.buf_base_ptr, total_size);
            }

            // unregister file
            let _ = libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IOURING_UNREGISTER_FILES as libc::c_ulong,
                std::ptr::null::<c_void>(),
                0u32,
            );

            // unregister buffers
            let _ = libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IOURING_UNREGISTER_BUFFERS as libc::c_ulong,
                std::ptr::null::<c_void>(),
                0u32,
            );

            // close ring
            let _ = libc::close(self.ring_fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::linux::File;
    use tempfile::TempDir;

    fn create_iouring() -> (IOUring, File, TempDir, std::path::PathBuf) {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("temp_io_uring");

        let file = unsafe { File::new(&path).expect("new file") };
        unsafe { file.zero_extend(NUM_BUFFER_PAGE * DEFAULT_PAGE_SIZE) }.expect("zero extend file");
        let io_ring = unsafe { IOUring::new(file.fd()).expect("Failed to init") };

        (io_ring, file, tmp, path)
    }

    mod iouring {
        use super::*;

        #[test]
        fn test_iouring_init() {
            let (io_ring, _file, _tmp, _) = create_iouring();

            assert!(io_ring.ring_fd >= 0, "Ring fd must be non-negative");
            assert!(io_ring.file_fd >= 0, "File fd must be non-negative");

            assert!(io_ring.params.sq_off.array != 0, "SQE's offset must be set by kernel");
            assert!(io_ring.params.cq_off.cqes != 0, "CQE's offset must be set by kernel");

            assert!(!io_ring.rings.sq_ptr.is_null(), "SQ pointer must be valid");
            assert!(!io_ring.rings.cq_ptr.is_null(), "CQ pointer must be valid");
            assert!(!io_ring.rings.sqes_ptr.is_null(), "SQEs pointer must be valid");
            assert!(!io_ring.buf_base_ptr.is_null(), "Base buf ptr must not be null");

            assert_eq!(
                io_ring.iovecs.len(),
                NUM_BUFFER_PAGE,
                "IOVEC lane must match with constant"
            );
            assert!(
                io_ring.buf_base_ptr != std::ptr::null_mut(),
                "Base buf ptr must not be 0"
            );
        }

        #[test]
        fn test_write_and_fsync() {
            let offset: u64 = 0;
            let dummy_data = "Dummy Data to write w/ fsync".as_bytes();
            let (mut io_ring, mut file, _tmp, path) = create_iouring();

            unsafe { io_ring.write(&dummy_data, offset) };
            std::thread::sleep(std::time::Duration::from_millis(1)); // manual sleep so write could be finished

            let data = std::fs::read(&path).expect("read from file");
            let buf = data[0..dummy_data.len()].to_vec();
            assert_eq!(dummy_data, &buf);
        }

        #[test]
        fn test_manual_queue_exhaustion() {
            let n = 0x64;
            let file_len = n * NUM_BUFFER_PAGE;
            let (mut io_ring, mut file, _tmp, path) = create_iouring();

            // Must extend file before random-offset writes
            unsafe { file.zero_extend(n * DEFAULT_PAGE_SIZE) }.expect("extend file");

            for i in 0x00..n {
                let dummy_data = vec![i as u8; DEFAULT_PAGE_SIZE];
                unsafe { io_ring.write(&dummy_data, (DEFAULT_PAGE_SIZE * i) as u64) };
            }

            // manual sleep so writes could be finished
            std::thread::sleep(std::time::Duration::from_millis(100));

            // validate written data
            let written = std::fs::read(&path).expect("read from file");
            for i in 0..n {
                let st_idx = i * DEFAULT_PAGE_SIZE;
                let expected_buf = vec![i as u8; DEFAULT_PAGE_SIZE];
                let buf: Vec<u8> = written[st_idx..(st_idx + DEFAULT_PAGE_SIZE)].to_vec();

                assert_eq!(expected_buf, buf);
            }
        }
    }

    mod buf_pool {
        use super::*;

        #[test]
        fn test_alloc_single_page() {
            let pool = BufPool::new(NUM_BUFFER_PAGE);
            let r = pool.alloc(1).expect("must alloc");
            assert_eq!(r.start, 0);
            assert_eq!(r.len, 1);
        }

        #[test]
        fn test_alloc_free_coalesce() {
            let pool = BufPool::new(8);

            // allocate 3 pages
            let a = pool.alloc(3).unwrap();
            assert_eq!(a.start, 0);
            assert_eq!(a.len, 3);

            // allocate 2 pages
            let b = pool.alloc(2).unwrap();
            assert_eq!(b.start, 3);
            assert_eq!(b.len, 2);

            // free both -> must coalesce to a single (0..5)
            pool.free_range(a);
            pool.free_range(b);

            // next allocation of 5 contiguous must succeed
            let c = pool.alloc(5).unwrap();
            assert_eq!(c.start, 0);
            assert_eq!(c.len, 5);
        }

        #[test]
        fn test_alloc_fail() {
            let pool = BufPool::new(4);
            let _ = pool.alloc(4).unwrap();
            assert!(pool.alloc(1).is_none(), "no space left");
        }
    }
}
