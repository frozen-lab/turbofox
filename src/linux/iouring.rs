use crate::{errors::InternalResult, logger::Logger};
use core::ptr::write_volatile;
use std::{
    ptr::{copy_nonoverlapping, read_volatile, write_bytes},
    sync::{
        atomic::{fence, AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

/// No. of page bufs pages registered w/ kernel for `io_uring`
pub(crate) const NUM_BUFFER_PAGE: usize = 128;
const _: () = assert!(
    NUM_BUFFER_PAGE > 0 && (NUM_BUFFER_PAGE & (NUM_BUFFER_PAGE - 1)) == 0,
    "NUM_BUFFER_PAGE must be power of 2"
);

// TODO: We shold take `size_buf_page` as config from user, so the dev's could
// optimize for there ideal buf size, so we could avoid resource waste!

/// Size of each page buf registered w/ kernel for `io_uring`
pub(crate) const SIZE_BUFFER_PAGE: usize = 128;
const _: () = assert!(
    SIZE_BUFFER_PAGE > 0 && (SIZE_BUFFER_PAGE & (SIZE_BUFFER_PAGE - 1)) == 0,
    "SIZE_BUFFER_PAGE must be power of 2"
);

const QUEUE_DEPTH: u32 = NUM_BUFFER_PAGE as u32 / 2; // 64 SQE entries, which is ~5 KiB of memory
const IOURING_FEAT_SINGLE_MMAP: u32 = 1;

const IOURING_REGISTER_BUFFERS: u32 = 0;
const IOURING_UNREGISTER_BUFFERS: u32 = 1;
const IOURING_REGISTER_FILES: u32 = 2;
const IOURING_UNREGISTER_FILES: u32 = 3;

const IOURING_OFF_SQ_RING: libc::off_t = 0;
const IOURING_OFF_CQ_RING: libc::off_t = 0x8000000;
const IOURING_OFF_SQES: libc::off_t = 0x10000000;

const IOURING_FIXED_FILE_IDX: i32 = 0; // as we only register one file, it's stored at 0th index.
const IOURING_FYSNC_USER_DATA: u64 = 0xFFFF_FFFF_FFFF_FFFF;

/// Maps the io_uring operation codes mirroring the original `io_uring_op` enum.
/// For reference, <https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h#L234>
enum IOUringOP {
    FSYNC = 3,
    WRITEFIXED = 5,
}

/// Maps the io_uring sqe flag bits mirroring original constants.
/// For reference, <https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h#L141>
enum IOUringSQEFlags {
    FIXEDFILE = 1 << 0,
    IOLINK = 1 << 2,
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
    union2: [u64; 3],
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

pub(crate) struct IOUring {
    ring_fd: i32,
    file_fd: i32,
    rings: RingPtrs,
    logger: Logger,
    params: IOUringParams,
    iovecs: Vec<libc::iovec>,
    buf_base_ptr: *mut libc::c_void,
    buf_pool: Arc<BufPool>,
    cq_poll_tx: Option<JoinHandle<()>>,
    cq_poll_shutdown_flag: Arc<AtomicBool>,
    num_buf_page: usize,
    size_buf_page: usize,
}

unsafe impl Send for IOUring {}
unsafe impl Sync for IOUring {}

impl IOUring {
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(crate) unsafe fn new(
        logging_enabled: bool,
        file_fd: i32,
        num_buf_page: usize,
        size_buf_page: usize,
    ) -> InternalResult<Option<Self>> {
        let mut params: IOUringParams = std::mem::zeroed();
        let logger = Logger::new(logging_enabled, "TurboFox (IOUring)");

        params.flags = 0x00;
        params.sq_thread_idle = 0x00;
        params.sq_thread_cpu = 0x00;

        let ring_fd =
            libc::syscall(libc::SYS_io_uring_setup, QUEUE_DEPTH, &mut params as *mut IOUringParams) as libc::c_int;

        if ring_fd < 0 {
            let errno = *libc::__errno_location();

            // NOTE: When io_uring is not available, we fallback to sequential I/O
            if errno == libc::ENOSYS {
                logger.warn("io_uring is not supported (requires Linux 5.1+)");
                return Ok(None);
            }

            let err = std::io::Error::last_os_error();
            logger.error(format!("Invalid ring_fd={ring_fd}! ERR => {err}"));
            return Err(err.into());
        }

        // NOTE: Sanity check for somehow if kernel does not set the sq_off and cq_off fields!
        debug_assert!(
            params.sq_off.array != 0 && params.cq_off.cqes != 0,
            "Kernel did not fill SQ/CQ offsets"
        );

        let rings = Self::mmap_rings(ring_fd, &params, &logger).map_err(|e| {
            libc::close(ring_fd);
            e
        })?;

        if let Err(e) = Self::register_files(ring_fd, file_fd, &logger) {
            clear_ring_mmaps_on_err(&rings, &params, &logger);
            libc::close(ring_fd);
            return Err(e);
        }

        let (iovecs, buf_base_ptr) =
            Self::register_buffers(ring_fd, num_buf_page, size_buf_page, &logger).map_err(|e| {
                libc::close(ring_fd);
                clear_ring_mmaps_on_err(&rings, &params, &logger);
                e
            })?;

        let buf_pool = Arc::new(BufPool::new(num_buf_page));

        let cqes_ptr = (rings.cq_ptr as *mut u8).add(params.cq_off.cqes as usize) as usize;
        let cq_tail_ptr = (rings.cq_ptr as *mut u8).add(params.cq_off.tail as usize) as usize;
        let cq_head_ptr = (rings.cq_ptr as *mut u8).add(params.cq_off.head as usize) as usize;
        let cq_mask_ptr = *((rings.cq_ptr as *mut u8).add(params.cq_off.ring_mask as usize) as *const u32);

        let (cq_poll_tx, cq_poll_shutdown_flag) = Self::spawn_cq_poll_tx(
            buf_pool.clone(),
            cq_head_ptr,
            cq_tail_ptr,
            cq_mask_ptr,
            cqes_ptr,
            &logger,
        );

        logger.debug(format!("IOUring init w/ ring_fd={ring_fd} file_fd={file_fd}"));

        Ok(Some(Self {
            rings,
            params,
            iovecs,
            ring_fd,
            file_fd,
            buf_pool,
            logger,
            buf_base_ptr,
            num_buf_page,
            size_buf_page,
            cq_poll_shutdown_flag,
            cq_poll_tx: Some(cq_poll_tx),
        }))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(crate) unsafe fn write(&self, buf: &[u8], write_offset: u64) -> InternalResult<()> {
        // sanity checks
        debug_assert!(!buf.is_empty(), "Input buffer must not be empty");
        debug_assert!(buf.len() <= self.size_buf_page, "Buffer is too large");

        let buf_idx = loop {
            if let Some(idx) = self.buf_pool.pop() {
                break idx;
            }

            // NOTE: As no buf is available, we suspend the current thread for 2 µs
            std::thread::park_timeout(std::time::Duration::from_micros(2));
        };

        let buf_ptr = self.buf_base_ptr.add(buf_idx * self.size_buf_page);
        copy_nonoverlapping(buf.as_ptr(), buf_ptr as *mut u8, buf.len());
        self.logger
            .trace(format!("[WRITE] Copied data into buf at idx={buf_idx}"));
        self.submit_write_and_fsync(buf_idx, write_offset)?;

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn spawn_cq_poll_tx(
        pool: Arc<BufPool>,
        cq_head_ptr: usize,
        cq_tail_ptr: usize,
        cq_mask_ptr: u32,
        cqes_ptr: usize,
        logger: &Logger,
    ) -> (JoinHandle<()>, Arc<AtomicBool>) {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();
        let tx_logger = logger.clone();

        let tx = std::thread::spawn(move || {
            loop {
                let cq_head = cq_head_ptr as *mut u32;
                let cq_tail = cq_tail_ptr as *mut u32;
                let cqes = cqes_ptr as *mut IOUringCQE;

                let head = read_volatile(cq_head);
                let tail = read_volatile(cq_tail);

                if head == tail {
                    // thread shutdown signal
                    if shutdown_flag_clone.load(Ordering::Acquire) {
                        tx_logger.info("[CQ_POLL_TX] received shutdown signal");
                        break;
                    }

                    std::thread::park_timeout(std::time::Duration::from_micros(10));
                    continue;
                }

                let idx = head & cq_mask_ptr;
                let cqe = core::ptr::read_volatile(cqes.add(idx as usize));

                // NOTE: Even if the write/fsync op has failed we still free up the buffer as its
                // free to be used for next op's!
                if cqe.res < 0 {
                    let err = std::io::Error::last_os_error();
                    tx_logger.error(format!("[CQ_POLL_TX] Write failed w/ err: {err}"));
                }

                // NOTE: We only assign buf for write, so we must skip the fsync calls!
                if cqe.user_data != IOURING_FYSNC_USER_DATA {
                    pool.push(cqe.user_data as usize);
                    tx_logger.debug(format!("[CQ_POLL_TX] freed the idx({})", cqe.user_data));
                }

                core::ptr::write_volatile(cq_head, head.wrapping_add(1));
            }
        });

        (tx, shutdown_flag)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn submit_write_and_fsync(&self, buf_idx: usize, write_offset: u64) -> InternalResult<()> {
        // sanity check
        debug_assert!(buf_idx < self.num_buf_page, "buf_idx is out of bounds");

        // SQE prep for WRITE_FIXED

        let (tail, sqe_idx) = self.next_sqe_index();
        let iov_base = self.iovecs[buf_idx].iov_base;
        let sqe_ptr = (self.rings.sqes_ptr as *mut IOUringSQE).add(sqe_idx as usize);
        write_bytes(sqe_ptr as *mut u8, 0, IOURING_SQE_SIZE); // zero init

        (*sqe_ptr).opcode = IOUringOP::WRITEFIXED as u8;
        (*sqe_ptr).flags = IOUringSQEFlags::IOLINK as u8 | IOUringSQEFlags::FIXEDFILE as u8;
        (*sqe_ptr).fd = IOURING_FIXED_FILE_IDX;
        (*sqe_ptr).len = self.iovecs[buf_idx].iov_len as u32;
        (*sqe_ptr).user_data = buf_idx as u64;
        (*sqe_ptr).off = SQEOffUnion { off: write_offset };
        (*sqe_ptr).addr = SQEAddrUnion { addr: iov_base as u64 };
        (*sqe_ptr).union2[0] = buf_idx as u64;

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

        write_volatile(sq_array.add(pos1), sqe_idx);
        write_volatile(sq_array.add(pos2), sqe_idx2);

        // NOTE: This fence ensures SQE and array writes are visible to kernel before updating the tail.
        // It's imp cause, it prevents reordering of SQE entries.
        fence(Ordering::Release);
        write_volatile(self.sq_tail_ptr(), tail.wrapping_add(2)); // new tail

        // Submit SQE (both) w/ `io_uring_enter` syscall

        let ret = libc::syscall(
            libc::SYS_io_uring_enter,
            self.ring_fd,
            2u32, // submit both entires
            0u32, // nonblocking
            0u32,
            std::ptr::null::<libc::sigset_t>(),
        ) as libc::c_int;

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            self.logger.error(format!(
                "submit_write_and_fsync() failed on io_uring_enter syscall: {err:?}"
            ));

            return Err(err.into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_rings(ring_fd: i32, params: &IOUringParams, logger: &Logger) -> InternalResult<RingPtrs> {
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
                logger.error(format!("Unable to create mmap => {:?}", err));
                return Err(err.into());
            }

            Ok(ptr)
        };

        // helper for cleanup on failure (unmap mapped objects)
        let cleanup = |sq: *mut libc::c_void, cq: *mut libc::c_void, sqes: *mut libc::c_void| {
            if !sqes.is_null() && sqes != libc::MAP_FAILED {
                let res = libc::munmap(sqes, params.sq_entries as usize * std::mem::size_of::<IOUringSQE>());

                if res < 0 {
                    let err = std::io::Error::last_os_error();
                    logger.error(format!("Unable to unmap SQE array => {:?}", err));
                }
            }

            if !cq.is_null() && cq != libc::MAP_FAILED && cq != sq {
                let res = libc::munmap(cq, cq_ring_sz);

                if res < 0 {
                    let err = std::io::Error::last_os_error();
                    logger.error(format!("Unable to unmap CQ => {:?}", err));
                }
            }

            if !sq.is_null() && sq != libc::MAP_FAILED {
                let res = libc::munmap(sq, if single_mmap { ring_sz } else { sq_ring_sz });

                if res < 0 {
                    let err = std::io::Error::last_os_error();
                    logger.error(format!("Unable to unmap SQ => {:?}", err));
                }
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
    unsafe fn register_files(ring_fd: i32, file_fd: i32, logger: &Logger) -> InternalResult<()> {
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
            logger.error(format!("Unable to register file! ERROR => {err}"));
            return Err(err.into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn register_buffers(
        ring_fd: i32,
        num_page: usize,
        page_size: usize,
        logger: &Logger,
    ) -> InternalResult<(Vec<libc::iovec>, *mut libc::c_void)> {
        let total_size = num_page * page_size;
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
            logger.error(format!("register_buffers() failed cause base_ptr mmap failed: {err}"));
            return Err(err.into());
        }

        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(num_page);

        for i in 0..num_page {
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
            logger.error(format!(
                "register_buffers() failed on registration syscall w/ res({ret}): {err}"
            ));
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
}

#[derive(Debug)]
struct BufPool {
    size: usize,
    head: AtomicU64,
    next: Vec<AtomicU32>,
}

impl BufPool {
    const LAST_IDX: u32 = u32::MAX;

    fn new(size: usize) -> Self {
        let head = AtomicU64::new(Self::pack(0, 0));
        let mut next = Vec::with_capacity(size);

        for i in 0..size {
            next.push(AtomicU32::new(if i + 1 == size {
                Self::LAST_IDX
            } else {
                (i + 1) as u32
            }));
        }

        Self { size, head, next }
    }

    #[cfg(test)]
    #[inline(always)]
    fn is_empty(&self) -> bool {
        let (idx, _) = Self::unpack(self.head.load(Ordering::Acquire));
        idx == Self::LAST_IDX
    }

    fn pop(&self) -> Option<usize> {
        loop {
            let observed = self.head.load(Ordering::Acquire);
            let (head_idx, head_tag) = Self::unpack(observed);

            // NOTE: no empty spot left in the pool, caller must wait!
            if head_idx == Self::LAST_IDX {
                return None;
            }

            let successor = self.next[head_idx as usize].load(Ordering::Relaxed);
            let new_tag = head_tag.wrapping_add(1);
            let new_packed = Self::pack(successor, new_tag);

            match self
                .head
                .compare_exchange_weak(observed, new_packed, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(head_idx as usize),
                Err(_) => std::thread::yield_now(),
            }
        }
    }

    fn push(&self, idx: usize) {
        // sanity check
        debug_assert!(idx < self.size, "idx is out of bounds");

        loop {
            let observed = self.head.load(Ordering::Acquire);
            let (head_idx, head_tag) = Self::unpack(observed);

            self.next[idx].store(head_idx, Ordering::Relaxed);

            let new_tag = head_tag.wrapping_add(1);
            let new_packed = Self::pack(idx as u32, new_tag);

            match self
                .head
                .compare_exchange_weak(observed, new_packed, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(_) => std::thread::yield_now(),
            }
        }
    }

    #[inline(always)]
    fn pack(idx: u32, tag: u32) -> u64 {
        (tag as u64) << 32 | idx as u64
    }

    #[inline(always)]
    fn unpack(v: u64) -> (u32, u32) {
        ((v & 0xFFFF_FFFF) as u32, (v >> 32) as u32)
    }
}

#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn clear_ring_mmaps_on_err(rings: &RingPtrs, params: &IOUringParams, logger: &Logger) {
    // Unmap the SQE's array map

    if !rings.sqes_ptr.is_null() {
        let res = libc::munmap(
            rings.sqes_ptr,
            params.sq_entries as usize * std::mem::size_of::<IOUringSQE>(),
        );

        if res < 0 {
            let err = std::io::Error::last_os_error();
            logger.warn(format!("Unable to unmap SQE array: {err}"));
        }
    }

    // Unmap the CQ map

    if !rings.cq_ptr.is_null() && rings.cq_ptr != rings.sq_ptr {
        let cq_size = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();
        let res = libc::munmap(rings.cq_ptr, cq_size);

        if res < 0 {
            let err = std::io::Error::last_os_error();
            logger.warn(format!("Unable to unmap CQ: {err}"));
        }
    }

    // Unmap the SQ map

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
            logger.warn(format!("Unable to unmap SQ: {err}"));
        }
    }
}

impl Drop for IOUring {
    fn drop(&mut self) {
        unsafe {
            // close the cq_poller
            if let Some(tx) = self.cq_poll_tx.take() {
                self.cq_poll_shutdown_flag.store(true, Ordering::Release);

                // wakes the thread, we avoid waiting for thread to wait up
                tx.thread().unpark();

                let _ = tx.join();
                self.logger.trace("[drop] CQ poller thread closed.");
            }

            // unmap rings (SQ, CQ & SQE)
            clear_ring_mmaps_on_err(&self.rings, &self.params, &self.logger);

            // unmap the buffer alocated space
            if !self.buf_base_ptr.is_null() {
                let total_size = self.num_buf_page * self.size_buf_page;
                let res = libc::munmap(self.buf_base_ptr, total_size);

                if res < 0 {
                    let err = std::io::Error::last_os_error();
                    self.logger.warn(format!("Unable to unmap SQE array: {err}"));
                } else {
                    self.logger.trace("[drop] Unmapped SQE array.");
                }
            }

            // unregister registered file
            let res = libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IOURING_UNREGISTER_FILES as libc::c_ulong,
                std::ptr::null::<libc::c_void>(),
                0u32,
            );

            if res < 0 {
                let err = std::io::Error::last_os_error();
                self.logger.warn(format!("Unable to unregister file: {err}"));
            } else {
                self.logger.trace("[drop] Unregistered file.");
            }

            // unregister registered buffers
            let res = libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                IOURING_UNREGISTER_BUFFERS as libc::c_ulong,
                std::ptr::null::<libc::c_void>(),
                0u32,
            );

            if res < 0 {
                let err = std::io::Error::last_os_error();
                self.logger.warn(format!("Unable to unregister buffers: {err}"));
            } else {
                self.logger.trace("[drop] Unregistered buffers.");
            }

            // close ring_fd
            let res = libc::close(self.ring_fd);

            if res < 0 {
                let err = std::io::Error::last_os_error();
                self.logger.warn(format!("Unable to close ring_fd: {err}"));
            } else {
                self.logger.trace("[drop] Closed rign_fd.");
            }

            self.logger.info("Dropped IOUring");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod io_uring {
        use super::*;
        use crate::logger::init_test_logger;
        use std::{
            fs::{File, OpenOptions},
            io::{Read, Seek},
            os::fd::AsRawFd,
            path::PathBuf,
        };
        use tempfile::TempDir;

        fn create_iouring(num_buf: usize, size_buf: usize) -> (IOUring, File, TempDir) {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("temp_io_uring");
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)
                .expect("file");

            let _ = init_test_logger("IOUring");
            let file_fd = file.as_raw_fd();
            let io_ring = unsafe { IOUring::new(true, file_fd, num_buf, size_buf).expect("Failed to create io_uring") }
                .expect("IOUring");

            (io_ring, file, tmp)
        }

        #[test]
        fn test_iouring_init() {
            let (io_ring, _file, _tmp) = create_iouring(NUM_BUFFER_PAGE, SIZE_BUFFER_PAGE);

            assert!(io_ring.ring_fd >= 0, "Ring fd must be non-negative");
            assert!(io_ring.file_fd >= 0, "File fd must be non-negative");

            assert!(io_ring.params.sq_off.array != 0, "SQE's offset must be set by kernel");
            assert!(io_ring.params.cq_off.cqes != 0, "CQE's offset must be set by kernel");

            assert!(!io_ring.rings.sq_ptr.is_null(), "SQ pointer must be valid");
            assert!(!io_ring.rings.cq_ptr.is_null(), "CQ pointer must be valid");
            assert!(!io_ring.rings.sqes_ptr.is_null(), "SQEs pointer must be valid");

            assert!(!io_ring.buf_base_ptr.is_null(), "Base buf ptr must not be null");
            assert!(!io_ring.buf_pool.is_empty(), "BufPool should not be empty");

            assert_eq!(
                io_ring.iovecs.len(),
                NUM_BUFFER_PAGE,
                "IOVEC lane must match with constant"
            );
            assert!(
                io_ring.buf_base_ptr != std::ptr::null_mut(),
                "Base buf ptr must not be 0"
            );

            drop(io_ring);
        }

        #[test]
        fn test_write_and_fsync() {
            let offset: u64 = 0;
            let dummy_data = "Dummy Data to write w/ fsync".as_bytes();

            let mut dummy_buf = vec![0u8; dummy_data.len()];
            let (mut io_ring, mut file, _tmp) = create_iouring(NUM_BUFFER_PAGE, SIZE_BUFFER_PAGE);

            unsafe { io_ring.write(&dummy_data, offset) };
            std::thread::sleep(std::time::Duration::from_millis(1)); // manual sleep so write could be finished
            file.read_exact(&mut dummy_buf).expect("read from file");

            assert_eq!(dummy_data, &dummy_buf);

            drop(io_ring);
        }

        #[test]
        fn test_manual_queue_exhaustion() {
            let n = 100;
            let file_len = n * NUM_BUFFER_PAGE;
            let (mut io_ring, mut file, _tmp) = create_iouring(20, SIZE_BUFFER_PAGE);

            // NOTE: This is required, as we append new data to file, and a io_uring queue
            // does not have order, so we could be writing at an offset that does not exists
            // yet, resulting in undefined behaviour!
            file.set_len(SIZE_BUFFER_PAGE as u64 * n as u64).expect("Set len");

            for i in 0..n {
                let dummy_data = vec![i as u8; SIZE_BUFFER_PAGE];
                unsafe { io_ring.write(&dummy_data, (SIZE_BUFFER_PAGE * i) as u64) };
            }

            // manual sleep so writes could be finished
            std::thread::sleep(std::time::Duration::from_millis(10));

            // validate written data
            for i in 0..n {
                let expected_buf = vec![i as u8; SIZE_BUFFER_PAGE];
                let mut buf = vec![0u8; SIZE_BUFFER_PAGE];

                file.seek(std::io::SeekFrom::Start((i * SIZE_BUFFER_PAGE) as u64));
                file.read_exact(&mut buf).expect("read from file");

                assert_eq!(expected_buf, buf);
            }

            drop(io_ring);
        }
    }

    mod buf_pool {
        use super::*;
        use std::sync::Arc;
        use std::thread;

        #[test]
        fn test_basic_push_pop() {
            let pool = BufPool::new(NUM_BUFFER_PAGE);

            let idx = pool.pop().expect("should pop");
            assert!(idx < NUM_BUFFER_PAGE);

            pool.push(idx);
            assert!(!pool.is_empty());
        }

        #[test]
        fn test_popping_till_empty() {
            let pool = BufPool::new(NUM_BUFFER_PAGE);

            // exhausts the head ptr
            for _ in 0..NUM_BUFFER_PAGE {
                assert!(pool.pop().is_some());
            }

            assert!(pool.pop().is_none());
            assert!(pool.is_empty());
        }

        #[test]
        fn test_push_pop_with_multiple_threades() {
            let pool = Arc::new(BufPool::new(NUM_BUFFER_PAGE));

            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let pool = pool.clone();

                    thread::spawn(move || {
                        for _ in 0..1000 {
                            if let Some(idx) = pool.pop() {
                                pool.push(idx);
                            } else {
                                std::thread::yield_now();
                            }
                        }
                    })
                })
                .collect();

            for t in threads {
                t.join().unwrap();
            }

            //
            // sanity check
            //

            let mut count = 0;

            while pool.pop().is_some() {
                count += 1;
            }

            assert_eq!(count, NUM_BUFFER_PAGE);
        }

        #[test]
        fn test_reuse_after_empty() {
            let pool = BufPool::new(NUM_BUFFER_PAGE);

            let mut popped = Vec::new();
            let mut count = 0;

            while let Some(idx) = pool.pop() {
                popped.push(idx);
            }

            assert!(pool.is_empty());

            for idx in popped {
                pool.push(idx);
            }

            while pool.pop().is_some() {
                count += 1;
            }

            assert_eq!(count, NUM_BUFFER_PAGE);
        }

        #[test]
        #[cfg(debug_assertions)]
        #[should_panic(expected = "idx is out of bounds")]
        fn test_push_invalid_index_panics() {
            let pool = BufPool::new(NUM_BUFFER_PAGE);

            // should panic as the idx is out of bounds
            pool.push(NUM_BUFFER_PAGE);
        }
    }
}
