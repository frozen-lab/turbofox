use crate::errors::InternalResult;

// TODO: We must take `max_value` as config from user, and it
// should match this, so we will not have race conditions, cause
// we must block new writes (thread sleep, etc.) if no bufs are
// available to write into

/// No. of page bufs we can store into mem for write syscalls
pub(super) const NUM_BUFFER_PAGES: usize = 128;

const QUEUE_DEPTH: u32 = 0x200; // 512 SQE entries, which is ~40 KiB of memory
const IOURING_FEAT_SINGLE_MMAP: u32 = 1;

const IOURING_REGISTER_BUFFERS: u32 = 0;
const IOURING_UNREGISTER_BUFFERS: u32 = 1;
const IOURING_REGISTER_FILES: u32 = 2;
const IOURING_UNREGISTER_FILES: u32 = 3;

const IOURING_OFF_SQ_RING: libc::off_t = 0;
const IOURING_OFF_CQ_RING: libc::off_t = 0x8000000;
const IOURING_OFF_SQES: libc::off_t = 0x10000000;

// as we only register one file, it's stored at 0th index.
const IOURING_FIXED_FILE_IDX: i32 = 0;

const IOURING_FYSNC_USER_DATA: u64 = 0xFFFF_FFFF_FFFF_FFFF;

/// Maps the io_uring operation codes mirroring the original `io_uring_op` enum.
/// For reference, <https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h#L234>
enum IOUringOP {
    FSYNC = 3,
    READFIXED = 4,
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

#[repr(C)]
#[derive(Copy, Clone, Debug)]
struct IOUringCQE {
    user_data: u64,
    res: i32,
    flags: u32,
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

const IOURING_SQE_SIZE: usize = std::mem::size_of::<IOUringSQE>();

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
    pub(super) unsafe fn new(file_fd: i32) -> InternalResult<Self> {
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

        let ring_fd =
            libc::syscall(libc::SYS_io_uring_setup, QUEUE_DEPTH, &mut params as *mut IOUringParams) as libc::c_int;

        if ring_fd < 0 {
            let errno = *libc::__errno_location();

            // TODO: When io_uring is not available, we need fallback system
            if errno == libc::ENOSYS {
                eprintln!("io_uring is not supported (requires Linux 5.1+)");
            }

            let err = std::io::Error::last_os_error();
            eprintln!("Invalid ring_fd={ring_fd}! ERR => {err}");

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
    #[inline(always)]
    pub(super) unsafe fn write(&mut self, buf: &[u8], write_offset: u64) -> InternalResult<()> {
        debug_assert!(!buf.is_empty(), "Input buffer must not be empty");
        debug_assert!(buf.len() <= self.buf_page_size, "Buffer is too large");

        let buf_idx = self.active_buf_idx;
        let buf_ptr = self.buf_base_ptr.add(buf_idx * self.buf_page_size);

        // copy data into registered buf
        std::ptr::copy_nonoverlapping(buf.as_ptr(), buf_ptr as *mut u8, buf.len());

        self.active_buf_idx = (buf_idx + 1) % NUM_BUFFER_PAGES;
        self.submit_write_and_fsync(buf_idx, write_offset)?;

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    pub(super) unsafe fn read(&mut self, read_offset: u64) -> InternalResult<Vec<u8>> {
        let buf_idx = self.active_buf_idx;
        self.active_buf_idx = (buf_idx + 1) % NUM_BUFFER_PAGES;

        self.submit_read_and_wait(buf_idx, read_offset)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn submit_read_and_wait(&self, buf_idx: usize, read_offset: u64) -> InternalResult<Vec<u8>> {
        // sanity check
        debug_assert!(buf_idx <= NUM_BUFFER_PAGES, "buf_idx is out of bounds");

        let (tail, sqe_idx) = self.next_sqe_index();
        let iov_base = self.iovecs[buf_idx].iov_base;
        let sqe_ptr = (self.rings.sqes_ptr as *mut IOUringSQE).add(sqe_idx as usize);

        core::ptr::write_bytes(sqe_ptr as *mut u8, 0, IOURING_SQE_SIZE); // zero init

        (*sqe_ptr).opcode = IOUringOP::READFIXED as u8;
        (*sqe_ptr).flags = 0x00;
        (*sqe_ptr).fd = self.file_fd;
        (*sqe_ptr).off = SQEOffUnion { off: read_offset };
        (*sqe_ptr).addr = SQEAddrUnion { addr: iov_base as u64 };
        (*sqe_ptr).len = self.buf_page_size as u32;
        (*sqe_ptr).user_data = buf_idx as u64;

        let arr = self.sq_array_ptr();
        core::ptr::write_volatile(arr.add((tail & self.sq_mask()) as usize), sqe_idx);

        // NOTE: This fence ensures SQE and array writes are visible to kernel before updating the tail.
        // It's imp cause, it prevents reordering of SQE entries.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

        core::ptr::write_volatile(self.sq_tail_ptr(), tail.wrapping_add(1)); // new tail

        let res = libc::syscall(
            libc::SYS_io_uring_enter,
            self.ring_fd,
            1u32,
            0u32,
            0u32,
            std::ptr::null::<libc::sigset_t>(),
        ) as libc::c_int;

        if res < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("submit_read() failed on io_uring_enter syscall: {err:?}");

            return Err(err.into());
        }

        loop {
            let cq_head = self.cq_head_ptr();
            let cq_tail = self.cq_tail_ptr();
            let head = core::ptr::read_volatile(cq_head);
            let tail = core::ptr::read_volatile(cq_tail);

            if head != tail {
                let idx = head & self.cq_mask();
                let cqe = core::ptr::read_volatile(self.cqes_ptr().add(idx as usize));
                core::ptr::write_volatile(cq_head, head.wrapping_add(1));

                eprintln!("INFO: Completed read: {:?}", cqe);

                let res = cqe.res;
                if res < 0 {
                    return Err(std::io::Error::from_raw_os_error(-res).into());
                }

                let read_len = res as usize;
                let src_ptr = self.iovecs[buf_idx].iov_base as *const u8;
                let mut data = vec![0u8; read_len];
                std::ptr::copy_nonoverlapping(src_ptr, data.as_mut_ptr(), read_len);

                return Ok(data);
            }
        }
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn submit_write_and_fsync(&self, buf_idx: usize, write_offset: u64) -> InternalResult<()> {
        // sanity check
        debug_assert!(buf_idx <= NUM_BUFFER_PAGES, "buf_idx is out of bounds");

        let (tail, sqe_idx) = self.next_sqe_index();

        // S_1: SQE prep for WRITE_FIXED

        let iov_base = self.iovecs[buf_idx].iov_base;
        let sqe_ptr = (self.rings.sqes_ptr as *mut IOUringSQE).add(sqe_idx as usize);
        core::ptr::write_bytes(sqe_ptr as *mut u8, 0, IOURING_SQE_SIZE); // zero init

        (*sqe_ptr).opcode = IOUringOP::WRITEFIXED as u8;
        (*sqe_ptr).flags = IOUringSQEFlags::IOLINK as u8 | IOUringSQEFlags::FIXEDFILE as u8;
        (*sqe_ptr).fd = IOURING_FIXED_FILE_IDX;
        (*sqe_ptr).len = self.iovecs[buf_idx].iov_len as u32;
        (*sqe_ptr).user_data = buf_idx as u64;
        (*sqe_ptr).off = SQEOffUnion { off: write_offset };
        (*sqe_ptr).addr = SQEAddrUnion { addr: iov_base as u64 };

        // S_2: SQE prep for (Linked) FSYNC

        let tail2 = tail + 1;
        let sqe_idx2 = tail2 & self.sq_mask();
        let sqe2_ptr = (self.rings.sqes_ptr as *mut IOUringSQE).add(sqe_idx2 as usize);

        (*sqe2_ptr).opcode = IOUringOP::FSYNC as u8;
        (*sqe2_ptr).flags = IOUringSQEFlags::FIXEDFILE as u8;
        (*sqe2_ptr).fd = IOURING_FIXED_FILE_IDX;
        (*sqe2_ptr).user_data = IOURING_FYSNC_USER_DATA;

        // S_3: Submit SQE's to SQ

        let sq_array = self.sq_array_ptr();
        let mask = self.sq_mask();

        let pos1 = (tail & mask) as usize;
        let pos2 = (tail2 & mask) as usize;

        core::ptr::write_volatile(sq_array.add(pos1), sqe_idx);
        core::ptr::write_volatile(sq_array.add(pos2), sqe_idx2);

        let new_tail = tail.wrapping_add(2);

        // NOTE: This fence ensures SQE and array writes are visible to kernel before updating the tail.
        // It's imp cause, it prevents reordering of SQE entries.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

        core::ptr::write_volatile(self.sq_tail_ptr(), new_tail); // new tail

        // S_4: Submit SQE (both) w/ `io_uring_enter` syscall
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
            eprintln!("submit_write_and_fsync() failed on io_uring_enter syscall: {err:?}");

            return Err(err.into());
        }

        Ok(())
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
    use std::{
        fs::{File, OpenOptions},
        io::Read,
        os::fd::AsRawFd,
        path::PathBuf,
    };
    use tempfile::TempDir;

    fn create_iouring() -> (IOUring, File, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("temp_io_uring");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("file");

        let file_fd = file.as_raw_fd();

        let io_ring = unsafe { IOUring::new(file_fd).expect("Failed to create io_uring") };

        (io_ring, file, tmp)
    }

    #[test]
    fn test_iouring_init() {
        let (io_ring, _file, _tmp) = create_iouring();

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

    #[test]
    fn test_write_and_fsync() {
        let offset: u64 = 0;
        let dummy_data = "Dummy Data to write w/ fsync".as_bytes();
        let mut dummy_buf = vec![0u8; dummy_data.len()];

        let (mut io_ring, mut file, _tmp) = create_iouring();

        unsafe {
            io_ring.write(&dummy_data, offset);
        }

        // manual sleep, so kernal could finish the process
        std::thread::sleep(std::time::Duration::from_millis(1));

        file.read_exact(&mut dummy_buf).expect("read from file");

        assert_eq!(dummy_data, &dummy_buf);
    }

    #[test]
    fn test_write_and_read() {
        let offset: u64 = 0;
        let dummy_data = b"Dummy Data to write and read";

        let (mut io_ring, _file, _tmp) = create_iouring();

        unsafe {
            io_ring.write(dummy_data, offset).expect("write failed");
            let read_back = io_ring.read(offset).expect("read failed");

            // only compare actual written length
            assert_eq!(&read_back[..dummy_data.len()], dummy_data);
        }
    }
}
